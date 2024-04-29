import logging
import pprint
import traceback
from dataclasses import dataclass
from typing import Dict, List, Optional

import sqlparse
from asgiref.sync import sync_to_async

from deepdive.database import get_db_client
from deepdive.gpt.openai_client import OpenAIClient
from deepdive.models import Database, Session, UnparsedQuery, Visualization
from deepdive.schema import DatabaseSchema, VizSpec
from deepdive.sql.parser import (
    SqlTree,
    format_query,
    is_sql_str_equal,
    normalize_query,
    parse_sql,
)
from deepdive.sql.processor import (
    LimitProcessor,
    MultiSqlProcessor,
    FilterBadQueriesProcessor,
)
from deepdive.viz.interpreter import VizSpecInterpreter
from deepdive.viz.processor import (
    AliasProcessor,
    MultiVizSpecProcessor,
    TablesProcessor,
)

logger = logging.getLogger(__name__)


def _fetch_schema(database: Database, tables: List[str]):
    db_schema = DatabaseSchema.model_validate_json(database.schema)

    # TEMP: disabled, to enable, need to merge and account for foreign keys / user provided updates
    # refetch for remote databases where the schema can change
    # if database.database_type in (
    #     Database.DatabaseType.SNOWFLAKE,
    #     Database.DatabaseType.BIGQUERY,
    # ):
    #     db_schema = fetch_schema(database)
    #     database.schema = db_schema.model_dump_json(exclude_none=True)
    #     database.save()

    db_table_names = [table.name for table in db_schema.tables]
    if not all(table in db_table_names for table in tables):
        raise Exception("Not all specified tables are in the DB! %s", str(tables))

    db_schema.tables = [table for table in db_schema.tables if table.name in tables]
    return db_schema


@dataclass
class DeepDiveResponse:
    sql_query: Optional[str] = None
    data: Optional[str] = None
    visualization_spec: Optional[str] = None
    error_message: Optional[str] = None


class DeepDiveClient:
    """
    Class encapsulating DeepDive core logic including converting question to SQL query,
    generating visualization spec, generating report, etc.
    """

    def __init__(self, session: Session):
        self.session = session
        self.db_client = None
        self.db_schema = _fetch_schema(session.database, session.tables)
        self.gpt_client = OpenAIClient(self.db_schema)
        self.sql_processor = MultiSqlProcessor(
            FilterBadQueriesProcessor(self.db_schema),
            LimitProcessor(500),
        )
        self.viz_spec_processor = MultiVizSpecProcessor(
            AliasProcessor(), TablesProcessor(self.db_schema)
        )
        self.viz_spec_interpreter = VizSpecInterpreter(self.db_schema)
        self.report_queries = {}

    async def initialize_async(self):
        self.db_client = await sync_to_async(get_db_client)(self.session.database)
        await self._get_report_queries_async()

    async def finalize_async(self):
        await sync_to_async(self.db_client.finalize)()

    async def process_question_async(self, question: str) -> DeepDiveResponse:
        example_queries = "\n".join(self.report_queries.values())
        sql_query = await self.gpt_client.construct_query_async(
            question, example_queries
        )
        return await self.process_query_async(sql_query)

    async def process_query_async(self, sql_query: str) -> DeepDiveResponse:
        sql_tree = parse_sql(sql_query)
        print("Generated SQL Query: " + sql_query)

        sql_tree = self.sql_processor.process(sql_tree)
        if not sql_tree:
            return DeepDiveResponse(
                sql_query=sql_query, error_message="Could not process SQL query"
            )

        viz_spec = await self._generate_viz_spec_async(sql_tree, sql_query)

        # for debugging, keep in dev
        print("Parsed SQL tree: ")
        pprint.pprint(sql_tree)
        if viz_spec:
            print("Generated viz spec: ")
            pprint.pprint(viz_spec.model_dump())

        if viz_spec:
            return await self.process_viz_spec_async(viz_spec)
        else:
            return DeepDiveResponse(
                sql_query=sql_query, error_message="Could not process SQL query"
            )

    async def process_viz_spec_async(self, viz_spec: VizSpec) -> DeepDiveResponse:
        viz_spec = self.viz_spec_processor.process(viz_spec)
        sql_query = self.viz_spec_interpreter.compile(viz_spec).build_str()
        response = await self._execute_query_async(sql_query)
        response.visualization_spec = viz_spec.model_dump_json()
        return response

    async def generate_report_async(self) -> Dict[str, DeepDiveResponse]:
        question_query_pairs = (
            await self.gpt_client.generate_questions_and_queries_async()
        )
        response = {}
        for pair in question_query_pairs:
            response[pair["question"]] = await self.process_query_async(pair["query"])
        return response

    def add_new_viz_to_report(self, viz: Visualization):
        self.report_queries[
            str(viz.id)
        ] = self.gpt_client.prompter.construct_visualization_example_prompt(viz)

    def remove_viz_from_report(self, viz_id: str):
        self.report_queries.pop(viz_id)

    async def _get_report_queries_async(self):
        async for viz in Visualization.objects.filter(session=self.session):
            self.report_queries[
                str(viz.id)
            ] = self.gpt_client.prompter.construct_visualization_example_prompt(viz)

    async def _execute_query_async(self, sql_query: Optional[str]) -> DeepDiveResponse:
        if not sql_query:
            return DeepDiveResponse()

        try:
            print(sql_query)
            df = await sync_to_async(self.db_client.execute_query)(sql_query)
        except Exception as ex:
            logger.error("Exception in _execute_query: ")
            traceback.print_exc()
            return DeepDiveResponse(
                sql_query=self._format_sql_query(sql_query), error_message=repr(ex)
            )

        return DeepDiveResponse(
            sql_query=format_query(sql_query),
            data=df.to_json(orient="table", index=True),
        )

    async def _generate_viz_spec_async(
        self, sql_tree: SqlTree, sql_query: str
    ) -> Optional[VizSpec]:
        try:
            viz_spec = self.viz_spec_interpreter.generate(sql_tree)

            if not is_sql_str_equal(
                sql_tree.build_str(),
                self.viz_spec_interpreter.compile(viz_spec).build_str(),
            ):
                await self._log_unparsed_query_async(sql_query, viz_spec)
            return viz_spec
        except Exception as ex:
            logger.error("Exception in generate_viz_spec: ")
            traceback.print_exc()
            await self._log_unparsed_query_async(sql_query, None)
        return None

    async def _log_unparsed_query_async(self, sql_query, viz_spec):
        logger.error("Failed to parse SQL query identically")
        logger.error("Original SQL query: ")
        logger.error(normalize_query(sql_query))
        if viz_spec:
            logger.error("Converted SQL query: ")
            logger.error(
                normalize_query(self.viz_spec_interpreter.compile(viz_spec).build_str())
            )
        await UnparsedQuery.objects.acreate(query=sql_query)

    def _format_sql_query(self, sql_query):
        return sqlparse.format(sql_query, reindent=True, keyword_case="lower")
