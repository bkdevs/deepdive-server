import logging

import sqlparse
from sqlparse.sql import IdentifierList
from deepdive.database.sqlite_helper import SQLITE_KEYWORDS
from sql_metadata import Parser

logger = logging.getLogger(__name__)


class SanitizeBigQueryProcessor:
    """
    TODO: test and remove if unnecessary

    BigQuery does not like table names that start with a digit but requires them to be backticked:
    https://stackoverflow.com/a/72961319

    It also requires that reserved keywords be backticked (e.g, 'by' or 'full')

    GPT also likes to generate queries without backticks (even though we specify them in the prompt), so we
    do that logic ourselves here
    """

    def process(self, query: str) -> str:
        parsed = Parser(query)

        for parsed_table in parsed.tables:
            # lazy to find list of comprehensive Google SQL keywords, so reuse SQLITE
            # as there's no downside to backticking if unneeded and SQLITE keywords are a superset of ANSI-SQL
            if parsed_table[0].isdigit() or parsed_table.lower() in SQLITE_KEYWORDS:
                # note: a bit wrong if column_name == table_name
                query = query.replace(parsed_table, f"`{parsed_table}`")

        statement = sqlparse.parse(query)[0]

        # we have to do this logic ourselves as sqlparse (and sql_metadata, which relies on sqlparse)
        # will parse out keywords in select clauses, meaning they don't get returned by parsed.columns
        in_select_clause = False
        for token in statement.tokens:
            if isinstance(token, IdentifierList):
                identifiers = set(token.get_identifiers())
                for subtoken in token.tokens:
                    if (
                        subtoken in identifiers
                        and subtoken.value.lower() in SQLITE_KEYWORDS
                    ):
                        subtoken.value = f"`{subtoken.value}`"

            if token.value.lower() == "select":
                in_select_clause = True
            elif token.value.lower() == "from":
                in_select_clause = False
            elif in_select_clause and token.value.lower() in SQLITE_KEYWORDS:
                token.value = f"`{token.value}`"

        return str(statement)
