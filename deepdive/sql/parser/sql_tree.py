from typing import List, Optional, Tuple, Union

from deepdive.schema import SqlDialect
from pydantic import BaseModel, ConfigDict
from pypika import Order, Query
from pypika.queries import Selectable, QueryBuilder
from pypika.dialects import (
    SQLLiteQuery,
    SnowflakeQuery,
    MySQLQuery,
)
from pypika.terms import Criterion, Term

import deepdive.sql.parser.pypika_patch  # noqa # pylint: disable=unused-import
from deepdive.sql.parser.util import sanitize_query

Table = Union[Selectable, str]
FromTerm = Table
SelectTerm = Union[str, Term]
GroupbyTerm = Union[str, Term]
WhereTerm = Criterion
HavingTerm = Criterion
OrderbyTerm = Tuple[Term, Order]
JoinOnTerm = Tuple[Table, Criterion]
LimitTerm = int


class SqlTree(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    sql_dialect: SqlDialect = SqlDialect.SQLITE
    select_terms: List[SelectTerm] = []
    from_term: Optional[FromTerm] = None
    joinon_terms: List[JoinOnTerm] = []
    where_term: Optional[WhereTerm] = None
    having_term: Optional[HavingTerm] = None
    groupby_terms: List[GroupbyTerm] = []
    orderby_term: Optional[OrderbyTerm] = None
    limit_term: Optional[LimitTerm] = None

    def add_select_term(self, term: SelectTerm):
        self.select_terms.append(term)

    def add_groupby_term(self, term: GroupbyTerm):
        self.groupby_terms.append(term)

    def build_str(self) -> Optional[str]:
        query = self._get_query()
        query.as_keyword = True  # return use "as" in between column/table aliases
        for joinon_term in self.joinon_terms:
            query = query.join(joinon_term[0]).on(joinon_term[1])

        query = query.select(*self.select_terms)
        if self.limit_term:
            query = query.limit(self.limit_term)
        if self.groupby_terms:
            query = query.groupby(*self.groupby_terms)
        if self.orderby_term:
            query = query.orderby(self.orderby_term[0], order=self.orderby_term[1])
        if self.where_term:
            query = query.where(self.where_term)
        if self.having_term:
            query = query.having(self.having_term)

        return query.get_sql()

    def _get_query(self) -> QueryBuilder:
        if self.sql_dialect == SqlDialect.SQLITE:
            return SQLLiteQuery.from_(self.from_term)
        elif self.sql_dialect == SqlDialect.SNOWFLAKE_SQL:
            return SnowflakeQuery.from_(self.from_term)
        elif self.sql_dialect == SqlDialect.GOOGLE_SQL:
            return MySQLQuery.from_(self.from_term)
        elif self.sql_dialect == SqlDialect.MY_SQL:
            return MySQLQuery.from_(self.from_term)
        return Query.from_(self.from_term)
