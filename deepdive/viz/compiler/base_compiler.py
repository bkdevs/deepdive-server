import logging

from typing import Dict, Optional, List

from pypika import Order, Table
from pypika import functions as fn
from pypika.terms import Field, Term, LiteralValue, BasicCriterion, Criterion

from abc import abstractmethod
from deepdive.schema import (
    Breakdown,
    Filter,
    SortBy,
    VizSpec,
    XAxis,
    YAxis,
    DatabaseSchema,
    VizType,
)
from deepdive.sql.parser.term_parser import parse_term
from deepdive.sql.parser.sql_tree import OrderbyTerm, SqlTree, WhereTerm, JoinOnTerm
from deepdive.viz.compiler.compiler import VizSpecCompiler
from deepdive.viz.compiler.helper import column_to_term

logger = logging.getLogger(__name__)


class BaseCompiler(VizSpecCompiler):
    """
    Compiles a VizSpec into a SQLTree

    The logic succintly described is:
    - x_axis:
        - zero or one
        - included in select and groupby
        - can have a binner specified: one of (datetime, numeric)
            - binner results in a SQL function being applied, e.g, strftime
            - if alias is specified, SQL function is only applied in the select
            - group by / where clauses will use the alias instead
            - e.g, select strftime("%Y", t) as t_year, group by t_year
        - can have a domain specified: one of (comparison, numeric)
            - if specified, results in a where clause
            - where clauses goes first
    - y_axises:
        - zero or many
        - included in select clause
        - if x_axis is specified, all y_axises must be aggregated, e.g, AVG/COUNT
    - breakdowns:
        - zero or many
        - included in select and groupby
        - can have aliases specified - groupby will use alias if available
            - e.g, select a as b, group by b
    - filters:
        - zero or many
        - all filters are boolean _anded_
        - x_axis domain filter comes first if specified
        - filter can specified on any column or alias
    - sort_by:
        - zero or one
        - references one of x_axis, y_axis, breakdown
        - uses alias if available, otherwise recomputes the function
        - e.g, select strftime("%Y", t) as t_year order by t_year

    Key note:
        - we build an internal representation of column to term and column to aliases
        - viz spec references _columns_ it does not reference SQL terms or aliases
        - this allows us invariance in the front-end
            - e.g, we apply some aggregation to a y-axis, the rest of the query is updated
            - e.g, we change the binner on an x-axis, the rest of the query is updated
    """

    def __init__(self, db_schema: DatabaseSchema) -> "BaseCompiler":
        self.db_schema = db_schema
        self.join_clauses = self._construct_join_clauses(db_schema)

    def compile(self, viz_spec: VizSpec) -> Optional[SqlTree]:
        result = SqlTree()
        result.sql_dialect = self.db_schema.sql_dialect

        columns_to_terms = self._columns_to_terms(viz_spec)
        columns_to_aliases = self._columns_to_aliases(viz_spec)

        if viz_spec.x_axis:
            # add the term to select always
            result.add_select_term(columns_to_terms[viz_spec.x_axis.name])

            # prefer using alias in group by / where if specified
            term_or_alias = columns_to_terms[viz_spec.x_axis.name]
            if viz_spec.x_axis.name in columns_to_aliases:
                term_or_alias = columns_to_aliases[viz_spec.x_axis.name]

            result.add_groupby_term(term_or_alias)
            where_term = self.x_axis_to_where(viz_spec.x_axis, term_or_alias)
            if where_term:
                result.where_term = where_term

        if viz_spec.breakdowns:
            for breakdown in viz_spec.breakdowns:
                result.add_select_term(columns_to_terms[breakdown.name])
                if breakdown.name in columns_to_aliases:
                    result.add_groupby_term(columns_to_aliases[breakdown.name])
                else:
                    result.add_groupby_term(columns_to_terms[breakdown.name])

        if viz_spec.y_axises:
            for y_axis in viz_spec.y_axises:
                result.add_select_term(self.y_axis_to_term(y_axis))

        if viz_spec.tables:
            result.from_term = viz_spec.tables[0]
            if len(viz_spec.tables) > 1:
                result.joinon_terms = self.tables_to_joinon_terms(viz_spec.tables)

        if viz_spec.filters:
            where = self.filter_to_where(viz_spec.filters[0])
            for i in range(1, len(viz_spec.filters)):
                where = where & self.filter_to_where(viz_spec.filters[i])

            if result.where_term:  # x-axis added where already
                result.where_term &= where
            else:
                result.where_term = where

        if viz_spec.limit:
            result.limit_term = viz_spec.limit

        if viz_spec.sort_by:
            result.orderby_term = self.sort_by_to_term(
                columns_to_terms, columns_to_aliases, viz_spec.sort_by
            )

        return self._sanitize_tree(result)

    @abstractmethod
    def x_axis_to_term(self, x_axis: XAxis) -> Term:
        pass

    def y_axis_to_term(self, y_axis: YAxis) -> Term:
        term = column_to_term(y_axis.name)
        if y_axis.unparsed:
            term = LiteralValue(y_axis.name)

        if y_axis.aggregation == "COUNT":
            term = fn.Count(y_axis.name)
        elif y_axis.aggregation:
            term = fn.AggregateFunction(y_axis.aggregation, term)

        if y_axis.alias:
            term = term.as_(y_axis.alias)
        return term

    def breakdown_to_term(self, breakdown: Breakdown) -> Term:
        if breakdown.unparsed:
            term = LiteralValue(breakdown.name)
        else:
            term = column_to_term(breakdown.name)
        if breakdown.alias:
            term = term.as_(breakdown.alias)
        return term

    def x_axis_to_where(self, x_axis: XAxis, x_axis_term: Term) -> Optional[WhereTerm]:
        if not x_axis.domain:
            return None

        domainMin, domainMax = x_axis.domain

        if domainMin and domainMax:
            return x_axis_term[domainMin:domainMax]
        elif domainMin:
            return x_axis_term >= domainMin
        elif domainMax:
            return x_axis_term <= domainMax

        return None

    def filter_to_where(self, viz_filter: Filter) -> Optional[WhereTerm]:
        where_term = None
        filter_type = viz_filter.filter_type
        if filter_type == "comparison":
            if len(viz_filter.values) == 1:
                viz_filter_value = viz_filter.values[0]
                if viz_filter_value == "null":
                    where_term = Field(viz_filter.name).isnull()
                else:
                    where_term = Field(viz_filter.name) == viz_filter_value
            else:
                where_term = Field(viz_filter.name).isin(viz_filter.values)
        elif filter_type == "numeric":
            domainMin, domainMax = viz_filter.domain
            if domainMin is not None and domainMax is not None:
                where_term = Field(viz_filter.name)[domainMin:domainMax]
            elif domainMin is not None:
                where_term = Field(viz_filter.name) > domainMin
            elif domainMax is not None:
                where_term = Field(viz_filter.name) < domainMax
        elif filter_type == "like":
            where_term = Field(viz_filter.name).like(viz_filter.values[0])
        elif filter_type == "complex":
            filter_term = parse_term(viz_filter.expression)
            if not isinstance(filter_term, BasicCriterion):
                logger.error(
                    "Complex filter expression is not a basic criterion: "
                    + viz_filter.expression
                )
            where_term = filter_term
        else:
            raise ValueError("Unsupported filter type: " + viz_filter)

        if viz_filter.negate:
            where_term = where_term.negate()

        return where_term

    def sort_by_to_term(
        self,
        columns_to_terms: Dict[str, Term],
        columns_to_aliases: Dict[str, Term],
        sort_by: SortBy,
    ) -> OrderbyTerm:
        if sort_by.unparsed:
            return (parse_term(sort_by.name), Order[sort_by.direction])
        if sort_by.name in columns_to_aliases:
            return (columns_to_aliases[sort_by.name], Order[sort_by.direction])
        elif sort_by.name in columns_to_terms:
            return (columns_to_terms[sort_by.name], Order[sort_by.direction])
        else:
            return (sort_by.name, Order[sort_by.direction])

    def _sanitize_tree(self, sql_tree: SqlTree) -> SqlTree:
        if "*" in [str(term) for term in sql_tree.select_terms]:
            sql_tree.select_terms = ["*"]

        return sql_tree

    def _columns_to_terms(self, viz_spec: VizSpec) -> Dict[str, Term]:
        columns_to_terms = {}
        if viz_spec.x_axis:
            columns_to_terms[viz_spec.x_axis.name] = self.x_axis_to_term(
                viz_spec.x_axis
            )

        if viz_spec.breakdowns:
            for breakdown in viz_spec.breakdowns:
                columns_to_terms[breakdown.name] = self.breakdown_to_term(breakdown)

        if viz_spec.y_axises:
            for y_axis in viz_spec.y_axises:
                columns_to_terms[y_axis.name] = self.y_axis_to_term(y_axis)

        return columns_to_terms

    def _columns_to_aliases(self, viz_spec: VizSpec) -> Dict[str, Term]:
        columns_to_aliases = {}
        if viz_spec.x_axis and viz_spec.x_axis.alias:
            columns_to_aliases[viz_spec.x_axis.name] = Field(viz_spec.x_axis.alias)

        if viz_spec.breakdowns:
            for breakdown in viz_spec.breakdowns:
                if breakdown.alias:
                    columns_to_aliases[breakdown.name] = Field(breakdown.alias)

        if viz_spec.y_axises:
            for y_axis in viz_spec.y_axises:
                if y_axis.alias:
                    columns_to_aliases[y_axis.name] = Field(y_axis.alias)

        return columns_to_aliases

    def tables_to_joinon_terms(self, tables: List[str]) -> List[JoinOnTerm]:
        added_tables = [tables[0]]  # we take the first table to be from
        join_on_terms = []

        for table_to_add in tables[1:]:
            for added_table in added_tables:
                table_join_clauses = self.join_clauses.get(added_table, {})
                if table_to_add in table_join_clauses:
                    join_on_terms.append(
                        (Table(table_to_add), table_join_clauses[table_to_add])
                    )
                    added_tables.append(table_to_add)

        return join_on_terms

    def _construct_join_clauses(
        self, db_schema: DatabaseSchema
    ) -> Dict[str, Dict[str, Criterion]]:
        # note doesn't support multiple ways to join a table pair (takes first)
        if not db_schema.foreign_keys:
            return {}

        join_clauses = {}
        for foreign_key in db_schema.foreign_keys:
            primary, reference = foreign_key.primary, foreign_key.reference
            primary_table = primary.split(".")[0]
            reference_table = reference.split(".")[0]

            if primary_table not in join_clauses:
                join_clauses[primary_table] = {}
            join_clauses[primary_table][reference_table] = Field(primary) == Field(
                reference
            )
            if reference_table not in join_clauses:
                join_clauses[reference_table] = {}
            join_clauses[reference_table][primary_table] = Field(reference) == Field(
                primary
            )

        return join_clauses
