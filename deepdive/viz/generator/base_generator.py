from __future__ import annotations

import logging
from abc import abstractmethod
from dataclasses import dataclass
from typing import Dict, List, Optional, Union, get_args

from pypika import Table
from pypika.enums import Boolean, Equality, Matching
from pypika.terms import (
    AggregateFunction,
    BasicCriterion,
    ComplexCriterion,
    ContainsCriterion,
    NullCriterion,
    NotNullCriterion,
    Field,
    Not,
    RangeCriterion,
    Term,
    ValueWrapper,
)

from deepdive.schema import (
    AggregationFunctions,
    Breakdown,
    DatabaseSchema,
    Filter,
    SortBy,
    VizSpec,
    XAxis,
    YAxis,
)
from deepdive.sql.parser import SqlTree
from deepdive.sql.parser.sql_tree import (
    GroupbyTerm,
    OrderbyTerm,
    SelectTerm,
    SqlTree,
    WhereTerm,
)
from deepdive.sql.parser.term_parser import UnparsedField
from deepdive.sql.parser.util import normalize_query, sanitize_query
from deepdive.viz.generator.generator import VizSpecGenerator
from deepdive.viz.generator.helper import (
    aliases_to_terms,
    term_is_function,
    term_to_str,
)
from deepdive.viz.processor import VizSpecProcessor

logger = logging.getLogger(__name__)


VizSpecAxis = Union[XAxis, YAxis, Breakdown]
BaseCriterion = Union[
    BasicCriterion,
    RangeCriterion,
    ContainsCriterion,
    ComplexCriterion,
    NullCriterion,
    NotNullCriterion,
    Not,
    UnparsedField,
]


@dataclass
class VizSpecBuilder:
    """
    A separate VizSpecBuilder to keep the validation properties in VizSpec as we construct the object

    This is also useful for term_to_axes, which "registers" the axes as we select them to be referenced
    And for alias resolution.

    """

    generator: BaseGenerator
    aliases_to_terms: Dict[str, Term]
    term_to_axes: Dict[str, VizSpecAxis]

    x_axis: Optional[XAxis]
    y_axises: List[YAxis]
    breakdowns: List[Breakdown]
    filters: List[Filter]
    tables: Optional[List[str]]
    limit: Optional[int]
    sort_by: Optional[SortBy]

    def __init__(self, generator: BaseGenerator, sql_tree: SqlTree):
        self.generator = generator
        self.aliases_to_terms = aliases_to_terms(sql_tree)
        self.term_to_axes = {}
        self.x_axis = None
        self.y_axises = []
        self.breakdowns = []
        self.filters = []
        self.tables = self._generate_tables(sql_tree)
        self.limit = self._generate_limit(sql_tree)
        self.sort_by = None

    def set_x_axis(self, term: GroupbyTerm):
        term = self._resolve_alias(term)
        self.x_axis = self.generator.term_to_x_axis(term)
        self.term_to_axes[term_to_str(term)] = self.x_axis

    def add_y_axis(self, term: SelectTerm):
        if self.y_axises is None:
            self.y_axises = []

        y_axis = self.generator.term_to_y_axis(term)
        self.y_axises.append(y_axis)
        self.term_to_axes[term_to_str(term)] = y_axis

    def add_breakdown(self, term: GroupbyTerm):
        if self.breakdowns is None:
            self.breakdowns = []

        term = self._resolve_alias(term)
        breakdown = self.generator.term_to_breakdown(term)
        self.breakdowns.append(breakdown)
        self.term_to_axes[term_to_str(term)] = breakdown

    def add_sortby(self, term: OrderbyTerm):
        self.sort_by = self.generator.term_to_sortby(
            term, self.aliases_to_terms, self.term_to_axes
        )

    def add_where(self, term: WhereTerm):
        if self.filters is None:
            self.filters = []

        where_terms = self._unpack_where(term)
        for term in where_terms:
            viz_filter = self.generator.term_to_filter(term, self.term_to_axes)
            if self._filter_can_be_domain(viz_filter):
                self.x_axis.domain = viz_filter.domain
            else:
                self.filters.append(viz_filter)

    def _unpack_where(self, where: WhereTerm) -> List[BaseCriterion]:
        if isinstance(where, ComplexCriterion) and where.comparator == Boolean.and_:
            return self._unpack_where(where.left) + self._unpack_where(where.right)
        elif isinstance(where, get_args(BaseCriterion)):
            return [where]
        else:
            raise ValueError("Unknown criterion type: " + where)

    def _filter_can_be_domain(self, viz_filter: Filter) -> bool:
        if viz_filter.filter_type != "numeric":
            return False
        if not self.x_axis or self.x_axis.domain:
            return False

        return self.x_axis.name == viz_filter.name or (
            self.x_axis.alias and self.x_axis.alias == viz_filter.name
        )

    def _resolve_alias(self, term: Term):
        if term_to_str(term) in self.aliases_to_terms:
            term = self.aliases_to_terms[term_to_str(term)]
        return term

    def _generate_tables(self, sql_tree: SqlTree) -> List[str]:
        return [self._resolve_table_name(sql_tree.from_term)] + [
            self._resolve_table_name(joinon_term[0])
            for joinon_term in sql_tree.joinon_terms
        ]

    def _resolve_table_name(self, term: Union[Table, str]) -> str:
        return term._table_name if isinstance(term, Table) else term

    def _generate_limit(self, sql_tree: SqlTree) -> Optional[int]:
        if sql_tree.limit_term:
            return sql_tree.limit_term
        return None

    def build(self) -> VizSpec:
        viz_spec = VizSpec(
            x_axis=self.x_axis,
            y_axises=self.y_axises,
            breakdowns=self.breakdowns,
            filters=self.filters,
            sort_by=self.sort_by,
            tables=self.tables,
            limit=self.limit,
        )
        return viz_spec


class BaseGenerator(VizSpecGenerator):
    def __init__(self, db_schema: DatabaseSchema, viz_spec_processor: VizSpecProcessor):
        self.db_schema = db_schema
        self.viz_spec_processor = viz_spec_processor

    def generate(self, sql_tree: SqlTree) -> Optional[VizSpec]:
        """
        Generates a visualization spec given a SqlTree

        There's often multiple valid visualization specs for a single SqlTree
        But a single visualization spec can only generate a single SqlTree

        To illustrate why, say we're given the query:
        > select COUNT(*), a, b group by a, b

        This could be either:
        - VizSpec(x_axis=XAxis("a"), breakdowns=[Breakdown("b")], y_axises=[YAxis("*", aggregation="COUNT")])
        - VizSpec(x_axis=XAxis("b"), breakdowns=[Breakdown("a")], y_axises=[YAxis("*", aggregation="COUNT")])

        i.e, we can't know what the right x_axis is if multiple group by columns are specified

        So this function is a heuristic, where we generate a reasonable visualization spec given a SQL tree
        It is guaranteed that that spec maps back to the SQL tree

        But it is not guaranteed that it is the only (or most relevant) spec
        """
        if not sql_tree.select_terms:
            return None

        select_only_terms = self._select_only_terms(
            sql_tree.select_terms, sql_tree.groupby_terms
        )

        builder = VizSpecBuilder(self, sql_tree)

        if select_only_terms:
            [builder.add_y_axis(term) for term in select_only_terms]
        if sql_tree.groupby_terms:
            function_terms = [
                term
                for term in sql_tree.groupby_terms
                if term_is_function(builder._resolve_alias(term))
            ]
            non_function_terms = [
                term
                for term in sql_tree.groupby_terms
                if not term_is_function(builder._resolve_alias(term))
            ]
            if len(function_terms) == 0:
                builder.set_x_axis(non_function_terms[0])
                if len(non_function_terms) > 1:
                    [builder.add_breakdown(term) for term in non_function_terms[1:]]
            elif len(function_terms) > 0:
                builder.set_x_axis(function_terms[0])
                if len(non_function_terms) > 0:
                    [builder.add_breakdown(term) for term in non_function_terms]
            else:
                # TOOD: correct this? breakdown right now doesn't allow for functions
                # it could (arguably should)
                raise ValueError(
                    "Multiple function terms specified in group by, cannot parse"
                )

        if sql_tree.orderby_term:
            builder.add_sortby(sql_tree.orderby_term)
        if sql_tree.where_term:
            builder.add_where(sql_tree.where_term)

        return self.viz_spec_processor.process(builder.build())

    @abstractmethod
    def term_to_x_axis(self, term: GroupbyTerm) -> XAxis:
        pass

    def term_to_y_axis(self, term: SelectTerm) -> YAxis:
        if isinstance(term, str) and term == "*":
            return YAxis(name="*")

        y_axis = None
        if isinstance(term, AggregateFunction):
            aggregation = term.name
            if aggregation.upper() in get_args(AggregationFunctions):
                # all supported aggregation functions only have a single arg
                arg = term.args[0]
                unparsed = False
                if isinstance(arg, Field):
                    arg = arg.name
                elif isinstance(arg, ValueWrapper):
                    arg = arg.value
                else:  # for all other terms, get their inner SQL
                    arg = arg.get_sql()
                    unparsed = True
                y_axis = YAxis(
                    name=arg, aggregation=aggregation.upper(), unparsed=unparsed
                )
            else:
                raise ValueError("Unknown aggregation function: " + term)
        elif isinstance(term, UnparsedField):
            y_axis = YAxis(name=term.name, unparsed=True)
        elif isinstance(term, Field):
            y_axis = YAxis(name=term.name)
        else:
            logger.error(
                "Could not convert term to y_axis, defaulting to literal: " + term
            )
            y_axis = YAxis(
                name=sanitize_query(normalize_query(term.get_sql())), unparsed=True
            )

        if term.alias:
            y_axis.alias = term.alias

        return y_axis

    def term_to_breakdown(self, term: GroupbyTerm) -> Breakdown:
        breakdown = None

        if isinstance(term, Field):
            breakdown = Breakdown(name=term.name)
        else:
            logger.error(
                "Could not convert term to breakdown, defaulting to literal: " + term
            )
            breakdown = Breakdown(
                name=sanitize_query(normalize_query(term.get_sql())), unparsed=True
            )

        if term.alias:
            breakdown.alias = breakdown.alias

        return breakdown

    def term_to_sortby(
        self,
        orderby_term: Optional[OrderbyTerm],
        aliases_to_terms: Dict[str, Term],
        term_to_axes: Dict[str, VizSpecAxis],
    ) -> Optional[SortBy]:
        """
        This method must run _after_ we've assigned x-axis, y-axises, and breakdowns
        This is because we're trying to get the inner column name associated to use in sort_by

        So we rely on the knowledge of what has already been assigned to avoid re-computing any
        field / parsing logic here.

        e.g,
        > select strftime("%Y", started_at) order by strftime("%Y", started_at)
        > VizSpec(XAxis(name="started_at", binner=...), sortby=SortBy(name="started_at"))
        """
        if not orderby_term:
            return None

        term, order = orderby_term
        if term_to_str(term) in aliases_to_terms:
            term = aliases_to_terms[term_to_str(term)]

        if term_to_str(term) in term_to_axes:
            return SortBy(
                name=term_to_axes[
                    term_to_str(term)
                ].name,  # all x-axis/y-axis/breakdown has a name
                direction=order.value.lower(),
                unparsed=term_to_axes[term_to_str(term)].unparsed,
            )
        elif "*" in term_to_axes and isinstance(term, Field):
            # if we're selecting *, allow any sort by column to be specified
            return SortBy(
                name=term.name,
                direction=order.value.lower(),
            )
        else:
            # note, we could choose to implement logic to "add" this term into X-Axis or Y-Axis
            # but for now, adding in a catch all
            logger.error(
                "Ordering by expression not in select or group by, defaulting to literal"
            )

            return SortBy(
                name=sanitize_query(normalize_query(term.get_sql())),
                direction=order.value.lower(),
                unparsed=True,
            )

    def term_to_filter(
        self, where: BaseCriterion, term_to_axes: Dict[str, VizSpecAxis]
    ) -> Filter:
        if isinstance(where, ComplexCriterion):
            return self._complex_criterion_to_filter(where)
        elif isinstance(where, NullCriterion) or isinstance(where, NotNullCriterion):
            return self._null_criterion_to_filter(where, term_to_axes)
        elif isinstance(where, BasicCriterion):
            return self._basic_criterion_to_filter(where, term_to_axes)
        elif isinstance(where, RangeCriterion):
            return self._range_criterion_to_filter(where, term_to_axes)
        elif isinstance(where, ContainsCriterion):
            return self._contains_criterion_to_filter(where, term_to_axes)
        elif isinstance(where, Not):
            _filter = self.term_to_filter(where.term, term_to_axes)
            _filter.negate = True
            return _filter
        elif isinstance(where, UnparsedField):
            return self._unparsed_field_to_filter(where)

        raise ValueError("Unsupported Criterion type in filter: " + where)

    def _complex_criterion_to_filter(self, where: ComplexCriterion) -> Filter:
        return Filter(
            name="complex_filter",
            filter_type="complex",
            expression=sanitize_query(normalize_query(where.get_sql())),
        )

    def _null_criterion_to_filter(
        self,
        where: Union[NullCriterion, NotNullCriterion],
        term_to_axes: Dict[str, VizSpecAxis],
    ) -> Filter:
        return Filter(
            name=self._term_to_filter_name(where.term, term_to_axes),
            filter_type="comparison",
            values=["null"],
        )

    def _basic_criterion_to_filter(
        self,
        where: BasicCriterion,
        term_to_axes: Dict[str, VizSpecAxis],
    ) -> Filter:
        if isinstance(where.left, Field):
            if where.comparator in (
                Equality.lt,
                Equality.lte,
            ):  # right now, this means we treat gte/lte as gt/lt
                # of format, Field("a") < 10
                return Filter(
                    name=self._term_to_filter_name(where.left, term_to_axes),
                    filter_type="numeric",
                    domain=[None, where.right.value],
                )
            elif where.comparator in (Equality.gt, Equality.gte):
                # of format, Field("a") > 10
                return Filter(
                    name=self._term_to_filter_name(where.left, term_to_axes),
                    filter_type="numeric",
                    domain=[where.right.value, None],
                )
            elif where.comparator == Equality.eq:
                # of format, Field("a") == 10
                return Filter(
                    name=self._term_to_filter_name(where.left, term_to_axes),
                    filter_type="comparison",
                    values=[where.right.value],
                )
            elif where.comparator == Equality.ne:
                # of format, Field("a") <> 10
                return Filter(
                    name=self._term_to_filter_name(where.left, term_to_axes),
                    filter_type="comparison",
                    values=[where.right.value],
                    negate=True,
                )
            elif where.comparator == Matching.like:
                # of format, Field("a").like("%MC%")
                return Filter(
                    name=self._term_to_filter_name(where.left, term_to_axes),
                    filter_type="like",
                    values=[where.right.value],
                )
        elif isinstance(
            where.right, Field
        ):  # always true, but elif for syntax checking
            if where.comparator in (Equality.lt, Equality.lte):
                # of format, 10 < Field("a")
                return Filter(
                    name=self._term_to_filter_name(where.right, term_to_axes),
                    filter_type="numeric",
                    domain=[where.left.value, None],
                )
            elif where.comparator in (Equality.gt, Equality.gte):
                # of format, 10 > Field("a")
                return Filter(
                    name=self._term_to_filter_name(where.right, term_to_axes),
                    filter_type="numeric",
                    domain=[None, where.left.value],
                )
            elif where.comparator == Equality.eq:
                # of format 10 == Field("a")
                return Filter(
                    name=self._term_to_filter_name(where.right, term_to_axes),
                    filter_type="comparison",
                    values=[where.left.value],
                )
            elif where.comparator == Equality.ne:
                # of format 10 <> Field("a")
                return Filter(
                    name=self._term_to_filter_name(where.right, term_to_axes),
                    filter_type="comparison",
                    values=[where.left.value],
                    negate=True,
                )
        logger.error(
            "Cannot translate BasicCriterion into Filter, defaulting to SQL literal: "
            + where
        )
        return Filter(
            name="complex_filter",
            filter_type="complex",
            expression=sanitize_query(normalize_query(where.get_sql())),
        )

    def _range_criterion_to_filter(
        self,
        where: RangeCriterion,
        term_to_axes: Dict[str, VizSpecAxis],
    ) -> Filter:
        return Filter(
            name=self._term_to_filter_name(where.term, term_to_axes),
            filter_type="numeric",
            domain=[where.start.value, where.end.value],
        )

    def _contains_criterion_to_filter(
        self,
        where: ContainsCriterion,
        term_to_axes: Dict[str, VizSpecAxis],
    ) -> Filter:
        return Filter(
            name=self._term_to_filter_name(where.term, term_to_axes),
            filter_type="comparison",
            values=[value.value for value in where.container.values],
            negate=where._is_negated,
        )

    def _unparsed_field_to_filter(self, where: UnparsedField) -> Filter:
        return Filter(
            name="complex_filter",
            filter_type="complex",
            expression=sanitize_query(normalize_query(where.name)),
        )

    def _term_to_filter_name(
        self, term: Term, term_to_axes: Dict[str, VizSpecAxis]
    ) -> str:
        if term_to_str(term) in term_to_axes:
            return term_to_axes[term_to_str(term)].name
        return term.name

    def _select_only_terms(
        self, select_terms: List[SelectTerm], groupby_terms: List[GroupbyTerm]
    ) -> List[SelectTerm]:
        groupby_term_strings = {term_to_str(term): term for term in groupby_terms}

        select_only_terms = []
        for term in select_terms:
            if (
                not isinstance(term, str)
                and term.alias
                and term.alias in groupby_term_strings
            ):
                continue
            if term_to_str(term) in groupby_term_strings:
                continue
            select_only_terms.append(term)

        return select_only_terms
