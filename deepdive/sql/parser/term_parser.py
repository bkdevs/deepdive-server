"""
Although the code in appears broken with inspection, it is not. Sly uses some hacky syntax.

https://sly.readthedocs.io/en/latest/sly.html#writing-a-parser
"""
import logging
from typing import Optional

from pypika import Bracket, Case, Not, Order, Schema
from pypika import analytics as an
from pypika import functions as fn
from pypika.enums import DatePart, SqlTypes
from pypika.functions import Cast, Extract
from pypika.terms import Field, NullValue, Star, Term, ValueWrapper
from sly import Lexer, Parser

AGGREGATE_FUNCTION_NAMES = {
    "COUNT",
    "SUM",
    "SUM_FLOAT",
    "MIN",
    "MAX",
    "AVG",
    "STD",
    "STDDEV",
    "APPROXIMATE_PERCENTILE",
}


class ExpressionSyntaxError(Exception):
    pass


class PyPikaLexer(Lexer):
    # Set of token names.   This is always required
    tokens = {
        NAME,
        QUOTED_NAME,
        DECIMAL,
        INTEGER,
        STRING,
        PLUS,
        MINUS,
        TIMES,
        DIVIDE,
        MODULO,
        EQ,
        LT,
        LE,
        GT,
        GE,
        NE,
        NE2,
        TRUE,
        FALSE,
        NULL,
        NULLS,
        IN,
        IS,
        AS,
        FROM,
        BY,
        LIKE,
        ILIKE,
        NOT,
        AND,
        OR,
        USING,
        PARAMETERS,
        PERCENTILE,
        CASE,
        WHEN,
        THEN,
        ELSE,
        END,
        DBL_PIPE,
        DISTINCT,
        BETWEEN,
        OVER,
        PARTITION,
        ORDER,
        ASC,
        DESC,
        IGNORE,
        CAST,
        APPROXIMATE_PERCENTILE,
        EXTRACT,
        INTEGER_TYPE,
        FLOAT_TYPE,
        NUMERIC_TYPE,
        SIGNED_TYPE,
        UNSIGNED_TYPE,
        BOOLEAN_TYPE,
        CHAR_TYPE,
        VARCHAR_TYPE,
        BINARY_TYPE,
        VARBINARY_TYPE,
        LONG_TYPE,
        YEAR,
        QUARTER,
        MONTH,
        WEEK,
        DAY,
        HOUR,
        MINUTE,
        SECOND,
        MICROSECOND,
    }

    special_tokens = {
        "IN": IN,
        "IS": IS,
        "AS": AS,
        "FROM": FROM,
        "BY": BY,
        "NULL": NULL,
        "NULLS": NULLS,
        "NOT": NOT,
        "AND": AND,
        "OR": OR,
        "CASE": CASE,
        "WHEN": WHEN,
        "THEN": THEN,
        "ELSE": ELSE,
        "END": END,
        "DISTINCT": DISTINCT,
        "BETWEEN": BETWEEN,
        "TRUE": TRUE,
        "FALSE": FALSE,
        "OVER": OVER,
        "IGNORE": IGNORE,
        "PARTITION": PARTITION,
        "USING": USING,
        "PARAMETERS": PARAMETERS,
        "PERCENTILE": PERCENTILE,
        "ORDER": ORDER,
        "ASC": ASC,
        "DESC": DESC,
        # Special Functions
        "CAST": CAST,
        "APPROXIMATE_PERCENTILE": APPROXIMATE_PERCENTILE,
        "EXTRACT": EXTRACT,
        "LIKE": LIKE,
        "ILIKE": ILIKE,
        # TYPES
        "INTEGER": INTEGER_TYPE,
        "FLOAT": FLOAT_TYPE,
        "NUMERIC": NUMERIC_TYPE,
        "SIGNED": SIGNED_TYPE,
        "UNSIGNED": UNSIGNED_TYPE,
        "BOOLEAN": BOOLEAN_TYPE,
        "CHAR": CHAR_TYPE,
        "VARCHAR": VARCHAR_TYPE,
        "BINARY": BINARY_TYPE,
        "VARBINARY": VARBINARY_TYPE,
        "LONG": LONG_TYPE,
        # TIME UNITS
        "YEAR": YEAR,
        "QUARTER": QUARTER,
        "MONTH": MONTH,
        "WEEK": WEEK,
        "DAY": DAY,
        "HOUR": HOUR,
        "MINUTE": MINUTE,
        "SECOND": SECOND,
        "MICROSECOND": MICROSECOND,
    }

    literals = {r".", r",", r"(", r")", r'"', r"'"}

    # String containing ignored characters
    ignore = " \t"

    @_(r'[\"`]([^"\n]|"")*[\"`]')
    def QUOTED_NAME(self, t):
        t.value = t.value[1:-1]
        return t

    @_(r"\'([^'\n]|'')*\'")
    def STRING(self, t):
        t.value = t.value[1:-1].replace("''", "'")
        return t

    @_(
        r"(\d*\.\d+)([eE][-+]?[0-9]+)?",
        r"(\d+\.\d*)([eE][-+]?[0-9]+)?",
        r"(\d+)[eE][-+]?[0-9]+",
    )
    def DECIMAL(self, t):
        t.value = float(t.value)
        return t

    @_(r"\d+")
    def INTEGER(self, t):
        t.value = int(t.value)
        return t

    # Regular expression rules for tokens
    PLUS = r"\+"
    MINUS = r"-"
    TIMES = r"\*"
    DIVIDE = r"/"
    MODULO = r"%"
    EQ = r"="
    NE = r"<>"
    NE2 = r"!="
    LE = r"<="
    LT = r"<"
    GE = r">="
    GT = r">"
    DBL_PIPE = r"\|\|"

    @_(r"[a-zA-Z][a-zA-Z0-9_@#]*")
    def NAME(self, t):
        upper_value = t.value.upper()
        if upper_value in self.special_tokens:
            t.type = self.special_tokens[upper_value]
        return t

    @_(r"\n+")
    def newline(self, t):
        self.lineno += t.value.count("\n")

    def error(self, t):
        raise ExpressionSyntaxError(
            f"Syntax Error: illegal value '{t.value}' on line {self.lineno}:{self.index}"
        )


def build_case(case, when_then_list, else_=None):
    for when, then in when_then_list:
        case = case.when(when, then)

    if else_ is not None:
        case = case.else_(else_)

    return case


def build_analytic(func, partitions=(), orders=()):
    for partition in partitions:
        func = func.over(partition)

    for order, by in orders:
        func = func.orderby(order, order=by)

    return func


class PyPikaParser(Parser):
    # Uncomment this in order to write debug logs
    # debugfile = 'parser.out'

    # Get the token list from the lexer (required)
    tokens = PyPikaLexer.tokens

    def __init__(self):
        super().__init__()

    precedence = (
        ("left", DBL_PIPE),
        ("left", PLUS, MINUS),
        ("left", TIMES, DIVIDE, MODULO),
        ("right", UMINUS),
        ("right", NOT),
    )

    @_("expression OR and_condition")
    def expression(self, p):
        return p.expression | p.and_condition

    @_("and_condition")
    def expression(self, p):
        return p.and_condition

    @_("and_condition AND condition")
    def and_condition(self, p):
        return p.and_condition & p.condition

    @_("condition")
    def and_condition(self, p):
        return p.condition

    @_("operand")
    def condition(self, p):
        return p.operand

    @_("operand EQ operand")
    def condition(self, p):
        return p.operand0 == p.operand1

    @_("operand NE operand", "operand NE2 operand")
    def condition(self, p):
        return p.operand0 != p.operand1

    @_("operand GT operand")
    def condition(self, p):
        return p.operand0 > p.operand1

    @_("operand GE operand")
    def condition(self, p):
        return p.operand0 >= p.operand1

    @_("operand LT operand")
    def condition(self, p):
        return p.operand0 < p.operand1

    @_("operand LE operand")
    def condition(self, p):
        return p.operand0 <= p.operand1

    @_('operand IN "(" operand_list ")"')
    def condition(self, p):
        return p.operand.isin(p.operand_list)

    @_('operand NOT IN "(" operand_list ")"')
    def condition(self, p):
        return p.operand.notin(p.operand_list)

    @_('operand_list "," operand')
    def operand_list(self, p):
        return p.operand_list + [p.operand]

    @_("operand")
    def operand_list(self, p):
        return [p.operand]

    @_("operand LIKE operand")
    def condition(self, p):
        return p.operand0.like(p.operand1)

    @_("operand NOT LIKE operand")
    def condition(self, p):
        return p.operand0.not_like(p.operand1)

    @_("operand ILIKE operand")
    def condition(self, p):
        return p.operand0.ilike(p.operand1)

    @_("operand NOT ILIKE operand")
    def condition(self, p):
        return p.operand0.not_ilike(p.operand1)

    @_("operand BETWEEN operand AND operand")
    def condition(self, p):
        return p.operand0.between(p.operand1, p.operand2)

    @_("operand NOT BETWEEN operand AND operand")
    def condition(self, p):
        return p.operand0.not_between(p.operand1, p.operand2)

    @_("operand IS NULL")
    def condition(self, p):
        return p.operand.isnull()

    @_("operand IS NOT NULL")
    def condition(self, p):
        return p.operand.notnull()

    @_("NOT expression")
    def condition(self, p):
        return Not(p.expression)

    @_('"(" expression ")"')
    def condition(self, p):
        return Bracket(p.expression)

    @_("factor DBL_PIPE factor")
    def operand(self, p):
        if isinstance(p.factor0, fn.Concat):
            p.factor0.args += [p.factor1]
            return p.factor0

        return fn.Concat(p.factor0, p.factor1)

    @_("factor")
    def operand(self, p):
        return p.factor

    @_("term TIMES term")
    def factor(self, p):
        return p.term0 * p.term1

    @_("term DIVIDE term")
    def factor(self, p):
        return p.term0 / p.term1

    @_("term MODULO term")
    def factor(self, p):
        return p.term0 % p.term1

    @_("term PLUS term")
    def factor(self, p):
        return p.term0 + p.term1

    @_("term MINUS term")
    def factor(self, p):
        return p.term0 - p.term1

    @_("MINUS term %prec UMINUS")
    def factor(self, p):
        return -p.term

    @_("term")
    def factor(self, p):
        return p.term

    @_(
        "value",
        "function",
        "case",
        "case_when",
        "operand",
    )
    def term(self, p):
        return p[0]

    @_('"(" operand ")"')
    def term(self, p):
        return Bracket(p.operand)

    @_('alias "." column_ref')
    def term(self, p):
        if p.alias:
            return Field(f"{p.alias}.{p.column_ref}")
        return Field(p.column_ref)

    @_("column_ref")
    def term(self, p):
        return Field(p.column_ref)

    @_(
        "string",
        "numeric",
        "boolean",
        "constant",
    )
    def value(self, p):
        return p[0]

    @_("null")
    def value(self, p):
        return p.null

    @_(
        "CASE term when_then_list ELSE expression END",
    )
    def case(self, p):
        return build_case(Case(p.term), p.when_then_list, p.expression)

    @_("CASE term when_then_list END")
    def case(self, p):
        return build_case(Case(p.term), p.when_then_list)

    @_("CASE when_then_list ELSE expression END")
    def case_when(self, p):
        return build_case(Case(), p.when_then_list, p.expression)

    @_("CASE when_then_list END")
    def case_when(self, p):
        return build_case(Case(), p.when_then_list)

    @_("when_then_list when_then_stmt")
    def when_then_list(self, p):
        return p.when_then_list + [p.when_then_stmt]

    @_("when_then_stmt")
    def when_then_list(self, p):
        return [p.when_then_stmt]

    @_("WHEN expression THEN expression")
    def when_then_stmt(self, p):
        return p.expression0, p.expression1

    @_("NAME", "QUOTED_NAME")
    def alias(self, p):
        return p[0]

    @_("alias")
    def column_ref(self, p):
        return p.alias

    @_("NULL")
    def null(self, p):
        return NullValue()

    @_("STRING")
    def string(self, p):
        return ValueWrapper(p.STRING)

    @_("DECIMAL", "INTEGER")
    def numeric(self, p):
        return ValueWrapper(p[0])

    @_("TRUE")
    def boolean(self, p):
        return ValueWrapper(True)

    @_("FALSE")
    def boolean(self, p):
        return ValueWrapper(False)

    @_("time_unit")
    def constant(self, p):
        return p[0]

    @_('data_type_with_arg "(" INTEGER ")"')
    def data_type(self, p):
        return p.data_type_with_arg(p.INTEGER)

    @_("data_type_with_arg")
    def data_type(self, p):
        return p.data_type_with_arg

    @_("LONG_TYPE VARCHAR_TYPE")
    def data_type(self, p):
        return SqlTypes.LONG_VARCHAR

    @_("LONG_TYPE VARBINARY_TYPE")
    def data_type(self, p):
        return SqlTypes.LONG_VARBINARY

    @_(
        "INTEGER_TYPE",
        "FLOAT_TYPE",
        "NUMERIC_TYPE",
        "SIGNED_TYPE",
        "UNSIGNED_TYPE",
        "BOOLEAN_TYPE",
    )
    def data_type(self, p):
        return getattr(SqlTypes, p[0].upper())

    @_("CHAR_TYPE ", "VARCHAR_TYPE", "BINARY_TYPE", "VARBINARY_TYPE")
    def data_type_with_arg(self, p):
        return getattr(SqlTypes, p[0].upper())

    @_(
        "YEAR",
        "QUARTER",
        "MONTH",
        "WEEK",
        "DAY",
        "HOUR",
        "MINUTE",
        "SECOND",
        "MICROSECOND",
    )
    def time_unit(self, p):
        time_unit_string = p[0]
        return DatePart[str.lower(time_unit_string)]

    # FUNCTIONS

    @_(
        "cast",
        "extract",
        "analytic",
        "approximate_percentile",
    )
    def function(self, p):
        return p[0]

    @_('NAME "(" DISTINCT arguments_list ")"')
    def function(self, p):
        upper_name = p.NAME.upper()
        return fn.DistinctOptionFunction(upper_name, *p.arguments_list).distinct()

    @_('NAME "(" ")"', 'NAME "(" arguments_list ")"')
    def function(self, p):
        upper_name = p.NAME.upper()
        args = p.arguments_list if "arguments_list" in p._namemap else []
        func = (
            fn.AggregateFunction
            if upper_name in AGGREGATE_FUNCTION_NAMES
            else fn.Function
        )
        if upper_name == "COUNT":
            return fn.Count(*args)
        return func(p.NAME, *args)

    @_('alias "." alias "(" ")"', 'alias "." alias "(" arguments_list ")"')
    def function(self, p):
        schema = p.alias0
        upper_name = p.alias1
        args = p.arguments_list if "arguments_list" in p._namemap else []
        func = (
            fn.AggregateFunction
            if upper_name in AGGREGATE_FUNCTION_NAMES
            else fn.Function
        )
        if upper_name == "COUNT":
            return fn.Count(*args)

        return func(p.NAME, schema=Schema(schema), *args)

    @_("TIMES")
    def arguments_list(self, p):
        return [Star()]

    @_('alias "." TIMES')
    def arguments_list(self, p):
        return [Star()]

    @_('arguments_list "," expression')
    def arguments_list(self, p):
        return p.arguments_list + [p.expression]

    @_("expression")
    def arguments_list(self, p):
        return [p.expression]

    @_('CAST "(" expression AS data_type ")"')
    def cast(self, p):
        return Cast(p.expression, p.data_type)

    @_('APPROXIMATE_PERCENTILE "(" term USING PARAMETERS PERCENTILE EQ DECIMAL ")"')
    def approximate_percentile(self, p):
        return fn.ApproximatePercentile(p.term, p.DECIMAL)

    @_('EXTRACT "(" time_unit FROM expression ")"')
    def extract(self, p):
        return Extract(p.time_unit, p.expression)

    # ANALYTIC FUNCTIONS

    @_('analytic_function OVER "(" partition_by ")"')
    def analytic(self, p):
        return build_analytic(p.analytic_function, partitions=p.partition_by)

    @_('analytic_function OVER "(" order_by ")"')
    def analytic(self, p):
        return build_analytic(p.analytic_function, orders=p.order_by)

    @_('analytic_function OVER "(" partition_by order_by ")"')
    def analytic(self, p):
        return build_analytic(
            p.analytic_function, partitions=p.partition_by, orders=p.order_by
        )

    @_("function_ignore_nulls")
    def analytic_function(self, p):
        return p.function_ignore_nulls

    @_("function")
    def analytic_function(self, p):
        return an.AnalyticFunction(p.function.name, *p.function.args)

    @_('NAME "(" arguments_list IGNORE NULLS ")"')
    def function_ignore_nulls(self, p):
        upper_name = p.NAME.upper()
        return an.IgnoreNullsAnalyticFunction(
            upper_name, *p.arguments_list
        ).ignore_nulls()

    @_("PARTITION BY arguments_list")
    def partition_by(self, p):
        return p.arguments_list

    @_("ORDER BY arguments_list_orientation")
    def order_by(self, p):
        return p.arguments_list_orientation

    @_('arguments_list_orientation "," expression orientation')
    def arguments_list_orientation(self, p):
        return p.arguments_list_orientation + [(p.expression, p.orientation)]

    @_('arguments_list_orientation "," expression')
    def arguments_list_orientation(self, p):
        return p.arguments_list_orientation + [(p.expression, None)]

    @_("expression orientation")
    def arguments_list_orientation(self, p):
        return [(p.expression, p.orientation)]

    @_("expression")
    def arguments_list_orientation(self, p):
        return [(p.expression, None)]

    @_("ASC")
    def orientation(self, p):
        return Order.asc

    @_("DESC")
    def orientation(self, p):
        return Order.desc

    def error(self, token):
        if token:
            lineno = getattr(token, "lineno", 0)
            index = getattr(token, "index", 0)
            raise ExpressionSyntaxError(
                f"Syntax error on line:column {lineno}:{index}, "
                f"unexpected value '{token.value}'"
            )
        raise ExpressionSyntaxError(
            "Parse error in input. Unexpected end of expression."
        )


class TermParser:
    def __init__(self):
        self.lexer = PyPikaLexer()
        self.parser = PyPikaParser()

    def parse(self, expression: str) -> Term:
        assert expression is not None
        tokens = self.lexer.tokenize(expression)
        result = self.parser.parse(tokens)
        if result is None:
            raise Exception(
                "Unable to parse. Either expression is invalid or unsupported."
            )
        return result


TERM_PARSER = TermParser()


logger = logging.getLogger(__name__)


class UnparsedField(Field):
    def __init__(self, name: str, alias: Optional[str] = None):
        super().__init__(name=name, alias=alias)


def _has_quotes(expr_str: str) -> str:
    return (
        (expr_str.startswith("'") and expr_str.endswith("'"))
        or (expr_str.startswith('"') and expr_str.endswith('"'))
        or (expr_str.startswith("`") and expr_str.endswith("`"))
    )


def _remove_quotes(expr_str: str) -> str:
    if _has_quotes(expr_str):
        return expr_str[1:-1]
    return expr_str


def parse_term(expr_str: str) -> Term:
    try:
        term = None

        if _has_quotes(expr_str):
            # if the whole expression is wrapped in quotes, parse it as an identifier
            return Field(expr_str[1:-1])

        if " as " in expr_str:  # TODO: should be in the lexer
            term_str, alias = expr_str.split(" as ")
            term = TERM_PARSER.parse(term_str).as_(_remove_quotes(alias))
        else:
            term = TERM_PARSER.parse(expr_str)

        if not isinstance(term, Term):
            raise ValueError("Failed to parse expression as term")
        return term
    except Exception as e:
        logging.error(e)
        logging.error("Failed to parse term, treating as literal: " + expr_str)

        if " as " in expr_str:
            term_str, alias = expr_str.split(" as ")
            return UnparsedField(term_str, _remove_quotes(alias))
        return UnparsedField(expr_str)
