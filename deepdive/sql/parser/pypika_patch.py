from typing import Any

from pypika.terms import BasicCriterion, ArithmeticExpression
from pypika.utils import format_alias_sql
from pypika.dialects import SnowflakeQueryBuilder


def get_sql_patched(
    self, quote_char: str = '"', with_alias: bool = False, **kwargs: Any
) -> str:
    """
    Pypika generates operators without a space, see:
    https://github.com/kayak/pypika/blob/30574f997c80851f7e940ad09a63e14a98871dd3/pypika/terms.py#L765-L772

    Which is syntactically correct, but messes up our SQL comparison (e.g, GPT tends to generate with spaces around operators and so do users)
    It's difficult for us to go the other way around (e.g, removing spaces around operators) as it's hard to determine what contexts in which that's appropriate.

    e.g, select * from customers where a = 10

    We would _not_ remove whitespaces around select*from , but _do_ want to remove around = 10
    Basically requires us to understand the lexing context

    So we monkeypatch instead as doing the change in BasicCriterion ensures consistency and correctness in context
    and generally speaking, adding whitespaces never hurts.
    """
    sql = "{left} {comparator} {right}".format(
        comparator=self.comparator.value,
        left=self.left.get_sql(quote_char=quote_char, **kwargs),
        right=self.right.get_sql(quote_char=quote_char, **kwargs),
    )
    if with_alias:
        return format_alias_sql(sql, self.alias, **kwargs)
    return sql


def get_arithmetic_sql(self, with_alias: bool = False, **kwargs: Any) -> str:
    left_op, right_op = [
        getattr(side, "operator", None) for side in [self.left, self.right]
    ]

    arithmetic_sql = "{left} {operator} {right}".format(
        operator=self.operator.value,
        left=(
            "({})" if self.left_needs_parens(self.operator, left_op) else "{}"
        ).format(self.left.get_sql(**kwargs)),
        right=(
            "({})" if self.right_needs_parens(self.operator, right_op) else "{}"
        ).format(self.right.get_sql(**kwargs)),
    )

    if with_alias:
        return format_alias_sql(arithmetic_sql, self.alias, **kwargs)

    return arithmetic_sql


BasicCriterion.get_sql = get_sql_patched
ArithmeticExpression.get_sql = get_arithmetic_sql

# Snowflake SQL does _not_ like quoting aliases and won't parse them
SnowflakeQueryBuilder.ALIAS_QUOTE_CHAR = None
SnowflakeQueryBuilder.QUERY_ALIAS_QUOTE_CHAR = None
