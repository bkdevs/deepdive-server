from typing import List, Optional

from pypika import Table
from pypika.terms import Field, Term
from sql_metadata import Parser
from sqlparse.sql import (
    Comparison,
    Function,
    Identifier,
    IdentifierList,
    Statement,
    Where,
    Having,
    Operation,
)
from sqlparse.tokens import Token

import deepdive.sql.parser.pypika_patch  # noqa # pylint: disable=unused-import
from deepdive.sql.parser.term_parser import parse_term, _remove_quotes
from deepdive.sql.parser.sql_tree import (
    SqlTree,
    SelectTerm,
    FromTerm,
    WhereTerm,
    HavingTerm,
    GroupbyTerm,
    OrderbyTerm,
    LimitTerm,
    JoinOnTerm,
)

# aka, the delimiters
# we do select ___ from ___ join ___ group by ___ order by ___  limit
SECTION_KEYWORDS = ["select", "group by", "having", "order by", "from", "join", "limit"]


def parse_statement(statement: Statement) -> SqlTree:
    if statement.get_type() != "SELECT":
        raise ValueError("Cannot parse non-select SQL statement: " + str(statement))

    section_terms = {}
    current_section_name = None
    current_section_terms = []

    for token in statement:
        # we don't look for _all_ keywords, as some common names, such as "month"
        # are considered keywords by sqlparse
        if token.is_keyword and token.value.lower() in SECTION_KEYWORDS:
            if current_section_name:
                _add_to_section_terms(
                    section_terms, current_section_name, current_section_terms
                )
                current_section_name = None
                current_section_terms = []

            keyword = token.value.lower()

            if (
                keyword != "limit" and keyword in SECTION_KEYWORDS
            ):  # we parse limit separately
                current_section_name = keyword
        elif (
            isinstance(token, Identifier)
            or isinstance(token, Function)
            or isinstance(token, Comparison)
            or isinstance(token, Operation)
            or token.ttype == Token.Wildcard
            or token.ttype == Token.Keyword
            or token.ttype == Token.Keyword.Order
        ) and current_section_name:
            current_section_terms.append(token.value)
        elif isinstance(token, IdentifierList) and current_section_name:
            current_section_terms.extend(
                [subtoken.value for subtoken in token.get_identifiers()]
            )
        elif isinstance(token, Where):
            section_terms["where"] = _parse_where_term(token)

    if current_section_name:
        _add_to_section_terms(
            section_terms, current_section_name, current_section_terms
        )

    return SqlTree(
        select_terms=_parse_select_terms(section_terms.get("select")),
        from_term=_parse_from_term(section_terms.get("from")),
        joinon_terms=_parse_joinon_terms(section_terms.get("join", None)),
        groupby_terms=_parse_groupby_terms(section_terms.get("group by", None)),
        orderby_term=_parse_orderby_term(section_terms.get("order by", None)),
        where_term=section_terms.get("where", None),
        having_term=_parse_having_term(section_terms.get("having", None)),
        limit_term=_parse_limit_term(statement),
    )


def _add_to_section_terms(section_terms, current_section_name, current_section_terms):
    if current_section_name == "join":
        if current_section_name not in section_terms:
            section_terms[current_section_name] = []
        section_terms[current_section_name].append(current_section_terms)
    else:
        section_terms[current_section_name] = current_section_terms


def _parse_select_terms(select_tokens: List[str]) -> List[SelectTerm]:
    select_terms = []
    for token in select_tokens:
        if token == "*":
            select_terms.append("*")
        else:
            select_terms.append(parse_term(token))
    return select_terms


def _parse_from_term(from_tokens: List[str]) -> Optional[FromTerm]:
    """
    sqlparse parses tokens after from in a list of tokens, e.g:
    'from': ['data', 'as', 'Table_Data']

    note this function inherently assumes no implicit joins, that is to say
    select table1, table2

    will _not_ work
    """
    if not from_tokens or len(from_tokens) == 0:
        return None
    from_tokens = [_remove_quotes(from_token) for from_token in from_tokens]

    if len(from_tokens) == 1:
        # e.g, syntax of: from data Table_data
        split_tokens = from_tokens[0].split(" ")
        if len(split_tokens) == 2:
            return Table(split_tokens[0]).as_(split_tokens[1])
        return from_tokens[0]
    elif len(from_tokens) == 2:  # e.g, syntax of: from data Table_data
        return Table(from_tokens[0]).as_(from_tokens[1])

    if "as" not in from_tokens:
        raise ValueError("Invalid from term: " + str(from_tokens))
    return _parse_table_alias(from_tokens)


def _parse_joinon_terms(joinon_terms: Optional[List[List[str]]]) -> List[JoinOnTerm]:
    if not joinon_terms:
        return []

    joinon_terms = [_parse_joinon_term(joinon_term) for joinon_term in joinon_terms]
    return [joinon_term for joinon_term in joinon_terms if joinon_term is not None]


def _parse_joinon_term(join_tokens: List[str]) -> Optional[JoinOnTerm]:
    if not join_tokens or len(join_tokens) == 0:
        return None

    if "on" not in join_tokens:
        raise ValueError("Join tokens does not contain on: " + str(join_tokens))
    on_index = join_tokens.index("on")

    return (
        _parse_table_alias(join_tokens[0:on_index]),
        parse_term(" ".join(join_tokens[on_index + 1 :]).strip()),
    )


def _parse_groupby_terms(
    groupby_tokens: Optional[List[str]],
) -> Optional[List[GroupbyTerm]]:
    if not groupby_tokens or len(groupby_tokens) == 0:
        return []

    return [parse_term(token) for token in groupby_tokens]


def _parse_where_term(where_token: Where) -> Optional[WhereTerm]:
    """
    sqlparse parses out Where as Where token type (which has a list of subtokens)
    e.g, where_token: Where ->
    ["where", " ", "b", " ", "=", "10"]

    We remove the where token from subtokens, join the rest as a string, and hand it over
    to our parse_term functionality to get a corresponding Pypika term
    """
    subtokens = list(where_token.flatten())
    first_token = subtokens[0]
    if first_token.ttype == Token.Keyword and first_token.value.lower() == "where":
        subtokens = subtokens[1:]

    where_expression = "".join([subtoken.value for subtoken in subtokens]).strip()
    return parse_term(where_expression)


def _parse_having_term(having_tokens: List[str]) -> Optional[HavingTerm]:
    if not having_tokens or len(having_tokens) == 0:
        return None

    having_expression = "".join(having_tokens).strip()
    return parse_term(having_expression)


def _parse_orderby_term(orderby_terms: Optional[List[str]]) -> Optional[OrderbyTerm]:
    """
    sqlparse parses order by terms delimiting on , but treats contiguous clauses as separate identifier tokens
    e.g, order by a asc, b desc -> ["a asc", "b desc"]

    we ignore all clauses but the first
    """
    if not orderby_terms:
        return None

    # we only support ordering by a single column as of now
    # i.e, this function _breaks_ if there is an order by asdf, sdfjkl
    orderby = orderby_terms[0]
    orderby = orderby.split(" ")
    direction = "ASC"
    if orderby[-1].lower() == "asc":
        orderby = orderby[0:-1]
    elif orderby[-1].lower() == "desc":
        direction = "DESC"
        orderby = orderby[0:-1]

    if len(orderby_terms) > 1 and orderby_terms[1].lower() == "desc":
        # a rare scenario that can occur if the orderby column is a _keyword_
        # in which sqlparse will _not_ parse the term as an identifier, e.g, ["a asc"]
        # but instead parse it as two separate keywords, e.g: ["count", "desc"]
        # so this is quite hacky, but safe
        direction = "DESC"

    return (parse_term(" ".join(orderby)), direction)


def _parse_limit_term(statement: Statement) -> Optional[LimitTerm]:
    """
    We use sqlmetadata (which internally does uses sqlparse plus many if else statements)
    to parse out the limit term for convenience
    """
    sql_metadata = Parser("".join(token.value for token in statement))
    return sql_metadata.limit_and_offset[0] if sql_metadata.limit_and_offset else None


def _parse_table_alias(tokens: List[str]) -> Optional[Table]:
    if "as" in tokens:
        as_index = tokens.index("as")
        return Table("".join(tokens[0:as_index])).as_("".join(tokens[as_index + 1 :]))
    elif len(tokens) == 1:
        split_tokens = tokens[0].split(" ")
        if len(split_tokens) == 2:
            return Table(split_tokens[0]).as_(split_tokens[1])
        return Table(tokens[0])
    else:
        raise ValueError("Invalid table tokens: " + str(tokens))
