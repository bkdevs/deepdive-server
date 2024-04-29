import sqlparse

from deepdive.sql.parser.statement_parser import parse_statement
from deepdive.sql.parser.util import sanitize_query
from deepdive.sql.parser.sql_tree import SqlTree


def parse_sql(sql_str: str) -> SqlTree:
    sanitized_query = sqlparse.format(sanitize_query(sql_str), keyword_case="lower")
    statements = sqlparse.parse(sanitized_query)
    if len(statements) != 1:
        raise ValueError("Cannot parse 2 or more SQL statements: ")
    return parse_statement(statements[0])
