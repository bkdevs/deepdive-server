import re

import sqlparse


def sanitize_query(query: str) -> str:
    if query == "":
        return ""

    # python re does not have variable length look back/forward
    # so we need to replace all the " (double quote) for a
    # temporary placeholder as we DO NOT want to replace those
    # in the strings as this is something that user provided
    def replace_quotes_in_string(match):
        return re.sub('"', "<!!__QUOTE__!!>", match.group())

    def replace_back_quotes_in_string(match):
        return re.sub("<!!__QUOTE__!!>", '"', match.group())

    # unify quoting in queries, replace double quotes to backticks
    # it's best to keep the quotes as they can have keywords
    # or digits at the beginning so we only strip them in SQLToken
    # as double quotes are not properly handled in sqlparse
    query = re.sub(r"'.*?'", replace_quotes_in_string, query)
    query = re.sub(r'"([^`]+?)"', r"`\1`", query)
    query = re.sub(r'"([^`]+?)"\."([^`]+?)"', r"`\1`.`\2`", query)
    query = re.sub(r"'.*?'", replace_back_quotes_in_string, query)
    query = re.sub(r"\s+", " ", query)

    return query.strip()


def format_query(query: str) -> str:
    return sqlparse.format(query, reindent=True, keyword_case="lower")


AGGREGATE_FUNCTIONS_UPPER = ["COUNT(", "AVG(", "MAX(", "MIN(", "SUM("]


def _lower_functions(query: str) -> str:
    for aggregate_function in AGGREGATE_FUNCTIONS_UPPER:
        if aggregate_function in query:
            query = query.replace(aggregate_function, aggregate_function.lower())
    return query


def normalize_query(query):
    # we wrap all identifiers in backticks: generally good as allows for keyword name identifiers and weird cases
    # e.g, column names that start with number, 40sDPM
    # this messes up the comparison though, so get rid of that
    query = sanitize_query(query).replace("`", "")

    # we explicitly append " asc " to queries which don't specify it
    # e.g, "order by date" -> "order by date asc"
    # these should be treated the same, so remove " asc " terms if they exist
    query = query.replace(" asc ", " ")
    query = query.replace(" ASC ", " ")

    # format query to account for whitespace differences
    query = format_query(query)

    # sqlparse.format(keyword_case="lower") doesn't treat function names as keywords
    query = _lower_functions(query)

    return query


def format_query_for_prompt(query):
    """
    GPT (as of our understanding now)

    Does not like quotes around identifiers and extraneous new lines / spaces
    As well as prefers upper case keywords
    """
    query = normalize_query(query)
    query = re.sub("\s+", " ", query)
    query = re.sub(" +", " ", query)

    return sqlparse.format(query, keyword_case="upper")


def is_sql_str_equal(original_expression, converted_expression):
    original_expression = normalize_query(original_expression)
    converted_expression = normalize_query(converted_expression)

    return original_expression == converted_expression
