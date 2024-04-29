CODE_BLOCK = "```"
SQL_CODE_BLOCK = "```sql"


def parse_sql(response: str) -> str:
    """
    Helper function to look for SQL query in the response and sanitize it
    """

    response = response.replace(SQL_CODE_BLOCK, CODE_BLOCK)
    if CODE_BLOCK in response:
        response = response.split(CODE_BLOCK)[1::2][0]
    response = response.replace(";", "")
    response = response.replace("\n", " ")

    return response
