from typing import Union
from pypika.terms import Field, Term


def column_to_term(column_name: str) -> Union[Term, str]:
    if column_name == "*":
        return "*"
    return Field(column_name)
