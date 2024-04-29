from typing import Dict, List, Union

from pypika.terms import Term, Function
from deepdive.sql.parser import SqlTree


def term_to_str(term: Union[str, Term]) -> str:
    if isinstance(term, str):
        return term
    return term.get_sql()


def aliases_to_terms(sql_tree: SqlTree) -> Dict[str, Term]:
    aliases_to_terms = {}
    for term in all_terms(sql_tree):
        if not isinstance(term, str) and term.alias:
            aliases_to_terms[term.alias] = term

    return aliases_to_terms


def all_terms(sql_tree: SqlTree) -> List[Union[str, Term]]:
    all_terms = []
    if sql_tree.select_terms:
        all_terms.extend(sql_tree.select_terms)
    if sql_tree.groupby_terms:
        all_terms.extend(sql_tree.groupby_terms)
    return all_terms


def term_is_function(term: Term) -> bool:
    return isinstance(term, Function)
