from typing import List

from deepdive.schema import DatabaseSchema, ForeignKey, TableSchema


def write_tables(tables: List[TableSchema]) -> str:
    prompt = ""
    for table in tables:
        columns = ",".join([c.name for c in table.columns])
        prompt += f"Table {table.name}, columns = [*,{columns}]\n"
    return prompt


def write_foreign_keys(foreign_keys: List[ForeignKey]) -> str:
    fkeys = ",".join([f"{key.primary} = {key.reference}" for key in foreign_keys])
    return f"Foreign_keys = [{fkeys}]\n"


def write_primary_keys(database: DatabaseSchema) -> str:
    return f"Primary_keys = [{','.join(database.primary_keys)}]"


def write_db(database: DatabaseSchema) -> str:
    prompt = write_tables(database.tables)
    if database.foreign_keys and len(database.foreign_keys) > 0:
        prompt += write_foreign_keys(database.foreign_keys)
    return prompt
