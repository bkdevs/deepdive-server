from typing import Optional

from deepdive.schema import DatabaseSchema, VizSpec
from deepdive.viz.generator import get_generator
from deepdive.viz.compiler import get_compiler
from deepdive.sql.parser import SqlTree


class VizSpecInterpreter:
    def __init__(self, db_schema: DatabaseSchema):
        self.db_schema = db_schema
        self.compiler = get_compiler(db_schema)
        self.generator = get_generator(db_schema)

    def compile(self, viz_spec: VizSpec) -> Optional[SqlTree]:
        return self.compiler.compile(viz_spec)

    def generate(self, sql_tree: SqlTree) -> Optional[VizSpec]:
        return self.generator.generate(sql_tree)
