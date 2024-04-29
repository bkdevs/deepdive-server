import json

from deepdive.sql.parser import format_query_for_prompt
from deepdive.models import Visualization
from deepdive.schema import DatabaseSchema

NUM_QUESTIONS = 4
QUESTION_QUERY_SCHEMA = [
    {
        "question": "string",
        "query": "string",
    }
]


class Prompter:
    def __init__(self, schema: DatabaseSchema):
        self.schema = schema
        self.db_description = self._construct_db_description()

    def generate_questions_prompt(self) -> str:
        prompt = (
            f"{self.db_description}\n"
            f"I want you to act as a data analyst and return {NUM_QUESTIONS} questions "
            f"to understand data in the above database. Return in JSON format."
        )
        return prompt

    def construct_query_prompt(self, question: str, example_queries: str) -> str:
        prompt = f"Complete {self.schema.sql_dialect.value} query only and with no explanation\n"
        prompt += f"{self.db_description}"
        if example_queries:
            prompt += f"\n\n{example_queries}\n"
            prompt += f"Q: {question}\nSQL: SELECT"
        else:
            prompt += f"{question}\nSELECT"
        print(prompt)
        return prompt

    def generate_questions_and_queries_prompt(self) -> str:
        prompt = (
            f"{self.db_description}\n"
            f"I want you to act as a data analyst and return {NUM_QUESTIONS} questions "
            f"and corresponding {self.schema.sql_dialect.value} queries in JSON format "
            f"with the following schema: {json.dumps(QUESTION_QUERY_SCHEMA)}\n"
            f"The resulting queries will be used to create visuals to be monitored in a dashboard."
        )
        print(prompt)
        return prompt

    def construct_visualization_example_prompt(self, viz: Visualization) -> str:
        prompt = f'Q: "{viz.question}"\n'
        prompt += f"SQL: {format_query_for_prompt(viz.sql_query)}\n"
        return prompt

    def generate_foreign_keys_prompt(self) -> str:
        pass

    def _construct_db_description(self) -> str:
        description = f"### {self.schema.sql_dialect.value} SQL tables, with their properties:\n#\n"
        for table in self.schema.tables:
            description += (
                f"# {table.name}({', '.join([c.name for c in table.columns])})\n"
            )
        description += "#\n### "
        return description
