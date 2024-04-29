import logging

from deepdive.gpt import GptClient
from deepdive.gpt.models.helper import parse_sql
from deepdive.gpt.open_ai import complete_prompt
from deepdive.schema import DatabaseSchema

logger = logging.getLogger(__name__)


def construct_prompt_header(db_schema: DatabaseSchema) -> str:
    prompt = f"""Complete {db_schema.sql_dialect.value} query only and with no explanation
### {db_schema.sql_dialect.value} SQL tables, with their properties:
# 
"""

    tables = db_schema.tables
    for index, table in enumerate(tables):
        columns = ",".join([c.name for c in table.columns])
        prompt += f"# {table.name}({columns})"
        if index == len(tables) - 1:
            prompt += ".\n"
        else:
            prompt += ";\n"

    prompt += "#\n"
    return prompt


def construct_prompt(prompt_header: str, message: str, example_queries) -> str:
    prompt = prompt_header
    prompt += example_queries
    prompt += f"### {message}\n"
    prompt += "SELECT"
    return prompt


class ZeroShotGPT(GptClient):
    """
    Zero-shot GPT text to SQL model, prompts based on: https://arxiv.org/pdf/2303.13547.pdf
    """

    def __init__(self, model, db_schema: DatabaseSchema):
        self.model = model
        self.prompt_header = construct_prompt_header(db_schema)

    def construct_query(self, question: str, example_queries: str = "") -> str:
        prompt = construct_prompt(self.prompt_header, question, example_queries)
        logging.warn(prompt)
        if self.model == "gpt-3.5-turbo":
            response = complete_prompt(prompt, model=self.model, temperature=0)
            return parse_sql("SELECT " + response)
        else:
            response = complete_prompt(
                prompt, model=self.model, max_tokens=2024, temperature=0
            )
            return parse_sql("SELECT " + response)
