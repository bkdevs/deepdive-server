import backoff
import openai
from typing import Dict, List

from deepdive.gpt.formatter import Formatter
from deepdive.gpt.prompter import Prompter
from deepdive.schema import DatabaseSchema

# REPLACE THIS
openai.organization = "REDACTED"
openai.api_key = "REDACTED"


class OpenAIClient:
    def __init__(self, db_schema: DatabaseSchema, model: str = "gpt-3.5-turbo"):
        self.model = model
        self.prompter = Prompter(db_schema)
        self.formatter = Formatter()

    @backoff.on_exception(backoff.expo, openai.OpenAIError)
    async def complete_prompt_async(self, prompt: str, **kwargs) -> str:
        messages = [{"role": "user", "content": prompt}]
        response = await openai.ChatCompletion.acreate(
            model=self.model, messages=messages, temperature=0, **kwargs
        )
        return response.choices[0].message.content
    
    async def generate_questions_async(self) -> List[str]:
        prompt = self.prompter.generate_questions_prompt()
        response = await self.complete_prompt_async(prompt)
        return self.formatter.format_response("generate_questions", response)

    async def construct_query_async(self, question: str, example_queries: str) -> str:
        prompt = self.prompter.construct_query_prompt(question, example_queries)
        extra_args = {
            "max_tokens": 600,
        }
        if example_queries:
            extra_args["stop"] = ["Q:"]
        response = await self.complete_prompt_async(prompt, **extra_args)
        return self.formatter.format_response("construct_query", response)

    async def generate_questions_and_queries_async(self) -> List[Dict[str, str]]:
        prompt = self.prompter.generate_questions_and_queries_prompt()
        response = await self.complete_prompt_async(prompt)
        return self.formatter.format_response("generate_questions_and_queries", response)

    async def generate_foreign_keys_async(self):
        pass
