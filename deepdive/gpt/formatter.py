import json
from typing import Dict, List

CODE_BLOCK = "```"
SQL_CODE_BLOCK = "```sql"


class Formatter:
    """
    Class to format response from OpenAI.
    """

    def format_response(self, operation: str, response: str) -> str:
        formatted = ""
        if operation == "generate_questions":
            formatted = self.format_generate_questions_response(response)
        elif operation == "construct_query":
            formatted = self.format_construct_query_response(response)
        elif operation == "generate_questions_and_queries":
            formatted = self.format_generate_questions_and_queries_response(response)
        return formatted
    
    def format_generate_questions_response(self, response: str) -> List[str]:
        response = json.loads(response)
        return response["questions"]

    def format_construct_query_response(self, response: str) -> str:
        return self._sanitize("SELECT " + response)

    def format_generate_questions_and_queries_response(self, response: str) -> List[Dict[str, str]]:
        question_query_pairs = json.loads(response)
        for pair in question_query_pairs:
            pair["query"] = self._sanitize(pair["query"])
        return question_query_pairs

    def format_generate_foreign_keys_response(self, response: str) -> str:
        pass

    def _sanitize(self, response: str) -> str:
        response = response.replace(SQL_CODE_BLOCK, CODE_BLOCK)
        if CODE_BLOCK in response:
            response = response.split(CODE_BLOCK)[1::2][0]
        response = response.replace(";", "")
        response = response.replace("\n", " ")
        return response
