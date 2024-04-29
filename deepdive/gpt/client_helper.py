from typing import Literal

from deepdive.gpt.client import GptClient
from deepdive.gpt.models.few_shot_gpt import FewShotGPT
from deepdive.gpt.models.zero_shot_gpt import ZeroShotGPT
from deepdive.schema import DatabaseSchema


def get_gpt_client(
    model: Literal["zero-shot", "few-shot"],
    openai_model: Literal["gpt-3.5-turbo", "gpt-4.0"],
    db_schema: DatabaseSchema,
) -> GptClient:
    if model == "zero-shot":
        return ZeroShotGPT(model=openai_model, db_schema=db_schema)
    elif model == "few-shot":
        return FewShotGPT(model=openai_model, db_schema=db_schema)
