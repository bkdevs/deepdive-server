import backoff
import openai

# REPLACE THIS
openai.organization = "REDACTED"
openai.api_key = "REDACTED"


@backoff.on_exception(backoff.expo, openai.OpenAIError)
def complete_prompt(prompt, **kwargs):
    messages = [{"role": "user", "content": prompt}]
    response = openai.ChatCompletion.create(messages=messages, **kwargs)
    return response.choices[0].message.content
