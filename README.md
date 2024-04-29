# deepdive-server

An hastily open-sourced failed startup. This is the backend of DeepDive.
What's here roughly reflects what's in the YouTube video plus a month or so of development: https://www.youtube.com/watch?v=mZ9mRn0zGo8

## overview

* the server is a Django REST app that interfaces with the front-end with both REST calls and websockets
  * it's technically two servers: ASGI and WSGI
    * ASGI handles websockets, which are created upon a "session" creation and directly interfaces with the corresponding web client
    * WSGI handles the REST, more typical "user login/user logout/fetch sessions" calls
  * the reasoning why is effectively sticky sessions: we want concurrent requests in a deepdive session to hit the same end host
  * by doing so - we can abuse a few traits to reduce latency.
    * SQL client initializations are slow (and network bound), i.e, Snowflake
    * we create in memory SQL dbs for each Excel file, if we were to do so with a typical REST architecture with no guarantee of stickiness, that would be potentially up to minutes
    * GPT contextual prompting, other hacks here and there
* the database is postgres, and will not run as-is
  * if you go through and look for all the "REPLACE THIS" comments, you'll get a pretty quick starter idea of how to go about it
* the core of the prompting logic is in [gpt/](https://github.com/bkdevs/deepdive-server/tree/main/deepdive/gpt)
  * we used GPT-3.5 for most of DeepDive, tested effectiveness of a few prompting strategies, but few prominent things:
    1. few shot prompts are quite a bit slower and more expensive, but does not significantly affect accuracy
    2. differentiation and sanitization of column names, table names impacts things significantly
    3. you can re-append few shot prompts in the context of the same session if the user edits (i.e, if he adds it to the report, this must be a good sample, so prepend it to few-shot)
* the core of the SQL parsing logic is in [sql/](https://github.com/bkdevs/deepdive-server/tree/main/deepdive/sql)
  * admittedly a very hacky attempt at writing a SQL decompiler that leans heavily on keyword
  * incomplete and doesn't work well for multi-statement SQL use-cases, but does satisfy these [tests](https://github.com/bkdevs/deepdive-server/tree/main/deepdive/test/sql)
  * the tests were mostly accumulated empirically for errors in parsing and non-reversibility (i.e, generate SQL -> tree -> viz spec -> tree -> SQL should lead to same result)
  * there's some tooling code to track for that as well in a separate DB
* a lot of the logic is in SQL post-processing, "viz spec" post-processing to embedify and guess at heuristics we think will work
  * mostly in [viz/](https://github.com/bkdevs/deepdive-server/tree/main/deepdive/viz)
* other things
  * Excel files are stored in S3, downloaded onto disk, loaded into an in-memory SQL db and deleted posthumously (so only reference is in process memory)
  * social auth and several adapters are implemented
    * mostly custom because there's a few odd parts of using Django REST with the third party lib that doesn't work well


yeah that's roughly about it. 
we're not planning to maintain the repository, but if anyone is willing (or wants to use it for commercial use), you can reach me at: pybbae@gmail.com