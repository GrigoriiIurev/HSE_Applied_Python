# from redis import asyncio as aioredis
# from config import REDIS_HOST, REDIS_PORT


# redis = aioredis.from_url(
#     f"redis://{REDIS_HOST}:{REDIS_PORT}",
#     decode_responses=True
# )


import os
from redis import asyncio as aioredis

REDIS_URL = os.getenv("REDIS_URL")

if REDIS_URL:
    redis = aioredis.from_url(
        REDIS_URL,
        decode_responses=True
    )
else:
    from config import REDIS_HOST, REDIS_PORT

    redis = aioredis.from_url(
        f"redis://{REDIS_HOST}:{REDIS_PORT}",
        decode_responses=True
    )