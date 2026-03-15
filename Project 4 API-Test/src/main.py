import uvicorn
from fastapi import FastAPI, Depends
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
import asyncio
from tasks.tasks import periodic_cleanup
from cache import redis
from fastapi_cache import FastAPICache
from fastapi_cache.backends.redis import RedisBackend

from auth.users import auth_backend, current_active_user, fastapi_users
from auth.schemas import UserCreate, UserRead
from auth.db import User, create_db_and_tables
from links.router import router as links_router
from tasks.router import router as tasks_router


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    FastAPICache.init(RedisBackend(redis), prefix="fastapi-cache")
    await create_db_and_tables()

    cleanup_task = asyncio.create_task(periodic_cleanup())
    try:
        yield
    finally:
        cleanup_task.cancel()
        with suppress(asyncio.CancelledError):
            await cleanup_task

app = FastAPI(lifespan=lifespan)


app.include_router(
    fastapi_users.get_auth_router(auth_backend),
    prefix="/auth/jwt",
    tags=["auth"],
)

app.include_router(
    fastapi_users.get_register_router(UserRead, UserCreate),
    prefix="/auth",
    tags=["auth"],
)

app.include_router(links_router)
app.include_router(tasks_router)


@app.get("/")
def root():
    return {"message": "URL Shortener API is running"}


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        reload=True,
        host="0.0.0.0",
        port=8000,
        log_level="info",
    )
