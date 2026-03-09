import asyncio
import fnmatch
import os
import sys
from importlib import reload
from pathlib import Path

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from fastapi_cache import FastAPICache
from fastapi_cache.backends.inmemory import InMemoryBackend
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

# Ensure application modules are importable during tests
PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = PROJECT_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.append(str(SRC_PATH))

# Use file-based SQLite DB for persistence across async connections during tests
TEST_DATABASE_URL = "sqlite+aiosqlite:///./tests/test.db"


class FakeRedis:
    def __init__(self):
        self._store: dict[str, str] = {}

    async def get(self, key: str):
        return self._store.get(key)

    async def set(self, key: str, value: str, ex: int | None = None):
        self._store[key] = value
        return True

    async def setex(self, key: str, expire: int, value: str):
        return await self.set(key, value)

    async def delete(self, *keys: str):
        removed = 0
        for key in keys:
            if key in self._store:
                removed += 1
                del self._store[key]
        return removed

    async def keys(self, pattern: str = "*"):
        return [
            key for key in self._store.keys()
            if fnmatch.fnmatch(key, pattern)
        ]

    async def scan_iter(self, match: str | None = None):
        for key in list(self._store.keys()):
            if match is None or fnmatch.fnmatch(key, match):
                yield key


@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
def app_environment():
    """
    Patch application dependencies (DB + Redis) to lightweight test doubles
    and reload modules so they pick up the overrides.
    """
    # Ensure tests directory exists for sqlite file
    os.makedirs("tests", exist_ok=True)

    import database
    import auth.db as auth_db
    import cache
    import models  # register Link table with Base metadata
    import tasks.tasks as tasks_tasks
    import tasks.router as tasks_router
    import links.router as links_router
    import main as main_module

    test_engine = create_async_engine(
        TEST_DATABASE_URL,
        future=True,
        echo=False,
    )
    testing_session = async_sessionmaker(
        test_engine,
        expire_on_commit=False,
    )

    database.engine = test_engine
    database.async_session_maker = testing_session
    auth_db.engine = test_engine

    fake_redis = FakeRedis()
    cache.redis = fake_redis

    # Reload modules so they re-bind patched dependencies
    tasks_tasks = reload(tasks_tasks)
    tasks_router = reload(tasks_router)
    links_router = reload(links_router)
    main_module = reload(main_module)
    main_module.redis = fake_redis
    FastAPICache.init(InMemoryBackend(), prefix="test-cache")

    return {
        "app": main_module.app,
        "engine": test_engine,
        "session_maker": testing_session,
        "redis": fake_redis,
        "auth_db": auth_db,
    }


@pytest_asyncio.fixture(autouse=True)
async def reset_database(app_environment):
    """Re-create schema before each test for isolation."""
    engine = app_environment["engine"]
    auth_db = app_environment["auth_db"]
    async with engine.begin() as conn:
        await conn.run_sync(auth_db.Base.metadata.drop_all)
        await conn.run_sync(auth_db.Base.metadata.create_all)


@pytest_asyncio.fixture
async def client(app_environment):
    app = app_environment["app"]
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as http_client:
        yield http_client


@pytest_asyncio.fixture
async def session(app_environment):
    testing_session = app_environment["session_maker"]
    async with testing_session() as db_session:
        yield db_session
