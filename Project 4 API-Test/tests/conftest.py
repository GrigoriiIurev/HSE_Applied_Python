import asyncio
import os
import sys
import uuid
from datetime import datetime
from pathlib import Path

import fakeredis.aioredis
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from pytest import MonkeyPatch
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = PROJECT_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

ENV_DEFAULTS = {
    "DB_USER": "test_user",
    "DB_PASS": "test_pass",
    "DB_HOST": "localhost",
    "DB_PORT": "5432",
    "DB_NAME": "test_db",
    "REDIS_HOST": "localhost",
    "REDIS_PORT": "6379",
    "SECRET": "test_secret",
    "BASE_URL": "http://testserver",
    "CLEANUP_AFTER_DAYS": "30",
    "CLEANUP_INTERVAL_SECONDS": "60",
}
for key, value in ENV_DEFAULTS.items():
    os.environ.setdefault(key, value)

import auth.db as auth_db
import cache
import database
import links.router as links_router
import main as main_module
import tasks.tasks as tasks_module
from auth.db import User
from database import get_async_session
from depends import get_current_user, get_optional_user
from main import app
from models import Link

TEST_DB_URL = "sqlite+aiosqlite:///./test.db"


@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
def fake_redis():
    return fakeredis.aioredis.FakeRedis(decode_responses=True)


@pytest.fixture(scope="session", autouse=True)
def patch_redis(fake_redis):
    mp = MonkeyPatch()
    mp.setattr(cache, "redis", fake_redis, raising=False)
    mp.setattr(links_router, "redis", fake_redis, raising=False)
    mp.setattr(tasks_module, "redis", fake_redis, raising=False)
    mp.setattr(main_module, "redis", fake_redis, raising=False)
    yield
    mp.undo()


@pytest.fixture(scope="session")
def test_engine(event_loop):
    engine = create_async_engine(
        TEST_DB_URL,
        echo=False,
        connect_args={"check_same_thread": False},
    )
    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    database.engine = engine
    database.async_session_maker = session_maker
    auth_db.engine = engine
    tasks_module.async_session_maker = session_maker
    return engine, session_maker


@pytest.fixture(autouse=True)
def stub_cache_clear(monkeypatch):
    async def _clear(*args, **kwargs):
        return 0

    monkeypatch.setattr("links.router.FastAPICache.clear", _clear)
    monkeypatch.setattr("tasks.tasks.FastAPICache.clear", _clear)
    yield


@pytest_asyncio.fixture(scope="session", autouse=True)
async def setup_database(test_engine):
    engine, _ = test_engine
    async with engine.begin() as conn:
        await conn.run_sync(auth_db.Base.metadata.drop_all)
        await conn.run_sync(auth_db.Base.metadata.create_all)
    yield
    await engine.dispose()


@pytest_asyncio.fixture(scope="session", autouse=True)
async def app_lifespan(test_engine, patch_redis):
    async with app.router.lifespan_context(app):
        yield


@pytest.fixture(scope="session")
def session_maker(test_engine):
    return test_engine[1]


@pytest_asyncio.fixture(autouse=True)
async def clean_database(session_maker):
    async with session_maker() as session:
        for table in reversed(auth_db.Base.metadata.sorted_tables):
            await session.execute(table.delete())
        await session.commit()
    yield


@pytest_asyncio.fixture(autouse=True)
async def clear_fake_redis(fake_redis):
    await fake_redis.flushall()
    yield


@pytest_asyncio.fixture
async def auth_user(session_maker):
    async with session_maker() as session:
        user = User(
            id=uuid.uuid4(),
            email="auth@example.com",
            hashed_password="hashed",
            is_active=True,
            is_superuser=False,
            is_verified=True,
        )
        session.add(user)
        await session.commit()
        return user


@pytest_asyncio.fixture
async def other_user(session_maker):
    async with session_maker() as session:
        user = User(
            id=uuid.uuid4(),
            email="other@example.com",
            hashed_password="hashed",
            is_active=True,
            is_superuser=False,
            is_verified=True,
        )
        session.add(user)
        await session.commit()
        return user


@pytest.fixture
def link_factory(session_maker):
    async def _create(**overrides):
        data = {
            "original_url": overrides.get("original_url", "https://example.com"),
            "short_code": overrides.get("short_code", uuid.uuid4().hex[:6]),
            "custom_alias": overrides.get("custom_alias"),
            "created_at": overrides.get("created_at", datetime.utcnow()),
            "expires_at": overrides.get("expires_at"),
            "click_count": overrides.get("click_count", 0),
            "last_used_at": overrides.get("last_used_at"),
            "owner_id": overrides.get("owner_id"),
        }
        link = Link(**data)
        async with session_maker() as session:
            session.add(link)
            await session.commit()
            await session.refresh(link)
            return link

    return _create


@pytest_asyncio.fixture
async def client(auth_user, session_maker):
    async def _get_test_session():
        async with session_maker() as session:
            yield session

    async def _get_current_user():
        return auth_user

    async def _get_optional_user():
        return None

    app.dependency_overrides[get_async_session] = _get_test_session
    app.dependency_overrides[get_current_user] = _get_current_user
    app.dependency_overrides[get_optional_user] = _get_optional_user

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as async_client:
        yield async_client

    app.dependency_overrides.clear()


@pytest.fixture
def fastapi_app():
    return app
