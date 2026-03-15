import sys
from pathlib import Path

import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from fastapi_cache import FastAPICache

# путь к src
PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = PROJECT_ROOT / "src"
sys.path.append(str(SRC_PATH))

from main import app
from auth.db import Base
from database import get_async_session
import models

from types import SimpleNamespace

class FakeRedis:
    async def get(self, *args, **kwargs):
        return None

    async def set(self, *args, **kwargs):
        return True

    async def delete(self, *args, **kwargs):
        return True
    
    async def clear(self, *args, **kwargs):
        return 0


from uuid import uuid4


async def fake_get_current_user():
    return SimpleNamespace(id=uuid4())

TEST_DB_URL = "sqlite+aiosqlite:///./test.db"


@pytest.fixture(scope="session")
def engine():
    engine = create_async_engine(TEST_DB_URL, echo=False)
    return engine


@pytest.fixture(scope="session")
def session_maker(engine):
    maker = async_sessionmaker(engine, expire_on_commit=False)


    import tasks.tasks as tasks_module
    tasks_module.async_session_maker = maker

    return maker

@pytest_asyncio.fixture(scope="session", autouse=True)
async def prepare_database(engine):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield
    await engine.dispose()


@pytest_asyncio.fixture(autouse=True)
async def reset_db(engine):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    yield

try:
    import cache
    import links.router as links_router
    import tasks.tasks as tasks_module

    fake_redis = FakeRedis()

    cache.redis = fake_redis
    links_router.redis = fake_redis
    tasks_module.redis = fake_redis
    FastAPICache.reset()
    FastAPICache.init(fake_redis, prefix="test-cache")
except Exception:
    pass

@pytest_asyncio.fixture
async def db_session(session_maker):
    async with session_maker() as session:
        yield session


@pytest_asyncio.fixture
async def client(db_session):

    async def override_get_session():
        yield db_session

    app.dependency_overrides[get_async_session] = override_get_session

    try:
        from depends import get_current_user
        app.dependency_overrides[get_current_user] = fake_get_current_user
    except Exception:
        pass

    transport = ASGITransport(app=app)

    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac

    app.dependency_overrides.clear()
