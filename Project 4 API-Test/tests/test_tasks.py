from datetime import datetime, timedelta
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

import tasks.tasks as tasks_module
from models import Link


@pytest.mark.asyncio
async def test_remove_expired_links_deletes_records(session_maker, link_factory, fake_redis, mocker):
    expired = await link_factory(
        short_code="expired-task",
        original_url="https://expired-task.example",
        expires_at=datetime.utcnow() - timedelta(days=1),
    )
    await link_factory(
        short_code="active-task",
        original_url="https://active-task.example",
        expires_at=datetime.utcnow() + timedelta(days=1),
    )

    await fake_redis.set(expired.short_code, expired.original_url)

    mock_clear = mocker.patch("tasks.tasks.FastAPICache.clear", new_callable=AsyncMock)

    await tasks_module.remove_expired_links()

    async with session_maker() as session:
        removed = await session.scalar(select(Link).where(Link.short_code == expired.short_code))
        assert removed is None
        remaining = await session.scalar(select(Link).where(Link.short_code == "active-task"))
        assert remaining is not None

    assert await fake_redis.get(expired.short_code) is None
    mock_clear.assert_awaited_once()


@pytest.mark.asyncio
async def test_remove_unused_links_respects_threshold(session_maker, link_factory, fake_redis, mocker):
    threshold_created = datetime.utcnow() - timedelta(days=40)
    stale = await link_factory(
        short_code="stale-task",
        created_at=threshold_created,
        last_used_at=threshold_created,
    )
    await link_factory(
        short_code="fresh-task",
        created_at=datetime.utcnow(),
        last_used_at=datetime.utcnow(),
    )

    await fake_redis.set(stale.short_code, stale.original_url)

    mock_clear = mocker.patch("tasks.tasks.FastAPICache.clear", new_callable=AsyncMock)

    await tasks_module.remove_unused_links()

    async with session_maker() as session:
        removed = await session.scalar(select(Link).where(Link.short_code == stale.short_code))
        assert removed is None
        remaining = await session.scalar(select(Link).where(Link.short_code == "fresh-task"))
        assert remaining is not None

    assert await fake_redis.get(stale.short_code) is None
    mock_clear.assert_awaited_once()


class StopLoop(Exception):
    pass


@pytest.mark.asyncio
async def test_periodic_cleanup_runs_both_tasks(mocker):
    mock_expired = mocker.patch("tasks.tasks.remove_expired_links", new_callable=AsyncMock)
    mock_unused = mocker.patch("tasks.tasks.remove_unused_links", new_callable=AsyncMock)

    async def fake_sleep(_):
        raise StopLoop

    mocker.patch("tasks.tasks.asyncio.sleep", side_effect=fake_sleep)

    with pytest.raises(StopLoop):
        await tasks_module.periodic_cleanup(interval_seconds=1)

    mock_expired.assert_awaited_once()
    mock_unused.assert_awaited_once()
