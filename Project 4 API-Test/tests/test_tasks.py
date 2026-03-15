import asyncio
import pytest
from datetime import datetime, timedelta
from sqlalchemy import select

import tasks.tasks as tasks_module
from models import Link


@pytest.mark.asyncio
async def test_remove_expired_links(client, session_maker):
    expired = Link(short_code="expired", original_url="https://expired.com", created_at=datetime.utcnow(), expires_at=datetime.utcnow() - timedelta(days=1), click_count=0)
    active = Link(short_code="active", original_url="https://active.com", created_at=datetime.utcnow(), expires_at=datetime.utcnow() + timedelta(days=1), click_count=0)
    async with session_maker() as session:
        session.add_all([expired, active])
        await session.commit()
    await tasks_module.remove_expired_links()
    async with session_maker() as session:
        result_expired = await session.scalar(select(Link).where(Link.short_code == "expired"))
        result_active = await session.scalar(select(Link).where(Link.short_code == "active"))
    assert result_expired is None and result_active is not None
    kickoff = await client.post("/tasks/cleanup-expired")
    assert kickoff.json()["task"] == "remove expired links"


@pytest.mark.asyncio
async def test_remove_unused_links(client, session_maker):
    old_time = datetime.utcnow() - timedelta(days=40)
    stale = Link(short_code="stale", original_url="https://stale.com", created_at=old_time, last_used_at=old_time, click_count=0)
    fresh = Link(short_code="fresh", original_url="https://fresh.com", created_at=datetime.utcnow(), last_used_at=datetime.utcnow(), click_count=0)
    async with session_maker() as session:
        session.add_all([stale, fresh])
        await session.commit()
    await tasks_module.remove_unused_links()
    async with session_maker() as session:
        result_stale = await session.scalar(select(Link).where(Link.short_code == "stale"))
        result_fresh = await session.scalar(select(Link).where(Link.short_code == "fresh"))
    assert result_stale is None and result_fresh is not None
    kickoff = await client.post("/tasks/cleanup-unused")
    assert kickoff.json()["task"] == "remove unused links"


@pytest.mark.asyncio
async def test_periodic_cleanup_iteration(monkeypatch):
    calls = []

    async def fake_remove_expired():
        calls.append("expired")

    async def fake_remove_unused():
        calls.append("unused")

    async def fake_sleep(_):
        raise asyncio.CancelledError

    monkeypatch.setattr(tasks_module, "remove_expired_links", fake_remove_expired)
    monkeypatch.setattr(tasks_module, "remove_unused_links", fake_remove_unused)
    monkeypatch.setattr(tasks_module.asyncio, "sleep", fake_sleep)

    with pytest.raises(asyncio.CancelledError):
        await tasks_module.periodic_cleanup(0)

    assert calls == ["expired", "unused"]
