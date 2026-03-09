import asyncio
from datetime import datetime, timedelta

from fastapi_cache import FastAPICache
from sqlalchemy import delete, or_

from cache import redis
from config import CLEANUP_AFTER_DAYS, CLEANUP_INTERVAL_SECONDS
from database import async_session_maker
from models import Link


async def _execute_delete(query):
    async with async_session_maker() as session:
        result = await session.execute(query.returning(Link.short_code))
        short_codes = result.scalars().all()
        await session.commit()
    return short_codes


async def remove_expired_links() -> None:
    query = delete(Link).where(
        Link.expires_at != None,
        Link.expires_at < datetime.utcnow()
    )
    removed_codes = await _execute_delete(query)
    if removed_codes:
        await redis.delete(*removed_codes)
        await FastAPICache.clear(namespace="link-stats")


async def remove_unused_links() -> None:
    threshold = datetime.utcnow() - timedelta(days=CLEANUP_AFTER_DAYS)
    query = delete(Link).where(
        or_(
            Link.last_used_at == None,
            Link.last_used_at < threshold,
        ),
        Link.created_at < threshold,
    )
    removed_codes = await _execute_delete(query)
    if removed_codes:
        await redis.delete(*removed_codes)
        await FastAPICache.clear(namespace="link-stats")


async def periodic_cleanup(interval_seconds: int = CLEANUP_INTERVAL_SECONDS) -> None:
    while True:
        await remove_expired_links()
        await remove_unused_links()
        await asyncio.sleep(interval_seconds)
