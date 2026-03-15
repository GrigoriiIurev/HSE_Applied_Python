import random
import string
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import RedirectResponse
from fastapi_cache import FastAPICache
from fastapi_cache.decorator import cache
from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from auth.db import User
from cache import redis
from database import get_async_session
from depends import get_current_user, get_optional_user
from models import Link
from .schemas import LinkCreate, LinkStats, LinkUpdate

router = APIRouter(
    prefix="/links",
    tags=["Links"]
)


def generate_short_code(length: int = 6):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))


@router.post("/shorten")
async def create_short_link(
    link: LinkCreate,
    session: AsyncSession = Depends(get_async_session),
    current_user: User | None = Depends(get_optional_user),
):
    if link.expires_at and link.expires_at <= datetime.utcnow():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="expires_at must be in the future",
        )
    while True:
        short_code = link.custom_alias or generate_short_code()

        existing = await session.scalar(
            select(Link).where(
                or_(Link.short_code == short_code, Link.custom_alias == short_code)
            )
        )

        if existing:
            if link.custom_alias:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Custom alias is already taken",
                )
            continue
        break

    new_link = Link(
        original_url=link.original_url,
        short_code=short_code,
        custom_alias=link.custom_alias,
        created_at=datetime.utcnow(),
        expires_at=link.expires_at,
        owner_id=current_user.id if current_user else None,
    )

    session.add(new_link)
    await session.commit()

    return {
        "status": "success",
        "short_code": short_code,
    }


@router.put("/{short_code}")
async def update_link(
    short_code: str,
    link_update: LinkUpdate,
    session: AsyncSession = Depends(get_async_session),
    current_user: User = Depends(get_current_user),
):
    link_obj = await session.scalar(select(Link).where(Link.short_code == short_code))

    if not link_obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Link not found")

    if link_obj.owner_id is None:
        link_obj.owner_id = current_user.id
    elif link_obj.owner_id != current_user.id:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Forbidden")

    link_obj.original_url = link_update.original_url

    await session.commit()

    await redis.delete(short_code)
    await FastAPICache.clear(namespace="link-stats")

    return {"status": "updated"}


@router.delete("/{short_code}")
async def delete_link(
    short_code: str,
    session: AsyncSession = Depends(get_async_session),
    current_user: User = Depends(get_current_user),
):
    link_obj = await session.scalar(select(Link).where(Link.short_code == short_code))

    if not link_obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Link not found")

    if link_obj.owner_id is None:
        link_obj.owner_id = current_user.id
    elif link_obj.owner_id != current_user.id:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Forbidden")

    await session.delete(link_obj)

    await session.commit()

    await redis.delete(short_code)
    await FastAPICache.clear(namespace="link-stats")

    return {"status": "deleted"}


@router.get("/search")
async def search_links(
    original_url: str,
    session: AsyncSession = Depends(get_async_session)
):

    query = select(Link).where(Link.original_url == original_url)

    result = await session.execute(query)

    links = result.scalars().all()

    return {
        "status": "success",
        "data": [
            {
                "short_code": link.short_code,
                "original_url": link.original_url,
                "created_at": link.created_at
            }
            for link in links
        ]
    }


@router.get("/{short_code}/stats")
@cache(expire=60, namespace="link-stats")
async def get_link_stats(
    short_code: str,
    session: AsyncSession = Depends(get_async_session)
):

    query = select(Link).where(Link.short_code == short_code)

    result = await session.execute(query)
    link = result.scalar()

    if not link:
        raise HTTPException(status_code=404, detail="Link not found")

    return LinkStats(
        original_url=link.original_url,
        created_at=link.created_at,
        click_count=link.click_count,
        last_used_at=link.last_used_at
    )


@router.get("/{short_code}")
async def redirect_to_original(
    short_code: str,
    session: AsyncSession = Depends(get_async_session)
):

    cached_url = await redis.get(short_code)

    if cached_url:
        return RedirectResponse(cached_url)

    query = select(Link).where(Link.short_code == short_code)

    result = await session.execute(query)
    link = result.scalar()

    if not link:
        raise HTTPException(status_code=404, detail="Link not found")

    if link.expires_at and link.expires_at < datetime.utcnow():
        await session.delete(link)
        await session.commit()
        await redis.delete(short_code)
        await FastAPICache.clear(namespace="link-stats")
        raise HTTPException(status_code=410, detail="Link expired")

    link.click_count += 1
    link.last_used_at = datetime.utcnow()

    await session.commit()

    await redis.set(short_code, link.original_url, ex=3600)

    return RedirectResponse(link.original_url)
