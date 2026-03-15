import pytest
from datetime import datetime, timedelta
from fastapi import status, HTTPException
from sqlalchemy import select
from links.router import (
    create_short_link,
    update_link as router_update,
    delete_link as router_delete,
    search_links,
    get_link_stats,
    redirect_to_original,
)
from links.schemas import LinkCreate, LinkUpdate
from models import Link
from types import SimpleNamespace
from uuid import uuid4


@pytest.mark.asyncio
async def test_create_short_link(client, session_maker):
    response = await client.post("/links/shorten", json={"original_url": "https://example.com"})
    assert response.status_code == status.HTTP_200_OK
    code = response.json()["short_code"]
    async with session_maker() as session:
        link = await session.scalar(select(Link).where(Link.short_code == code))
        assert link and link.original_url == "https://example.com"
    stats = await client.get(f"/links/{code}/stats")
    assert stats.status_code == status.HTTP_200_OK
    listing = await client.get("/links/search", params={"original_url": "https://example.com"})
    assert any(item["short_code"] == code for item in listing.json()["data"])
    expires = (datetime.utcnow() - timedelta(days=1)).isoformat()
    invalid = await client.post("/links/shorten", json={"original_url": "https://bad.example", "expires_at": expires})
    assert invalid.status_code == status.HTTP_400_BAD_REQUEST
    async with session_maker() as session:
        manual = await create_short_link(LinkCreate(original_url="https://manual.example", custom_alias="manual"), session, None)
    assert manual["short_code"] == "manual"


@pytest.mark.asyncio
async def test_create_duplicate_alias(client):
    await client.post("/links/shorten", json={"original_url": "https://example.com", "custom_alias": "test"})
    response = await client.post("/links/shorten", json={"original_url": "https://another.com", "custom_alias": "test"})
    assert response.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.asyncio
async def test_update_link(client, session_maker):
    response = await client.post("/links/shorten", json={"original_url": "https://example.com"})
    code = response.json()["short_code"]
    update = await client.put(f"/links/{code}", json={"original_url": "https://updated.com"})
    assert update.status_code == status.HTTP_200_OK
    async with session_maker() as session:
        stored = await session.scalar(select(Link).where(Link.short_code == code))
        assert stored.original_url == "https://updated.com"
    forbidden = await client.put(f"/links/{code}", json={"original_url": "https://blocked.com"})
    assert forbidden.status_code == status.HTTP_403_FORBIDDEN


@pytest.mark.asyncio
async def test_delete_link(client, session_maker):
    response = await client.post("/links/shorten", json={"original_url": "https://delete.com"})
    code = response.json()["short_code"]
    delete = await client.delete(f"/links/{code}")
    assert delete.status_code == status.HTTP_200_OK
    async with session_maker() as session:
        deleted = await session.scalar(select(Link).where(Link.short_code == code))
        assert deleted is None


@pytest.mark.asyncio
async def test_redirect(client, session_maker):
    response = await client.post("/links/shorten", json={"original_url": "https://redirect.com"})
    code = response.json()["short_code"]
    redirect = await client.get(f"/links/{code}", follow_redirects=False)
    assert redirect.status_code == status.HTTP_307_TEMPORARY_REDIRECT
    async with session_maker() as session:
        link = await session.scalar(select(Link).where(Link.short_code == code))
        link.expires_at = datetime.utcnow() - timedelta(days=1)
        await session.commit()
    expired = await client.get(f"/links/{code}", follow_redirects=False)
    assert expired.status_code == status.HTTP_410_GONE


@pytest.mark.asyncio
async def test_redirect_missing(client):
    root = await client.get("/")
    assert root.json()["message"] == "URL Shortener API is running"
    response = await client.get("/links/unknown", follow_redirects=False)
    assert response.status_code == status.HTTP_404_NOT_FOUND


@pytest.mark.asyncio
async def test_router_direct_usage(session_maker):
    user = SimpleNamespace(id=uuid4())
    async with session_maker() as session:
        await create_short_link(LinkCreate(original_url="https://unit.example", custom_alias="unit"), session, user)
        stats = await get_link_stats("unit", session)
        assert stats.original_url == "https://unit.example"
        results = await search_links("https://unit.example", session)
        assert results["data"]
        await router_update("unit", LinkUpdate(original_url="https://unit2.example"), session, user)
        with pytest.raises(HTTPException):
            await router_update("unit", LinkUpdate(original_url="https://fail.example"), session, SimpleNamespace(id=uuid4()))
        with pytest.raises(HTTPException):
            await router_delete("unit", session, SimpleNamespace(id=uuid4()))
        await router_delete("unit", session, user)
        await create_short_link(LinkCreate(original_url="https://go.example", custom_alias="go"), session, None)
        response = await redirect_to_original("go", session)
        assert response.status_code == status.HTTP_307_TEMPORARY_REDIRECT
        link = await session.scalar(select(Link).where(Link.short_code == "go"))
        link.expires_at = datetime.utcnow() - timedelta(days=1)
        await session.commit()
        with pytest.raises(HTTPException):
            await redirect_to_original("go", session)
