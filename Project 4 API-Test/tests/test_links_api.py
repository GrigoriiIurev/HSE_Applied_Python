from datetime import datetime, timedelta

import pytest
from fastapi import status
from sqlalchemy import select

from depends import get_optional_user
from links.router import (
    create_short_link,
    delete_link,
    generate_short_code,
    get_link_stats,
    redirect_to_original,
    search_links,
    update_link,
)
from links.schemas import LinkCreate, LinkUpdate
from models import Link


@pytest.mark.asyncio
async def test_create_short_link_anonymous(client, session_maker):
    response = await client.post(
        "/links/shorten",
        json={"original_url": "https://example.org"},
    )
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    assert data["status"] == "success"
    short_code = data["short_code"]

    async with session_maker() as session:
        db_link = await session.scalar(select(Link).where(Link.short_code == short_code))
        assert db_link is not None
        assert db_link.original_url == "https://example.org"
        assert db_link.owner_id is None


@pytest.mark.asyncio
async def test_create_short_link_with_custom_alias(client, session_maker):
    alias = "myalias"
    response = await client.post(
        "/links/shorten",
        json={
            "original_url": "https://custom.example",
            "custom_alias": alias,
        },
    )
    assert response.status_code == status.HTTP_200_OK
    assert response.json()["short_code"] == alias

    async with session_maker() as session:
        db_link = await session.scalar(select(Link).where(Link.short_code == alias))
        assert db_link.custom_alias == alias


@pytest.mark.asyncio
async def test_create_short_link_with_past_expiration(client):
    past = (datetime.utcnow() - timedelta(days=1)).isoformat()
    response = await client.post(
        "/links/shorten",
        json={
            "original_url": "https://expired.example",
            "expires_at": past,
        },
    )
    assert response.status_code == status.HTTP_400_BAD_REQUEST
    assert response.json()["detail"] == "expires_at must be in the future"


@pytest.mark.asyncio
async def test_create_short_link_rejects_duplicate_alias(client, link_factory):
    alias = "taken"
    await link_factory(short_code=alias, custom_alias=alias)

    response = await client.post(
        "/links/shorten",
        json={
            "original_url": "https://another.example",
            "custom_alias": alias,
        },
    )
    assert response.status_code == status.HTTP_400_BAD_REQUEST
    assert response.json()["detail"] == "Custom alias is already taken"


@pytest.mark.asyncio
async def test_create_short_link_assigns_owner_when_authenticated(
    client,
    fastapi_app,
    auth_user,
    session_maker,
):
    async def _optional_user():
        return auth_user

    previous = fastapi_app.dependency_overrides[get_optional_user]
    fastapi_app.dependency_overrides[get_optional_user] = _optional_user

    response = await client.post(
        "/links/shorten",
        json={"original_url": "https://owned.example"},
    )

    fastapi_app.dependency_overrides[get_optional_user] = previous

    assert response.status_code == status.HTTP_200_OK
    code = response.json()["short_code"]

    async with session_maker() as session:
        db_link = await session.scalar(select(Link).where(Link.short_code == code))
        assert db_link.owner_id == auth_user.id


@pytest.mark.asyncio
async def test_generate_short_link_regenerates_on_collision(client, mocker, link_factory):
    await link_factory(short_code="repeat")

    mocker.patch(
        "links.router.generate_short_code",
        side_effect=["repeat", "unique1"],
    )

    response = await client.post(
        "/links/shorten",
        json={"original_url": "https://regenerate.example"},
    )

    assert response.status_code == status.HTTP_200_OK
    assert response.json()["short_code"] == "unique1"


@pytest.mark.asyncio
async def test_update_link_assigns_owner_and_clears_cache(
    client,
    session_maker,
    auth_user,
    link_factory,
    fake_redis,
):
    link = await link_factory(original_url="https://old.example", owner_id=None, short_code="upd1")
    await fake_redis.set(link.short_code, "cached-value")

    response = await client.put(
        f"/links/{link.short_code}",
        json={"original_url": "https://new.example"},
    )

    assert response.status_code == status.HTTP_200_OK
    assert response.json()["status"] == "updated"

    async with session_maker() as session:
        db_link = await session.scalar(select(Link).where(Link.id == link.id))
        assert db_link.original_url == "https://new.example"
        assert db_link.owner_id == auth_user.id

    assert await fake_redis.get(link.short_code) is None


@pytest.mark.asyncio
async def test_update_link_forbidden(client, link_factory, other_user):
    link = await link_factory(owner_id=other_user.id, short_code="forbid1")

    response = await client.put(
        f"/links/{link.short_code}",
        json={"original_url": "https://blocked.example"},
    )

    assert response.status_code == status.HTTP_403_FORBIDDEN


@pytest.mark.asyncio
async def test_update_link_not_found(client):
    response = await client.put(
        "/links/notfound",
        json={"original_url": "https://missing.example"},
    )
    assert response.status_code == status.HTTP_404_NOT_FOUND


@pytest.mark.asyncio
async def test_delete_link_success(
    client,
    link_factory,
    session_maker,
    auth_user,
    fake_redis,
):
    link = await link_factory(owner_id=auth_user.id, short_code="delete1")
    await fake_redis.set(link.short_code, link.original_url)

    response = await client.delete(f"/links/{link.short_code}")

    assert response.status_code == status.HTTP_200_OK
    assert response.json()["status"] == "deleted"

    async with session_maker() as session:
        db_link = await session.scalar(select(Link).where(Link.short_code == link.short_code))
        assert db_link is None

    assert await fake_redis.get(link.short_code) is None


@pytest.mark.asyncio
async def test_delete_link_forbidden(client, link_factory, other_user):
    link = await link_factory(owner_id=other_user.id, short_code="delete2")

    response = await client.delete(f"/links/{link.short_code}")

    assert response.status_code == status.HTTP_403_FORBIDDEN


@pytest.mark.asyncio
async def test_redirect_to_original_persists_stats(
    client,
    link_factory,
    session_maker,
    fake_redis,
):
    link = await link_factory(short_code="redir1", original_url="https://redirect.example")

    response = await client.get(f"/links/{link.short_code}", follow_redirects=False)

    assert response.status_code == status.HTTP_307_TEMPORARY_REDIRECT
    assert response.headers["location"] == link.original_url

    async with session_maker() as session:
        db_link = await session.scalar(select(Link).where(Link.id == link.id))
        assert db_link.click_count == 1
        assert db_link.last_used_at is not None

    assert await fake_redis.get(link.short_code) == link.original_url


@pytest.mark.asyncio
async def test_redirect_uses_cache_without_touching_db(
    client,
    link_factory,
    session_maker,
    fake_redis,
):
    link = await link_factory(short_code="cached1", original_url="https://cached.example")
    await fake_redis.set(link.short_code, link.original_url)

    response = await client.get(f"/links/{link.short_code}", follow_redirects=False)

    assert response.status_code == status.HTTP_307_TEMPORARY_REDIRECT

    async with session_maker() as session:
        db_link = await session.scalar(select(Link).where(Link.id == link.id))
        assert db_link.click_count == 0
        assert db_link.last_used_at is None


@pytest.mark.asyncio
async def test_redirect_missing_link_returns_404(client):
    response = await client.get("/links/unknown", follow_redirects=False)
    assert response.status_code == status.HTTP_404_NOT_FOUND


@pytest.mark.asyncio
async def test_redirect_expired_link_is_removed(client, link_factory, session_maker, fake_redis):
    expired = await link_factory(
        short_code="expired1",
        original_url="https://expired.example",
        expires_at=datetime.utcnow() - timedelta(days=1),
    )

    response = await client.get(f"/links/{expired.short_code}", follow_redirects=False)

    assert response.status_code == status.HTTP_410_GONE

    async with session_maker() as session:
        db_link = await session.scalar(select(Link).where(Link.short_code == expired.short_code))
        assert db_link is None

    assert await fake_redis.get(expired.short_code) is None


@pytest.mark.asyncio
async def test_get_link_stats_success(client, link_factory):
    link = await link_factory(
        short_code="stats1",
        original_url="https://stats.example",
        click_count=5,
        last_used_at=datetime.utcnow(),
    )

    response = await client.get(f"/links/{link.short_code}/stats")

    assert response.status_code == status.HTTP_200_OK
    payload = response.json()
    assert payload["original_url"] == link.original_url
    assert payload["click_count"] == 5


@pytest.mark.asyncio
async def test_get_link_stats_not_found(client):
    response = await client.get("/links/missing/stats")
    assert response.status_code == status.HTTP_404_NOT_FOUND


@pytest.mark.asyncio
async def test_search_links_returns_matches(client, link_factory):
    url = "https://search.example"
    await link_factory(original_url=url, short_code="search1")
    await link_factory(original_url=url, short_code="search2")

    response = await client.get(f"/links/search?original_url={url}")

    assert response.status_code == status.HTTP_200_OK
    payload = response.json()
    assert payload["status"] == "success"
    assert len(payload["data"]) == 2


@pytest.mark.asyncio
async def test_tasks_cleanup_endpoints(client):
    resp_expired = await client.post("/tasks/cleanup-expired")
    resp_unused = await client.post("/tasks/cleanup-unused")

    assert resp_expired.status_code == status.HTTP_200_OK
    assert resp_unused.status_code == status.HTTP_200_OK
    assert resp_expired.json()["task"] == "remove expired links"
    assert resp_unused.json()["task"] == "remove unused links"


@pytest.mark.asyncio
async def test_root_endpoint(client):
    response = await client.get("/")
    assert response.status_code == status.HTTP_200_OK
    assert response.json()["message"] == "URL Shortener API is running"


def test_generate_short_code_default_length():
    code = generate_short_code()
    assert len(code) == 6
    assert code.isalnum()


def test_generate_short_code_custom_length():
    code = generate_short_code(10)
    assert len(code) == 10
    assert code.isalnum()


@pytest.mark.asyncio
async def test_create_short_link_direct(session_maker, auth_user):
    async with session_maker() as session:
        payload = LinkCreate(original_url="https://direct.example")
        result = await create_short_link(payload, session, auth_user)

        assert result["status"] == "success"
        code = result["short_code"]
        stored = await session.scalar(select(Link).where(Link.short_code == code))
        assert stored.owner_id == auth_user.id


@pytest.mark.asyncio
async def test_update_link_direct(session_maker, auth_user, link_factory, fake_redis):
    link = await link_factory(short_code="direct-update", owner_id=None)
    async with session_maker() as session:
        payload = LinkUpdate(original_url="https://updated.example")
        response = await update_link(link.short_code, payload, session, auth_user)
        assert response["status"] == "updated"
        updated = await session.scalar(select(Link).where(Link.id == link.id))
        assert updated.owner_id == auth_user.id
        assert updated.original_url == "https://updated.example"
        assert await fake_redis.get(link.short_code) is None


@pytest.mark.asyncio
async def test_delete_link_direct(session_maker, auth_user, link_factory, fake_redis):
    link = await link_factory(short_code="direct-delete", owner_id=auth_user.id)
    await fake_redis.set(link.short_code, link.original_url)
    async with session_maker() as session:
        response = await delete_link(link.short_code, session, auth_user)
        assert response["status"] == "deleted"
        remaining = await session.scalar(select(Link).where(Link.id == link.id))
        assert remaining is None
        assert await fake_redis.get(link.short_code) is None


@pytest.mark.asyncio
async def test_search_links_direct(session_maker, link_factory):
    url = "https://search-direct.example"
    await link_factory(original_url=url, short_code="search-direct")
    async with session_maker() as session:
        result = await search_links(url, session)
        assert result["data"]
        assert result["data"][0]["original_url"] == url


@pytest.mark.asyncio
async def test_get_link_stats_direct(session_maker, link_factory):
    link = await link_factory(
        short_code="stats-direct",
        original_url="https://stats-direct.example",
        click_count=3,
        last_used_at=datetime.utcnow(),
    )
    async with session_maker() as session:
        stats = await get_link_stats(link.short_code, session)
        assert stats.original_url == link.original_url
        assert stats.click_count == 3


@pytest.mark.asyncio
async def test_redirect_direct(session_maker, link_factory, fake_redis):
    link = await link_factory(short_code="direct-redirect", original_url="https://direct-redirect.example")
    async with session_maker() as session:
        response = await redirect_to_original(link.short_code, session)
        assert response.status_code == status.HTTP_307_TEMPORARY_REDIRECT
        updated = await session.scalar(select(Link).where(Link.id == link.id))
        assert updated.click_count == 1
        assert await fake_redis.get(link.short_code) == link.original_url
