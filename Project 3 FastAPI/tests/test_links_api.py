from datetime import datetime, timedelta

import pytest
from sqlalchemy import select

from config import CLEANUP_AFTER_DAYS
from models import Link
from tasks.tasks import remove_expired_links, remove_unused_links


async def register_user(client, email: str, password: str = "strongpass"):
    resp = await client.post(
        "/auth/register",
        json={"email": email, "password": password},
    )
    assert resp.status_code in (200, 201)
    return resp.json()


async def login_user(client, email: str, password: str = "strongpass") -> str:
    resp = await client.post(
        "/auth/jwt/login",
        data={"username": email, "password": password},
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    assert resp.status_code == 200
    return resp.json()["access_token"]


@pytest.mark.asyncio
async def test_anonymous_short_link_flow(client):
    create_resp = await client.post(
        "/links/shorten",
        json={"original_url": "https://example.org"},
    )
    assert create_resp.status_code == 200
    payload = create_resp.json()
    short_code = payload["short_code"]

    redirect_resp = await client.get(f"/links/{short_code}", follow_redirects=False)
    assert redirect_resp.status_code in (307, 308)
    assert redirect_resp.headers["location"] == "https://example.org"

    stats_resp = await client.get(f"/links/{short_code}/stats")
    assert stats_resp.status_code == 200
    stats = stats_resp.json()
    assert stats["click_count"] == 1

    search_resp = await client.get("/links/search", params={"original_url": "https://example.org"})
    assert search_resp.status_code == 200
    assert any(item["short_code"] == short_code for item in search_resp.json()["data"])


@pytest.mark.asyncio
async def test_custom_alias_and_uniqueness(client):
    alias = "customAlias"
    expires_at = (datetime.utcnow() + timedelta(minutes=5)).isoformat()

    first = await client.post(
        "/links/shorten",
        json={"original_url": "https://alias.test", "custom_alias": alias, "expires_at": expires_at},
    )
    assert first.status_code == 200
    assert first.json()["short_code"] == alias

    duplicate = await client.post(
        "/links/shorten",
        json={"original_url": "https://alias2.test", "custom_alias": alias, "expires_at": expires_at},
    )
    assert duplicate.status_code == 400


@pytest.mark.asyncio
async def test_authenticated_user_can_update_and_delete_own_link(client):
    await register_user(client, "owner@example.com")
    token = await login_user(client, "owner@example.com")
    headers = {"Authorization": f"Bearer {token}"}

    create_resp = await client.post(
        "/links/shorten",
        json={"original_url": "https://owner.test"},
        headers=headers,
    )
    short_code = create_resp.json()["short_code"]

    update_resp = await client.put(
        f"/links/{short_code}",
        json={"original_url": "https://owner-updated.test"},
        headers=headers,
    )
    assert update_resp.status_code == 200

    stats = await client.get(f"/links/{short_code}/stats")
    assert stats.status_code == 200
    assert stats.json()["original_url"] == "https://owner-updated.test"

    await register_user(client, "intruder@example.com", password="intruder")
    intruder_token = await login_user(client, "intruder@example.com", password="intruder")
    intruder_headers = {"Authorization": f"Bearer {intruder_token}"}

    forbidden = await client.put(
        f"/links/{short_code}",
        json={"original_url": "https://hacked.test"},
        headers=intruder_headers,
    )
    assert forbidden.status_code == 403

    delete_resp = await client.delete(f"/links/{short_code}", headers=headers)
    assert delete_resp.status_code == 200

    missing = await client.get(f"/links/{short_code}/stats")
    assert missing.status_code == 404


@pytest.mark.asyncio
async def test_expired_links_removed_by_cleanup(client, session):
    await register_user(client, "ttl@example.com")
    token = await login_user(client, "ttl@example.com")
    headers = {"Authorization": f"Bearer {token}"}

    expires_at = (datetime.utcnow() + timedelta(minutes=5)).isoformat()
    create_resp = await client.post(
        "/links/shorten",
        json={"original_url": "https://ttl.test", "expires_at": expires_at},
        headers=headers,
    )
    short_code = create_resp.json()["short_code"]

    # Force expiration by updating the DB directly
    link = await session.scalar(select(Link).where(Link.short_code == short_code))
    link.expires_at = datetime.utcnow() - timedelta(minutes=1)
    await session.commit()

    await remove_expired_links()

    resp = await client.get(f"/links/{short_code}")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_unused_links_cleanup(client, session):
    create_resp = await client.post(
        "/links/shorten",
        json={"original_url": "https://stale.test"},
    )
    short_code = create_resp.json()["short_code"]

    stale_threshold = datetime.utcnow() - timedelta(days=CLEANUP_AFTER_DAYS + 1)
    await session.execute(
        Link.__table__.update()
        .where(Link.short_code == short_code)
        .values(created_at=stale_threshold, last_used_at=None)
    )
    await session.commit()

    await remove_unused_links()

    resp = await client.get(f"/links/{short_code}/stats")
    assert resp.status_code == 404
