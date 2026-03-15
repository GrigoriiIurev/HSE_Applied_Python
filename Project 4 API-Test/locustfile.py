import random
import string
from typing import List, Tuple

from locust import HttpUser, between, task


def random_url() -> str:
    slug = "".join(random.choices(string.ascii_letters + string.digits, k=12))
    return f"https://stress.example/{slug}"


class URLShortenerUser(HttpUser):
    wait_time = between(1, 3)

    def on_start(self) -> None:
        self._links: List[Tuple[str, str]] = []

    @task(3)
    def create_short_link(self) -> None:
        original_url = random_url()
        payload = {"original_url": original_url}
        response = self.client.post("/links/shorten", json=payload)
        if response.status_code == 200:
            short_code = response.json().get("short_code")
            if short_code:
                self._links.append((short_code, original_url))

    @task(2)
    def redirect_short_link(self) -> None:
        if not self._links:
            return
        short_code, _ = random.choice(self._links)
        self.client.get(f"/links/{short_code}", allow_redirects=False)

    @task(1)
    def search_by_url(self) -> None:
        if not self._links:
            return
        _, original_url = random.choice(self._links)
        self.client.get("/links/search", params={"original_url": original_url})
