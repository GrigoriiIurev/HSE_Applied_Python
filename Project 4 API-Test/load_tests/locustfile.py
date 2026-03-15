import random
import string
from datetime import datetime

from locust import HttpUser, between, task


def _random_url_path():
    suffix = "".join(random.choices(string.ascii_letters + string.digits, k=8))
    return f"https://loadtest.example/{suffix}"


class LinkApiUser(HttpUser):
    wait_time = between(0.2, 1.0)

    def on_start(self):
        self.short_codes: list[str] = []

    @task(3)
    def create_short_link(self):
        payload = {"original_url": _random_url_path()}
        response = self.client.post("/links/shorten", json=payload)
        if response.status_code == 200:
            self.short_codes.append(response.json().get("short_code"))

    @task(2)
    def follow_short_link(self):
        if not self.short_codes:
            return
        short_code = random.choice(self.short_codes)
        self.client.get(f"/links/{short_code}", allow_redirects=False)

    @task(1)
    def fetch_stats(self):
        if not self.short_codes:
            return
        short_code = random.choice(self.short_codes)
        self.client.get(f"/links/{short_code}/stats")
