import time
import threading
import logging
from dataclasses import dataclass
from typing import Any

import requests

_log = logging.getLogger(__name__)


@dataclass
class TwitchToken:
    access_token: str
    expires_at: float


class TwitchClient:
    def __init__(self, client_id: str, client_secret: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self._token: TwitchToken | None = None
        self._rl_lock = threading.Lock()
        self._rl_reset_at: float = 0.0
        self._rl_remaining: int | None = None

    def _get_token(self) -> str:
        now = time.time()
        if self._token and self._token.expires_at - 30 > now:
            return self._token.access_token

        resp = self._request(
            method="POST",
            url="https://id.twitch.tv/oauth2/token",
            headers={"Client-Id": self.client_id},
            params={
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "grant_type": "client_credentials",
            },
            is_helix=False,
        )
        data = resp.json()
        access_token = data["access_token"]
        expires_in = int(data.get("expires_in", 3600))
        self._token = TwitchToken(access_token=access_token, expires_at=now + expires_in)
        return access_token

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._get_token()}",
            "Client-Id": self.client_id,
        }

    def _update_rate_limit_from_headers(self, resp: requests.Response) -> None:
        remaining = resp.headers.get("Ratelimit-Remaining")
        reset = resp.headers.get("Ratelimit-Reset")
        with self._rl_lock:
            if remaining is not None:
                try:
                    self._rl_remaining = int(remaining)
                except ValueError:
                    pass
            if reset is not None:
                try:
                    self._rl_reset_at = float(int(reset))
                except ValueError:
                    pass

    def _wait_if_rate_limited(self) -> None:
        # Defer when remaining is low, until reset time.
        LOW_WATERMARK = 5
        while True:
            with self._rl_lock:
                remaining = self._rl_remaining
                reset_at = self._rl_reset_at
            now = time.time()
            if remaining is None:
                return
            if remaining > LOW_WATERMARK:
                return
            if reset_at <= now + 0.001:
                return
            sleep_for = min(60.0, max(0.0, reset_at - now))
            _log.info("Twitch rate limit low (remaining=%s); deferring for %.2fs", remaining, sleep_for)
            time.sleep(sleep_for)

    def _request(
        self,
        method: str,
        url: str,
        headers: dict[str, str] | None = None,
        params: Any | None = None,
        timeout: int = 30,
        is_helix: bool = True,
        max_attempts: int = 5,
    ) -> requests.Response:
        attempt = 0
        backoff = 1.0

        while True:
            attempt += 1

            if is_helix:
                self._wait_if_rate_limited()

            resp = requests.request(method, url, headers=headers, params=params, timeout=timeout)
            if is_helix:
                self._update_rate_limit_from_headers(resp)

            # Token may have expired early; refresh and retry once.
            if is_helix and resp.status_code == 401 and attempt == 1:
                self._token = None
                headers = self._headers()
                continue

            # Rate limited: wait until reset (or backoff) then retry.
            if is_helix and resp.status_code == 429 and attempt < max_attempts:
                reset = resp.headers.get("Ratelimit-Reset")
                now = time.time()
                wait_for = None
                if reset:
                    try:
                        wait_for = max(0.0, float(int(reset)) - now)
                    except ValueError:
                        wait_for = None
                if wait_for is None:
                    wait_for = backoff
                    backoff = min(backoff * 2, 30.0)
                _log.warning("Twitch rate limited (429). Retrying in %.2fs (attempt %s/%s)", wait_for, attempt, max_attempts)
                time.sleep(wait_for)
                continue

            resp.raise_for_status()
            return resp

    def search_games(self, query: str, first: int = 20) -> list[dict[str, Any]]:
        resp = self._request(
            "GET",
            "https://api.twitch.tv/helix/search/categories",
            headers=self._headers(),
            params={"query": query, "first": min(max(first, 1), 100)},
        )
        data = resp.json()
        out: list[dict[str, Any]] = []
        for g in data.get("data", []):
            out.append(
                {
                    "id": g["id"],
                    "name": g["name"],
                    "box_art_url": g.get("box_art_url"),
                }
            )
        return out

    def get_games(self, ids: list[str]) -> list[dict[str, Any]]:
        ids = [i for i in ids if i]
        if not ids:
            return []
        out: list[dict[str, Any]] = []
        for i in range(0, len(ids), 100):
            chunk = ids[i : i + 100]
            resp = self._request(
                "GET",
                "https://api.twitch.tv/helix/games",
                headers=self._headers(),
                params=[("id", x) for x in chunk],
            )
            data = resp.json().get("data", [])
            for g in data:
                out.append({"id": g["id"], "name": g["name"], "box_art_url": g.get("box_art_url")})
        return out

    def fetch_streams_for_game(
        self,
        game_id: str,
        max_streams: int = 200,
        languages: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        languages = languages or []
        collected: list[dict[str, Any]] = []

        def fetch_page(after: str | None, language: str | None) -> tuple[list[dict[str, Any]], str | None]:
            params: list[tuple[str, str]] = [("game_id", game_id), ("first", "100")]
            if after:
                params.append(("after", after))
            if language:
                params.append(("language", language))

            resp = self._request(
                "GET",
                "https://api.twitch.tv/helix/streams",
                headers=self._headers(),
                params=params,
            )
            payload = resp.json()
            page = payload.get("data", [])
            cursor = payload.get("pagination", {}).get("cursor")
            return page, cursor

        # If languages is empty => all languages in one pass
        language_values = languages if languages else [None]

        for lang in language_values:
            after = None
            while len(collected) < max_streams:
                page, after = fetch_page(after, lang)
                for s in page:
                    collected.append(
                        {
                            "id": s["id"],
                            "user_id": s["user_id"],
                            "user_name": s.get("user_name"),
                            "title": s.get("title"),
                            "viewer_count": s.get("viewer_count"),
                            "started_at": s.get("started_at"),
                            "language": s.get("language"),
                            "thumbnail_url": s.get("thumbnail_url"),
                        }
                    )
                    if len(collected) >= max_streams:
                        break
                if not after or not page:
                    break

        # de-dupe if language loops overlap
        unique: dict[str, dict[str, Any]] = {}
        for s in collected:
            unique[s["id"]] = s
        return list(unique.values())

    def get_users(self, user_ids: list[str]) -> list[dict[str, Any]]:
        user_ids = [u for u in user_ids if u]
        if not user_ids:
            return []
        out: list[dict[str, Any]] = []
        for i in range(0, len(user_ids), 100):
            chunk = user_ids[i : i + 100]
            resp = self._request(
                "GET",
                "https://api.twitch.tv/helix/users",
                headers=self._headers(),
                params=[("id", x) for x in chunk],
            )
            for u in resp.json().get("data", []):
                out.append(
                    {
                        "user_id": u["id"],
                        "display_name": u.get("display_name"),
                        # "partner", "affiliate", or ""
                        "broadcaster_type": (u.get("broadcaster_type") or ""),
                    }
                )
        return out

    def get_follower_count(self, broadcaster_id: str) -> int | None:
        """
        Uses the Helix followers endpoint.
        Note: Twitch has changed access requirements for follower APIs over time; if this fails
        (401/403), the app will keep follower_count as NULL and retry later.
        """
        resp = self._request(
            "GET",
            "https://api.twitch.tv/helix/channels/followers",
            headers=self._headers(),
            params={"broadcaster_id": broadcaster_id, "first": 1},
            max_attempts=3,
        )
        if resp.status_code in (401, 403, 404):
            return None
        return int(resp.json().get("total", 0))


