import logging
import threading
import time
from typing import Callable

from services.db import TTL_SECONDS, Database
from services.twitch import TwitchClient

_log = logging.getLogger(__name__)


class StreamFetcher:
    def __init__(
        self,
        db: Database,
        twitch: TwitchClient,
        fetch_interval_seconds: int = 300,
        max_streams_per_game: int = 200,
        languages: list[str] | None = None,
        on_game_updated: Callable[[str], None] | None = None,
    ):
        self.db = db
        self.twitch = twitch
        self.fetch_interval_seconds = max(30, int(fetch_interval_seconds))
        self.max_streams_per_game = int(max_streams_per_game)
        self.languages = languages or []
        self.on_game_updated = on_game_updated

        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._run_loop, name="stream-fetcher", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)

    def _run_loop(self) -> None:
        # Small initial delay so the server boots first.
        time.sleep(1)
        while not self._stop.is_set():
            started = time.time()
            try:
                self._tick()
            except Exception:
                # Keep the loop alive no matter what.
                pass

            elapsed = time.time() - started
            sleep_for = max(1, self.fetch_interval_seconds - elapsed)
            self._stop.wait(timeout=sleep_for)

    def _tick(self) -> None:
        game_ids = self.db.get_tracked_games()
        if not game_ids:
            return

        # Make sure we have game metadata for these IDs (box art, name).
        if _log.isEnabledFor(logging.DEBUG):
            _log.debug("Fetching streams: tracked_games=%s", len(game_ids))
        games = self.twitch.get_games(game_ids)
        if games:
            self.db.upsert_games(games)

        for gid in game_ids:
            if _log.isEnabledFor(logging.DEBUG):
                _log.debug("Fetching streams for game_id=%s ...", gid)
            streams = self.twitch.fetch_streams_for_game(
                gid,
                max_streams=self.max_streams_per_game,
                languages=self.languages,
            )
            if _log.isEnabledFor(logging.DEBUG):
                _log.debug("Fetched streams for game_id=%s streams=%s", gid, len(streams))
            self.db.upsert_streams(gid, streams)

            # Upsert streamer broadcaster types in batch
            user_ids = list({s["user_id"] for s in streams if s.get("user_id")})
            if user_ids:
                users = self.twitch.get_users(user_ids)
                # Ensure they have follower TTL slots (so we can select for refresh)
                profiles = [
                    {
                        "user_id": u["user_id"],
                        "display_name": u.get("display_name"),
                        "broadcaster_type": u.get("broadcaster_type"),
                        "follower_count": None,
                        "follower_expires_at": None,
                    }
                    for u in users
                ]
                if profiles:
                    self.db.upsert_streamer_profiles(profiles)

            if self.on_game_updated:
                self.on_game_updated(gid)

        # Refresh follower counts for a limited batch each tick (cached for TTL_SECONDS)
        to_refresh = self.db.get_profiles_needing_followers(limit=25)
        if to_refresh:
            if _log.isEnabledFor(logging.DEBUG):
                _log.debug("Fetching follower counts: users=%s", len(to_refresh))
            refreshed = []
            now = int(time.time())
            ok_count = 0
            none_count = 0
            for uid in to_refresh:
                cnt = self.twitch.get_follower_count(uid)
                if cnt is None:
                    none_count += 1
                    # Retry later; don't hammer.
                    refreshed.append(
                        {
                            "user_id": uid,
                            "follower_count": None,
                            "follower_expires_at": now + 6 * 60 * 60,
                        }
                    )
                else:
                    ok_count += 1
                    refreshed.append(
                        {
                            "user_id": uid,
                            "follower_count": int(cnt),
                            "follower_expires_at": now + TTL_SECONDS,
                        }
                    )
            if refreshed:
                self.db.upsert_streamer_profiles(refreshed)
            if _log.isEnabledFor(logging.DEBUG):
                _log.debug("Follower counts updated: ok=%s deferred_or_unavailable=%s", ok_count, none_count)

        purged = self.db.purge_expired()
        if purged and _log.isEnabledFor(logging.DEBUG):
            _log.debug("Purged expired streams: deleted=%s", purged)


