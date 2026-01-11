import os
import sqlite3
import threading
import time
from contextlib import contextmanager
from typing import Any, Iterable

TTL_SECONDS = 7 * 24 * 60 * 60


class Database:
    def __init__(self, path: str):
        self.path = path
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        self._init_lock = threading.Lock()
        self._initialized = False

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA foreign_keys=ON;")
        return conn

    @contextmanager
    def session(self) -> Iterable[sqlite3.Connection]:
        self.init()
        conn = self.connect()
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def init(self) -> None:
        if self._initialized:
            return
        with self._init_lock:
            if self._initialized:
                return
            with self.connect() as conn:
                conn.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS games (
                      id TEXT PRIMARY KEY,
                      name TEXT NOT NULL,
                      box_art_url TEXT,
                      updated_at INTEGER NOT NULL
                    );

                    CREATE TABLE IF NOT EXISTS tracked_games (
                      game_id TEXT PRIMARY KEY REFERENCES games(id) ON DELETE CASCADE,
                      last_requested_at INTEGER NOT NULL
                    );

                    CREATE TABLE IF NOT EXISTS streamer_profiles (
                      user_id TEXT PRIMARY KEY,
                      display_name TEXT,
                      broadcaster_type TEXT, -- "partner", "affiliate", or ""
                      follower_count INTEGER,
                      follower_expires_at INTEGER,
                      updated_at INTEGER NOT NULL
                    );

                    CREATE TABLE IF NOT EXISTS streams (
                      id TEXT PRIMARY KEY,
                      game_id TEXT NOT NULL REFERENCES games(id) ON DELETE CASCADE,
                      user_id TEXT NOT NULL,
                      user_name TEXT,
                      title TEXT,
                      viewer_count INTEGER,
                      started_at TEXT,
                      language TEXT,
                      thumbnail_url TEXT,
                      is_live INTEGER NOT NULL,
                      last_seen_at INTEGER NOT NULL,
                      ended_at INTEGER
                    );

                    CREATE INDEX IF NOT EXISTS idx_streams_game_live ON streams(game_id, is_live);
                    CREATE INDEX IF NOT EXISTS idx_streams_last_seen ON streams(last_seen_at);
                    CREATE INDEX IF NOT EXISTS idx_profiles_follower_exp ON streamer_profiles(follower_expires_at);
                    """
                )
                conn.commit()
            self._initialized = True

    def upsert_games(self, games: list[dict[str, Any]]) -> None:
        now = int(time.time())
        with self.session() as conn:
            conn.executemany(
                """
                INSERT INTO games(id, name, box_art_url, updated_at)
                VALUES(?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                  name=excluded.name,
                  box_art_url=excluded.box_art_url,
                  updated_at=excluded.updated_at
                """,
                [(g["id"], g["name"], g.get("box_art_url"), now) for g in games],
            )

    def touch_tracked_games(self, game_ids: list[str]) -> None:
        now = int(time.time())
        with self.session() as conn:
            conn.executemany(
                """
                INSERT INTO tracked_games(game_id, last_requested_at)
                VALUES(?, ?)
                ON CONFLICT(game_id) DO UPDATE SET last_requested_at=excluded.last_requested_at
                """,
                [(gid, now) for gid in game_ids],
            )

    def get_tracked_games(self) -> list[str]:
        cutoff = int(time.time()) - TTL_SECONDS
        with self.session() as conn:
            rows = conn.execute(
                "SELECT game_id FROM tracked_games WHERE last_requested_at >= ? ORDER BY last_requested_at DESC",
                (cutoff,),
            ).fetchall()
            return [r["game_id"] for r in rows]

    def upsert_streams(self, game_id: str, streams: list[dict[str, Any]]) -> None:
        now = int(time.time())
        with self.session() as conn:
            # mark missing live streams as inactive for this game
            fetched_ids = {s["id"] for s in streams}
            if fetched_ids:
                placeholders = ",".join("?" for _ in fetched_ids)
                conn.execute(
                    f"""
                    UPDATE streams
                    SET is_live=0, ended_at=?
                    WHERE game_id=? AND is_live=1 AND id NOT IN ({placeholders})
                    """,
                    (now, game_id, *sorted(fetched_ids)),
                )
            else:
                conn.execute(
                    "UPDATE streams SET is_live=0, ended_at=? WHERE game_id=? AND is_live=1",
                    (now, game_id),
                )

            conn.executemany(
                """
                INSERT INTO streams(
                  id, game_id, user_id, user_name, title, viewer_count,
                  started_at, language, thumbnail_url, is_live, last_seen_at, ended_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, NULL)
                ON CONFLICT(id) DO UPDATE SET
                  game_id=excluded.game_id,
                  user_id=excluded.user_id,
                  user_name=excluded.user_name,
                  title=excluded.title,
                  viewer_count=excluded.viewer_count,
                  started_at=excluded.started_at,
                  language=excluded.language,
                  thumbnail_url=excluded.thumbnail_url,
                  is_live=1,
                  last_seen_at=excluded.last_seen_at,
                  ended_at=NULL
                """,
                [
                    (
                        s["id"],
                        game_id,
                        s["user_id"],
                        s.get("user_name"),
                        s.get("title"),
                        int(s.get("viewer_count") or 0),
                        s.get("started_at"),
                        s.get("language"),
                        s.get("thumbnail_url"),
                        now,
                    )
                    for s in streams
                ],
            )

    def upsert_streamer_profiles(self, profiles: list[dict[str, Any]]) -> None:
        now = int(time.time())
        with self.session() as conn:
            conn.executemany(
                """
                INSERT INTO streamer_profiles(
                  user_id, display_name, broadcaster_type, follower_count, follower_expires_at, updated_at
                ) VALUES(?, ?, ?, ?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                  display_name=COALESCE(excluded.display_name, streamer_profiles.display_name),
                  broadcaster_type=COALESCE(excluded.broadcaster_type, streamer_profiles.broadcaster_type),
                  follower_count=COALESCE(excluded.follower_count, streamer_profiles.follower_count),
                  follower_expires_at=COALESCE(excluded.follower_expires_at, streamer_profiles.follower_expires_at),
                  updated_at=excluded.updated_at
                """,
                [
                    (
                        p["user_id"],
                        p.get("display_name"),
                        p.get("broadcaster_type"),
                        p.get("follower_count"),
                        p.get("follower_expires_at"),
                        now,
                    )
                    for p in profiles
                ],
            )

    def get_profiles_needing_followers(self, limit: int = 50) -> list[str]:
        now = int(time.time())
        with self.session() as conn:
            rows = conn.execute(
                """
                SELECT user_id FROM streamer_profiles
                WHERE follower_expires_at IS NULL OR follower_expires_at <= ?
                ORDER BY COALESCE(follower_expires_at, 0) ASC
                LIMIT ?
                """,
                (now, limit),
            ).fetchall()
            return [r["user_id"] for r in rows]

    def purge_expired(self) -> int:
        cutoff = int(time.time()) - TTL_SECONDS
        with self.session() as conn:
            cur = conn.execute("DELETE FROM streams WHERE last_seen_at < ?", (cutoff,))
            return int(cur.rowcount or 0)

    def query_streams(
        self,
        game_ids: list[str],
        require_broadcaster_type: str | None = None,  # "partner"|"affiliate"|"verified"|None
        min_viewers: int | None = None,
        max_viewers: int | None = None,
        min_followers: int | None = None,
        max_followers: int | None = None,
        ignored_user_ids: set[str] | None = None,
    ) -> dict[str, Any]:
        if not game_ids:
            return {"games": []}

        ignored_user_ids = ignored_user_ids or set()

        where: list[str] = ["s.game_id IN ({})".format(",".join("?" for _ in game_ids))]
        params: list[Any] = list(game_ids)

        where.append("s.is_live=1")

        if require_broadcaster_type in ("partner", "affiliate"):
            where.append("p.broadcaster_type = ?")
            params.append(require_broadcaster_type)
        elif require_broadcaster_type == "verified":
            where.append("p.broadcaster_type IN ('partner','affiliate')")

        if min_viewers is not None:
            where.append("s.viewer_count >= ?")
            params.append(int(min_viewers))
        if max_viewers is not None:
            where.append("s.viewer_count <= ?")
            params.append(int(max_viewers))

        if min_followers is not None:
            where.append("p.follower_count IS NOT NULL AND p.follower_count >= ?")
            params.append(int(min_followers))
        if max_followers is not None:
            where.append("p.follower_count IS NOT NULL AND p.follower_count <= ?")
            params.append(int(max_followers))

        if ignored_user_ids:
            where.append("s.user_id NOT IN ({})".format(",".join("?" for _ in ignored_user_ids)))
            params.extend(sorted(ignored_user_ids))

        sql = f"""
        SELECT
          g.id AS game_id, g.name AS game_name, g.box_art_url AS game_box_art_url,
          s.id AS stream_id, s.user_id, s.user_name, s.title, s.viewer_count, s.started_at, s.language,
          s.thumbnail_url,
          p.broadcaster_type, p.follower_count
        FROM streams s
        JOIN games g ON g.id = s.game_id
        LEFT JOIN streamer_profiles p ON p.user_id = s.user_id
        WHERE {" AND ".join(where)}
        ORDER BY g.name ASC, s.viewer_count DESC
        """

        games: dict[str, dict[str, Any]] = {}
        with self.session() as conn:
            rows = conn.execute(sql, params).fetchall()
            for r in rows:
                gid = r["game_id"]
                if gid not in games:
                    games[gid] = {
                        "game": {
                            "id": gid,
                            "name": r["game_name"],
                            "box_art_url": r["game_box_art_url"],
                        },
                        "streams": [],
                    }
                games[gid]["streams"].append(
                    {
                        "id": r["stream_id"],
                        "user_id": r["user_id"],
                        "user_name": r["user_name"],
                        "title": r["title"],
                        "viewer_count": r["viewer_count"],
                        "started_at": r["started_at"],
                        "language": r["language"],
                        "thumbnail_url": r["thumbnail_url"],
                        "broadcaster_type": r["broadcaster_type"] or "",
                        "follower_count": r["follower_count"],
                    }
                )

        # Ensure we return empty game cards too (so UI can show "no streams right now")
        with self.session() as conn:
            game_rows = conn.execute(
                f"SELECT id, name, box_art_url FROM games WHERE id IN ({','.join('?' for _ in game_ids)})",
                game_ids,
            ).fetchall()
            for gr in game_rows:
                gid = gr["id"]
                games.setdefault(
                    gid,
                    {
                        "game": {"id": gid, "name": gr["name"], "box_art_url": gr["box_art_url"]},
                        "streams": [],
                    },
                )

        # preserve followed order
        ordered = [games[gid] for gid in game_ids if gid in games]
        return {"games": ordered}


