import json
import logging
import os
import time
from typing import Any

from flask import Flask, Response, jsonify, render_template, request

from services.db import Database
from services.fetcher import StreamFetcher
from services.sse import SseHub
from services.twitch import TwitchClient
from utils.config import load_config

_log = logging.getLogger(__name__)


def _parse_csv(value: str | None) -> list[str]:
    if not value:
        return []
    return [v.strip() for v in value.split(",") if v.strip()]


def _parse_int(value: str | None) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except ValueError:
        return None


def create_app() -> Flask:
    cfg = load_config("config.json")

    logging.basicConfig(
        level=logging.DEBUG if cfg.debug else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    app = Flask(
        __name__,
        static_folder=os.path.join("web", "static"),
        template_folder=os.path.join("web", "templates"),
    )

    db = Database(cfg.database_path)
    twitch = TwitchClient(cfg.twitch_client_id, cfg.twitch_client_secret)
    hub = SseHub()
    fetcher = StreamFetcher(
        db=db,
        twitch=twitch,
        fetch_interval_seconds=cfg.fetch_interval_seconds,
        max_streams_per_game=cfg.max_streams_per_game,
        languages=cfg.languages,
        on_game_updated=hub.publish_game_updated,
    )
    fetcher.start()

    @app.get("/")
    def index() -> str:
        # Session can be shared via URL; frontend will persist to cookies.
        boot_json = json.dumps(
            {
                "server_time": int(time.time()),
                "fetch_interval_seconds": cfg.fetch_interval_seconds,
            }
        )
        return render_template("index.html", boot_json=boot_json)

    @app.get("/api/search_games")
    def api_search_games() -> Response:
        q = (request.args.get("q") or "").strip()
        if not q:
            return jsonify({"games": []})
        games = twitch.search_games(q, first=20)
        if games:
            db.upsert_games(games)
        return jsonify({"games": games})

    @app.post("/api/touch_tracked")
    def api_touch_tracked() -> Response:
        payload: dict[str, Any] = request.get_json(force=True, silent=True) or {}
        game_ids = [str(x) for x in (payload.get("game_ids") or []) if str(x).strip()]
        game_ids = list(dict.fromkeys(game_ids))  # stable dedupe
        if not game_ids:
            return jsonify({"ok": True})

        # ensure game rows exist
        games = twitch.get_games(game_ids)
        if games:
            db.upsert_games(games)
        db.touch_tracked_games(game_ids)
        return jsonify({"ok": True})

    @app.get("/api/streams")
    def api_streams() -> Response:
        game_ids = _parse_csv(request.args.get("game_ids"))
        game_ids = list(dict.fromkeys(game_ids))

        status = (request.args.get("status") or "any").strip().lower()
        require_broadcaster_type = None
        if status in ("partner", "affiliate", "verified"):
            require_broadcaster_type = status

        ignored = set(_parse_csv(request.args.get("ignored")))
        min_viewers = _parse_int(request.args.get("min_viewers"))
        max_viewers = _parse_int(request.args.get("max_viewers"))
        min_followers = _parse_int(request.args.get("min_followers"))
        max_followers = _parse_int(request.args.get("max_followers"))

        if game_ids:
            db.touch_tracked_games(game_ids)

        data = db.query_streams(
            game_ids=game_ids,
            require_broadcaster_type=require_broadcaster_type,
            min_viewers=min_viewers,
            max_viewers=max_viewers,
            min_followers=min_followers,
            max_followers=max_followers,
            ignored_user_ids=ignored,
        )
        if _log.isEnabledFor(logging.DEBUG):
            total_streams = sum(len(g.get("streams") or []) for g in (data.get("games") or []))
            _log.debug(
                "Sending streams to client: games=%s total_streams=%s status=%s ignored=%s viewers=%s..%s followers=%s..%s",
                len(game_ids),
                total_streams,
                require_broadcaster_type or "any",
                len(ignored),
                min_viewers,
                max_viewers,
                min_followers,
                max_followers,
            )
        return jsonify(data)

    @app.get("/api/sse")
    def api_sse() -> Response:
        game_ids = set(_parse_csv(request.args.get("game_ids")))
        client = hub.subscribe(game_ids)

        def gen():
            try:
                # initial hello
                yield 'event: hello\ndata: {"ok": true}\n\n'
                last_ping = time.time()
                while True:
                    try:
                        msg = client.q.get(timeout=15)
                        yield msg
                    except Exception:
                        # keepalive
                        now = time.time()
                        if now - last_ping >= 15:
                            last_ping = now
                            yield "event: ping\ndata: {}\n\n"
            finally:
                hub.unsubscribe(client.id)

        return Response(
            gen(),
            mimetype="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    return app


if __name__ == "__main__":
    app = create_app()
    cfg = load_config("config.json")
    app.run(host="0.0.0.0", port=5000, debug=cfg.debug, threaded=True)
