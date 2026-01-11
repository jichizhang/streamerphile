import json
import os
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class AppConfig:
    twitch_client_id: str
    twitch_client_secret: str
    debug: bool = False
    fetch_interval_seconds: int = 300
    max_streams_per_game: int = 200
    languages: list[str] | None = None
    database_path: str = "data/streamerphile.sqlite3"


def _load_json(path: str) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_config(config_path: str = "config.json") -> AppConfig:
    if not os.path.exists(config_path):
        raise FileNotFoundError(
            f"Missing {config_path}. Copy config.json.example to config.json and fill it in."
        )

    raw = _load_json(config_path)
    languages = raw.get("languages", [])
    if languages is None:
        languages = []

    return AppConfig(
        twitch_client_id=raw["twitch_client_id"],
        twitch_client_secret=raw["twitch_client_secret"],
        debug=bool(raw.get("debug", False)),
        fetch_interval_seconds=int(raw.get("fetch_interval_seconds", 300)),
        max_streams_per_game=int(raw.get("max_streams_per_game", 200)),
        languages=list(languages),
        database_path=str(raw.get("database_path", "data/streamerphile.sqlite3")),
    )


