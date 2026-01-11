import json
import logging
import queue
import threading
import time
import uuid
from dataclasses import dataclass

_log = logging.getLogger(__name__)


@dataclass
class Client:
    id: str
    game_ids: set[str]
    q: "queue.Queue[str]"


class SseHub:
    def __init__(self):
        self._lock = threading.Lock()
        self._clients: dict[str, Client] = {}

    def subscribe(self, game_ids: set[str]) -> Client:
        client = Client(id=str(uuid.uuid4()), game_ids=set(game_ids), q=queue.Queue(maxsize=100))
        with self._lock:
            self._clients[client.id] = client
        return client

    def unsubscribe(self, client_id: str) -> None:
        with self._lock:
            self._clients.pop(client_id, None)

    def publish_game_updated(self, game_id: str) -> None:
        payload = json.dumps({"type": "game_updated", "game_id": game_id, "ts": int(time.time())})
        msg = f"event: game_updated\ndata: {payload}\n\n"

        with self._lock:
            clients = list(self._clients.values())

        pushed = 0
        for c in clients:
            if game_id not in c.game_ids:
                continue
            try:
                c.q.put_nowait(msg)
                pushed += 1
            except queue.Full:
                # drop updates for slow clients
                pass

        if _log.isEnabledFor(logging.DEBUG):
            _log.debug("SSE publish: game_id=%s clients_notified=%s total_clients=%s", game_id, pushed, len(clients))


