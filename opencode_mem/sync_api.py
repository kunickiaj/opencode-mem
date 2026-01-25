from __future__ import annotations

import json
import os
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from typing import Any, cast
from urllib.parse import parse_qs, urlparse

from .db import DEFAULT_DB_PATH
from .store import MemoryStore, ReplicationOp
from .sync_identity import ensure_device_identity

PROTOCOL_VERSION = "1"


def _read_json_body(handler: BaseHTTPRequestHandler) -> dict[str, Any] | None:
    length = int(handler.headers.get("Content-Length", "0") or 0)
    if length <= 0:
        return None
    raw = handler.rfile.read(length)
    try:
        data = json.loads(raw.decode("utf-8"))
    except json.JSONDecodeError:
        return None
    return data if isinstance(data, dict) else None


def _send_json(handler: BaseHTTPRequestHandler, payload: dict[str, Any], status: int = 200) -> None:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


def _is_authorized(store: MemoryStore, headers: Any) -> bool:
    peers = store.conn.execute(
        "SELECT peer_device_id, pinned_fingerprint FROM sync_peers"
    ).fetchall()
    if not peers:
        return True
    device_id = headers.get("X-Opencode-Device")
    signature = headers.get("X-Opencode-Signature")
    if not device_id or not signature:
        row = store.conn.execute(
            """
            SELECT 1
            FROM sync_peers
            WHERE peer_device_id = ? AND pinned_fingerprint IS NULL
            """,
            (device_id,),
        ).fetchone()
        return row is not None
    row = store.conn.execute(
        "SELECT 1 FROM sync_peers WHERE peer_device_id = ?",
        (device_id,),
    ).fetchone()
    return row is not None


def build_sync_handler(db_path: Path | None = None):
    resolved_db = Path(db_path or os.environ.get("OPENCODE_MEM_DB") or DEFAULT_DB_PATH)

    class SyncHandler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: object) -> None:  # noqa: A003
            if os.environ.get("OPENCODE_MEM_SYNC_LOGS") == "1":
                super().log_message(format, *args)

        def _store(self) -> MemoryStore:
            return MemoryStore(resolved_db)

        def _unauthorized(self) -> None:
            _send_json(self, {"error": "unauthorized"}, status=401)

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path == "/v1/status":
                store = self._store()
                try:
                    device_id, fingerprint = ensure_device_identity(store.conn)
                except Exception:
                    _send_json(self, {"error": "internal_error"}, status=500)
                else:
                    _send_json(
                        self,
                        {
                            "device_id": device_id,
                            "protocol_version": PROTOCOL_VERSION,
                            "fingerprint": fingerprint,
                        },
                    )
                finally:
                    store.close()
                return

            if parsed.path == "/v1/ops":
                store = self._store()
                try:
                    if not _is_authorized(store, self.headers):
                        self._unauthorized()
                        return
                    params = parse_qs(parsed.query)
                    cursor = params.get("since", [None])[0]
                    limit_value = params.get("limit", ["200"])[0]
                    try:
                        limit = max(1, min(int(limit_value), 1000))
                    except (TypeError, ValueError):
                        limit = 200
                    ops, next_cursor = store.load_replication_ops_since(cursor, limit=limit)
                    _send_json(self, {"ops": ops, "next_cursor": next_cursor})
                finally:
                    store.close()
                return

            _send_json(self, {"error": "not_found"}, status=404)

        def do_POST(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path != "/v1/ops":
                _send_json(self, {"error": "not_found"}, status=404)
                return
            store = self._store()
            try:
                if not _is_authorized(store, self.headers):
                    self._unauthorized()
                    return
                data = _read_json_body(self)
                if data is None:
                    _send_json(self, {"error": "invalid_json"}, status=400)
                    return
                ops = data.get("ops")
                if not isinstance(ops, list):
                    _send_json(self, {"error": "invalid_ops"}, status=400)
                    return
                normalized_ops: list[dict[str, Any]] = []
                for op in ops:
                    if not isinstance(op, dict):
                        continue
                    normalized_ops.append(op)
                result = store.apply_replication_ops(cast(list[ReplicationOp], normalized_ops))
                _send_json(self, result)
            finally:
                store.close()

    return SyncHandler
