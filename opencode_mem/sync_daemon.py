from __future__ import annotations

import datetime as dt
import json
import threading
from http.client import HTTPConnection, HTTPSConnection
from http.server import HTTPServer
from pathlib import Path
from typing import Any, cast
from urllib.parse import urlencode, urlparse

from . import db
from .store import MemoryStore, ReplicationOp
from .sync_api import build_sync_handler
from .sync_discovery import record_peer_success, record_sync_attempt


def _build_base_url(address: str) -> str:
    trimmed = address.strip().rstrip("/")
    if not trimmed:
        return ""
    parsed = urlparse(trimmed)
    if parsed.scheme:
        return trimmed
    return f"http://{trimmed}"


def _request_json(
    method: str,
    url: str,
    *,
    headers: dict[str, str] | None = None,
    body: dict[str, Any] | None = None,
    timeout_s: float = 3.0,
) -> tuple[int, dict[str, Any] | None]:
    parsed = urlparse(url)
    if not parsed.hostname:
        raise ValueError("missing hostname")
    if parsed.scheme == "https":
        conn = HTTPSConnection(parsed.hostname, parsed.port or 443, timeout=timeout_s)
    else:
        conn = HTTPConnection(parsed.hostname, parsed.port or 80, timeout=timeout_s)
    path = parsed.path or "/"
    if parsed.query:
        path = f"{path}?{parsed.query}"
    payload = None
    body_bytes = None
    if body is not None:
        body_bytes = json.dumps(body, ensure_ascii=False).encode("utf-8")
    request_headers = {"Accept": "application/json"}
    if body_bytes is not None:
        request_headers["Content-Type"] = "application/json"
        request_headers["Content-Length"] = str(len(body_bytes))
    if headers:
        request_headers.update(headers)
    conn.request(method, path, body=body_bytes, headers=request_headers)
    resp = conn.getresponse()
    raw = resp.read()
    if raw:
        try:
            payload = json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError:
            payload = None
    conn.close()
    return resp.status, payload if isinstance(payload, dict) else None


def _get_replication_cursor(
    store: MemoryStore, peer_device_id: str
) -> tuple[str | None, str | None]:
    row = store.conn.execute(
        """
        SELECT last_applied_cursor, last_acked_cursor
        FROM replication_cursors
        WHERE peer_device_id = ?
        """,
        (peer_device_id,),
    ).fetchone()
    if row is None:
        return None, None
    return row["last_applied_cursor"], row["last_acked_cursor"]


def _set_replication_cursor(
    store: MemoryStore,
    peer_device_id: str,
    *,
    last_applied: str | None = None,
    last_acked: str | None = None,
) -> None:
    now = dt.datetime.now(dt.UTC).isoformat()
    row = store.conn.execute(
        "SELECT 1 FROM replication_cursors WHERE peer_device_id = ?",
        (peer_device_id,),
    ).fetchone()
    if row is None:
        store.conn.execute(
            """
            INSERT INTO replication_cursors(
                peer_device_id,
                last_applied_cursor,
                last_acked_cursor,
                updated_at
            )
            VALUES (?, ?, ?, ?)
            """,
            (peer_device_id, last_applied, last_acked, now),
        )
    else:
        store.conn.execute(
            """
            UPDATE replication_cursors
            SET last_applied_cursor = COALESCE(?, last_applied_cursor),
                last_acked_cursor = COALESCE(?, last_acked_cursor),
                updated_at = ?
            WHERE peer_device_id = ?
            """,
            (last_applied, last_acked, now, peer_device_id),
        )
    store.conn.commit()


def sync_once(
    store: MemoryStore,
    peer_device_id: str,
    addresses: list[str],
    *,
    limit: int = 200,
) -> dict[str, Any]:
    last_applied, last_acked = _get_replication_cursor(store, peer_device_id)
    headers = {"X-Opencode-Device": store.device_id}
    error: str | None = None
    for address in addresses:
        base_url = _build_base_url(address)
        if not base_url:
            continue
        try:
            query = urlencode({"since": last_applied or "", "limit": limit})
            status, payload = _request_json("GET", f"{base_url}/v1/ops?{query}", headers=headers)
            if status != 200 or payload is None:
                raise RuntimeError("peer ops fetch failed")
            ops = payload.get("ops")
            if not isinstance(ops, list):
                raise RuntimeError("invalid ops response")
            applied = store.apply_replication_ops(cast(list[ReplicationOp], ops))
            next_cursor = payload.get("next_cursor")
            if isinstance(next_cursor, str) and next_cursor:
                _set_replication_cursor(store, peer_device_id, last_applied=next_cursor)
                last_applied = next_cursor

            outbound_ops, outbound_cursor = store.load_replication_ops_since(
                last_acked, limit=limit
            )
            status, payload = _request_json(
                "POST",
                f"{base_url}/v1/ops",
                headers=headers,
                body={"ops": outbound_ops},
            )
            if status != 200 or payload is None:
                raise RuntimeError("peer ops push failed")
            if outbound_ops and outbound_cursor:
                _set_replication_cursor(store, peer_device_id, last_acked=outbound_cursor)
                last_acked = outbound_cursor

            record_peer_success(store.conn, peer_device_id, base_url)
            record_sync_attempt(
                store.conn,
                peer_device_id,
                ok=True,
                ops_in=applied.get("inserted", 0) + applied.get("updated", 0),
                ops_out=len(outbound_ops),
            )
            return {
                "ok": True,
                "address": base_url,
                "ops_in": len(ops),
                "ops_out": len(outbound_ops),
            }
        except Exception as exc:
            error = str(exc)
            continue
    record_sync_attempt(store.conn, peer_device_id, ok=False, error=error)
    return {"ok": False, "error": error}


def sync_daemon_tick(store: MemoryStore) -> list[dict[str, Any]]:
    rows = store.conn.execute("SELECT peer_device_id, addresses_json FROM sync_peers").fetchall()
    results: list[dict[str, Any]] = []
    for row in rows:
        peer_device_id = str(row["peer_device_id"])
        raw_addresses = db.from_json(row["addresses_json"]) if row["addresses_json"] else []
        if not isinstance(raw_addresses, list):
            raw_addresses = []
        addresses = [str(item) for item in raw_addresses if isinstance(item, str)]
        results.append(sync_once(store, peer_device_id, addresses))
    return results


def run_sync_daemon(
    host: str,
    port: int,
    interval_s: int,
    *,
    db_path: Path | None = None,
    stop_event: threading.Event | None = None,
) -> None:
    handler = build_sync_handler(db_path)
    server = HTTPServer((host, port), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    stop = stop_event or threading.Event()
    try:
        while not stop.wait(interval_s):
            store = MemoryStore(db_path or db.DEFAULT_DB_PATH)
            try:
                sync_daemon_tick(store)
            finally:
                store.close()
    finally:
        server.shutdown()
