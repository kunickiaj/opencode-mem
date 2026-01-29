from __future__ import annotations

import hashlib
import json
import os
import re
import socket
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

from . import viewer_raw_events
from .config import load_config  # noqa: F401
from .db import DEFAULT_DB_PATH
from .observer import _load_opencode_config
from .raw_event_flush import flush_raw_events  # noqa: F401
from .store import MemoryStore
from .viewer_html import VIEWER_HTML
from .viewer_http import (
    read_json_body,
    reject_cross_origin,
    send_html_response,
    send_json_response,
)
from .viewer_routes import config as viewer_routes_config
from .viewer_routes import memory as viewer_routes_memory
from .viewer_routes import stats as viewer_routes_stats
from .viewer_routes import sync as viewer_routes_sync

DEFAULT_VIEWER_HOST = "127.0.0.1"
DEFAULT_VIEWER_PORT = 38888
DEFAULT_PROVIDER_OPTIONS = ("openai", "anthropic")


def _strip_private(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"<private>.*?</private>", "", text, flags=re.DOTALL | re.IGNORECASE)


def _strip_private_obj(value: Any) -> Any:
    if isinstance(value, str):
        return _strip_private(value)
    if isinstance(value, list):
        return [_strip_private_obj(item) for item in value]
    if isinstance(value, dict):
        return {k: _strip_private_obj(v) for k, v in value.items()}
    return value


RawEventAutoFlusher = viewer_raw_events.RawEventAutoFlusher
RawEventSweeper = viewer_raw_events.RawEventSweeper
RAW_EVENT_FLUSHER = viewer_raw_events.RAW_EVENT_FLUSHER
RAW_EVENT_SWEEPER = viewer_raw_events.RAW_EVENT_SWEEPER


def _load_provider_options() -> list[str]:
    config = _load_opencode_config()
    provider_config = config.get("provider", {})
    providers: list[str] = []
    if isinstance(provider_config, dict):
        providers = [key for key in provider_config if isinstance(key, str) and key]
    if not providers:
        return list(DEFAULT_PROVIDER_OPTIONS)
    combined = sorted(set(providers) | set(DEFAULT_PROVIDER_OPTIONS))
    return combined


class ViewerHandler(BaseHTTPRequestHandler):
    def _send_json(self, payload: dict, status: int = 200) -> None:
        send_json_response(self, payload, status=status)

    def _send_html(self) -> None:
        send_html_response(self, VIEWER_HTML)

    def _read_json(self) -> dict[str, Any] | None:
        return read_json_body(self)

    def _reject_cross_origin(self) -> bool:
        return reject_cross_origin(self)

    def log_message(self, format: str, *args: object) -> None:  # noqa: A003
        if os.environ.get("OPENCODE_MEM_VIEWER_LOGS") == "1":
            super().log_message(format, *args)

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/":
            self._send_html()
            return

        is_api = parsed.path.startswith("/api/")
        store: MemoryStore | None = None
        try:
            store = MemoryStore(os.environ.get("OPENCODE_MEM_DB") or DEFAULT_DB_PATH)
            if viewer_routes_stats.handle_get(self, store, parsed.path, parsed.query):
                return
            if parsed.path == "/api/raw-events/status":
                params = parse_qs(parsed.query)
                limit = int(params.get("limit", ["25"])[0])
                self._send_json(
                    {
                        "items": store.raw_event_backlog(limit=limit),
                        "totals": store.raw_event_backlog_totals(),
                    }
                )
                return
            if viewer_routes_memory.handle_get(self, store, parsed.path, parsed.query):
                return
            if viewer_routes_config.handle_get(
                self,
                path=parsed.path,
                load_provider_options=_load_provider_options,
            ):
                return
            if viewer_routes_sync.handle_get(self, store, parsed.path, parsed.query):
                return
            self.send_response(404)
            self.end_headers()
        except Exception as exc:  # pragma: no cover
            if is_api:
                payload: dict[str, Any] = {"error": "internal server error"}
                if os.environ.get("OPENCODE_MEM_VIEWER_DEBUG") == "1":
                    payload["detail"] = str(exc)
                self._send_json(payload, status=500)
                return
            self.send_response(500)
            self.end_headers()
        finally:
            if store is not None:
                store.close()

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if self._reject_cross_origin():
            return
        if parsed.path in {"/api/sync/peers/rename", "/api/sync/actions/sync-now"}:
            payload = self._read_json()
            if parsed.path == "/api/sync/actions/sync-now" and payload is None:
                payload = {}
            store = MemoryStore(os.environ.get("OPENCODE_MEM_DB") or DEFAULT_DB_PATH)
            try:
                if viewer_routes_sync.handle_post(self, store, parsed.path, payload):
                    return
            finally:
                store.close()
        if parsed.path == "/api/raw-events":
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length).decode("utf-8") if length else ""
            try:
                payload = json.loads(raw) if raw else {}
            except json.JSONDecodeError:
                self._send_json({"error": "invalid json"}, status=400)
                return
            if not isinstance(payload, dict):
                self._send_json({"error": "payload must be an object"}, status=400)
                return

            try:
                store = MemoryStore(os.environ.get("OPENCODE_MEM_DB") or DEFAULT_DB_PATH)
            except Exception as exc:  # pragma: no cover
                response: dict[str, Any] = {"error": "internal server error"}
                if os.environ.get("OPENCODE_MEM_VIEWER_DEBUG") == "1":
                    response["detail"] = str(exc)
                self._send_json(response, status=500)
                return
            try:
                cwd = payload.get("cwd")
                if cwd is not None and not isinstance(cwd, str):
                    self._send_json({"error": "cwd must be string"}, status=400)
                    return
                project = payload.get("project")
                if project is not None and not isinstance(project, str):
                    self._send_json({"error": "project must be string"}, status=400)
                    return
                started_at = payload.get("started_at")
                if started_at is not None and not isinstance(started_at, str):
                    self._send_json({"error": "started_at must be string"}, status=400)
                    return

                items = payload.get("events")
                if items is None:
                    items = [payload]
                if not isinstance(items, list):
                    self._send_json({"error": "events must be a list"}, status=400)
                    return

                default_session_id = str(payload.get("opencode_session_id") or "")
                if default_session_id.startswith("msg_"):
                    self._send_json({"error": "invalid opencode_session_id"}, status=400)
                    return

                inserted = 0
                last_seen_by_session: dict[str, int] = {}
                meta_by_session: dict[str, dict[str, str]] = {}
                session_ids: set[str] = set()
                batch: list[dict[str, Any]] = []
                batch_by_session: dict[str, list[dict[str, Any]]] = {}
                for item in items:
                    if not isinstance(item, dict):
                        self._send_json({"error": "event must be an object"}, status=400)
                        return
                    opencode_session_id = str(
                        item.get("opencode_session_id") or default_session_id or ""
                    )
                    if not opencode_session_id:
                        self._send_json({"error": "opencode_session_id required"}, status=400)
                        return
                    if opencode_session_id.startswith("msg_"):
                        self._send_json({"error": "invalid opencode_session_id"}, status=400)
                        return
                    event_id = str(item.get("event_id") or "")
                    event_type = str(item.get("event_type") or "")
                    if not event_type:
                        self._send_json({"error": "event_type required"}, status=400)
                        return
                    event_seq_value = item.get("event_seq")
                    if event_seq_value is not None:
                        try:
                            int(str(event_seq_value))
                        except (TypeError, ValueError):
                            self._send_json({"error": "event_seq must be int"}, status=400)
                            return

                    ts_wall_ms = item.get("ts_wall_ms")
                    if ts_wall_ms is not None:
                        try:
                            ts_wall_ms = int(ts_wall_ms)
                        except (TypeError, ValueError):
                            self._send_json({"error": "ts_wall_ms must be int"}, status=400)
                            return
                        last_seen_by_session[opencode_session_id] = max(
                            last_seen_by_session.get(opencode_session_id, ts_wall_ms),
                            ts_wall_ms,
                        )
                    ts_mono_ms = item.get("ts_mono_ms")
                    if ts_mono_ms is not None:
                        try:
                            ts_mono_ms = float(ts_mono_ms)
                        except (TypeError, ValueError):
                            self._send_json({"error": "ts_mono_ms must be number"}, status=400)
                            return
                    event_payload = item.get("payload")
                    if event_payload is None:
                        event_payload = {}
                    if not isinstance(event_payload, dict):
                        self._send_json({"error": "payload must be an object"}, status=400)
                        return

                    item_cwd = item.get("cwd")
                    if item_cwd is not None and not isinstance(item_cwd, str):
                        self._send_json({"error": "cwd must be string"}, status=400)
                        return
                    item_project = item.get("project")
                    if item_project is not None and not isinstance(item_project, str):
                        self._send_json({"error": "project must be string"}, status=400)
                        return
                    item_started_at = item.get("started_at")
                    if item_started_at is not None and not isinstance(item_started_at, str):
                        self._send_json({"error": "started_at must be string"}, status=400)
                        return

                    event_payload = _strip_private_obj(event_payload)

                    if not event_id:
                        # Backwards-compat: derive a stable id for legacy senders.
                        if event_seq_value is not None:
                            raw_id = json.dumps(
                                {"s": event_seq_value, "t": event_type, "p": event_payload},
                                sort_keys=True,
                                ensure_ascii=False,
                            )
                            event_hash = hashlib.sha256(raw_id.encode("utf-8")).hexdigest()[:16]
                            event_id = f"legacy-seq-{event_seq_value}-{event_hash}"
                        else:
                            raw_id = json.dumps(
                                {
                                    "t": event_type,
                                    "p": event_payload,
                                    "w": ts_wall_ms,
                                    "m": ts_mono_ms,
                                },
                                sort_keys=True,
                                ensure_ascii=False,
                            )
                            event_id = (
                                "legacy-" + hashlib.sha256(raw_id.encode("utf-8")).hexdigest()[:16]
                            )
                    event_entry = {
                        "event_id": event_id,
                        "event_type": event_type,
                        "payload": event_payload,
                        "ts_wall_ms": ts_wall_ms,
                        "ts_mono_ms": ts_mono_ms,
                    }
                    batch.append(event_entry)

                    session_ids.add(opencode_session_id)
                    batch_by_session.setdefault(opencode_session_id, []).append(dict(event_entry))

                    if item_cwd or item_project or item_started_at:
                        per_session = meta_by_session.setdefault(opencode_session_id, {})
                        if item_cwd:
                            per_session["cwd"] = item_cwd
                        if item_project:
                            per_session["project"] = item_project
                        if item_started_at:
                            per_session["started_at"] = item_started_at

                if len(session_ids) == 1:
                    single_session_id = next(iter(session_ids))
                    result = store.record_raw_events_batch(
                        opencode_session_id=single_session_id,
                        events=batch,
                    )
                    inserted = int(result["inserted"])
                else:
                    # Fallback: handle multiple sessions individually.
                    for sid, sid_events in batch_by_session.items():
                        result = store.record_raw_events_batch(
                            opencode_session_id=sid,
                            events=sid_events,
                        )
                        inserted += int(result["inserted"])

                for meta_session_id in session_ids:
                    session_meta = meta_by_session.get(meta_session_id, {})
                    apply_request_meta = (
                        len(session_ids) == 1 or meta_session_id == default_session_id
                    )
                    store.update_raw_event_session_meta(
                        opencode_session_id=meta_session_id,
                        cwd=session_meta.get("cwd") or (cwd if apply_request_meta else None),
                        project=session_meta.get("project")
                        or (project if apply_request_meta else None),
                        started_at=session_meta.get("started_at")
                        or (started_at if apply_request_meta else None),
                        last_seen_ts_wall_ms=last_seen_by_session.get(meta_session_id),
                    )
                    RAW_EVENT_FLUSHER.note_activity(meta_session_id)
                self._send_json({"inserted": inserted, "received": len(items)})
                return
            except Exception as exc:  # pragma: no cover
                response: dict[str, Any] = {"error": "internal server error"}
                if os.environ.get("OPENCODE_MEM_VIEWER_DEBUG") == "1":
                    response["detail"] = str(exc)
                self._send_json(response, status=500)
            finally:
                store.close()

        if viewer_routes_config.handle_post(
            self,
            path=parsed.path,
            load_provider_options=_load_provider_options,
        ):
            return

        self.send_response(404)
        self.end_headers()

    def do_DELETE(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if self._reject_cross_origin():
            return
        store = MemoryStore(os.environ.get("OPENCODE_MEM_DB") or DEFAULT_DB_PATH)
        try:
            if viewer_routes_sync.handle_delete(self, store, parsed.path):
                return
            self.send_response(404)
            self.end_headers()
        finally:
            store.close()


def _serve(host: str, port: int) -> None:
    RAW_EVENT_SWEEPER.start()
    server = HTTPServer((host, port), ViewerHandler)
    server.serve_forever()


def start_viewer(
    host: str = DEFAULT_VIEWER_HOST,
    port: int = DEFAULT_VIEWER_PORT,
    background: bool = False,
) -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(0.2)
        try:
            if sock.connect_ex((host, port)) == 0:
                return
        except OSError:
            pass
    if background:
        thread = threading.Thread(target=_serve, args=(host, port), daemon=True)
        thread.start()
    else:
        _serve(host, port)
