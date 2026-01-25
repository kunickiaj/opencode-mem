import http.client
import json
import threading
from http.server import HTTPServer
from pathlib import Path

from opencode_mem.store import MemoryStore
from opencode_mem.sync_api import build_sync_handler


def _start_server(db_path: Path) -> tuple[HTTPServer, int]:
    handler = build_sync_handler(db_path)
    server = HTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, int(server.server_address[1])


def test_ops_cursor_paging(tmp_path: Path) -> None:
    store = MemoryStore(tmp_path / "mem.sqlite")
    try:
        session_id = store.start_session(
            cwd=str(tmp_path),
            git_remote=None,
            git_branch=None,
            user="tester",
            tool_version="test",
            project="/tmp/project-a",
        )
        store.remember(session_id, kind="note", title="A", body_text="One")
        store.remember(session_id, kind="note", title="B", body_text="Two")
    finally:
        store.close()

    server, port = _start_server(tmp_path / "mem.sqlite")
    try:
        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=2)
        conn.request("GET", "/v1/ops?limit=1")
        resp = conn.getresponse()
        payload = json.loads(resp.read().decode("utf-8"))
        assert resp.status == 200
        assert len(payload.get("ops", [])) == 1
        cursor = payload.get("next_cursor")
        assert cursor
        conn.close()

        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=2)
        conn.request("GET", f"/v1/ops?limit=1&since={cursor}")
        resp = conn.getresponse()
        payload = json.loads(resp.read().decode("utf-8"))
        assert resp.status == 200
        assert len(payload.get("ops", [])) == 1
        assert payload.get("next_cursor")
    finally:
        server.shutdown()
