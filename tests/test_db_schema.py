from __future__ import annotations

from pathlib import Path

from codemem import db


def test_initialize_schema_sets_user_version(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("CODEMEM_EMBEDDING_DISABLED", "1")
    conn = db.connect(tmp_path / "mem.sqlite")
    try:
        db.initialize_schema(conn)
        row = conn.execute("PRAGMA user_version").fetchone()
    finally:
        conn.close()

    assert row is not None
    assert int(row[0]) == db.SCHEMA_VERSION


def test_initialize_schema_skips_reinit_but_keeps_kind_normalization(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("CODEMEM_EMBEDDING_DISABLED", "1")
    conn = db.connect(tmp_path / "mem.sqlite")
    try:
        db.initialize_schema(conn)

        conn.execute(
            """
            INSERT INTO sessions(started_at, cwd, project, user, tool_version)
            VALUES (?, ?, ?, ?, ?)
            """,
            ("2026-01-01T00:00:00Z", "/tmp", "proj", "me", "test"),
        )
        session_id = int(conn.execute("SELECT id FROM sessions").fetchone()[0])
        conn.execute(
            """
            INSERT INTO memory_items(session_id, kind, title, body_text, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (session_id, "project", "t", "b", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"),
        )
        conn.commit()

        def _unexpected_reinit(_conn):
            raise AssertionError(
                "initialize_schema should not rerun full migration at current version"
            )

        monkeypatch.setattr(db, "_initialize_schema_v1", _unexpected_reinit)
        db.initialize_schema(conn)

        kind = conn.execute("SELECT kind FROM memory_items LIMIT 1").fetchone()[0]
    finally:
        conn.close()

    assert kind == "decision"
