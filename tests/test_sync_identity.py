import os
from pathlib import Path

from codemem import db, sync_identity


def _write_fake_keys(private_key_path: Path, public_key_path: Path) -> None:
    private_key_path.parent.mkdir(parents=True, exist_ok=True)
    private_key_path.write_text("private-key")
    public_key_path.write_text("public-key")
    os.chmod(private_key_path, 0o600)


def _write_recovered_keys(private_key_path: Path, public_key_path: Path) -> None:
    private_key_path.parent.mkdir(parents=True, exist_ok=True)
    private_key_path.write_text("recovered-private")
    public_key_path.write_text(
        "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAINotARealKeyButValidFormat recovered@local\n"
    )
    os.chmod(private_key_path, 0o600)


def test_device_identity_persists(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(sync_identity, "_generate_keypair", _write_fake_keys)
    conn = db.connect(tmp_path / "mem.sqlite")
    try:
        db.initialize_schema(conn)
        keys_dir = tmp_path / "keys"

        device_id, fingerprint = sync_identity.ensure_device_identity(conn, keys_dir=keys_dir)
        again_device_id, again_fingerprint = sync_identity.ensure_device_identity(
            conn, keys_dir=keys_dir
        )

        assert device_id == again_device_id
        assert fingerprint == again_fingerprint

        row = conn.execute("SELECT device_id, fingerprint FROM sync_device LIMIT 1").fetchone()
        assert row is not None
        assert row["device_id"] == device_id

        mode = (keys_dir / "device.key").stat().st_mode & 0o777
        assert mode == 0o600
    finally:
        conn.close()


def test_load_private_key(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(sync_identity, "_generate_keypair", _write_fake_keys)
    conn = db.connect(tmp_path / "mem.sqlite")
    try:
        db.initialize_schema(conn)
        keys_dir = tmp_path / "keys"
        sync_identity.ensure_device_identity(conn, keys_dir=keys_dir)
        private_key = sync_identity.load_private_key(keys_dir)
        assert private_key == b"private-key"
    finally:
        conn.close()


def test_keychain_load_prefers_secret_tool(monkeypatch, tmp_path: Path) -> None:
    db_path = tmp_path / "mem.sqlite"
    monkeypatch.setenv("CODEMEM_DB", str(db_path))
    monkeypatch.setenv("CODEMEM_SYNC_KEY_STORE", "keychain")
    monkeypatch.setattr(sync_identity.sys, "platform", "linux")
    monkeypatch.setattr(sync_identity, "_secret_tool_available", lambda: True)

    class Result:
        def __init__(self) -> None:
            self.returncode = 0
            self.stdout = b"secret-key"

    def fake_run(command, input=None, capture_output=None, check=False):
        return Result()

    monkeypatch.setattr(sync_identity.subprocess, "run", fake_run)
    conn = db.connect(db_path)
    try:
        db.initialize_schema(conn)
        conn.execute(
            "INSERT INTO sync_device(device_id, public_key, fingerprint, created_at) VALUES (?, ?, ?, ?)",
            ("dev-1", "pub", "fp", "2026-01-24T00:00:00Z"),
        )
        conn.commit()
    finally:
        conn.close()

    private_key = sync_identity.load_private_key()
    assert private_key == b"secret-key"


def test_ensure_device_identity_recovers_invalid_existing_keys(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(sync_identity, "_generate_keypair", _write_recovered_keys)
    monkeypatch.setattr(sync_identity, "_ssh_keygen_available", lambda: False)
    conn = db.connect(tmp_path / "mem.sqlite")
    try:
        db.initialize_schema(conn)
        keys_dir = tmp_path / "keys"
        keys_dir.mkdir(parents=True, exist_ok=True)
        private_key_path = keys_dir / "device.key"
        public_key_path = keys_dir / "device.key.pub"
        private_key_path.write_text("private-key")
        public_key_path.write_text("public-key")

        sync_identity.ensure_device_identity(conn, keys_dir=keys_dir)

        assert private_key_path.read_text() == "recovered-private"
        assert public_key_path.read_text().startswith("ssh-ed25519 ")
        assert list(keys_dir.glob("device.key.invalid-*"))
        assert list(keys_dir.glob("device.key.pub.invalid-*"))
    finally:
        conn.close()
