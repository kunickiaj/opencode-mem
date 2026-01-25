import os
from pathlib import Path

from opencode_mem import db, sync_identity


def _write_fake_keys(private_key_path: Path, public_key_path: Path) -> None:
    private_key_path.parent.mkdir(parents=True, exist_ok=True)
    private_key_path.write_text("private-key")
    public_key_path.write_text("public-key")
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
