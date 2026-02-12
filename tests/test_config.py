import json
from pathlib import Path

import pytest

from codemem.config import get_env_overrides, load_config, read_config_file, write_config_file


def test_read_config_file_rejects_invalid_json(tmp_path: Path) -> None:
    config_path = tmp_path / "config.json"
    config_path.write_text("{not-json}")
    with pytest.raises(ValueError, match="invalid config json"):
        read_config_file(config_path)


def test_write_config_file_roundtrip(tmp_path: Path) -> None:
    config_path = tmp_path / "config.json"
    data = {"observer_provider": "openai", "observer_max_chars": 9000}
    write_config_file(data, config_path)
    assert json.loads(config_path.read_text()) == data
    assert read_config_file(config_path) == data


def test_get_env_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CODEMEM_OBSERVER_PROVIDER", "anthropic")
    monkeypatch.setenv("CODEMEM_OBSERVER_MODEL", "claude-4.5-haiku")
    overrides = get_env_overrides()
    assert overrides["observer_provider"] == "anthropic"
    assert overrides["observer_model"] == "claude-4.5-haiku"


def test_load_config_invalid_int_env_does_not_crash_and_warns(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    config_path = tmp_path / "config.json"
    config_path.write_text("{}\n")
    monkeypatch.setenv("CODEMEM_CONFIG", str(config_path))
    monkeypatch.setenv("CODEMEM_SYNC_PORT", "nope")
    with pytest.warns(RuntimeWarning, match="sync_port"):
        cfg = load_config(config_path)
    assert cfg.sync_port == 7337


def test_load_config_invalid_config_value_does_not_crash_and_warns(tmp_path: Path) -> None:
    config_path = tmp_path / "config.json"
    config_path.write_text('{"sync_port": "abc"}\n')
    with pytest.warns(RuntimeWarning, match="sync_port"):
        cfg = load_config(config_path)
    assert cfg.sync_port == 7337


def test_load_config_reads_hybrid_retrieval_enabled_from_file(tmp_path: Path) -> None:
    config_path = tmp_path / "config.json"
    config_path.write_text('{"hybrid_retrieval_enabled": true}\n')

    cfg = load_config(config_path)

    assert cfg.hybrid_retrieval_enabled is True


def test_load_config_reads_hybrid_retrieval_enabled_from_env(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    config_path = tmp_path / "config.json"
    config_path.write_text("{}\n")
    monkeypatch.setenv("CODEMEM_CONFIG", str(config_path))
    monkeypatch.setenv("CODEMEM_HYBRID_RETRIEVAL_ENABLED", "1")

    cfg = load_config(config_path)

    assert cfg.hybrid_retrieval_enabled is True


def test_load_config_hybrid_retrieval_env_overrides_file(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    config_path = tmp_path / "config.json"
    config_path.write_text('{"hybrid_retrieval_enabled": true}\n')
    monkeypatch.setenv("CODEMEM_HYBRID_RETRIEVAL_ENABLED", "0")

    cfg = load_config(config_path)

    assert cfg.hybrid_retrieval_enabled is False


def test_load_config_hybrid_retrieval_invalid_env_uses_default(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    config_path = tmp_path / "config.json"
    config_path.write_text("{}\n")
    monkeypatch.setenv("CODEMEM_HYBRID_RETRIEVAL_ENABLED", "maybe")

    cfg = load_config(config_path)

    assert cfg.hybrid_retrieval_enabled is False


def test_load_config_reads_hybrid_shadow_settings(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    config_path = tmp_path / "config.json"
    config_path.write_text(
        '{"hybrid_retrieval_shadow_log": true, "hybrid_retrieval_shadow_sample_rate": 0.5}\n'
    )
    monkeypatch.setenv("CODEMEM_HYBRID_RETRIEVAL_SHADOW_LOG", "1")
    monkeypatch.setenv("CODEMEM_HYBRID_RETRIEVAL_SHADOW_SAMPLE_RATE", "0.25")

    cfg = load_config(config_path)

    assert cfg.hybrid_retrieval_shadow_log is True
    assert cfg.hybrid_retrieval_shadow_sample_rate == 0.25


def test_load_config_clamps_hybrid_shadow_sample_rate(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    config_path = tmp_path / "config.json"
    config_path.write_text("{}\n")
    monkeypatch.setenv("CODEMEM_HYBRID_RETRIEVAL_SHADOW_SAMPLE_RATE", "5")

    cfg = load_config(config_path)

    assert cfg.hybrid_retrieval_shadow_sample_rate == 1.0
