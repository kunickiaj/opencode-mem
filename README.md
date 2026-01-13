# opencode-mem

A lightweight persistent-memory companion for OpenCode. Captures terminal sessions (and tool calls) as memories, serves a viewer, and exposes an OpenCode plugin that records tool usage automatically.

## Quick setup

```bash
uv pip install -e .
# or
pip install -e .
```

Optionally point the SQLite store somewhere else:

```bash
export OPENCODE_MEM_DB=~/opencode-mem.sqlite
```

## CLI commands

- `opencode-mem init-db` – initialize the database.
- `opencode-mem run -- <cmd>` – run any command while automatically capturing transcripts and artifacts.
- `opencode-mem stats` / `opencode-mem recent` / `opencode-mem search` – inspect stored memories.
- `opencode-mem serve` – launch the web viewer (the plugin also auto-starts it).

## Plugin mode

When OpenCode starts inside this repo (or when the plugin is copied into `~/.config/opencode/plugin/`), `.opencode/plugin/opencode-mem.js` loads automatically. It:

1. Tracks every tool invocation (`tool.execute.after`).
2. Flushes captured events when the session idles, errors, or before compaction (`experimental.session.compacting`).
3. Starts the viewer automatically and prints startup stats with a link.
4. Posts payloads into `python -m opencode_mem.plugin_ingest` (uses `python3` by default).

### Environment hints for the plugin

| Env var | Description |
| --- | --- |
| `OPENCODE_MEM_PYTHON` | Override the Python binary the plugin spawns (defaults to `python3`). |
| `OPENCODE_MEM_VIEWER` | Set to `0`, `false`, or `off` to skip auto-starting the viewer. |
| `OPENCODE_MEM_VIEWER_HOST`, `OPENCODE_MEM_VIEWER_PORT` | Customize the viewer host/port printed on startup. |
| `OPENCODE_MEM_PLUGIN_DEBUG` | Set to `1`, `true`, or `yes` to log plugin lifecycle events via `client.app.log`. |

## Observation classification model

The ingest pipeline now classifies memories into categories (`prompt`, `discovery`, `change`, `decision`). The defaults are:

- **OpenAI**: `gpt-5.1-codex-mini` (requires `OPENCODE_MEM_OBSERVATION_API_KEY`).
- **Anthropic**: `claude-3-haiku` (set `OPENCODE_MEM_OBSERVATION_PROVIDER=anthropic` along with `OPENCODE_MEM_OBSERVATION_API_KEY`).

If no API key is provided, a faster keyword-based heuristic runs instead. Override the exact model with `OPENCODE_MEM_OBSERVATION_MODEL`.

## Running OpenCode with the plugin

1. Start OpenCode inside this repo (or make the plugin global so it globs in everywhere).
2. Every tooling session now creates a memory entry and pushes typed artifacts into SQLite.
3. Use `opencode-mem stats` / `recent` to see sessions and confirm the plugin ingested them.
4. Browse the viewer at the printed URL.
