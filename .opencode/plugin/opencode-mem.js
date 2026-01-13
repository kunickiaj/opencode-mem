export const OpencodeMemPlugin = async ({ project, client, directory, worktree }) => {
  const events = []
  const maxEvents = Number.parseInt(
    process.env.OPENCODE_MEM_PLUGIN_MAX_EVENTS || "200",
    10
  )
  const maxChars = Number.parseInt(
    process.env.OPENCODE_MEM_PLUGIN_MAX_EVENT_CHARS || "8000",
    10
  )
  const cwd = worktree || directory || process.cwd()
  const debug = ["1", "true", "yes"].includes(
    (process.env.OPENCODE_MEM_PLUGIN_DEBUG || "").toLowerCase()
  )
  const log = async (level, message, extra = {}) => {
    if (!debug) {
      return
    }
    await client.app.log({
      service: "opencode-mem",
      level,
      message,
      extra,
    })
  }
  const pythonBin = process.env.OPENCODE_MEM_PYTHON || "python3"
  const viewerEnabled = !["0", "false", "off"].includes(
    (process.env.OPENCODE_MEM_VIEWER || "1").toLowerCase()
  )
  const viewerHost = process.env.OPENCODE_MEM_VIEWER_HOST || "127.0.0.1"
  const viewerPort = process.env.OPENCODE_MEM_VIEWER_PORT || "37777"
  let sessionStartedAt = null
  let viewerStarted = false
  let startupShown = false

  const startViewer = async () => {
    if (!viewerEnabled || viewerStarted) {
      return
    }
    viewerStarted = true
    await log("info", "starting opencode-mem viewer", { cwd })
    Bun.spawn({
      cmd: [pythonBin, "-m", "opencode_mem.cli", "serve", "--background"],
      cwd,
      env: process.env,
      stdout: "pipe",
      stderr: "pipe",
    })
  }

  const runCli = async (args) => {
    const proc = Bun.spawn({
      cmd: [pythonBin, "-m", "opencode_mem.cli", ...args],
      cwd,
      env: process.env,
      stdout: "pipe",
      stderr: "pipe",
    })
    const [exitCode, stdout, stderr] = await Promise.all([
      proc.exited,
      new Response(proc.stdout).text(),
      new Response(proc.stderr).text(),
    ])
    return { exitCode, stdout, stderr }
  }

  const showStartupInfo = async () => {
    if (startupShown) {
      return
    }
    startupShown = true
    const stats = await runCli(["stats"])
    const recent = await runCli(["recent", "--limit", "3"])
    const lines = [
      "opencode-mem ready",
      `viewer: http://${viewerHost}:${viewerPort}`,
    ]
    if (stats.exitCode === 0 && stats.stdout.trim()) {
      lines.push("", "stats:", stats.stdout.trim())
    }
    if (recent.exitCode === 0 && recent.stdout.trim()) {
      lines.push("", "recent:", recent.stdout.trim())
    }
    const message = lines.join("\n")
    console.log(message)
    await log("info", "opencode-mem startup info", {
      message,
      statsExitCode: stats.exitCode,
      recentExitCode: recent.exitCode,
      statsStderr: stats.stderr,
      recentStderr: recent.stderr,
    })
  }

  await log("info", "opencode-mem plugin initialized", { cwd })
  await startViewer()

  const truncate = (value) => {
    if (value === undefined || value === null) {
      return null
    }
    const text = String(value)
    if (Number.isNaN(maxChars) || maxChars <= 0) {
      return ""
    }
    if (text.length <= maxChars) {
      return text
    }
    return `${text.slice(0, maxChars)}\n[opencode-mem] event truncated\n`
  }

  const safeStringify = (value) => {
    if (value === undefined || value === null) {
      return null
    }
    if (typeof value === "string") {
      return value
    }
    try {
      return JSON.stringify(value)
    } catch (err) {
      return String(value)
    }
  }

  const recordEvent = (event) => {
    events.push(event)
    if (Number.isFinite(maxEvents) && maxEvents > 0 && events.length > maxEvents) {
      events.splice(0, events.length - maxEvents)
    }
  }

  const flushEvents = async () => {
    if (!events.length) {
      return
    }
    const payload = {
      cwd,
      project: project?.root || project?.name || null,
      started_at: sessionStartedAt || new Date().toISOString(),
      events: [...events],
    }
    const input = JSON.stringify(payload)
    const proc = Bun.spawn({
      cmd: [pythonBin, "-m", "opencode_mem.plugin_ingest"],
      cwd,
      env: process.env,
      stdin: new Blob([input]),
      stdout: "pipe",
      stderr: "pipe",
    })
    const [exitCode, stdout, stderr] = await Promise.all([
      proc.exited,
      new Response(proc.stdout).text(),
      new Response(proc.stderr).text(),
    ])
    if (exitCode !== 0) {
      await client.app.log({
        service: "opencode-mem",
        level: "error",
        message: "Failed to ingest opencode-mem plugin events",
        extra: { exitCode, stdout, stderr },
      })
      return
    }
    events.length = 0
    sessionStartedAt = null
  }

  return {
    "session.created": async () => {
      sessionStartedAt = new Date().toISOString()
      await showStartupInfo()
    },
    "tool.execute.after": async (input, output) => {
      const args = output?.args ?? input?.args ?? {}
      const result = output?.result ?? output?.output ?? output?.data ?? null
      const error = output?.error ?? null
      recordEvent({
        type: "tool.execute.after",
        tool: input?.tool || output?.tool || "unknown",
        args,
        result: truncate(safeStringify(result)),
        error: truncate(safeStringify(error)),
        timestamp: new Date().toISOString(),
      })
    },
    "session.idle": async () => {
      await flushEvents()
    },
    "session.error": async () => {
      await flushEvents()
    },
    "experimental.session.compacting": async () => {
      await flushEvents()
    },
  }
}
