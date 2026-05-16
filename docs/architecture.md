# Vasal — Architecture Decisions

**Status**: Design complete — implementation plan in `docs/plan.md`
**Language**: Rust (single static binary, ~5-10MB target)
**Goal**: OSS, protocol-first, general-purpose host agent

---

## Project Identity

A lightweight, general-purpose agent that:
- Executes tasks dispatched by any control plane that speaks the protocol
- Manages sidecar lifecycle (install, upgrade, health-check, remove)
- Observes infrastructure via sidecars and reports state to the CP
- Self-upgrades with rollback
- Maintains a structured audit trail

The agent is a **task executor + unit lifecycle manager + sensor + self-maintainer**.

It is NOT:
- A decision-maker about infrastructure state
- A reconciler / desired-state convergence engine for infrastructure
- Tied to running on the host it manages (jumpbox pattern is valid)
- Opinionated about what software it manages (SQL, Redis, LVS — all via sidecars)

---

## Confirmed Design Decisions

### DD-01: Shell is the only built-in executor

Everything else (SQL, HTTP with JWT, Ansible, Salt, Puppet, custom logic) is a sidecar.

**Rationale**: The agent has no domain knowledge. SQL is not a primitive — it's a concern of the `sql-ctrl` sidecar. HTTP-to-third-parties with JWT management is a concern of an `http-ctrl` sidecar. This keeps the agent binary small, general-purpose, and free of opinions about what runs on the host.

Shell is built-in because it's the bootstrap primitive — you need it to install the first sidecar.

---

### DD-02: Transport is configurable — HTTP poll or gRPC stream for task dispatch

| Mode | How it works | When to use |
|---|---|---|
| **Poll** (HTTP) | Agent GETs pending tasks on an interval | Firewall-constrained envs, simpler CP implementations |
| **Stream** (gRPC) | Agent opens bidirectional stream, CP pushes tasks | Low-latency dispatch, production-grade deployments |

Both modes use the same task schema. The agent doesn't care how it received the task.

Agent-initiated HTTP POST remains the path for:
- Heartbeat
- Registration
- Audit event forwarding
- Task result reporting
- Observation/state reports

---

### DD-03: No infrastructure reconciliation — agent is a sensor + actuator

The agent does NOT auto-correct infrastructure state (MySQL down, Redis misconfigured, etc.). That's dangerous without domain knowledge and fleet context.

Instead:
- **Observe**: Agent calls sidecars periodically (health/discover), reports what it sees to CP.
- **Act on command**: CP decides corrective actions and dispatches tasks. Agent executes them.
- **CP is the brain**: Only the CP has fleet-wide context to make safe decisions.

The agent auto-corrects ONLY its own ecosystem (sidecars, self-version).

---

### DD-04: Sidecar management generalizes to package/unit management

Sidecars and packages have identical lifecycle operations:
- Download artifact, verify SHA-256
- Install (extract, dpkg, rpm, etc.)
- Start / Stop / Restart
- Health check
- Upgrade with rollback

Abstraction: **Managed Unit**. A sidecar is a managed unit that additionally speaks the agent's IPC protocol. A package is a managed unit without IPC.

```
ManagedUnit {
  name: string,
  kind: "sidecar" | "package",
  version: string,
  artifact: { url, sha256 },
  state: "running" | "installed" | "stopped" | "absent",
  health_check: { ... },       // optional
  config: { ... },             // optional
  ipc: { socket: path },       // only for sidecars
}
```

The CP declares which managed units should exist. The agent ensures they do.

---

### DD-05: Agent is not necessarily co-located with managed infrastructure

The agent can run on a jumpbox and manage remote hosts. Example: one agent on a jumpbox manages a 3-node MySQL cluster via a `mysql-ctrl` sidecar that connects to remote hosts.

The sidecar protocol doesn't assume locality. The `target` (which host/endpoint to act on) is part of the task/sidecar payload — opaque to the agent.

Deployment topology (1:1 agent:host, or 1:N agent:cluster) is an operational decision, not an architectural constraint.

---

### DD-06: Credentials are not stored in the agent

The agent is not a credential store. Per-task, the CP tells the agent WHERE to get credentials:
- **HTTP provider**: a URL the agent calls to fetch credentials before execution
- **Sidecar provider**: a credential-provider sidecar (e.g., `vault-ctrl`) that the agent calls

Credential lifecycle (rotation, caching, revocation) is the provider's concern. The agent fetches, injects into the task execution context, and discards.

---

### DD-07: Tasks are one-shot OR long-running (continuous)

Two task lifecycle models:
- **One-shot**: Execute, capture output, report result, done.
- **Continuous**: Execute repeatedly at a defined interval (e.g., "report MySQL status every 30s") until the CP sends a cancel/stop signal.

Continuous tasks report on **every tick**, not on state change. Rationale: the CP needs raw data to detect trends (e.g., GTID drift) and act before a hard state transition occurs. Waiting for state change loses early-warning capability.

Both must be cancellable by the CP.

---

### DD-07a: Task chaining with rollback

The CP can dispatch a **task chain** — a sequential list of steps executed in order:
- Steps execute strictly sequentially (no parallel execution on a single chain).
- Each step MAY define a rollback action.
- On first step failure: abort the chain, execute the failed step's rollback, report failure to CP.
- No parallel step execution — avoids indeterminate states when one succeeds and another fails.

Rationale: At 10K hosts, round-tripping to the CP between every step creates too much chatter. A chain reduces this to one dispatch, one result. The CP still controls the plan — it just sends the full plan at once.

---

### DD-07b: Task type is an explicit discriminator field

Tasks carry an explicit `type` field. No implicit detection via field presence.

Types:
- `exec` — execute something (shell or sidecar call)
- `cancel` — stop a running task
- `install` — install a managed unit
- `upgrade` — upgrade a managed unit
- `remove` — remove a managed unit
- `self_upgrade` — upgrade the agent itself

Each type has its own required/optional field set, validated at receipt.

---

### DD-07c: Credential resolution is per-task, mode defined in task spec

Two resolution modes, choosable per credential in the task:
- **Eager (agent-resolved)**: Agent fetches credentials before execution begins, injects them into the execution context.
- **Lazy (sidecar-resolved)**: Agent passes the credential provider reference to the sidecar. Sidecar fetches on its own.

The task spec declares which mode applies per credential entry. Both are valid depending on whether the sidecar has network access to the credential provider.

---

### DD-08: Self-upgrade is a first-class feature

Pattern: download new binary, verify SHA-256, write state file with upgrade metadata, replace binary (atomic rename), restart. New instance reads state file, reports result, deletes file. Rollback if new binary doesn't become healthy within timeout.

In Rust: fast restart (<100ms), no JVM warmup.

---

### DD-09: Structured audit trail

Every significant event (task received, task started, task completed, sidecar installed, upgrade attempted, credential fetched, etc.) is logged to a local append-only store (SQLite) and forwarded to the CP via batched HTTP POST.

Local store survives agent restarts. Forwarding is best-effort with retry/backoff.

---

### DD-10: Sidecar IPC is Unix domain socket + length-prefixed JSON-RPC 2.0

From the original Garuda proposal — still valid:
- Unix socket: no port management, no network auth, local only
- 4-byte big-endian length prefix: unambiguous framing
- JSON-RPC 2.0: language-agnostic, testable with `socat`, no schema compilation

Every sidecar exposes a socket at a known path. The agent connects per-request (no persistent connection — Unix socket connect is ~50us).

---

### DD-11: Observation is just a continuous exec task to a sidecar

No special observation mechanism in the agent core. Observing infrastructure = a continuous task with `executor: "sidecar"`, `method: "submit"`, at a configured `interval_ms`.

The CP schedules observation by dispatching a continuous task. Cancels it when it no longer wants reports. The agent treats it identically to any other continuous task.

---

### DD-12: Identity and auth — one-time key bootstrap + grant/refresh token flow

**Bootstrap**:
- PXE coordinator (human) places a one-time key in `/etc/<agent_name>/onetimeauth.toml`
- On first start, agent presents this key to the auth provider
- Auth provider validates, issues a refresh token (and access token)
- One-time key is invalidated after use (single-use)

**Ongoing auth**:
- Agent uses access token for all CP communication
- When access token expires, agent uses refresh token to obtain a new one
- Standard grant/refresh token flow (similar to OAuth2 client_credentials + refresh)

**Auth provider is pluggable**:
- The agent doesn't hardcode an auth implementation
- Auth provider is anything that implements the token contract (issue token from one-time key, refresh token)
- CP can be the auth provider, or it can be a separate service (Keycloak, Vault, custom)
- Configured in agent config: endpoint URL + provider type

```toml
# /etc/<agent_name>/onetimeauth.toml (written by PXE coordinator, single-use)
[bootstrap]
key = "a1b2c3d4-one-time-use-uuid"
auth_endpoint = "https://auth.internal/v1/token"
```

---

### DD-13: Config format is TOML

Agent config, bootstrap config, and all agent-local configuration files use TOML.

Rationale: human-readable, well-specified, strong Rust ecosystem support (`toml` crate), less ambiguous than YAML.

---

### DD-14: Sidecar protocol is hybrid sync/async — sidecar decides

The sidecar protocol has four methods: `submit`, `status`, `cancel`, `health`.

**`submit` is the single entry point for all work.** The sidecar responds in one of two ways:
- **Synchronous** (common case): returns full result immediately. Sidecar is stateless. No task state stored.
- **Asynchronous** (long-running): returns `{ status: "accepted", task_id: "..." }`. Agent polls `status` until terminal state.

The sidecar decides the mode — not the agent. Agent reacts to whatever comes back.

This means:
- Simple sidecars (SQL, HTTP, health checks) are pure request/response. No state management. Trivial to implement.
- Complex sidecars (backup, migration) opt into async by returning `accepted`. They manage their own task state.
- The agent logic is uniform: call `submit`, if complete → done, if accepted → poll.

**Protocol methods:**

| Method | When called | Purpose |
|---|---|---|
| `submit(params)` | On every task dispatch to sidecar | Submit work. Returns result OR accepted. |
| `status(task_id)` | Only after `submit` returned `accepted` | Poll for async task status. |
| `cancel(task_id)` | Only for async tasks, on CP cancel or timeout | Abort in-progress work. |
| `health()` | Unit manager health checks (independent of tasks) | Sidecar liveness. |

**Response states from `submit` and `status`:**

| State | Meaning | Terminal? |
|---|---|---|
| `accepted` | Work started, poll me (only from `submit`) | No |
| `running` | In progress (only from `status`) | No |
| `completed` | Done, success | Yes |
| `failed` | Done, error | Yes |
| `cancelled` | Aborted | Yes |

---

### DD-15: Sidecar protocol — poll backoff, message limits, error codes

**Poll backoff for async tasks:**

```
Poll 1:  immediate (0ms after submit response)
Poll 2:  100ms
Poll 3:  200ms
Poll 4:  500ms
Poll 5+: 1s (capped)
```

Immediate first poll catches tasks that finish quickly. 1s cap avoids hammering long-running tasks.
If task `timeout_ms` is exceeded, agent stops polling and calls `cancel`.

**Max message size: 4MB (4,194,304 bytes).**

- 4-byte length prefix technically supports up to 4GB; agent enforces 4MB limit.
- Covers realistic output sizes (large query results, log dumps, config files).
- Sidecar MUST truncate output exceeding this and set `"truncated": true` in the response.
- Agent rejects incoming messages exceeding 4MB with error -32600.

**Error codes:**

Standard JSON-RPC 2.0:

| Code | Meaning |
|---|---|
| -32700 | Parse error (malformed JSON) |
| -32600 | Invalid request (missing fields, message too large) |
| -32601 | Method not found |
| -32602 | Invalid params |
| -32603 | Internal error (unexpected sidecar failure) |

Application-specific:

| Code | Meaning |
|---|---|
| -32000 | Execution timeout |
| -32001 | Task not found (invalid task_id on status/cancel) |
| -32002 | Task already cancelled |
| -32003 | Credential error (missing, rejected, expired) |
| -32004 | Target unreachable (sidecar can't reach remote host/service) |
| -32005 | Capacity exceeded (sidecar overloaded, can't accept more work) |

Agent maps these to appropriate `TaskResult.status` values when reporting to CP.

---

### DD-16: eBPF observer as a reference sidecar

An `ebpf-observer` sidecar is a project-shipped reference sidecar for passive, kernel-level observation.

**What it does:**
- Loads eBPF programs and attaches to kernel tracepoints, kprobes, XDP hooks
- Exposes kernel-level metrics via the standard sidecar protocol (submit/status/health)
- Observes infrastructure without touching or querying the managed software

**Why a sidecar (not built-in):**
- Requires elevated privileges (CAP_BPF, CAP_PERFMON) — separate process limits blast radius
- Not every deployment needs it — opt-in
- eBPF programs evolve with kernel versions — independent upgrade cycle
- Agent itself runs with minimal privileges

**Implementation:**
- Rust + `aya` crate (pure-Rust eBPF, no BCC/LLVM runtime dependency)
- Statically compiled, small binary
- Ships with a set of built-in probes (TCP retransmit, block I/O latency, OOM, connection rate)
- Extensible: CP can reference additional probe definitions shipped with the sidecar

**Example probes:**

| Probe | eBPF hook | Detects |
|---|---|---|
| `tcp_retransmit` | `tcp_retransmit_skb` kprobe | Network issues before app reports unhealthy |
| `blk_io_latency` | block layer tracepoints | Storage degradation before queries timeout |
| `oom_kill` | `oom_kill_process` tracepoint | Immediate OOM notification |
| `connection_rate` | `tcp_v4_connect` / XDP | DDoS or thundering herd |
| `syscall_errors` | `sys_exit` tracepoint | Application misbehavior at kernel boundary |
| `file_access` | `security_file_open` LSM hook | Security anomaly detection |

**Interaction model:**
- CP dispatches a continuous task targeting `ebpf-observer` sidecar
- On each tick, agent calls `submit` with desired probe snapshot
- Sidecar returns current kernel-level metrics (synchronous — eBPF map reads are instant)
- Agent reports to CP

This gives the CP kernel-level telemetry with zero instrumentation of managed software.

---

### DD-17: Unit management — no manifest push, lifecycle via tasks, status via heartbeat

**How the CP manages units:**
- No separate manifest channel. The CP uses existing task types (`install`, `upgrade`, `remove`) to manage units.
- The "manifest" is the CP's internal model. The agent doesn't see or store a full manifest.
- When the CP wants a sidecar installed, it sends an `install` task. Upgrade → `upgrade` task. Remove → `remove` task.

**How the agent reports unit status:**
- Included in every heartbeat payload. No separate reporting channel.

```
Heartbeat {
  agent_id:       string,
  agent_version:  string,
  uptime_sec:     u64,
  timestamp:      u64,

  units: [
    { name: "mysql-ctrl", kind: "sidecar", version: "1.2.0",
      state: "running", health: "ok", pid: 4521 },
    { name: "backup-ctrl", kind: "sidecar", version: "0.9.1",
      state: "running", health: "degraded", pid: 4580,
      health_error: "disk space low" },
    { name: "mariadb-server", kind: "package", version: "10.6.12",
      state: "installed" },
  ],

  active_tasks: {
    oneshot:    2,
    continuous: 3,
    total:      5,
  }
}
```

The CP diffs heartbeat reports against its internal desired state and sends lifecycle tasks as needed.

**Health checks:**
- **Sidecars**: agent calls the protocol-mandated `health()` method. Standard, automatic.
- **Packages (non-sidecar units)**: the `install` task MAY include an optional health check command (shell command, exit 0 = healthy). Agent runs it periodically and reports result in heartbeat.
- If no health check is specified for a package, agent reports state as `installed` (no health opinion).

---

### DD-18: Agent config — TOML, hot-reloadable, config_reload signal

**Config file**: `/etc/<agent_name>/config.toml`

```toml
[agent]
id = "agent-uuid-here"           # assigned after registration, persisted
name = "db-prod-blr1-042"        # human-readable identifier
data_dir = "/var/lib/agentd"     # state store, task journal, audit log
socket_dir = "/run/agentd"       # sidecar Unix sockets
log_level = "info"               # trace, debug, info, warn, error

[transport]
mode = "grpc"                    # "grpc" or "poll"

[transport.poll]
endpoint = "https://cp.internal/api/v1"
interval_sec = 10

[transport.grpc]
endpoint = "https://cp.internal:9090"
reconnect_interval_sec = 5

[heartbeat]
interval_sec = 10
endpoint = "https://cp.internal/api/v1/heartbeat"

[audit]
endpoint = "https://cp.internal/api/v1/audit"
batch_size = 50
flush_interval_sec = 5

[auth]
provider = "https://auth.internal/v1/token"
token_file = "/var/lib/agentd/token.json"     # persisted refresh token

[shell]
default_timeout_ms = 300000      # 5 min
max_concurrent = 4
working_dir = "/tmp/agentd"

[units]
artifact_cache_dir = "/var/cache/agentd"
health_check_interval_sec = 30
```

**Not in the config file:**
- Credentials (per-task from CP)
- Unit manifest (CP manages via tasks)
- One-time bootstrap key (separate file: `onetimeauth.toml`, single-use)

**Hot-reloadable fields** (applied on `config_reload` signal without restart):
- `agent.log_level`
- `shell.max_concurrent`
- `heartbeat.interval_sec`
- `units.health_check_interval_sec`
- `audit.batch_size`, `audit.flush_interval_sec`

**Restart-required fields:**
- `transport.mode`
- `transport.*.endpoint`
- `agent.data_dir`, `agent.socket_dir`
- `auth.provider`

**Config reload mechanism:**
- Agent supports a `config_reload` signal (Unix signal, e.g., SIGHUP) to re-read the config file and apply hot-reloadable fields without restart.
- CP can also trigger reload by sending a task (shell task that sends the signal, or a dedicated mechanism).
- On reload: agent logs which fields changed, applies hot-reloadable ones, warns if restart-required fields changed (but does not restart itself).

---

## Architecture Component Map

```
Agent Core (Rust, single static binary):
  +-- Transport (gRPC stream / HTTP poll -- configurable)
  +-- Task Router
  |     +-- Shell Executor (built-in, the only built-in)
  |     +-- Sidecar Dispatcher (Unix socket IPC to named sidecar)
  +-- Unit Manager (lifecycle: install, start, health, upgrade, rollback)
  |     +-- manages sidecars (units with IPC)
  |     +-- manages packages (units without IPC)
  +-- Observation Loop (periodic sidecar health/discover, report to CP)
  +-- Credential Resolver (fetch from HTTP or sidecar provider per-task)
  +-- State Store (SQLite -- managed units, task journal, audit log)
  +-- Self-Upgrade Module
  +-- Audit Forwarder (batched HTTP POST to CP)
  +-- Heartbeat Sender (periodic HTTP POST)
```

---

## Task DSL — Working Design

### Task Envelope (common to all types)

```
Task {
  id:          uuid,
  type:        "exec" | "cancel" | "install" | "upgrade" | "remove" | "self_upgrade",
  priority:    "critical" | "high" | "normal" | "low",
  tags:        map<string, string>,       // opaque CP metadata, agent passes through in reports
}
```

### type: "exec" (one-shot)

```
ExecTask {
  ...Task,
  kind:        "oneshot",
  executor:    "shell" | "sidecar",
  target:      string,                    // sidecar name (ignored for shell)
  method:      string,                    // JSON-RPC method (ignored for shell)
  payload:     object,                    // forwarded to executor — script text, params, etc.
  timeout_ms:  u64,
  credentials: [CredentialRef],           // see below
}
```

### type: "exec" (continuous)

```
ContinuousExecTask {
  ...Task,
  kind:        "continuous",
  executor:    "shell" | "sidecar",
  target:      string,
  method:      string,
  payload:     object,
  interval_ms: u64,                       // how often to execute
  timeout_ms:  u64,                       // per-tick execution timeout
  credentials: [CredentialRef],
}
```

Agent reports result on **every tick**. CP cancels when it no longer wants reports.

### type: "cancel"

```
CancelTask {
  ...Task,
  target_task_id: uuid,                   // which running task to stop
}
```

### type: "install"

```
InstallTask {
  ...Task,
  unit: ManagedUnit,                      // full unit spec (name, kind, version, artifact, sha256, etc.)
}
```

### type: "upgrade"

```
UpgradeTask {
  ...Task,
  unit_name:       string,
  target_version:  string,
  artifact:        { url: string, sha256: string },
  rollback:        { version: string, artifact: { url, sha256 } },
}
```

### type: "remove"

```
RemoveTask {
  ...Task,
  unit_name: string,
  purge:     bool,                        // remove config/data too, or just the binary?
}
```

### type: "self_upgrade"

```
SelfUpgradeTask {
  ...Task,
  target_version:  string,
  artifact:        { url: string, sha256: string },
  rollback:        { version: string, artifact: { url, sha256 } },
}
```

### Task Chain (sequential execution with rollback)

```
TaskChain {
  id:    uuid,
  steps: [
    {
      task:     ExecTask,                 // the action to perform
      rollback: ExecTask | null,          // action to undo on failure (optional)
    },
  ],
  on_failure: "rollback_failed" | "rollback_all",
  tags: map<string, string>,
}
```

- `rollback_failed`: On step N failure, rollback only step N, then abort.
- `rollback_all` **(default)**: On step N failure, rollback step N, then N-1, ..., then 1 (reverse order).

CP decides the strategy per chain. Agent honours it. If not specified, `rollback_all`.

### Credential Reference

```
CredentialRef {
  name:     string,                       // logical name, injected as key into execution context
  resolve:  "eager" | "lazy",

  // For eager (agent fetches before execution):
  provider: "http" | "sidecar",
  endpoint: string,                       // URL for http, sidecar name for sidecar
  method:   string,                       // sidecar JSON-RPC method (if provider=sidecar)
  params:   object,                       // additional params for the fetch

  // For lazy (passed through to sidecar, sidecar fetches itself):
  // Agent forwards this entire CredentialRef to the sidecar in the request.
  // Sidecar is responsible for resolution.
}
```

### Task Result (agent reports to CP)

```
TaskResult {
  task_id:      uuid,
  chain_id:     uuid | null,              // if part of a chain
  step_index:   u32 | null,              // which step in chain (0-indexed)
  status:       "success" | "failed" | "cancelled" | "timeout" | "rolled_back",
  exit_code:    i32 | null,              // for shell tasks
  stdout:       string,
  stderr:       string,
  duration_ms:  u64,
  timestamp:    u64,                     // epoch ms
  error:        string | null,           // human-readable error if failed
}
```

For continuous tasks, the agent sends a `TaskResult` on every tick with the same `task_id`.

---

## Project Structure

See `docs/plan.md` for full project layout and implementation phases.

**Project name**: Vasal
**Binary**: `vasal`
**Workspace crates**: `vasal-core`, `vasal-protocol`, `vasal-sidecar-sdk`
**Reference sidecars**: `sql-ctrl`, `ebpf-observer`, `echo-ctrl` (test harness)
**Config**: TOML, at `/etc/vasal/config.toml`
**Data**: `/var/lib/vasal/`
**Sockets**: `/run/vasal/`

---

## Context / Background

This design is informed by — but intentionally unconstrained by — the Garuda Agent proposal (see `notes/proposal.md`). That proposal was scoped to Java + Dropwizard, tied to PhonePe internal systems. This project is:
- Rust (no runtime, minimal footprint)
- Protocol-first (any CP can speak to it)
- OSS from day one
- General-purpose (no opinions about MySQL, Redis, etc.)

The Garuda proposal's patterns (sidecar IPC over Unix socket, self-upgrade via state file handoff, PXE bootstrap) are valid and carry over as implementation ideas. The framing is different: not "fix an internal Java agent" but "build a general-purpose primitive."
