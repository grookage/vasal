# Vasal — System Architecture

## What this is

A single-binary daemon that sits on a host, pulls work from a control plane,
executes it, and reports back. No HTTP server. No inbound ports. The agent is
always the client.

```
                        ┌─────────────────────┐
                        │    Control Plane     │
                        │  (not in this repo)  │
                        └──────┬──────▲───────┘
                               │      │
                    tasks down  │      │  results, heartbeats, audit up
                    (pull/push) │      │  (always HTTP POST or gRPC stream)
                               │      │
                        ┌──────▼──────┴───────┐
                        │       vasal         │
                        │   (this binary)     │
                        └──┬──────┬──────┬────┘
                           │      │      │
                    ┌──────▼┐  ┌──▼───┐  ▼
                    │ shell ││ sidecar│  SQLite
                    │/bin/sh││  IPC   │  (state)
                    └───────┘│ (UDS)  │
                             └───┬────┘
                          ┌──────▼──────┐
                          │  sql-ctrl   │
                          │  echo-ctrl  │
                          │  ebpf-obs   │
                          │  (any proc) │
                          └─────────────┘
```

That's it. No service mesh, no sidecar injection, no orchestrator.
One binary, one config file, one SQLite database.

---

## The main loop

Everything flows through a single `tokio::select!` in `main.rs:run()`:

```
loop {
    select! {
        shutdown         => break
        result from task => forward to CP via transport
        work from CP     => dispatch to TaskManager
    }
}
```

Three branches, biased in that order. Shutdown always wins. Results get
forwarded before new work is accepted (backpressure-friendly).

The transport and the task executors never talk to each other directly.
They communicate through an `mpsc::channel<TaskResult>(256)`:

```
  Transport                  TaskManager                  Executors
  ─────────                  ───────────                  ─────────
  recv_tasks() ────────────► submit(task) ──spawn──────► shell / sidecar
                                                              │
  send_result() ◄──── mpsc channel ◄── result_tx.send() ◄────┘
```

The channel is bounded at 256. If the CP is slow to accept results, executors
block. This is intentional — it prevents the agent from accumulating unbounded
state during a network partition. Results are also written to the local journal
(SQLite) before being sent, so nothing is lost.

---

## How a task executes

### Shell task

```
CP dispatches:
  { type: "exec", executor: "shell", payload: { script: "df -h" }, timeout_ms: 5000 }

Agent:
  1. TaskManager.submit() — records audit event "task.received"
  2. Acquires semaphore permit (max_concurrent, default 4)
  3. Resolves eager credentials → injects as env vars
  4. Spawns /bin/sh -c "df -h" via tokio::process::Command
  5. Races: child.wait() vs timeout vs cancellation token
  6. Captures stdout + stderr (in memory, not streamed)
  7. Builds TaskResult { status, exit_code, stdout, stderr, duration_ms }
  8. Writes to task_journal (SQLite)
  9. Writes audit event "task.completed" or "task.failed"
  10. Sends result through mpsc channel → main loop → transport.send_result()
```

If timeout fires first: `child.start_kill()` + `child.wait()`. No SIGTERM
grace period — kill immediately. The task result gets `status: timeout`.

If cancellation fires (CP sent a cancel task): same kill path, `status: cancelled`.

### Sidecar task

```
CP dispatches:
  { type: "exec", executor: "sidecar", target: "sql-ctrl", method: "submit",
    payload: { action: "query", sql: "SELECT 1" } }

Agent:
  1. TaskManager.submit() — same as above
  2. Resolves eager credentials
  3. Connects to /run/vasal/sql-ctrl.sock (Unix domain socket)
  4. Sends: [4-byte BE length][JSON-RPC 2.0 request]
  5. Reads: [4-byte BE length][JSON-RPC 2.0 response]
  6. If response is "completed" → done
     If response is "accepted" → poll status with backoff (0, 100, 200, 500, 1000ms cap)
  7. Same result → journal → audit → channel path as shell
```

Connection is per-request. No persistent socket. Unix socket connect is ~50μs.
4MB max message size, enforced on both sides.

### Task chain

```
CP dispatches:
  { id: "chain-1", steps: [
      { task: { script: "mkdir /data" },      rollback: { script: "rm -rf /data" } },
      { task: { script: "mount /dev/sdb1" },  rollback: { script: "umount /data" } },
      { task: { script: "chown mysql /data" }, rollback: null },
  ], on_failure: "rollback_all" }

Agent:
  Step 0: mkdir  → success → continue
  Step 1: mount  → FAIL
  Rollback step 1: umount  → execute
  Rollback step 0: rm -rf  → execute
  Report: [result0: success, result1: failed]
```

Chain gets ONE semaphore permit for the whole chain (not per-step).
Steps run strictly sequential. No parallelism within a chain.

`rollback_all` rolls back in reverse order (N, N-1, ..., 0).
`rollback_failed` rolls back only the failed step, then aborts.

---

## Transport

Two modes, selected by config. The agent doesn't care which one is active —
both feed into the same `TaskManager`.

### Poll (HTTP)

```
every 10s:
  GET  {endpoint}/tasks/pending  → [Task, Task, ...]
  POST {endpoint}/tasks/result   ← TaskResult
```

Simple. Works through firewalls, NAT, proxies. The 10s interval means task
dispatch latency is 0-10s. Fine for most workloads.

### gRPC (bidirectional stream)

```
agent opens stream → CP pushes tasks in real-time
agent sends results, heartbeats back through same stream
reconnects with exponential backoff (1s → 2s → 4s → ... → 30s cap)
```

Sub-second dispatch latency. The stream uses `tonic` with JSON-encoded bytes
inside protobuf envelopes — proto is the transport frame, serde types are the
source of truth. No `.proto` files for task/result types; they live in
`vasal-protocol` as Rust structs.

The gRPC transport wraps `tonic::Streaming<T>` in a `tokio::sync::Mutex`
because `Streaming` is `Send` but not `Sync`. This is fine — the mutex is
only held during message reads.

---

## Background subsystems

Four things run concurrently alongside the main loop, all as `tokio::spawn`
tasks gated by the same `CancellationToken`:

### Heartbeat sender (`heartbeat.rs`)

```
every {heartbeat.interval_sec}:
  POST {heartbeat.endpoint}
  body: { agent_id, version, uptime, units: [...], active_tasks: { oneshot, continuous } }
```

The CP diffs this against its desired state. If a sidecar should be installed
but isn't in the heartbeat, the CP sends an install task. The agent never
reconciles on its own.

Heartbeat failure is logged and ignored. It does not affect task execution.

### Audit forwarder (`audit.rs`)

Every event (task received, completed, failed, cancelled, unit installed,
credential fetched, agent started, agent shutdown) gets a row in the local
`audit_log` SQLite table. The forwarder periodically batches unforwarded rows
and POSTs them to the CP.

```
every {audit.flush_interval_sec}:
  SELECT * FROM audit_log WHERE forwarded = 0 LIMIT {batch_size}
  POST {audit.endpoint} ← [event, event, ...]
  UPDATE audit_log SET forwarded = 1 WHERE id IN (...)
```

On failure: exponential backoff (1s → 2s → ... → 60s cap). Events are never
lost — they accumulate locally until the CP is reachable. On shutdown, one
final flush attempt is made.

### Unit health checker (`unit/health.rs`)

```
every {units.health_check_interval_sec}:
  for each unit in state store where state = "running" or "installed":
    if sidecar → call health() over UDS
    if package  → run health_check.command via shell (if configured)
    if health changed → update state store → reflected in next heartbeat
```

### Task counts watcher

Not a separate task — integrated into `TaskManager`. A `watch::channel` tracks
`{ oneshot: N, continuous: M, total: N+M }`. The heartbeat sender subscribes.
Updated on every task start/finish.

---

## State

One SQLite database at `{data_dir}/state.db`. WAL mode. Three tables:

```sql
units          — name (PK), kind, version, state, health, pid, socket_path, config_json
task_journal   — task_id, chain_id, step_index, status, exit_code, stdout, stderr, duration_ms
audit_log      — timestamp, event_type, task_id, detail_json, forwarded (bool)
```

Access pattern: `Arc<Mutex<Connection>>` + `spawn_blocking` from async code.
No connection pool — one connection, one mutex. SQLite handles concurrency
via WAL. Writes are fast (sub-ms for single rows).

The task journal is a ring buffer — prune to keep last N entries. The audit
log grows until forwarded, then stays (for forensics). No automatic cleanup
of forwarded events yet.

---

## Sidecar protocol

Any process that listens on a Unix socket and speaks this wire format is a
valid sidecar. No Rust required.

```
Wire format:
  [4 bytes: big-endian payload length][JSON-RPC 2.0 payload]

Methods:
  health()              → { status: "ok"|"degraded"|"unhealthy", version, error? }
  submit(params)        → { status: "completed", stdout, stderr }     (sync)
                       OR { status: "accepted", task_id }             (async)
  status(task_id)       → { status: "running"|"completed"|"failed"|"cancelled", ... }
  cancel(task_id)       → { cancelled: true }
```

Sync sidecars implement `health` + `submit`. That's it. Two methods.
Async sidecars additionally implement `status` + `cancel`.

The agent doesn't care which mode the sidecar uses. It calls `submit`,
looks at the response, and either reports the result or starts polling.

Error codes are standard JSON-RPC 2.0 (-32700 through -32603) plus
application codes (-32000 through -32005) for timeout, not found, etc.

---

## Credential flow

Credentials are never stored. They're resolved per-task, injected, discarded.

```
Two modes per credential (declared in task spec):

  Eager:  agent fetches BEFORE execution
          → HTTP GET to credential endpoint
          → or JSON-RPC call to a credential-provider sidecar
          → inject as env var (shell) or request param (sidecar)

  Lazy:   agent passes the CredentialRef to the sidecar as-is
          → sidecar fetches on its own
          → used when sidecar has network access to the provider
```

Why per-task and not cached: credentials may differ between tasks (different
DB users, different service accounts). Caching introduces staleness and
invalidation complexity. The credential provider handles caching if it wants to.

---

## Auth

Bootstrap → token refresh. Standard pattern.

```
First boot:
  1. Read /etc/vasal/onetimeauth.toml  (placed by provisioning system)
  2. POST one-time key to auth endpoint
  3. Receive access_token + refresh_token
  4. Persist to {token_file}
  5. Delete one-time key file

Ongoing:
  - Inject Authorization: Bearer {access_token} into CP requests
  - Refresh before expiry using refresh_token

No auth:
  - If no token file and no bootstrap key → run unauthenticated
  - Logged as warning, not fatal
```

Auth is optional. The agent works without it. This matters for dev/test
where you don't want to stand up an auth provider.

---

## Concurrency model

```
Tokio multi-threaded runtime (default: 1 thread per core)

Concurrency limits:
  - shell.max_concurrent (default 4) — semaphore-gated
  - Chains hold ONE permit for their entire duration
  - No limit on concurrent sidecar IPC calls (they're fast, ~ms)
  - Background tasks (heartbeat, audit, health) run independently

Cancellation:
  - Global CancellationToken for shutdown
  - Per-task CancellationToken for cancel requests
  - child_token() for chain steps (cancel chain → cancel current step)
```

No thread pool for shell execution. Shell tasks run on `tokio::process::Command`
which spawns real OS processes. The semaphore prevents fork-bombing the host.

---

## Config reload

```
SIGHUP → re-read config.toml → apply hot-reloadable fields:
  - log_level
  - max_concurrent
  - heartbeat_interval_sec
  - health_check_interval_sec
  - audit_batch_size, audit_flush_interval_sec

Fields that require restart (logged as warnings if changed):
  - transport.mode, transport.*.endpoint
  - agent.data_dir, agent.socket_dir
  - auth.provider
```

Hot-reloadable fields propagate through `watch::channel<RuntimeConfig>`.
Background tasks subscribe and pick up changes on their next loop iteration.
No lock contention — watch channels are lock-free reads.

---

## Deployment

```
Package:  .deb via cargo-deb
Binary:   /usr/bin/vasal
Config:   /etc/vasal/config.toml          (conffile — preserved across upgrades)
State:    /var/lib/vasal/state.db
Sockets:  /run/vasal/*.sock
Cache:    /var/cache/vasal/               (downloaded artifacts)
Service:  vasal.service (systemd)
User:     vasal:vasal (system, nologin)

Systemd hardening:
  NoNewPrivileges=true
  ProtectSystem=strict
  ProtectHome=true
  PrivateTmp=true
  ReadWritePaths=/var/lib/vasal /run/vasal
```

The binary is statically linked (rusqlite bundled, TLS via rustls).
No runtime dependencies. Copy it anywhere and it runs.

Self-upgrade: download new binary → write pending-upgrade.json → atomic
rename over /usr/bin/vasal → systemd restarts → new process reads state
file → reports success → deletes state file. If the new binary doesn't
start, systemd's Restart=on-failure kicks in with the old binary (which
is gone, so this actually needs the rollback artifact — the self-upgrade
module handles this).

---

## What the CP needs to implement

Vasal is the agent half. The CP (not in this repo) needs:

```
Required endpoints (for poll mode):
  GET  /tasks/pending          → return JSON array of tasks for this agent
  POST /tasks/result           → accept TaskResult JSON
  POST /heartbeat              → accept Heartbeat JSON
  POST /audit                  → accept [AuditEvent, ...] JSON array

Required endpoints (for gRPC mode):
  AgentDispatch.TaskStream     → bidirectional stream (see dispatch.proto)

Optional:
  POST /auth/token             → bootstrap + refresh token flow
  GET  /artifacts/{sha256}     → serve unit artifacts for install/upgrade
```

The CP decides what to run, when, and on which agent. The agent is the
dumb executor. All intelligence lives in the CP.

---

## File map

Where to find things when you need to change them:

```
What you want to change                  Where it lives
─────────────────────────                ──────────────
Task types, wire format                  crates/vasal-protocol/src/task.rs
Sidecar protocol types                   crates/vasal-protocol/src/sidecar.rs
Error codes                              crates/vasal-protocol/src/error.rs
Sidecar SDK (socket server, framing)     crates/vasal-sidecar-sdk/src/
Agent main loop                          crates/vasal-core/src/main.rs
Task routing + dispatch                  crates/vasal-core/src/task/router.rs
Shell execution                          crates/vasal-core/src/task/shell.rs
Sidecar IPC client                       crates/vasal-core/src/task/sidecar.rs
Chain executor                           crates/vasal-core/src/task/chain.rs
Transport trait + poll + gRPC            crates/vasal-core/src/transport/
Config schema + hot-reload               crates/vasal-core/src/config.rs
SQLite state store                       crates/vasal-core/src/state.rs
gRPC protobuf definition                 proto/vasal/v1/dispatch.proto
Debian packaging                         crates/vasal-core/debian/
```

---

## Trade-offs made

| Decision | Upside | Downside |
|---|---|---|
| JSON over protobuf for task types | One source of truth (Rust structs), human-readable on wire, no proto compilation | ~2-5x larger payloads, slower parse. Doesn't matter at agent scale. |
| SQLite for state | Zero-dep, embedded, survives restarts, WAL gives concurrent reads | Single-writer. Fine for an agent's write volume (~10 writes/sec peak). |
| Per-request sidecar connections | No connection management, no stale connections, trivially correct | 50μs overhead per call. Negligible. |
| Bounded result channel (256) | Backpressure, bounded memory | Executors block if CP is unreachable and channel fills. Results are in journal though. |
| No streaming stdout | Simple capture model, bounded memory | Can't stream large outputs. 4MB max. Good enough for commands; not for `mysqldump`. |
| Shell is the only built-in executor | Agent stays small and generic | Can't do anything useful without a shell. Fine — every host has /bin/sh. |
| Auth is optional | Works in dev/test without auth infra | Easy to accidentally deploy unauthenticated in prod. Config lint can catch this. |
