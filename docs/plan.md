# Vasal — Implementation Plan

**Project**: Vasal — a lightweight, protocol-first, general-purpose host agent
**Language**: Rust
**Architecture**: See `docs/architecture.md`

---

## Project Structure

```
vasal/
├── Cargo.toml                      # workspace root
├── LICENSE
│
├── docs/
│   └── architecture.md             # design decisions (DD-01 through DD-18)
│
├── crates/
│   ├── vasal-core/                 # the agent binary
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── main.rs
│   │       ├── config.rs
│   │       ├── transport/
│   │       │   ├── mod.rs
│   │       │   ├── grpc.rs
│   │       │   └── poll.rs
│   │       ├── task/
│   │       │   ├── mod.rs
│   │       │   ├── router.rs
│   │       │   ├── shell.rs
│   │       │   ├── chain.rs
│   │       │   ├── continuous.rs
│   │       │   └── sidecar.rs
│   │       ├── unit/
│   │       │   ├── mod.rs
│   │       │   ├── install.rs
│   │       │   ├── upgrade.rs
│   │       │   └── health.rs
│   │       ├── auth/
│   │       │   ├── mod.rs
│   │       │   └── token.rs
│   │       ├── credential.rs
│   │       ├── heartbeat.rs
│   │       ├── audit.rs
│   │       ├── self_upgrade.rs
│   │       └── state.rs
│   │
│   ├── vasal-protocol/             # shared types — the protocol crate
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── task.rs
│   │       ├── sidecar.rs
│   │       ├── heartbeat.rs
│   │       ├── unit.rs
│   │       ├── credential.rs
│   │       └── error.rs
│   │
│   └── vasal-sidecar-sdk/          # SDK for sidecar authors (Rust)
│       ├── Cargo.toml
│       └── src/
│           ├── lib.rs
│           ├── server.rs
│           └── handler.rs
│
├── sidecars/
│   ├── echo-ctrl/                  # test sidecar — echo/mirror for integration tests
│   │   ├── Cargo.toml
│   │   └── src/
│   │       └── main.rs
│   ├── sql-ctrl/                   # reference: MySQL/Postgres sidecar
│   │   ├── Cargo.toml
│   │   └── src/
│   │       └── main.rs
│   └── ebpf-observer/              # reference: kernel-level observation
│       ├── Cargo.toml
│       └── src/
│           └── main.rs
│
└── proto/
    └── vasal/
        └── v1/
            ├── transport.proto
            └── types.proto
```

---

## Phases

### Phase 1: Foundation — Protocol Types + Sidecar SDK

**Goal**: Define every shared type. Build the sidecar SDK. Have a working test sidecar. This is the contract layer that everything else depends on.

**Crates**: `vasal-protocol`, `vasal-sidecar-sdk`, `sidecars/echo-ctrl`

**Work**:
- `vasal-protocol`:
  - Task types: `Task`, `ExecTask`, `ContinuousExecTask`, `CancelTask`, `InstallTask`, `UpgradeTask`, `RemoveTask`, `SelfUpgradeTask`
  - Task chain: `TaskChain`, `ChainStep`, `RollbackStrategy`
  - Task result: `TaskResult`
  - Sidecar IPC: `SubmitRequest`, `SubmitResponse` (sync + async variants), `StatusRequest`, `StatusResponse`, `CancelRequest`, `CancelResponse`, `HealthRequest`, `HealthResponse`
  - Heartbeat: `Heartbeat`, `UnitStatus`, `ActiveTasks`
  - Unit: `ManagedUnit`, `UnitKind`, `Artifact`, `HealthCheck`
  - Credential: `CredentialRef`, `ResolveMode`
  - Error: JSON-RPC error codes, `ProtocolError` type
  - All types derive `Serialize`, `Deserialize` (serde), `Debug`, `Clone`
- `vasal-sidecar-sdk`:
  - Unix socket listener (tokio `UnixListener`)
  - Length-prefixed framing: 4-byte big-endian length + JSON payload
  - JSON-RPC 2.0 request/response parsing
  - `SidecarHandler` trait with methods: `health`, `submit`, `status`, `cancel`
  - Max message size enforcement (4MB)
- `sidecars/echo-ctrl`:
  - Minimal sidecar using the SDK
  - `submit`: echoes back the payload as stdout (synchronous)
  - `health`: returns ok + version
  - Used as test harness in later phases

**Tests**:
- Protocol types: serialization round-trips (JSON), validation of required fields, edge cases (empty payload, max-size payload)
- Sidecar SDK: start socket server, connect as client, send submit, receive response, verify framing correctness, message size rejection
- echo-ctrl: end-to-end test — start echo-ctrl, connect, submit work, verify echo response

**Review gate**: All protocol types are defined and serializable. SDK handles socket lifecycle and framing correctly. echo-ctrl works end-to-end.

---

### Phase 2: Agent Skeleton — Config, State Store, Signals

**Goal**: The agent binary boots, loads config, opens a SQLite state store, handles SIGHUP and SIGTERM. Does nothing useful yet — but the foundation is solid.

**Crate**: `vasal-core`

**Work**:
- `config.rs`:
  - Parse `/etc/vasal/config.toml` (or path from CLI arg)
  - Strongly typed config struct matching DD-18 schema
  - Hot-reload on SIGHUP: re-read file, apply hot-reloadable fields, log changes, warn on restart-required changes
- `state.rs`:
  - SQLite via `rusqlite`
  - Tables: `units` (managed units + status), `task_journal` (execution history, ring: keep last N), `audit_log` (append-only events)
  - Schema migrations embedded in binary (simple SQL files, run on startup)
  - WAL mode enabled
- `main.rs`:
  - CLI arg parsing (config path, version flag)
  - Logging via `tracing` + `tracing-subscriber`
  - Signal handling: SIGHUP → config reload, SIGTERM → graceful shutdown
  - Startup sequence: load config → open state store → (placeholder for later phases)

**Tests**:
- Config: parse valid TOML, reject invalid TOML, hot-reload changes correct fields, warns on restart-required changes
- State store: CRUD on units table, append + query audit log, task journal insert + ring cleanup
- Signal handling: send SIGHUP to process, verify config reload fires; SIGTERM triggers graceful shutdown

**Review gate**: Agent boots cleanly, loads config, opens state store, handles signals. `cargo run` starts and shuts down gracefully.

---

### Phase 3: Task Execution Engine

**Goal**: The agent can execute shell tasks, dispatch to sidecars, run task chains with rollback, run continuous tasks, and resolve credentials. This is the core of the agent.

**Crate**: `vasal-core` (modules: `task/`)

**Work**:
- `task/shell.rs`:
  - Shell executor via `tokio::process::Command`
  - Credentials injected as environment variables
  - Timeout enforcement (kill process on timeout)
  - Stdout/stderr capture with size limits
  - Working directory from config
- `task/sidecar.rs`:
  - Connect to sidecar Unix socket
  - Send length-prefixed JSON-RPC `submit` request
  - Handle sync response (completed/failed) and async response (accepted → poll status with backoff)
  - Cancel support
  - Timeout enforcement
- `task/router.rs`:
  - Accept a `Task`, route by `type` field
  - `exec` → shell or sidecar based on `executor` field
  - `cancel` → find running task, cancel it
  - Other types (`install`, `upgrade`, `remove`, `self_upgrade`) → placeholder, wired in Phase 5
- `task/chain.rs`:
  - Execute steps sequentially
  - On failure: execute rollback per `on_failure` strategy (`rollback_all` or `rollback_failed`)
  - Report per-step results
- `task/continuous.rs`:
  - Tick loop at `interval_ms`
  - Execute task on each tick
  - Report result on each tick
  - Cancellable via cancel task
- `credential.rs`:
  - Eager resolution: HTTP GET to credential endpoint, or submit to credential-provider sidecar
  - Lazy resolution: pass `CredentialRef` through to sidecar in request params
  - Inject resolved credentials into task execution context

**Tests**:
- Shell executor: run `echo hello`, verify stdout. Run `sleep 999` with 100ms timeout, verify kill. Verify credentials appear as env vars, NOT in process args.
- Sidecar dispatcher: start echo-ctrl from Phase 1, dispatch submit, verify response. Test async flow with a purpose-built slow-echo sidecar. Test cancel. Test timeout.
- Task router: dispatch each task type, verify correct routing.
- Chain execution: 3-step chain, all succeed. 3-step chain, step 2 fails, verify rollback_all (steps 2, 1 rolled back). Verify rollback_failed (only step 2 rolled back). Step with no rollback action defined.
- Continuous tasks: start continuous task, verify multiple ticks fire, cancel, verify it stops.
- Credential resolution: eager HTTP (mock HTTP server), eager sidecar (echo-ctrl), lazy passthrough.

**Review gate**: Full task execution pipeline works. Shell tasks run, sidecar dispatch works against echo-ctrl, chains roll back correctly, continuous tasks tick and cancel.

---

### Phase 4: Transport + Heartbeat + Audit

**Goal**: The agent can talk to a control plane — receive tasks (poll or stream), send heartbeats, forward audit events, report task results.

**Crate**: `vasal-core` (modules: `transport/`, `heartbeat.rs`, `audit.rs`)

**Work**:
- `transport/poll.rs`:
  - HTTP GET to fetch pending tasks at configured interval
  - HTTP POST to report task results
  - Uses `reqwest` with connection pooling
  - Auth header injection (bearer token from auth module — stubbed in this phase)
- `transport/grpc.rs`:
  - Bidirectional gRPC stream via `tonic`
  - Agent connects to CP, receives task stream, sends results
  - Reconnection on disconnect with backoff
- `transport/mod.rs`:
  - `Transport` trait abstracting poll vs stream
  - Selected by config `transport.mode`
  - Both feed tasks into the same task router from Phase 3
- `heartbeat.rs`:
  - Periodic HTTP POST at configured interval
  - Payload: agent version, uptime, unit statuses (from state store), active task counts
  - Failure: log + increment miss count, do NOT affect task execution
- `audit.rs`:
  - Append events to SQLite `audit_log` table
  - Forwarder: background task reads unforwarded events, batches them, HTTP POSTs to audit endpoint
  - Backoff on failure, retry, mark forwarded on success
  - Respects `batch_size` and `flush_interval_sec` from config
- `proto/vasal/v1/`:
  - `types.proto`: Task, TaskResult, Heartbeat message definitions
  - `transport.proto`: gRPC service definition (`TaskStream`, `ReportResult`)
  - Build script for `tonic-build` codegen

**Tests**:
- Poll transport: mock HTTP server, verify task fetch, result reporting, auth header presence
- gRPC transport: mock gRPC server (tonic), verify stream connection, task receipt, result send, reconnection on drop
- Heartbeat: mock endpoint, verify periodic POST, verify payload structure, verify continues on failure
- Audit: append events, verify batched forwarding, verify retry on failure, verify batch size respected
- Protobuf: verify generated code compiles, round-trip serialize/deserialize matches JSON protocol types

**Review gate**: Agent can receive tasks from a mock CP (both poll and gRPC), execute them, report results, send heartbeats, and forward audit events.

---

### Phase 5: Unit Management + Self-Upgrade

**Goal**: The agent can install, upgrade, and remove managed units (sidecars and packages). It can upgrade itself.

**Crate**: `vasal-core` (modules: `unit/`, `self_upgrade.rs`)

**Work**:
- `unit/install.rs`:
  - Download artifact (HTTP GET to artifact URL)
  - Verify SHA-256
  - Install: extract tarball or run `dpkg -i` / `rpm -i` (based on artifact type)
  - For sidecars: write systemd unit file, start service, probe `health()`
  - For packages: run optional health check command if provided
  - Persist unit to state store
- `unit/upgrade.rs`:
  - Download new artifact, verify SHA-256
  - Stop current version
  - Install new version
  - Start, health check
  - On health check failure: rollback (stop new, install old, start old, health check)
  - Update state store
- `unit/health.rs`:
  - Periodic health check loop
  - Sidecars: call `health()` over Unix socket
  - Packages: run health check shell command (if defined)
  - Update unit status in state store (reflected in heartbeat)
- `self_upgrade.rs`:
  - Download new agent binary, verify SHA-256
  - Write `pending-upgrade.json` state file to data_dir
  - Replace binary via atomic rename
  - Restart (exec self, or rely on systemd restart)
  - On startup: check for `pending-upgrade.json`, report result to CP, delete file
  - Rollback: if new binary doesn't start within timeout, upgrade script restores previous
- Wire `install`, `upgrade`, `remove`, `self_upgrade` task types into task router

**Tests**:
- Unit install: mock artifact HTTP server, download + verify SHA, install sidecar (echo-ctrl binary), verify health check passes, verify state store updated
- Unit upgrade: install v1, upgrade to v2, verify new version running, verify rollback on bad binary (binary that fails health check)
- Unit remove: install, remove, verify stopped and state store cleaned
- Self-upgrade: simulate upgrade by writing state file, restarting with flag, verify state file consumed and result reported. Test rollback path.
- Health check loop: install echo-ctrl, verify periodic health status in state store. Kill echo-ctrl, verify status transitions to unhealthy.

**Review gate**: Full unit lifecycle works. Self-upgrade with rollback works. Health checks run and update heartbeat payload.

---

### Phase 6: Auth + Bootstrap

**Goal**: The agent can bootstrap from a one-time key, obtain tokens, and authenticate all CP communication.

**Crate**: `vasal-core` (module: `auth/`)

**Work**:
- `auth/token.rs`:
  - Token store: access token + refresh token, persisted to `token_file`
  - Auto-refresh: background task refreshes access token before expiry
  - Token injection: middleware/hook that adds `Authorization: Bearer <token>` to all CP HTTP requests and gRPC metadata
- `auth/mod.rs`:
  - Bootstrap flow: read `onetimeauth.toml`, POST one-time key to auth endpoint, receive token pair, persist, delete (or invalidate) one-time key file
  - Normal flow: load persisted tokens, start auto-refresh
  - Pluggable: auth endpoint URL from config, agent doesn't care what's behind it
- Integrate auth with:
  - Transport (poll HTTP, gRPC metadata)
  - Heartbeat sender
  - Audit forwarder
  - Credential resolver (HTTP provider calls)
  - Unit manager (artifact downloads)

**Tests**:
- Bootstrap: mock auth endpoint, provide one-time key, verify token pair received and persisted, verify one-time key file is cleaned up
- Token refresh: mock auth endpoint, issue short-lived access token, verify auto-refresh fires before expiry
- Token injection: verify all outgoing HTTP requests carry bearer token
- Expired token: verify refresh triggers on 401 response, retries original request with new token
- Invalid one-time key: verify agent exits with clear error (not a crash loop)

**Review gate**: Agent bootstraps from one-time key, authenticates all communication, auto-refreshes tokens. Invalid bootstrap fails cleanly.

---

### Phase 7: Reference Sidecars

**Goal**: Ship two production-quality reference sidecars that demonstrate the protocol and are genuinely useful.

**Crates**: `sidecars/sql-ctrl`, `sidecars/ebpf-observer`

**Work**:
- `sidecars/sql-ctrl`:
  - Built on `vasal-sidecar-sdk`
  - MySQL support via `sqlx` (async, compile-time checked queries not required — dynamic SQL)
  - PostgreSQL support via `sqlx` (same binary, selected by `params.driver` field)
  - `submit` (action: `query`): connect using provided credentials, execute SQL, return result as stdout (synchronous)
  - `submit` (action: `discover`): run preset discovery queries (SHOW SLAVE STATUS, cluster status, etc.), return structured JSON
  - `health`: verify sidecar process is up, optionally ping DB
  - Connection per-request (no pool — credentials come per-request, pooling doesn't make sense)
- `sidecars/ebpf-observer`:
  - Built on `vasal-sidecar-sdk` + `aya` crate
  - Ships with built-in probes: tcp_retransmit, blk_io_latency, oom_kill
  - `submit` (action: `snapshot`): read current eBPF map values for requested probes, return as JSON (synchronous — map reads are instant)
  - `submit` (action: `attach`): attach a probe (async — returns accepted, runs until cancelled)
  - `health`: return attached probes, kernel version compatibility
  - Requires `CAP_BPF` + `CAP_PERFMON` (documented, not silent failure)

**Tests**:
- sql-ctrl: integration test against a real MySQL/Postgres (docker container in CI). Submit a query, verify result. Discover, verify structured output. Invalid credentials, verify error -32003. Timeout, verify error -32000.
- ebpf-observer: unit tests for probe logic. Integration tests require Linux + kernel >= 5.8 (CI flag: `--feature ebpf-integration`). Attach tcp_retransmit probe, generate traffic, verify counter increments.
- Both sidecars: protocol compliance tests — verify JSON-RPC framing, error codes, health response format, max message size handling.

**Review gate**: Both sidecars pass protocol compliance tests and integration tests. sql-ctrl works against real MySQL/Postgres. ebpf-observer attaches probes on supported kernels.

---

### Phase 8: Integration Testing + Polish

**Goal**: End-to-end tests proving the full system works. Documentation. CI pipeline.

**Work**:
- Integration test suite:
  - Mock CP (small HTTP + gRPC server in test harness)
  - Start agent + echo-ctrl sidecar
  - Full flows:
    - Agent boots, bootstraps auth, sends first heartbeat
    - CP sends exec task (shell), agent executes, reports result
    - CP sends exec task (sidecar), agent dispatches to echo-ctrl, reports result
    - CP sends task chain (3 steps), step 2 fails, verify rollback_all
    - CP sends continuous task, verify periodic results, cancel, verify stops
    - CP sends install task (install echo-ctrl as a managed unit), verify heartbeat includes it
    - CP sends upgrade task, verify version change
    - CP sends cancel for running task, verify cancellation
    - Config reload (SIGHUP), verify hot-reload fields applied
    - Graceful shutdown (SIGTERM), verify in-flight tasks complete or cancel cleanly
- Documentation:
  - README.md: what Vasal is, quickstart, architecture overview
  - Sidecar authoring guide: how to implement a sidecar in any language using the JSON-RPC protocol
  - Protocol reference: task types, sidecar methods, error codes, message format
- CI:
  - `cargo check`, `cargo clippy`, `cargo fmt --check`
  - `cargo test` (unit + integration, excluding ebpf-integration)
  - Optional: `cargo test --features ebpf-integration` on Linux runners

**Tests**: The integration test suite described above IS this phase's test.

**Review gate**: All integration tests pass. Documentation is complete and accurate. CI is green. `cargo build --release` produces a single static binary.

---

## Phase Summary

| Phase | Scope | Depends on | Key deliverable |
|---|---|---|---|
| 1 | Protocol + SDK + echo-ctrl | — | Shared types, sidecar SDK, test harness |
| 2 | Agent skeleton | Phase 1 | Config, state store, signals, boots cleanly |
| 3 | Task execution engine | Phase 1, 2 | Shell, sidecar dispatch, chains, continuous, credentials |
| 4 | Transport + heartbeat + audit | Phase 1, 2, 3 | Agent talks to CP (poll + gRPC), heartbeats, audit trail |
| 5 | Unit management + self-upgrade | Phase 1, 2, 3 | Install/upgrade/remove units, self-upgrade with rollback |
| 6 | Auth + bootstrap | Phase 4 | One-time key → token refresh, all comms authenticated |
| 7 | Reference sidecars | Phase 1 | sql-ctrl, ebpf-observer |
| 8 | Integration + polish | All | End-to-end tests, docs, CI |

Every phase ends with a **manual review gate**. No phase starts until the prior phase passes review.
