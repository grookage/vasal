# Vasal

A lightweight, protocol-first, general-purpose host agent written in Rust.

Vasal executes tasks dispatched by any control plane that speaks its protocol,
manages sidecar and package lifecycles, observes infrastructure via sidecars,
and self-upgrades with rollback. It ships as a single static binary (~5-10 MB).

## What Vasal Is

- **Task executor** -- runs shell commands and dispatches work to sidecars
- **Unit lifecycle manager** -- installs, upgrades, health-checks, and removes
  sidecars and packages
- **Sensor + actuator** -- observes infrastructure via sidecars, reports to the
  control plane, acts on command
- **Self-maintainer** -- upgrades its own binary with atomic replace and rollback

## What Vasal Is Not

- A decision-maker about infrastructure state
- A reconciler or desired-state convergence engine
- Opinionated about what software it manages (SQL, Redis, LVS -- all via sidecars)
- Tied to running on the host it manages (the jumpbox pattern is valid)

## Architecture

The full design is documented in 18 design decisions:

```
docs/arch.md 
```

### Component Map

```
Agent Core (single static binary):
  +-- Transport (gRPC stream / HTTP poll -- configurable)
  +-- Task Router
  |     +-- Shell Executor (the only built-in executor)
  |     +-- Sidecar Dispatcher (Unix socket IPC to named sidecars)
  |     +-- Chain Executor (sequential multi-step with rollback)
  |     +-- Continuous Executor (interval-based repeating tasks)
  +-- Unit Manager (install, start, health, upgrade, rollback, remove)
  +-- Credential Resolver (HTTP or sidecar provider, per-task, eager/lazy)
  +-- State Store (SQLite WAL -- units, task journal, audit log)
  +-- Self-Upgrade Module (download, SHA-256 verify, atomic replace, rollback)
  +-- Auth Manager (bootstrap from onetimeauth.toml, token refresh)
  +-- Audit Forwarder (batched HTTP POST with backoff)
  +-- Heartbeat Sender (periodic HTTP POST with unit reports)
```

## Building

```bash
cargo build --release
```

## Packaging

### Debian / Ubuntu

Build a `.deb` package using [`cargo-deb`](https://crates.io/crates/cargo-deb):

```bash
cargo install cargo-deb
cargo deb -p vasal-core
```

For cross-compilation to a Linux target from macOS:

```bash
# Install the target toolchain
rustup target add x86_64-unknown-linux-gnu

# Build the .deb
cargo deb -p vasal-core --target x86_64-unknown-linux-gnu
```

Install on the target host:

```bash
sudo dpkg -i vasal_0.1.0-1_amd64.deb
```

The package:
- Installs the binary to `/usr/bin/vasal`
- Drops a default config at `/etc/vasal/config.toml` (preserved across upgrades)
- Installs a systemd unit (`vasal.service`)
- Creates a `vasal` system user/group and runtime directories on first install
- Does **not** auto-start — edit the config with your CP endpoints first, then:

```bash
sudo vim /etc/vasal/config.toml    # set your control plane URLs
sudo systemctl enable --now vasal
```

Uninstall:

```bash
sudo apt remove vasal               # keeps config and state
sudo apt purge vasal                 # removes everything including /var/lib/vasal
```

### Binary only

If you don't need packaging, the agent is a single static binary:

```bash
cargo build --release -p vasal-core
scp target/release/vasal host:/usr/local/bin/
```

## Testing

```bash
cargo test --workspace
```

Integration tests spawn the real `echo-ctrl` binary over Unix domain sockets
for true end-to-end sidecar IPC validation.

Lint:

```bash
cargo clippy -- -D warnings
```

## Sidecar Protocol

Sidecars communicate with the agent over Unix domain sockets using
**4-byte big-endian length-prefixed JSON-RPC 2.0**.

Four methods:

| Method   | Purpose                              | Required |
|----------|--------------------------------------|----------|
| `health` | Liveness check                       | Yes      |
| `submit` | Submit work (sync or async response) | Yes      |
| `status` | Poll async task progress             | Async only |
| `cancel` | Cancel async task                    | Async only |

### Writing a Sidecar in Rust

```rust
use async_trait::async_trait;
use vasal_protocol::sidecar::*;
use vasal_protocol::ProtocolError;
use vasal_sidecar_sdk::{SidecarHandler, SidecarServer};

struct MySidecar;

#[async_trait]
impl SidecarHandler for MySidecar {
    fn name(&self) -> &str { "my-sidecar" }

    async fn health(&self) -> HealthResponse {
        HealthResponse {
            status: HealthStatus::Ok,
            version: Some("0.1.0".into()),
            error: None,
            metadata: None,
        }
    }

    async fn submit(
        &self,
        params: serde_json::Value,
    ) -> Result<SubmitResponse, ProtocolError> {
        // Your logic here.
        Ok(SubmitResponse::Completed {
            stdout: "done".into(),
            stderr: String::new(),
            truncated: false,
        })
    }
}
```

Synchronous sidecars implement only `health` and `submit`. For long-running
operations, additionally override `status` and `cancel`, and return
`SubmitResponse::Accepted { task_id }` from `submit`.

### Writing a Sidecar in Any Language

Any process that:

1. Listens on a Unix domain socket
2. Reads 4-byte big-endian length + JSON payload
3. Responds to `health` and `submit` JSON-RPC 2.0 methods
4. Writes 4-byte big-endian length + JSON response

...is a valid Vasal sidecar. No Rust required.

## Task Types

| Type           | Wire value       | Purpose                         |
|----------------|------------------|---------------------------------|
| Exec           | `"exec"`         | Run a shell command or sidecar call |
| Cancel         | `"cancel"`       | Stop a running task             |
| Install        | `"install"`      | Install a managed unit          |
| Upgrade        | `"upgrade"`      | Upgrade a managed unit          |
| Remove         | `"remove"`       | Remove a managed unit           |
| Self-Upgrade   | `"self_upgrade"` | Upgrade the agent binary        |

Exec tasks support one-shot and continuous (interval-based) execution,
and can be chained with per-step rollback.

## Configuration

The agent reads a TOML config file (default `/etc/vasal/config.toml`,
override with `--config`):

```toml
[agent]
log_level = "info"           # hot-reloadable via SIGHUP

[transport]
mode = "poll"                # "poll" or "grpc"

[transport.poll]
endpoint = "https://cp.internal/api/v1"
interval_sec = 10

[heartbeat]
interval_sec = 10
endpoint = "https://cp.internal/api/v1/heartbeat"

[audit]
endpoint = "https://cp.internal/api/v1/audit"
batch_size = 50
flush_interval_sec = 5

[auth]
provider = "https://auth.internal/v1/token"

[shell]
max_concurrent = 4           # hot-reloadable
default_timeout_ms = 300000

[units]
health_check_interval_sec = 30
```

Hot-reloadable fields take effect on `SIGHUP` without restarting the agent.

## Credential Resolution

Credentials are resolved per-task, never cached, and discarded after use:

- **Eager** -- agent fetches the secret (via HTTP or sidecar IPC) before
  execution and injects it as an environment variable (shell) or request
  param (sidecar).
- **Lazy** -- agent forwards the credential reference to the sidecar as-is
  for self-resolution.

## Implementation Status

| Phase | Scope                                    | Status   |
|-------|------------------------------------------|----------|
| 1     | Protocol types + SDK + echo-ctrl         | Complete |
| 2     | Agent skeleton (CLI, config, state, signals) | Complete |
| 3     | Task execution engine (shell, sidecar, chain, continuous) | Complete |
| 4     | Transport + heartbeat + audit            | Complete |
| 5     | Unit management (install, upgrade, health) + self-upgrade | Complete |
| 6     | Auth + bootstrap                         | Complete |
| 7     | Reference sidecars (sql-ctrl, ebpf-observer) | Complete |
| 8     | Integration testing + polish             | Complete |

## License

Apache-2.0
