# execraft-runtime

`execraft-runtime` is a Rust-based execution runtime for the Execraft ecosystem.
It provides a production-friendly HTTP API and CLI for task submission, scheduling, execution, persistence, and status tracking.

## Why This Project

- Unified runtime surface for control-plane integrations
- Async task execution with queue, worker, timeout, and retry controls
- Durable task state with SQLite + task artifacts on disk
- Runtime introspection with health/readiness/metrics endpoints
- Docker-first deployment and CI-friendly operation

## Core Features

- **HTTP API**
  - submit task
  - query task status
  - cancel task
  - fetch task events
  - runtime info/capabilities/config/resources
  - health/readiness and Prometheus metrics
- **CLI**
  - `serve`, `submit`, `status`, `wait`, `kill`, `run`
- **Persistence**
  - SQLite metadata
  - task artifacts under `tasks/<task_id>/`
- **Execution**
  - internal shim process model
  - timeout and cancellation support
- **Capability Negotiation**
  - runtime capability probing
  - requested/effective execution plan visibility

## Project Structure

```text
execraft-runtime/
??? src/                # runtime core modules
??? tests/              # e2e and behavior tests
??? docs/               # architecture, API, CLI, deployment
??? scripts/            # quickstart and utility scripts
??? Dockerfile
??? Cargo.toml
```

## Quick Start

### Option A: Docker (Recommended)

```bash
docker build -t execraft-runtime:local .
docker run --rm -p 8080:8080 -v execraft-data:/data execraft-runtime:local
```

Runtime default:
- listen: `0.0.0.0:8080`
- data dir: `/data`

### Option B: Local Rust

```bash
cargo build --release
cargo run -- serve --listen-addr 127.0.0.1:8080 --data-dir ./data
```

## Minimal API Example

Submit task:

```bash
curl -sS -X POST "http://127.0.0.1:8080/api/v1/tasks" \
  -H "Content-Type: application/json" \
  -d '{"execution":{"kind":"command","program":"/bin/sh","args":["-c","echo hello"]}}'
```

Check status:

```bash
curl -sS "http://127.0.0.1:8080/api/v1/tasks/<task_id>"
```

## Development

```bash
cargo fmt
cargo clippy --all-targets --all-features -- -D warnings
cargo test
```

## Documentation

- `docs/README.md`
- `docs/architecture.md`
- `docs/api.md`
- `docs/cli.md`
- `docs/deployment.md`
- `docs/development.md`

## License

MIT License. See `LICENSE`.

