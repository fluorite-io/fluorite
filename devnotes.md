# Turbine Development Notes

## Build Commands

```bash
# Build entire workspace
cargo build --workspace

# Build specific crate
cargo build -p turbine-agent
cargo build -p turbine-wire
cargo build -p turbine-schema
cargo build -p turbine-common

# Release build
cargo build --workspace --release
```

## Test Commands

```bash
# Run all tests
cargo test --workspace

# Run tests for specific crate
cargo test -p turbine-agent
cargo test -p turbine-wire
cargo test -p turbine-schema

# Run specific test
cargo test -p turbine-agent test_distribute_acks

# Run tests with output
cargo test --workspace -- --nocapture

# Run tests with backtrace on failure
RUST_BACKTRACE=1 cargo test --workspace
```

## Lint Commands

```bash
# Run clippy on entire workspace
cargo clippy --workspace -- -D warnings

# Run clippy on specific crate
cargo clippy -p turbine-agent -- -D warnings
cargo clippy -p turbine-wire -- -D warnings
cargo clippy -p turbine-schema -- -D warnings

# Format code
cargo fmt --all

# Check formatting without applying
cargo fmt --all -- --check
```

## Database Commands

```bash
# Start Postgres (assuming Docker)
docker run -d --name turbine-postgres \
  -e POSTGRES_PASSWORD=turbine \
  -e POSTGRES_DB=turbine \
  -p 5433:5432 \
  postgres:16

# Connect to Postgres
psql -h localhost -p 5433 -U postgres -d turbine

# Run migrations (using sqlx)
sqlx migrate run

# Create new migration
sqlx migrate add <migration_name>
```

## Crate Structure

```
crates/
├── turbine-common/     # Shared types: IDs, errors, Record, Segment
├── turbine-wire/       # Wire protocol: varint, producer/consumer encoding
├── turbine-schema/     # Schema registry: canonicalization, compatibility, HTTP API
├── turbine-agent/      # Agent server: batching, TBIN, S3, WebSocket
└── turbine-core/       # Core Avro handling (existing)
```

## Key Files

| Crate | File | Purpose |
|-------|------|---------|
| turbine-wire | `varint.rs` | Zigzag varint encoding (Avro-compatible) |
| turbine-wire | `producer.rs` | ProduceRequest/Response encoding |
| turbine-wire | `consumer.rs` | FetchRequest/Response encoding |
| turbine-agent | `tbin.rs` | TBIN file format (ZSTD + footer index) |
| turbine-agent | `buffer.rs` | Request batching and merging |
| turbine-agent | `dedup.rs` | LRU dedup cache |
| turbine-agent | `server.rs` | Simple WebSocket server |
| turbine-agent | `batched_server.rs` | Batched server with flush loop |
| turbine-schema | `canonical.rs` | Schema canonicalization + SHA-256 |
| turbine-schema | `compat.rs` | Backward compatibility checking |
| turbine-schema | `registry.rs` | Database-backed schema registry |
| turbine-schema | `api.rs` | HTTP API endpoints |

## Test Counts (as of last run)

| Crate | Unit Tests | Integration | DB Integration | E2E WebSocket | Coordinator |
|-------|------------|-------------|----------------|---------------|-------------|
| turbine-agent | 49 | 5 | 7 | 4 | 10 |
| turbine-common | 20 | - | - | - | - |
| turbine-core | 18 | - | - | - | - |
| turbine-schema | 31 | - | - | - | - |
| turbine-sdk | 7 | - | - | - | - |
| turbine-wire | 42 | - | - | - | - |
| **Total** | **167** | **5** | **7** | **4** | **10** |

**Grand Total: 193 tests + 3 doc tests = 196**

## Running Integration Tests

```bash
# Run all tests (requires DATABASE_URL)
cargo test --workspace

# Run only unit tests (no DB required)
cargo test -p turbine-agent --lib
cargo test -p turbine-wire
cargo test -p turbine-schema

# Run integration tests (no DB required)
cargo test -p turbine-agent --test integration

# Run DB integration tests (requires DATABASE_URL)
DATABASE_URL=postgres://postgres:turbine@localhost:5433 cargo test -p turbine-agent --test db_integration

# Run E2E WebSocket tests (requires DATABASE_URL)
DATABASE_URL=postgres://postgres:turbine@localhost:5433 cargo test -p turbine-agent --test e2e_websocket

# Run coordinator integration tests (requires DATABASE_URL)
DATABASE_URL=postgres://postgres:turbine@localhost:5433 cargo test -p turbine-agent --test coordinator_integration
```

## Git Workflow

```bash
# Check status
git status

# View recent commits
git log --oneline -10

# Commit with co-author
git commit -m "$(cat <<'EOF'
commit message here

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>
EOF
)"

# Amend last commit
git commit --amend -m "new message"
```

## Performance Profiling

```bash
# Generate flamegraph (requires cargo-flamegraph)
cargo flamegraph --bin turbine -- <args>

# Profile with perf
perf record -g cargo run --release -- <args>
perf report
```

## Environment Variables

```bash
# Database
DATABASE_URL=postgres://postgres:turbine@localhost:5433/turbine

# AWS S3 (for local testing with MinIO)
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
AWS_ENDPOINT_URL=http://localhost:9000

# Logging
RUST_LOG=debug
RUST_LOG=turbine_agent=debug,turbine_wire=info
```

## Local S3 with MinIO

```bash
# Start MinIO
docker run -d --name minio \
  -p 9000:9000 \
  -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data --console-address ":9001"

# Create bucket
aws --endpoint-url http://localhost:9000 s3 mb s3://turbine
```

## Useful Cargo Commands

```bash
# Check what would be compiled
cargo check --workspace

# Show dependency tree
cargo tree -p turbine-agent

# Update dependencies
cargo update

# Show outdated dependencies
cargo outdated

# Generate docs
cargo doc --workspace --open
```
