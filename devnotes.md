# Flourine Development Notes

## Build Commands

```bash
# Preferred unified task runner
cargo xtask help

# Build entire workspace
cargo build --workspace

# Build specific crate
cargo build -p flourine-broker
cargo build -p flourine-wire
cargo build -p flourine-schema
cargo build -p flourine-common

# Release build
cargo build --workspace --release
```

## Unified Cargo Automation (`xtask`)

```bash
# Show all unified commands
cargo xtask help

# Regenerate protobuf code for Java/Python + rebuild Rust wire proto
cargo xtask gen-proto

# Build Rust workspace
cargo xtask build

# Run Rust tests
cargo xtask test-rust

# Run all flourine-broker integration suites (includes ignored tests)
# Auto-detects suites from crates/flourine-broker/tests/*.rs.
# Note: this includes cross_language_e2e, which requires Java + Python SDK toolchains.
# Uses DATABASE_URL if set, otherwise defaults to postgres://postgres:postgres@localhost:5433
cargo xtask test-db

# Run SDK tests (Java + Python)
cargo xtask test-sdk

# Run all tests across all modules/languages (Rust + Java + Python + DB suites)
# Uses DATABASE_URL if set, otherwise defaults to postgres://postgres:postgres@localhost:5433
cargo xtask test-all

# Full local CI: gen-proto + build + all tests
cargo xtask ci
```

## Adding New Modules / Suites

```bash
# 1) New Rust crate/module in workspace
# Put it under crates/<name>; workspace membership is auto-detected by:
# members = ["crates/*", "xtask"]

# 2) New flourine-broker integration test suite
# Add crates/flourine-broker/tests/<suite_name>.rs
# No xtask code change needed: `cargo xtask test-db` auto-detects *.rs suites.

# 3) New protobuf schema file
# Add proto/<name>.proto and update language wiring if used by SDK/runtime code.
# Current `cargo xtask gen-proto` regenerates from proto/flourine_wire.proto.

# 4) New SDK language
# Add it under sdks/<language>/ and then extend `xtask test-sdk`
# (currently runs Java + Python explicitly).
```

## Test Commands

```bash
# Run all tests
cargo test --workspace

# Run all tests across all modules/languages (Rust + Java + Python + DB suites)
cargo xtask test-all

# Run tests for specific crate
cargo test -p flourine-broker
cargo test -p flourine-wire
cargo test -p flourine-schema

# Run specific test
cargo test -p flourine-broker test_distribute_acks

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
cargo clippy -p flourine-broker -- -D warnings
cargo clippy -p flourine-wire -- -D warnings
cargo clippy -p flourine-schema -- -D warnings

# Format code
cargo fmt --all

# Check formatting without applying
cargo fmt --all -- --check
```

## Database Commands

```bash
# Start Postgres (assuming Docker)
docker run -d --name flourine-postgres \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=flourine \
  -p 5433:5432 \
  postgres:16

# Connect to Postgres
psql -h localhost -p 5433 -U postgres -d flourine

# Apply authoritative schema migration (single consolidated file)
psql -h localhost -p 5433 -U postgres -d flourine -f migrations/001_init.sql

# Reset schema quickly (local/dev only)
psql -h localhost -p 5433 -U postgres -d flourine -c "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"
psql -h localhost -p 5433 -U postgres -d flourine -f migrations/001_init.sql
```

## Crate Structure

```
crates/
├── flourine-common/     # Shared types: IDs, errors, Record, RecordBatch, BatchAck
├── flourine-wire/       # Wire protocol: varint, writer/reader encoding
├── flourine-schema/     # Schema registry: canonicalization, compatibility, HTTP API
├── flourine-broker/      # Broker server: batching, TBIN, S3, WebSocket
└── flourine-core/       # Core Avro handling (existing)
```

## Key Files

| Crate | File | Purpose |
|-------|------|---------|
| flourine-wire | `varint.rs` | Zigzag varint encoding (Avro-compatible) |
| flourine-wire | `writer.rs` | AppendRequest/Response encoding |
| flourine-wire | `reader.rs` | Read/group protocol encoding (read, join, heartbeat, rejoin, commit) |
| flourine-broker | `tbin.rs` | TBIN file format (ZSTD + footer index) |
| flourine-broker | `buffer.rs` | Request batching and merging |
| flourine-broker | `dedup.rs` | LRU dedup cache |
| flourine-broker | `batched_server.rs` | WebSocket server with batching + flush loop |
| flourine-broker | `coordinator.rs` | Reader-group coordination and assignment |
| flourine-broker | `admin/topics.rs` | Admin API topic lifecycle endpoints |
| flourine-broker | `bin/flourine-broker.rs` | Broker binary (WebSocket + Admin API + shutdown) |
| flourine-schema | `canonical.rs` | Schema canonicalization + SHA-256 |
| flourine-schema | `compat.rs` | Backward compatibility checking |
| flourine-schema | `registry.rs` | Database-backed schema registry |
| flourine-schema | `api.rs` | HTTP API endpoints |

## Test Suite Discovery

```bash
# List flourine-broker integration suites
ls crates/flourine-broker/tests/*.rs | xargs -n1 basename | sed 's/\.rs$//'

# Only ignored suites (currently cross_language_e2e)
rg -n "#\\[ignore" crates/flourine-broker/tests
```

## Running Integration Tests

```bash
# Run all tests in workspace
# Note: flourine-broker integration tests require local Postgres on :5433
cargo test --workspace

# Run only unit tests (no DB required)
cargo test -p flourine-broker --lib
cargo test -p flourine-wire
cargo test -p flourine-schema

# Run flourine-broker integration tests that do not need external SDK toolchains
# DATABASE_URL must NOT include a database name; tests create/drop per-test DBs.
DATABASE_URL=postgres://postgres:postgres@localhost:5433 cargo test -p flourine-broker --tests -- --nocapture

# Run integration tests without DB setup (pure in-memory suite)
cargo test -p flourine-broker --test integration

# Run specific test suites with database:

# Admin API integration tests
DATABASE_URL=postgres://postgres:postgres@localhost:5433 cargo test -p flourine-broker --test admin_api_integration -- --nocapture

# Auth integration tests
DATABASE_URL=postgres://postgres:postgres@localhost:5433 cargo test -p flourine-broker --test auth_integration -- --nocapture

# DB integration tests
DATABASE_URL=postgres://postgres:postgres@localhost:5433 cargo test -p flourine-broker --test db_integration -- --nocapture

# E2E WebSocket tests
DATABASE_URL=postgres://postgres:postgres@localhost:5433 cargo test -p flourine-broker --test e2e_websocket -- --nocapture

# E2E Reader Groups tests
DATABASE_URL=postgres://postgres:postgres@localhost:5433 cargo test -p flourine-broker --test e2e_reader_groups -- --nocapture

# Cross-language E2E tests (requires Java + Python SDK toolchains)
DATABASE_URL=postgres://postgres:postgres@localhost:5433 cargo test -p flourine-broker --test cross_language_e2e -- --include-ignored

# Coordinator integration tests
DATABASE_URL=postgres://postgres:postgres@localhost:5433 cargo test -p flourine-broker --test coordinator_integration -- --nocapture

# Jepsen-inspired tests (run each suite explicitly)
DATABASE_URL=postgres://postgres:postgres@localhost:5433 cargo test -p flourine-broker --test jepsen_reader_groups -- --nocapture
DATABASE_URL=postgres://postgres:postgres@localhost:5433 cargo test -p flourine-broker --test jepsen_crash -- --nocapture
DATABASE_URL=postgres://postgres:postgres@localhost:5433 cargo test -p flourine-broker --test jepsen_linearizability -- --nocapture
DATABASE_URL=postgres://postgres:postgres@localhost:5433 cargo test -p flourine-broker --test jepsen_offset -- --nocapture
DATABASE_URL=postgres://postgres:postgres@localhost:5433 cargo test -p flourine-broker --test jepsen_partition -- --nocapture

# Negative/error handling tests
DATABASE_URL=postgres://postgres:postgres@localhost:5433 cargo test -p flourine-broker --test negative_tests -- --nocapture

# One-shot full DB suite run (continues after individual suite failures)
/bin/zsh -lc 'export DATABASE_URL=postgres://postgres:postgres@localhost:5433; tests=(admin_api_integration auth_integration coordinator_integration db_integration e2e_reader_groups e2e_websocket integration jepsen_reader_groups jepsen_crash jepsen_linearizability jepsen_offset jepsen_partition negative_tests); for t in $tests; do echo "=== $t ==="; cargo test -q -p flourine-broker --test $t -- --nocapture || true; done'

# Cross-language suite run (ignored by default)
DATABASE_URL=postgres://postgres:postgres@localhost:5433 cargo test -p flourine-broker --test cross_language_e2e -- --include-ignored --nocapture
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
# Benchmark (broker e2e load test)
# Notes:
# - DATABASE_URL must not include a database name (tests create isolated DBs)
# - test_e2e_load_one_million_requests is ignored by default; run with --ignored
#
# 300k request benchmark (faster iteration)
DATABASE_URL=postgres://postgres:postgres@localhost:5433 \
FLOURINE_LOAD_PRODUCERS=30 \
FLOURINE_LOAD_BATCHES_PER_PRODUCER=10000 \
FLOURINE_LOAD_PARTITIONS=32 \
FLOURINE_LOAD_RECORDS_PER_BATCH=128 \
FLOURINE_LOAD_PAYLOAD_BYTES=32 \
FLOURINE_LOAD_MAX_IN_FLIGHT=64 \
FLOURINE_LOAD_FETCH_TIMEOUT_SECS=600 \
FLOURINE_LOAD_ENABLE_OTEL=1 \
cargo test -p flourine-broker --test e2e_load test_e2e_load_one_million_requests -- --ignored --nocapture

# 1M request benchmark
DATABASE_URL=postgres://postgres:postgres@localhost:5433 \
FLOURINE_LOAD_PRODUCERS=40 \
FLOURINE_LOAD_BATCHES_PER_PRODUCER=25000 \
FLOURINE_LOAD_PARTITIONS=32 \
FLOURINE_LOAD_RECORDS_PER_BATCH=128 \
FLOURINE_LOAD_PAYLOAD_BYTES=32 \
FLOURINE_LOAD_MAX_IN_FLIGHT=64 \
FLOURINE_LOAD_FETCH_TIMEOUT_SECS=1200 \
FLOURINE_LOAD_ENABLE_OTEL=0 \
cargo test -p flourine-broker --test e2e_load test_e2e_load_one_million_requests -- --ignored --nocapture

# Generate flamegraph (requires cargo-flamegraph)
cargo flamegraph -p flourine-broker --bin flourine-broker -- <args>

# Flamegraph with 1.2M request load test (40 * 30,000, records_per_batch=128)
DATABASE_URL=postgres://postgres:postgres@localhost:5433 \
FLOURINE_LOAD_PRODUCERS=40 \
FLOURINE_LOAD_BATCHES_PER_PRODUCER=30000 \
FLOURINE_LOAD_PARTITIONS=32 \
FLOURINE_LOAD_RECORDS_PER_BATCH=128 \
FLOURINE_LOAD_PAYLOAD_BYTES=32 \
FLOURINE_LOAD_MAX_IN_FLIGHT=64 \
FLOURINE_LOAD_FETCH_TIMEOUT_SECS=1800 \
FLOURINE_LOAD_ENABLE_OTEL=0 \
cargo flamegraph -p flourine-broker --test e2e_load -- test_e2e_load_one_million_requests --ignored --nocapture

# Profile with perf
perf record -g cargo run -p flourine-broker --bin flourine-broker --release -- <args>
perf report
```

## Environment Variables

```bash
# Database
DATABASE_URL=postgres://postgres:postgres@localhost:5433

# AWS S3 (for local testing with MinIO)
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
AWS_ENDPOINT_URL=http://localhost:9000

# Logging
RUST_LOG=debug
RUST_LOG=flourine_broker=debug,flourine_wire=info
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
aws --endpoint-url http://localhost:9000 s3 mb s3://flourine
```

## Useful Cargo Commands

```bash
# Check what would be compiled
cargo check --workspace

# Show dependency tree
cargo tree -p flourine-broker

# Update dependencies
cargo update

# Show outdated dependencies
cargo outdated

# Generate docs
cargo doc --workspace --open
```

## Pending

- [ ] Focus next optimization on reducing append ack path queueing: `flush_buffer_residency` and `flush_queue_wait` are the largest controllable contributors after dedup.
- [ ] Keep writer batching enabled in perf goals; `records_per_batch=1` dramatically underutilizes throughput.
- [ ] Evaluate dedup lock contention under high in-flight single-writer load (`dedup_check` remains material in request-heavy runs).
- [ ] Decide whether to add an explicit writer benchmark profile in CI: `1 writer + batched requests` with `records/sec` and `bytes/sec` targets.

### Latest Findings (2026-02-13)

- 1.2M request flamegraph run (`40 x 30,000`, `records_per_batch=1`):
  - `req_rps=70,895`, `payload_Bps=2,268,639`, `payload_MiBps=2.16`
  - `req_p50=37.951ms`, `req_p95=46.015ms`, `req_p99=50.047ms`
  - Hot metrics:
    - `append_enqueue_to_ack` avg `20.705ms`
    - `flush_buffer_residency` avg `12.079ms`
    - `dedup_check` avg `6.672ms`
    - `flush_queue_wait` avg `2.788ms`
- Batched writer confirmation (`records_per_batch=100`) with same 300k records:
  - `30 writers`: `rec_rps=744,982`, `payload_MiBps=22.74`
  - `1 writer`: `rec_rps=449,469`, `payload_MiBps=13.72`
  - Conclusion: use `records/sec` and `bytes/sec` as primary throughput metrics; request/sec is no longer the primary KPI when batching is enabled.

### Latest Findings (2026-02-14)

- Expanded load sweep (`20 writers`, payload `32B`, `max_in_flight=64`) confirms batching is a dominant lever:
  - `records_per_batch=1` (`80,000 req`): `rec_rps=37,906`, `payload_MiBps=1.16`, `req_p95=41.343ms`
  - `records_per_batch=100` (`80,000 req`): `rec_rps=2,265,236`, `payload_MiBps=69.13`, `req_p95=67.199ms`
  - `records_per_batch=250` (`30,000 req`): `rec_rps=2,980,241`, `payload_MiBps=90.95`, `req_p95=131.839ms`
  - `records_per_batch=500` (`20,000 req`): `rec_rps=3,009,584`, `payload_MiBps=91.85`, `req_p95=384.255ms`
- Throughput gains flatten above `250` while request latency and queueing rise sharply.
- Default for `test_e2e_load_one_million_requests` is now `records_per_batch=128` unless overridden by `FLOURINE_LOAD_RECORDS_PER_BATCH`.
