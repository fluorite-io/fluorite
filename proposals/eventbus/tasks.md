# Eventbus Tasks

Canonical task list for the eventbus workstream. This replaces the prior proposal docs in this directory and is aligned to the current codebase.

Last updated: 2026-02-15

## Completed in code

- [x] Core wire protocol and message unions (`crates/flourine-wire/src/`)
- [x] Batched broker append/read path (`crates/flourine-broker/src/batched_server.rs`)
- [x] Broker buffer, dedup, and coordinator (`crates/flourine-broker/src/buffer.rs`, `crates/flourine-broker/src/dedup.rs`, `crates/flourine-broker/src/coordinator.rs`)
- [x] FL format read/write (`crates/flourine-broker/src/fl.rs`)
- [x] Object store abstraction + S3/LocalFS impl (`crates/flourine-broker/src/object_store.rs`)
- [x] Admin API for topics, API keys, ACLs, groups (`crates/flourine-broker/src/admin/`)
- [x] API key auth + ACL checks with TTL cache (`crates/flourine-broker/src/auth/`)
- [x] Prometheus metrics and `/metrics` endpoint (`crates/flourine-broker/src/metrics.rs`, `crates/flourine-broker/src/admin/mod.rs`)
- [x] Graceful shutdown path for WS + admin server (`crates/flourine-broker/src/shutdown.rs`, `crates/flourine-broker/src/bin/flourine-broker.rs`)
- [x] OpenTelemetry export wiring (OTLP, env-driven) (`crates/flourine-broker/src/bin/flourine-broker.rs`)
- [x] End-to-end load tests against real WebSocket path (`crates/flourine-broker/tests/e2e_load.rs`)
- [x] Rust SDK writer + group reader (`crates/flourine-sdk/src/`)
- [x] Java SDK writer + group reader (`sdks/java/flourine-sdk/src/main/java/io/flourine/sdk/`)
- [x] Python SDK writer + group reader (`sdks/python/flourine/`)
- [x] E2E, auth, admin, DB, Jepsen-style, and cross-language tests (`crates/flourine-broker/tests/`)

## Outstanding (current scope)

- [ ] JavaScript SDK (Node + browser parity with Rust/Java/Python)
  - Expected location: `sdks/javascript/`
  - Must support writer + group reader + auth + integration tests

- [ ] S3 orphan file garbage collection
  - No background GC job found in broker
  - Implement safe orphan detection and deletion using object-store delete path

- [ ] Safe topic deletion with data cleanup
  - `DELETE /topics/:id` currently deletes only from `topics` table (`crates/flourine-broker/src/admin/topics.rs`)
  - Add cleanup plan for related batch index rows and object-store objects

- [ ] Chaos/failover test suite beyond current Jepsen tests
  - Add explicit scenarios: crash mid-flush, S3 latency/failure, Postgres failover

- [ ] Ops package: alert rules, dashboards, runbooks
  - Metrics exist, but no alert/runbook/dashboard artifacts are present in repo

- [ ] Health/readiness endpoints and broker registration policy
  - `/metrics` exists; health/readiness endpoints are not defined
  - Add readiness behavior during startup/drain and LB integration guidance

- [ ] Auth mode expansion (if still required)
  - Current implementation is API-key based (`crates/flourine-broker/src/auth/`)
  - Add JWT/OIDC path only if product requirement remains

## Deferred to v2

- [ ] Iceberg sink path
- [ ] Multi-topic reader groups
- [ ] Configurable quotas/rate limits
- [ ] Multi-tenancy resource caps
- [ ] Cross-region replication
- [ ] Topic-name to topic-id resolution layer in SDKs
