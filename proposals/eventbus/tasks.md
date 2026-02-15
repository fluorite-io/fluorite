# Eventbus Tasks

Canonical task list for the eventbus workstream. This replaces the prior proposal docs in this directory and is aligned to the current codebase.

Last updated: 2026-02-15

## Completed in code

- [x] Core wire protocol and message unions (`crates/turbine-wire/src/`)
- [x] Batched broker append/read path (`crates/turbine-agent/src/batched_server.rs`)
- [x] Broker buffer, dedup, and coordinator (`crates/turbine-agent/src/buffer.rs`, `crates/turbine-agent/src/dedup.rs`, `crates/turbine-agent/src/coordinator.rs`)
- [x] TBIN format read/write (`crates/turbine-agent/src/tbin.rs`)
- [x] Object store abstraction + S3/LocalFS impl (`crates/turbine-agent/src/object_store.rs`)
- [x] Admin API for topics, API keys, ACLs, groups (`crates/turbine-agent/src/admin/`)
- [x] API key auth + ACL checks with TTL cache (`crates/turbine-agent/src/auth/`)
- [x] Prometheus metrics and `/metrics` endpoint (`crates/turbine-agent/src/metrics.rs`, `crates/turbine-agent/src/admin/mod.rs`)
- [x] Graceful shutdown path for WS + admin server (`crates/turbine-agent/src/shutdown.rs`, `crates/turbine-agent/src/bin/turbine-agent.rs`)
- [x] OpenTelemetry export wiring (OTLP, env-driven) (`crates/turbine-agent/src/bin/turbine-agent.rs`)
- [x] End-to-end load tests against real WebSocket path (`crates/turbine-agent/tests/e2e_load.rs`)
- [x] Rust SDK writer + group reader (`crates/turbine-sdk/src/`)
- [x] Java SDK writer + group reader (`sdks/java/turbine-sdk/src/main/java/io/turbine/sdk/`)
- [x] Python SDK writer + group reader (`sdks/python/turbine/`)
- [x] E2E, auth, admin, DB, Jepsen-style, and cross-language tests (`crates/turbine-agent/tests/`)

## Outstanding (current scope)

- [ ] JavaScript SDK (Node + browser parity with Rust/Java/Python)
  - Expected location: `sdks/javascript/`
  - Must support writer + group reader + auth + integration tests

- [ ] S3 orphan file garbage collection
  - No background GC job found in broker
  - Implement safe orphan detection and deletion using object-store delete path

- [ ] Safe topic deletion with data cleanup
  - `DELETE /topics/:id` currently deletes only from `topics` table (`crates/turbine-agent/src/admin/topics.rs`)
  - Add cleanup plan for related batch index rows and object-store objects

- [ ] Chaos/failover test suite beyond current Jepsen tests
  - Add explicit scenarios: crash mid-flush, S3 latency/failure, Postgres failover

- [ ] Ops package: alert rules, dashboards, runbooks
  - Metrics exist, but no alert/runbook/dashboard artifacts are present in repo

- [ ] Health/readiness endpoints and broker registration policy
  - `/metrics` exists; health/readiness endpoints are not defined
  - Add readiness behavior during startup/drain and LB integration guidance

- [ ] Auth mode expansion (if still required)
  - Current implementation is API-key based (`crates/turbine-agent/src/auth/`)
  - Add JWT/OIDC path only if product requirement remains

## Deferred to v2

- [ ] Iceberg sink path
- [ ] Multi-topic reader groups
- [ ] Configurable quotas/rate limits
- [ ] Multi-tenancy resource caps
- [ ] Cross-region replication
- [ ] Topic-name to topic-id resolution layer in SDKs
