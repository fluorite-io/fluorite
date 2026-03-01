# Contributing to Fluorite

## CLA Requirement

By submitting a pull request, you agree to the terms of our
[Contributor License Agreement](CLA.md). This is required for all contributions
because Fluorite is dual-licensed under AGPL-3.0-only and a commercial license.

## Development Setup

```bash
# Start Postgres
cd docker && docker-compose up -d

# Build
cargo build

# Run tests
cargo test

# Run the broker
export DATABASE_URL=postgres://postgres:postgres@localhost:5433/fluorite
cargo run -p fluorite-broker --bin fluorite-broker
```

## Code Style

See [CLAUDE.md](.claude/CLAUDE.md) for coding conventions, naming, error
handling, and testing guidelines. Run `cargo fmt` and `cargo clippy` before
submitting.

## Pull Requests

- One logical change per PR
- Include tests for new behavior
- Keep PRs small and focused
