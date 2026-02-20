# flourine-cli

CLI tool for interacting with a running Flourine cluster.

## Local Setup

### 1. Start PostgreSQL

```bash
cd docker
docker-compose up -d
```

This starts PostgreSQL on port 5433 and auto-applies migrations.

### 2. Start the broker

```bash
export DATABASE_URL=postgres://postgres:postgres@localhost:5433/flourine
cargo run -p flourine-broker --bin flourine-broker
```

The broker starts two servers:
- WebSocket on `127.0.0.1:9000` (data plane: write/read)
- Admin HTTP on `127.0.0.1:9001` (management: topics, schemas, ACLs)

### 3. Bootstrap an API key

The admin API requires authentication. Bootstrap creates an admin key directly in the database:

```bash
export DATABASE_URL=postgres://postgres:postgres@localhost:5433/flourine
cargo run -p flourine-cli -- bootstrap
```

This prints a key like `tb_<uuid>_<secret>`. Export it:

```bash
export FLOURINE_API_KEY=tb_...
```

### 4. Use the CLI

```bash
# Create a topic
flourine topic create my-events --partitions 3

# Write a record (--topic accepts name or numeric ID)
flourine write --topic my-events '{"name":"alice"}'

# Write with explicit key and schema
flourine write --topic my-events --key user-123 --schema-id 1 '{"name":"alice"}'

# Pipe from stdin
echo '{"name":"bob"}' | flourine write --topic my-events

# Live-tail with TUI
flourine tail --topic my-events

# Tail as JSON with offset range
flourine tail --topic my-events --start 0:0 --end 0:10 --output json
```

## Commands

### `bootstrap`
Create an admin API key directly via the database. Requires `DATABASE_URL`.

### `topic`
```bash
flourine topic list
flourine topic create my-topic --partitions 3
flourine topic get 1
flourine topic update 1 --retention-hours 48
flourine topic delete 1
```

### `schema`
```bash
flourine schema register --topic-id 1 --file schema.avsc
flourine schema get 100
flourine schema list --topic-id 1
flourine schema check --topic-id 1 --file schema.avsc
```

### `write`
```bash
flourine write --topic my-events '{"name":"alice"}'
flourine write --topic 1 --key user-123 --schema-id 100 '{"name":"alice"}'
echo '{"data":"value"}' | flourine write --topic my-events
```

`--schema-id` defaults to the latest schema registered for the topic.

### `tail`
```bash
flourine tail --topic my-events                     # TUI (default)
flourine tail --topic my-events --output json       # JSONL to stdout
flourine tail --topic 1 --start 0:0,1:0 --end 0:100,1:100 --output json
```

- `--start` / `--end`: `partition:offset,...` format
- `--output`: `tui` (default) or `json`
- TUI: press `q` or `Esc` to quit

## Configuration

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--admin-url` | | `http://localhost:9001` | Admin API URL |
| `--ws-url` | | `ws://localhost:9000` | WebSocket URL |
| `--api-key` | `FLOURINE_API_KEY` | | API key for authentication |

## Architecture

- `bootstrap.rs` — Create admin API key via direct database access
- `client.rs` — HTTP client wrapping admin + schema registry APIs
- `topic.rs` — Topic CRUD commands
- `schema.rs` — Schema register/get/list/check commands
- `write.rs` — Record append via SDK Writer (WebSocket)
- `tail.rs` — Live-tail via SDK Reader with TUI or JSON output
