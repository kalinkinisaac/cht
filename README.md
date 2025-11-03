# cht – ClickHouse Operations Toolkit

Modular Python package of a reusable toolkit for day‑to‑day ClickHouse maintenance.  It ships with composable helpers for cluster access, table lifecycle management, Kafka engine operations, and ad‑hoc SQL utilities, plus a Docker recipe for local testing.

## Features
- `Cluster` wrapper with structured logging, bulk execution helper, and disk usage introspection.
- `Table` convenience API for backups, restore flows, MV replay, and metadata inspection.
- High‑level operations (`rebuild_table_via_mv`, duplicate analysis, row sync utilities).
- Kafka tooling for consumer group rotation and CREATE TABLE diffing.
- Lightweight SQL helpers (identifier formatting, `remote()` builder, hash comparison scaffolding).
- Test suite (`pytest`) covering the core control flow and edge cases.
- Docker Compose environment that spins up a ClickHouse server with optional bootstrap SQL.

## Getting Started
```bash
python -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

For local development:
```bash
pip install -r requirements-dev.txt
```

Quick sanity check:
```python
from cht import Cluster, Table

cluster = Cluster(name="local", host="localhost", user="developer", password="developer")
table = Table("events", database="analytics", cluster=cluster)

if table.exists():
    print("Columns:", table.get_columns())
```

## Running Tests
```bash
pytest
```

## Building a Wheel Locally
```bash
python -m pip install --upgrade build
python -m build --wheel
```
The wheel lands under `dist/` and can be installed via `pip install dist/<file>.whl`.

## Docker Environment
Start a local ClickHouse service (HTTP: `8123`, native TCP: `9000`):
```bash
docker compose up -d
```
Initialisation scripts placed under `docker/init/*.sql` run automatically on first boot. Stop the stack with:
```bash
docker compose down
```

## Project Layout
```
src/cht/            # Package modules
tests/              # Pytest-based unit tests
docker-compose.yml  # Local ClickHouse stack
docker/init/        # Optional bootstrap SQL scripts
```

## Next Steps
- Point the Docker stack at staging data and exercise the high-level helpers.
- Extend the test suite with integration scenarios once a sandbox ClickHouse instance is available.
- Publish GitHub releases from the `release` branch to produce downloadable wheels (see `.github/workflows/build-wheel.yml`).
