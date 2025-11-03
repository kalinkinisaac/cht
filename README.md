# cht – ClickHouse Operations Toolkit

Modular Python package of a reusable toolkit for day‑to‑day ClickHouse maintenance.  It ships with composable helpers for cluster access, table lifecycle management, Kafka engine operations, and ad‑hoc SQL utilities, plus a Docker recipe for local testing.

## Installation

### Install from GitHub Release (Recommended)
```bash
# Install the latest wheel (fastest)
pip install https://github.com/kalinkinisaac/cht/releases/download/v0.1.0/cht-0.1.0-py3-none-any.whl
```

### Install from Source
```bash
# Install from git repository (latest)
pip install git+https://github.com/kalinkinisaac/cht.git

# Install specific version
pip install git+https://github.com/kalinkinisaac/cht.git@v0.1.0
```

### Verify Installation
```python
import cht
print("cht version:", cht.__version__)
```

## Features
- `Cluster` wrapper with structured logging, bulk execution helper, and disk usage introspection.
- `Table` convenience API for backups, restore flows, MV replay, and metadata inspection.
- High‑level operations (`rebuild_table_via_mv`, duplicate analysis, row sync utilities).
- Kafka tooling for consumer group rotation and CREATE TABLE diffing.
- Lightweight SQL helpers (identifier formatting, `remote()` builder, hash comparison scaffolding).
- Test suite (`pytest`) covering the core control flow and edge cases.
- Docker Compose environment that spins up a ClickHouse server with optional bootstrap SQL.

## Development Setup

### Quick Setup
```bash
# Clone the repository
git clone https://github.com/kalinkinisaac/cht.git
cd cht

# Run the setup script (creates venv and installs dependencies)
./scripts/setup-dev.sh

# Activate the environment
source .venv/bin/activate
```

### Manual Setup
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install --upgrade pip
pip install -r requirements-dev.txt
pip install -e .
```

### Quick Start Example
```python
from cht import Cluster, Table

cluster = Cluster(name="local", host="localhost", user="developer", password="developer")
table = Table("events", database="analytics", cluster=cluster)

if table.exists():
    print("Columns:", table.get_columns())
```

## Development

### Running Tests
```bash
pytest                    # Run all tests
pytest -v                # Verbose output
pytest tests/test_*.py   # Run specific test files
```

### Code Quality
```bash
black src tests          # Format code
isort src tests          # Sort imports
flake8 src tests         # Lint code
```

### Building Wheels
```bash
python -m build          # Build source dist and wheel
python -m build --wheel  # Build wheel only
```

The wheel will be created in `dist/` and can be installed via `pip install dist/<file>.whl`.

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
