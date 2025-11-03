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
- **DataFrame integration** for seamless pandas ↔ ClickHouse workflows with automatic type mapping.
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
import pandas as pd

cluster = Cluster(name="local", host="localhost", user="developer", password="developer")

# Traditional table operations
table = Table("events", database="analytics", cluster=cluster)
if table.exists():
    print("Columns:", table.get_columns())

# DataFrame integration
df = pd.DataFrame({
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Charlie"],
    "timestamp": pd.to_datetime(["2023-01-01", "2023-01-02", "2023-01-03"])
})

# Create table from DataFrame
temp_table = Table.from_df(
    df, 
    cluster=cluster,
    database="temp", 
    name="users",
    mode="overwrite",
    engine="MergeTree",
    order_by=["id"]
)

# Load table back to DataFrame
result_df = temp_table.to_df()
print(result_df)
```

## DataFrame Integration

The library provides seamless integration between pandas DataFrames and ClickHouse tables with automatic type mapping and table creation.

### Creating Tables from DataFrames

```python
import pandas as pd
from cht import Cluster, Table

cluster = Cluster(name="local", host="localhost", user="developer", password="developer")

# Sample DataFrame
df = pd.DataFrame({
    "user_id": [1, 2, 3, 4],
    "email": ["alice@example.com", "bob@example.com", "charlie@example.com", "diana@example.com"],
    "created_at": pd.to_datetime(["2023-01-01", "2023-01-02", "2023-01-03", "2023-01-04"]),
    "is_active": [True, False, True, True],
    "score": [85.5, 92.0, 78.3, 96.7]
})

# Create table with automatic type mapping
table = Table.from_df(
    df,
    cluster=cluster,
    database="analytics", 
    name="users",
    mode="overwrite",  # or "append"
    engine="MergeTree",
    order_by=["user_id"],
    partition_by=["toYYYYMM(created_at)"]
)
```

### Loading Tables to DataFrames

```python
# Load entire table as DataFrame
df = table.to_df()
print(df)
```

### Type Mapping

The library automatically maps pandas dtypes to appropriate ClickHouse types:

| Pandas Type | ClickHouse Type |
|-------------|-----------------|
| `bool` | `UInt8` |
| `int8/16/32/64` | `Int8/16/32/64` |
| `uint8/16/32/64` | `UInt8/16/32/64` |
| `float32/64` | `Float32/64` |
| `datetime64` | `DateTime64(3)` |
| `category` | `String` |
| `object` | `String` |

### Mode Options

```python
# Overwrite mode (default) - drops and recreates table
table = Table.from_df(df, cluster=cluster, mode="overwrite")

# Append mode - adds data to existing table
table = Table.from_df(df, cluster=cluster, mode="append")
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
