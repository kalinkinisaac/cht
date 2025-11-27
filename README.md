# cht – ClickHouse Operations Toolkit

Modular Python package of a reusable toolkit for day‑to‑day ClickHouse maintenance.  It ships with composable helpers for cluster access, table lifecycle management, Kafka engine operations, and ad‑hoc SQL utilities, plus a Docker recipe for local testing.

## Installation

### Install from GitHub Release (Recommended)
```bash
# Install latest from git (always up-to-date, no version pinning needed)
pip install git+https://github.com/kalinkinisaac/cht.git

# Or install latest release wheel (specific version)
pip install https://github.com/kalinkinisaac/cht/releases/latest/download/cht-0.2.1-py3-none-any.whl

# Or install a specific version
pip install https://github.com/kalinkinisaac/cht/releases/download/v0.2.0/cht-0.2.0-py3-none-any.whl
```

### Single-File Distribution (No pip needed!)

Just download one file and run - no installation required! Inspired by [copyparty](https://github.com/9001/copyparty).

#### Self-Extracting File (Recommended)
```bash
# Download the self-extracting file (includes all dependencies)
curl -O https://github.com/kalinkinisaac/cht/releases/latest/download/cht-sfx.py

# Run directly - extracts automatically on first use
python cht-sfx.py --version
python cht-sfx.py  # Interactive mode

# Import CHT in your scripts
python -c "exec(open('cht-sfx.py').read()); from cht import Cluster; print('CHT loaded!')"
```

**Features:**
- ✅ No dependencies to install - everything bundled
- ✅ Works with any Python 3.10+ installation
- ✅ Auto-extracts to ~/.cache/cht-sfx on first run
- ✅ ~27MB download (includes pandas, clickhouse-connect)
- ✅ Same functionality as pip-installed CHT

#### Zipapp Distribution (Lightweight)
```bash
# Download the lightweight zipapp (requires pre-installed dependencies)
curl -O https://github.com/kalinkinisaac/cht/releases/latest/download/cht.pyz

# Install dependencies first
pip install clickhouse-connect>=0.6.8 pandas>=1.5

# Run zipapp
python cht.pyz --version
python -c "import sys; sys.path.insert(0, 'cht.pyz'); from cht import Cluster"
```

**Features:**
- ✅ Only ~250KB download
- ⚠️ Requires dependencies pre-installed  
- ✅ Standard Python zipapp format
- ✅ Good for environments with existing pandas/clickhouse-connect

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
- **Dependency Graph Mapping** for visualizing table and materialized view relationships across databases.
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
from cht import Table
import pandas as pd
from datetime import timedelta

# Set default cluster once
Table.set_default_cluster("local", host="localhost", user="developer", password="developer")

# DataFrame integration - SIMPLE API
df = pd.DataFrame({
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Charlie"],
    "timestamp": pd.to_datetime(["2023-01-01", "2023-01-02", "2023-01-03"])
})

# Create temporary table with TTL (expires in 1 day by default)
table = Table.from_df(df, name="users")
print(f"Created table: {table}")

# Create with custom TTL (expires in 2 hours)
table = Table.from_df(df, name="events", ttl=timedelta(hours=2))

# Create permanent table (no TTL)
table = Table.from_df(df, name="permanent_data", ttl=None)

# Load data back to DataFrame
result_df = table.to_df()
print(result_df)
```

### Table Constructor Flexibility

The `Table` class supports multiple flexible constructor patterns for convenient table references:

```python
from cht import Table

# Simple table name (uses 'default' database)
table = Table('users')                     # → default.users

# Database.table syntax
table = Table('analytics.events')          # → analytics.events  

# Explicit parameters (database first, table second)
table = Table('events', 'analytics')       # → events.analytics
table = Table(database_or_fqdn='events', table_name='analytics')  # → events.analytics

# With specific cluster
table = Table('analytics.events', cluster=my_cluster)
```

## Dependency Graph Analysis

Discover and visualize table relationships across your ClickHouse cluster:

```python
from cht import Cluster

# Connect and analyze dependencies
cluster = Cluster("prod", host="localhost", user="developer", password="developer")
graph = cluster.get_dependency_graph()

# Basic analysis
print(f"Found {len(graph.nodes)} tables and {len(graph.edges)} dependencies")

# Detect circular dependencies
cycles = graph.find_cycles()
if cycles:
    print(f"⚠️ Found {len(cycles)} circular dependencies")

# Find most influential tables
influential = graph.get_most_influential_tables(limit=5)
for table, score in influential:
    print(f"Critical table: {table.fqdn} (score: {score:.2f})")

# Export for visualization
graph.to_graphml("pipeline.graphml")  # For Gephi
graph.visualize("network.png")        # Generate diagram
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

# Load with row limit for large tables
df_sample = table.to_df(limit=1000)

# Load with FINAL modifier for deduplicated data (ReplacingMergeTree, etc.)
df_final = table.to_df(final=True)

# Combine limit and FINAL
df_final_sample = table.to_df(limit=1000, final=True)

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

### 🆕 Automatic Nullable Column Detection

CHT v0.4.3+ automatically detects columns with missing values (NaN/NaT/None) and creates them as nullable in ClickHouse:

```python
# DataFrame with missing datetime values - this now works automatically!
df_with_nulls = pd.DataFrame({
    "bidder_id": [1, 2, 3],
    "start_date": pd.to_datetime(["2023-01-01", "2023-01-02", "2023-01-03"]),
    "end_date": pd.to_datetime(["2023-01-10", None, "2023-01-12"])  # NaT value
})

# ✅ This works automatically with auto_nullable=True (default)
table = Table.from_df(df_with_nulls, cluster=cluster, name="bidders")

# Generated ClickHouse schema:
# - bidder_id: Int64
# - start_date: DateTime64(3)
# - end_date: Nullable(DateTime64(3))  ← Automatically detected as nullable
```

**Manual Control Options:**

```python
# Disable auto-detection and specify manually
table = Table.from_df(
    df_with_nulls,
    cluster=cluster,
    column_types={"end_date": "Nullable(DateTime64(3))"},
    auto_nullable=False  # Disable auto-detection
)

# Check what would be auto-detected
from cht.dataframe import detect_nullable_columns
nullable_cols = detect_nullable_columns(df_with_nulls)
print(nullable_cols)  # {'end_date': 'Nullable(DateTime64(3))'}
```

**Automatic Nullable Types:**

| Column with Nulls | Nullable ClickHouse Type |
|-------------------|---------------------------|
| `int64` + NaN | `Nullable(Int64)` |
| `float64` + NaN | `Nullable(Float64)` |
| `datetime64` + NaT | `Nullable(DateTime64(3))` |
| `object/string` + None | `Nullable(String)` |
| `bool` + None | `Nullable(UInt8)` |### Mode Options

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
pytest tests/test_graph.py  # Run dependency graph tests
```

### Examples
See the `examples/` directory for comprehensive usage demonstrations:
- `examples/basic_usage.py` - Core CHT functionality
- `examples/dependency_graph_basic.py` - Live dependency analysis
- `examples/dependency_graph_mock.py` - Mock data demonstration  
- `examples/advanced_operations.py` - Production patterns

### Code Quality
```bash
black src tests examples  # Format code
isort src tests examples  # Sort imports
flake8 src tests examples # Lint code
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
src/cht/                # Package modules
tests/                  # Pytest-based unit tests
examples/               # Usage examples and demonstrations
docker-compose.yml      # Local ClickHouse stack
docker/init/            # Optional bootstrap SQL scripts
CHANGELOG.md            # Version history and release notes
PROJECT_STRUCTURE.md    # Development guidelines and best practices
```

## Documentation

- [Examples Guide](examples/README.md) - Comprehensive usage examples
- [Dependency Graph Mapping](GRAPH_MAPPING.md) - Graph analysis features
- [Project Structure](PROJECT_STRUCTURE.md) - Development guidelines
- [Contributing Guide](CONTRIBUTING.md) - Contribution workflow
- [Changelog](CHANGELOG.md) - Version history

## Next Steps
- Point the Docker stack at staging data and exercise the high-level helpers.
- Extend the test suite with integration scenarios once a sandbox ClickHouse instance is available.
- Publish GitHub releases from the `release` branch to produce downloadable wheels (see `.github/workflows/build-wheel.yml`).
