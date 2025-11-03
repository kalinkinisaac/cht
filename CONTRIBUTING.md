# Contributing to CHT (ClickHouse Toolkit)

Welcome to the CHT project! This guide will help you set up your development environment and understand our contribution workflow.

## Quick Start

### Prerequisites

- **Python 3.10+** (required)
- **Git** for version control
- **Docker** (optional, for local ClickHouse testing)

### Development Setup

#### 1. Clone the Repository
```bash
git clone https://github.com/kalinkinisaac/cht.git
cd cht
```

#### 2. Set Up Development Environment

**Option A: Automated Setup (Recommended)**
```bash
# Run the setup script to create venv and install dependencies
./scripts/setup-dev.sh

# Activate the environment
source .venv/bin/activate
```

**Option B: Manual Setup**
```bash
# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Upgrade pip and install dependencies
pip install --upgrade pip
pip install -r requirements-dev.txt

# Install package in development mode
pip install -e .
```

#### 3. Verify Installation
```bash
# Check that the package is installed
python -c "import cht; print(f'CHT version: {cht.__version__}')"

# Run tests to ensure everything works
pytest
```

## Development Workflow

### Code Quality Standards

We maintain high code quality through automated tools:

#### **Formatting with Black**
```bash
# Format all code
black src/ tests/

# Check formatting without making changes
black --check src/ tests/
```

#### **Import Sorting with isort**
```bash
# Sort imports
isort src/ tests/

# Check import sorting
isort --check-only src/ tests/
```

#### **Linting with flake8**
```bash
# Check code style
flake8 src/ tests/
```

#### **Type Checking with Pylint**
```bash
# Run pylint on source code
pylint src/cht/

# Run pylint on specific file
pylint src/cht/cluster.py
```

#### **All Quality Checks**
```bash
# Run all quality checks at once
black --check src/ tests/ && \
isort --check-only src/ tests/ && \
flake8 src/ tests/ && \
pylint src/cht/
```

### Running Tests

#### **Unit Tests**
```bash
# Run all tests
pytest

# Run tests with verbose output
pytest -v

# Run specific test file
pytest tests/test_cluster.py

# Run with coverage
pytest --cov=cht --cov-report=html
```

#### **Integration Tests**
```bash
# Start Docker ClickHouse (required for integration tests)
docker compose up -d

# Run integration test
python test_docker_integration.py

# Stop Docker when done
docker compose down
```

### Local ClickHouse Testing

For testing DataFrame integration and other features:

```bash
# Start ClickHouse server
docker compose up -d

# ClickHouse will be available at:
# - HTTP interface: http://localhost:8123
# - Native TCP: localhost:9000
# - Credentials: developer/developer

# Test connection
python -c "
from cht import Cluster
cluster = Cluster(name='test', host='localhost', user='developer', password='developer')
print('✓ Connected to ClickHouse')
"

# Stop when done
docker compose down
```

## Project Structure

```
cht/
├── src/cht/              # Main package source
│   ├── __init__.py       # Package initialization
│   ├── cluster.py        # Cluster connection management
│   ├── table.py          # Table operations and DataFrame integration
│   ├── dataframe.py      # DataFrame utilities and type mapping
│   ├── operations.py     # High-level operations
│   ├── kafka.py          # Kafka engine utilities
│   └── sql_utils.py      # SQL helper functions
├── tests/                # Unit tests
│   ├── test_cluster.py
│   ├── test_table.py
│   ├── test_dataframe.py
│   └── ...
├── docker/               # Docker configuration
│   └── init/             # ClickHouse initialization scripts
├── scripts/              # Development scripts
│   └── setup-dev.sh
├── .github/              # GitHub Actions workflows
│   └── workflows/
├── pyproject.toml        # Project configuration
├── requirements.txt      # Production dependencies
├── requirements-dev.txt  # Development dependencies
├── .pylintrc            # Pylint configuration
└── README.md            # Project documentation
```

## Key Components

### **Cluster Class** (`src/cht/cluster.py`)
- Connection management with lazy initialization
- Structured logging for all operations
- Read-only mode support
- Introspection utilities (disk usage, table statistics)

### **Table Class** (`src/cht/table.py`)
- High-level table operations
- DataFrame integration with `to_df()` and `from_df()`
- Backup/restore workflows
- Materialized view management

### **DataFrame Integration** (`src/cht/dataframe.py`)
- Automatic pandas ↔ ClickHouse type mapping
- Table creation from DataFrames
- Bulk data insertion utilities

## Contribution Guidelines

### **Making Changes**

1. **Create a Feature Branch**
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Write Code**
   - Follow existing code style and patterns
   - Add type annotations for all functions
   - Include docstrings for public methods

3. **Write Tests**
   ```bash
   # Add tests for new functionality
   # Tests should be in tests/ directory
   # Follow existing test patterns
   ```

4. **Run Quality Checks**
   ```bash
   # Ensure all checks pass before committing
   black src/ tests/
   isort src/ tests/
   flake8 src/ tests/
   pylint src/cht/
   pytest
   ```

5. **Commit Changes**
   ```bash
   git add .
   git commit -m "feat: add DataFrame filtering support"
   ```

### **Code Style**

- **Line Length**: 100 characters maximum
- **Formatting**: Use Black formatter
- **Import Sorting**: Use isort with Black profile
- **Type Hints**: Required for all public functions
- **Docstrings**: Required for all public classes and methods

#### **Example Code Style**
```python
from typing import Optional, List
import pandas as pd

def create_table_from_dataframe(
    cluster: Cluster,
    df: pd.DataFrame,
    table_name: str,
    *,
    database: str = "default",
    engine: str = "MergeTree",
    order_by: Optional[List[str]] = None,
) -> None:
    """Create a ClickHouse table from a pandas DataFrame.
    
    Args:
        cluster: ClickHouse cluster connection
        df: Source DataFrame
        table_name: Target table name
        database: Target database name
        engine: ClickHouse table engine
        order_by: Columns for ORDER BY clause
    """
    # Implementation here
    pass
```

### **Testing**

- **Unit Tests**: Test individual components in isolation
- **Integration Tests**: Test with real ClickHouse instance
- **Mock External Dependencies**: Use pytest-mock for testing
- **Coverage**: Aim for >90% test coverage

#### **Example Test**
```python
import pytest
from unittest.mock import Mock
from cht.cluster import Cluster

def test_cluster_connection():
    """Test that cluster connects properly."""
    cluster = Cluster(name="test", host="localhost")
    
    # Mock the connection
    cluster._client = Mock()
    cluster._client.ping.return_value = True
    
    assert cluster.client.ping() is True
```

### **Commit Messages**

Use conventional commit format:
- `feat:` New features
- `fix:` Bug fixes
- `docs:` Documentation changes
- `test:` Test additions/changes
- `refactor:` Code refactoring
- `chore:` Maintenance tasks

Examples:
```
feat: add DataFrame filtering support to Table.to_df()
fix: handle empty DataFrames in type mapping
docs: update README with DataFrame examples
test: add integration tests for Docker setup
```

## Release Process

1. **Version Bumping**
   ```bash
   # Update version in pyproject.toml
   # Update __init__.py with new version
   ```

2. **Create Release Branch**
   ```bash
   git checkout -b release/v0.2.0
   ```

3. **Update Documentation**
   ```bash
   # Update README.md
   # Update CHANGELOG.md
   ```

4. **Tag Release**
   ```bash
   git tag -a v0.2.0 -m "Release version 0.2.0"
   git push origin v0.2.0
   ```

## Getting Help

### **Common Issues**

**ClickHouse Connection Failed**
```bash
# Make sure Docker is running
docker compose up -d

# Check logs
docker compose logs clickhouse

# Verify credentials (developer/developer)
```

**Import Errors**
```bash
# Reinstall in development mode
pip install -e .

# Check Python path
python -c "import sys; print(sys.path)"
```

**Test Failures**
```bash
# Run specific failing test
pytest tests/test_cluster.py::test_specific_function -v

# Run with debug output
pytest -s --tb=long
```

### **Documentation**

- **README.md**: Project overview and usage examples
- **Docstrings**: Inline code documentation
- **Type Hints**: Function signatures and return types
- **Tests**: Living examples of how code should work

### **Community**

- **Issues**: Report bugs and request features on GitHub
- **Discussions**: Ask questions in GitHub Discussions
- **Pull Requests**: Submit code contributions

## Architecture Notes for Newcomers

### **Design Principles**

1. **High-Level API First**: Users should work with `Table.from_df()`, not low-level clients
2. **Type Safety**: Strict type annotations throughout
3. **Logging**: Structured logging for all operations
4. **Error Handling**: Clear error messages with context
5. **Testing**: Comprehensive unit and integration tests

### **Key Design Patterns**

- **Lazy Initialization**: Connections created only when needed
- **Builder Pattern**: Fluent APIs for complex operations
- **Dependency Injection**: Easy testing with mock objects
- **Configuration Over Convention**: Explicit parameters preferred

### **Performance Considerations**

- **Connection Pooling**: Single connection per Cluster instance
- **Bulk Operations**: DataFrame operations handle large datasets efficiently
- **Memory Management**: Streaming for large query results
- **Type Mapping**: Optimized pandas ↔ ClickHouse conversions

Welcome to the project! If you have questions, don't hesitate to open an issue or start a discussion.