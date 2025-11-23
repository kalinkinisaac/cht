# Project Structure and Development Guidelines

This document outlines the CHT project structure following Google's Python project best practices.

## Directory Structure

```
cht/                                    # Project root
├── src/                               # Source code (Google style: separate src/)
│   └── cht/                          # Package directory
│       ├── __init__.py               # Package initialization and exports
│       ├── cluster.py                # Core cluster connection management
│       ├── table.py                  # Table operations and DataFrame integration
│       ├── dataframe.py              # DataFrame utilities and type mapping
│       ├── graph.py                  # Dependency graph analysis (NEW)
│       ├── operations.py             # High-level automation helpers
│       ├── kafka.py                  # Kafka engine utilities
│       ├── sql_utils.py              # SQL helper functions
│       └── temp_tables.py            # Temporary table management
│
├── tests/                            # Test suite (Google style: separate tests/)
│   ├── test_cluster.py               # Cluster functionality tests
│   ├── test_table.py                 # Table operations tests
│   ├── test_dataframe.py             # DataFrame integration tests
│   ├── test_graph.py                 # Graph mapping tests (NEW)
│   ├── test_operations.py            # High-level operations tests
│   ├── test_kafka.py                 # Kafka utilities tests
│   ├── test_sql_utils.py             # SQL utilities tests
│   ├── test_table_default_cluster.py # Default cluster tests
│   └── test_temp_tables.py           # Temporary table tests
│
├── examples/                         # Usage examples (Google style: examples/)
│   ├── README.md                     # Examples documentation
│   ├── basic_usage.py                # Core functionality demo
│   ├── dependency_graph_basic.py     # Live dependency analysis
│   ├── dependency_graph_mock.py      # Mock data demonstration
│   └── advanced_operations.py        # Production patterns
│
├── docker/                           # Docker configuration
│   └── init/                         # ClickHouse initialization scripts
│       └── 001-create-database.sql
│
├── scripts/                          # Development and deployment scripts
│   ├── setup-dev.sh                 # Development environment setup
│   ├── build-sfx.py                 # Build self-extracting file
│   ├── build-zipapp.py              # Build zipapp distribution
│   └── build-all.sh                 # Build all single-file distributions
│
├── .github/                          # GitHub configuration
│   └── workflows/                    # CI/CD workflows
│       ├── build-wheel.yml           # Package building
│       └── test.yml                  # Test automation
│
├── docs/                             # Documentation (optional expansion)
│   ├── api/                          # API reference
│   ├── guides/                       # User guides
│   └── development/                  # Development documentation
│
├── CHANGELOG.md                      # Version history (NEW - Google style)
├── README.md                         # Project overview and quick start
├── CONTRIBUTING.md                   # Contribution guidelines
├── GRAPH_MAPPING.md                  # Feature-specific documentation (NEW)
├── SINGLE_FILE_DISTRIBUTION.md      # Single-file distribution guide (NEW)
├── pyproject.toml                    # Project configuration (Google style)
├── requirements.txt                  # Production dependencies
├── requirements-dev.txt              # Development dependencies
├── docker-compose.yml                # Local development environment
└── .gitignore                        # Git ignore rules
```

## Code Organization Principles

### 1. Google Python Style Guide Compliance

#### **Module Structure**
```python
#!/usr/bin/env python3
\"\"\"Brief description of the module.

Longer description explaining:
- Purpose and scope
- Key classes and functions
- Usage examples

Example:
    from cht import Cluster
    cluster = Cluster(\"local\", \"localhost\")
\"\"\"

# Standard library imports
import logging
from typing import Dict, List, Optional

# Third-party imports
import pandas as pd
import clickhouse_connect

# Local imports
from .sql_utils import format_identifier
from .cluster import Cluster
```

#### **Class Structure**
```python
class TableManager:
    \"\"\"Manages ClickHouse table operations with lifecycle support.
    
    This class provides high-level table management including backup,
    restore, and dependency tracking operations.
    
    Attributes:
        cluster: ClickHouse cluster connection
        default_database: Default database for operations
        
    Example:
        manager = TableManager(cluster)
        manager.backup_table(\"events\", \"analytics\")
    \"\"\"
    
    def __init__(self, cluster: Cluster) -> None:
        \"\"\"Initialize the table manager.
        
        Args:
            cluster: Configured ClickHouse cluster connection
            
        Raises:
            ConnectionError: If cluster connection fails
        \"\"\"
        self._cluster = cluster
        self._logger = logging.getLogger(__name__)
    
    def backup_table(
        self,
        table_name: str,
        database: str = \"default\",
        *,
        backup_suffix: str = \"_backup\",
        verify: bool = True,
    ) -> str:
        \"\"\"Create a backup of the specified table.
        
        Args:
            table_name: Name of the table to backup
            database: Database containing the table
            backup_suffix: Suffix for backup table name
            verify: Whether to verify backup integrity
            
        Returns:
            Name of the created backup table
            
        Raises:
            TableNotFoundError: If source table doesn't exist
            BackupError: If backup creation fails
            
        Example:
            backup_name = manager.backup_table(\"events\", \"analytics\")
            print(f\"Created backup: {backup_name}\")
        \"\"\"
        # Implementation here
        pass
```

#### **Function Structure**
```python
def analyze_table_dependencies(
    cluster: Cluster,
    table_name: str,
    *,
    database: str = \"default\",
    include_views: bool = True,
    max_depth: int = 5,
) -> Dict[str, List[str]]:
    \"\"\"Analyze dependencies for a ClickHouse table.
    
    Discovers both incoming and outgoing dependencies through materialized
    views and table relationships.
    
    Args:
        cluster: ClickHouse cluster connection
        table_name: Name of the table to analyze
        database: Database containing the table
        include_views: Whether to include view dependencies
        max_depth: Maximum dependency depth to traverse
        
    Returns:
        Dictionary mapping dependency types to lists of table names:
        {
            'depends_on': ['source_table1', 'source_table2'],
            'depended_by': ['target_table1', 'target_view1']
        }
        
    Raises:
        TableNotFoundError: If specified table doesn't exist
        AnalysisError: If dependency analysis fails
        
    Example:
        deps = analyze_table_dependencies(cluster, \"events\", \"analytics\")
        print(f\"Table depends on: {deps['depends_on']}\")
    \"\"\"
    # Implementation here
    pass
```

### 2. Type Safety and Documentation

#### **Type Annotations**
- All public functions have complete type annotations
- Use `typing` module for complex types
- Prefer specific types over `Any`

```python
from typing import Dict, List, Optional, Union, Tuple

def process_query_results(
    results: List[Tuple[str, int, float]],
    *,
    filter_threshold: Optional[float] = None,
) -> Dict[str, Union[int, float]]:
    \"\"\"Process query results with optional filtering.\"\"\"
    pass
```

#### **Docstring Standards**
- Use Google docstring format
- Include Args, Returns, Raises sections
- Provide usage examples for public APIs
- Document complex algorithms inline

### 3. Error Handling

#### **Exception Hierarchy**
```python
class CHTError(Exception):
    \"\"\"Base exception for CHT operations.\"\"\"
    pass

class ConnectionError(CHTError):
    \"\"\"Raised when cluster connection fails.\"\"\"
    pass

class TableNotFoundError(CHTError):
    \"\"\"Raised when specified table doesn't exist.\"\"\"
    pass

class DependencyError(CHTError):
    \"\"\"Raised when dependency analysis fails.\"\"\"
    pass
```

#### **Error Handling Patterns**
```python
def safe_operation_with_retry(operation: Callable, max_retries: int = 3) -> Any:
    \"\"\"Execute operation with exponential backoff retry.\"\"\"
    import time
    
    for attempt in range(max_retries):
        try:
            return operation()
        except (ConnectionError, TimeoutError) as e:
            if attempt == max_retries - 1:
                raise
            
            wait_time = 2 ** attempt
            logging.warning(f\"Operation failed (attempt {attempt + 1}), retrying in {wait_time}s: {e}\")
            time.sleep(wait_time)
```

### 4. Testing Strategy

#### **Test Organization**
```python
class TestDependencyGraph:
    \"\"\"Test suite for dependency graph functionality.\"\"\"
    
    def setup_method(self) -> None:
        \"\"\"Set up test fixtures before each test method.\"\"\"
        self.mock_cluster = self._create_mock_cluster()
        self.sample_graph = self._create_sample_graph()
    
    def test_graph_construction(self) -> None:
        \"\"\"Test basic graph construction from mock data.\"\"\"
        # Given
        tables = [(\"db1\", \"table1\"), (\"db1\", \"table2\")]
        views = [(\"db1\", \"view1\", \"CREATE MATERIALIZED VIEW ...\")]
        
        # When
        graph = DependencyGraph.from_cluster_data(tables, views)
        
        # Then
        assert len(graph.nodes) == 3
        assert len(graph.edges) == 1
    
    def test_cycle_detection(self) -> None:
        \"\"\"Test detection of circular dependencies.\"\"\"
        # Test implementation
        pass
    
    @pytest.mark.parametrize(\"input_data,expected_count\", [
        ([(\"db1\", \"t1\"), (\"db1\", \"t2\")], 2),
        ([(\"db1\", \"t1\")], 1),
        ([], 0),
    ])
    def test_table_counting(self, input_data, expected_count) -> None:
        \"\"\"Test table counting with various inputs.\"\"\"
        # Parameterized test implementation
        pass
```

#### **Test Categories**
- **Unit Tests**: Test individual functions and classes in isolation
- **Integration Tests**: Test interaction between components
- **End-to-End Tests**: Test complete workflows with real/mock ClickHouse
- **Property Tests**: Test with generated inputs using Hypothesis

### 5. Logging and Monitoring

#### **Structured Logging**
```python
import logging
import json

class StructuredLogger:
    \"\"\"Structured logger for CHT operations.\"\"\"
    
    def __init__(self, name: str) -> None:
        self.logger = logging.getLogger(name)
        
    def log_operation(
        self,
        operation: str,
        table: str,
        *,
        duration: Optional[float] = None,
        rows_affected: Optional[int] = None,
        success: bool = True,
        **kwargs
    ) -> None:
        \"\"\"Log structured operation data.\"\"\"
        log_data = {
            \"operation\": operation,
            \"table\": table,
            \"success\": success,
            \"timestamp\": time.time(),
        }
        
        if duration is not None:
            log_data[\"duration_ms\"] = duration * 1000
        if rows_affected is not None:
            log_data[\"rows_affected\"] = rows_affected
            
        log_data.update(kwargs)
        
        level = logging.INFO if success else logging.ERROR
        self.logger.log(level, json.dumps(log_data))
```

### 6. Performance Considerations

#### **Lazy Loading**
```python
class ClusterManager:
    \"\"\"Cluster manager with lazy connection initialization.\"\"\"
    
    def __init__(self, config: Dict[str, str]) -> None:
        self._config = config
        self._client: Optional[Client] = None
    
    @property
    def client(self) -> Client:
        \"\"\"Get client with lazy initialization.\"\"\"
        if self._client is None:
            self._client = self._create_client()
        return self._client
```

#### **Resource Management**
```python
from contextlib import contextmanager

@contextmanager
def temporary_table(cluster: Cluster, table_name: str):
    \"\"\"Context manager for temporary table lifecycle.\"\"\"
    try:
        # Create temporary table
        table = Table(table_name, cluster=cluster)
        yield table
    finally:
        # Cleanup even if exception occurs
        try:
            table.drop()
        except Exception as e:
            logging.warning(f\"Failed to cleanup temporary table {table_name}: {e}\")
```

### 7. Configuration Management

#### **Settings Hierarchy**
1. **Command Line Arguments**: Highest priority
2. **Environment Variables**: Middle priority  
3. **Configuration Files**: Lowest priority
4. **Defaults**: Fallback values

```python
@dataclass
class CHTConfig:
    \"\"\"CHT configuration with environment variable support.\"\"\"
    
    host: str = \"localhost\"
    port: int = 8123
    user: str = \"default\"
    password: str = \"\"
    database: str = \"default\"
    read_only: bool = False
    
    @classmethod
    def from_env(cls) -> 'CHTConfig':
        \"\"\"Create configuration from environment variables.\"\"\"
        return cls(
            host=os.getenv('CHT_HOST', cls.host),
            port=int(os.getenv('CHT_PORT', str(cls.port))),
            user=os.getenv('CHT_USER', cls.user),
            password=os.getenv('CHT_PASSWORD', cls.password),
            database=os.getenv('CHT_DATABASE', cls.database),
            read_only=os.getenv('CHT_READ_ONLY', '').lower() == 'true',
        )
```

## Development Workflow

### 1. Local Development Setup
```bash
# Clone repository
git clone https://github.com/kalinkinisaac/cht.git
cd cht

# Set up development environment
./scripts/setup-dev.sh
source .venv/bin/activate

# Install package in development mode
pip install -e .

# Start local ClickHouse
docker compose up -d
```

### 2. Code Quality Automation
```bash
# Format code
black src/ tests/ examples/

# Sort imports  
isort src/ tests/ examples/

# Type checking
mypy src/cht/

# Linting
flake8 src/ tests/ examples/

# Run tests
pytest tests/ -v

# Run specific test category
pytest tests/test_graph.py -v
```

### 3. Documentation Generation
```bash
# Generate API documentation
sphinx-apidoc -o docs/api src/cht/

# Build documentation
sphinx-build docs/ docs/_build/

# Serve documentation locally
python -m http.server 8000 --directory docs/_build/
```

### 4. Release Process
```bash
# Update version numbers
# - pyproject.toml
# - src/cht/__init__.py

# Update CHANGELOG.md

# Create release tag
git tag -a v0.5.0 -m \"Release version 0.5.0\"
git push origin v0.5.0

# GitHub Actions will build and release automatically
```

## Best Practices Summary

1. **Code Style**: Follow Google Python Style Guide
2. **Type Safety**: Use comprehensive type annotations
3. **Documentation**: Write clear docstrings with examples
4. **Testing**: Maintain >90% test coverage
5. **Error Handling**: Use specific exceptions with clear messages
6. **Logging**: Use structured logging for operations
7. **Performance**: Implement lazy loading and resource management
8. **Configuration**: Support multiple configuration sources
9. **Dependencies**: Minimize external dependencies
10. **Backwards Compatibility**: Maintain API compatibility across minor versions

This structure ensures the CHT project remains maintainable, testable, and follows industry best practices for Python development.