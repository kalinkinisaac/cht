# CHT Examples

This directory contains practical examples demonstrating CHT (ClickHouse Toolkit) functionality.

## Getting Started

1. **Install CHT**:
   ```bash
   pip install git+https://github.com/kalinkinisaac/cht.git
   ```

2. **Start ClickHouse** (for live examples):
   ```bash
   docker compose up -d
   ```

3. **Run examples**:
   ```bash
   python examples/basic_usage.py
   ```

## Examples Overview

### Basic Usage
- **`basic_usage.py`** - Core CHT functionality demonstration
  - Cluster connections and table operations
  - DataFrame integration
  - Backup and restore workflows

### Dependency Graph Mapping
- **`dependency_graph_basic.py`** - Live ClickHouse dependency analysis
  - Requires running ClickHouse instance
  - Creates sample tables and materialized views
  - Demonstrates graph discovery and analysis

- **`dependency_graph_mock.py`** - Mock data demonstration
  - Works without ClickHouse connection
  - Shows complete graph functionality
  - Perfect for learning the API

### Advanced Features
- **`advanced_operations.py`** - High-level automation examples
- **`kafka_integration.py`** - Kafka engine utilities
- **`performance_analysis.py`** - Cluster optimization techniques

## Example Categories

### 🚀 **Beginner Examples**
Perfect for getting started with CHT:
- Basic cluster connections
- Simple table operations  
- DataFrame workflows

### 🔧 **Intermediate Examples**
Production-ready patterns:
- Backup and restore automation
- Materialized view management
- Data pipeline discovery

### ⚡ **Advanced Examples**
Complex scenarios and optimizations:
- Multi-cluster synchronization
- Performance monitoring
- Custom operation pipelines

## Prerequisites

### Required
- Python 3.10+
- CHT library installed

### Optional (for specific examples)
- Docker (for local ClickHouse)
- NetworkX (for advanced graph analysis)
- Matplotlib (for visualizations)
- Graphviz (for diagram generation)

Install optional dependencies:
```bash
pip install networkx matplotlib graphviz plotly
```

## Running Examples

### With Live ClickHouse
```bash
# Start ClickHouse
docker compose up -d

# Run live examples
python examples/dependency_graph_basic.py
python examples/basic_usage.py
```

### Mock Examples (No Dependencies)
```bash
# Run without ClickHouse
python examples/dependency_graph_mock.py
```

## Example Structure

Each example follows Google Python style guidelines:

```python
#!/usr/bin/env python3
"""Brief description of what this example demonstrates.

Longer description explaining:
- What the example shows
- Prerequisites and setup
- Expected output

Example:
    $ python example_name.py
    
    Expected output here...
"""

# Standard library imports
import logging
from typing import Dict, List

# Third-party imports
import pandas as pd

# Local imports
from cht import Cluster, Table


def main() -> None:
    """Main example function with clear steps."""
    # Implementation here
    pass


if __name__ == "__main__":
    main()
```

## Best Practices Demonstrated

### Error Handling
Examples show proper exception handling:
- Connection failures
- Missing tables/databases
- Permission issues

### Logging
Structured logging throughout:
- Operation progress
- Error details
- Performance metrics

### Resource Management
Clean resource usage:
- Proper connection cleanup
- Temporary table lifecycle
- Memory-efficient operations

### Documentation
Each example includes:
- Clear docstrings
- Inline comments for complex logic
- Usage instructions
- Expected outputs

## Contributing Examples

When adding new examples:

1. **Follow naming convention**: `category_feature.py`
2. **Use Google Python style**: Docstrings, type hints, formatting
3. **Include error handling**: Don't assume perfect conditions
4. **Add to README**: Update this file with description
5. **Test thoroughly**: Ensure examples work as documented

### Example Template
```python
#!/usr/bin/env python3
"""Template for new CHT examples.

This template demonstrates the structure and style for CHT examples.
Copy this file and modify for your specific use case.

Prerequisites:
    - CHT library installed
    - ClickHouse running (if using live data)
    - Any additional dependencies

Example:
    $ python your_example.py
    
    Output:
    ✓ Connected to ClickHouse
    ✓ Example completed successfully
"""

from typing import Optional
import logging

from cht import Cluster


def setup_logging() -> None:
    """Configure logging for the example."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )


def main() -> None:
    """Main example function."""
    setup_logging()
    logger = logging.getLogger(__name__)
    
    try:
        # Your example code here
        logger.info("Example completed successfully")
        
    except Exception as e:
        logger.error(f"Example failed: {e}")
        raise


if __name__ == "__main__":
    main()
```

## Troubleshooting

### Common Issues

**ClickHouse Connection Failed**
```bash
# Check if ClickHouse is running
docker compose ps

# Check logs
docker compose logs clickhouse

# Restart if needed
docker compose restart clickhouse
```

**Import Errors**
```bash
# Reinstall CHT in development mode
pip install -e .

# Check installation
python -c "import cht; print(f'CHT version: {cht.__version__}')"
```

**Permission Denied**
- Verify ClickHouse credentials in examples
- Check read-only vs read-write access requirements
- Review database permissions

### Getting Help

1. **Documentation**: Check `README.md` and `GRAPH_MAPPING.md`
2. **Tests**: Look at `tests/` for usage patterns
3. **Issues**: Create GitHub issue with example details
4. **Discussions**: Use GitHub Discussions for questions

## Additional Resources

- [Main Documentation](../README.md)
- [Graph Mapping Guide](../GRAPH_MAPPING.md)
- [Contributing Guide](../CONTRIBUTING.md)
- [API Reference](../src/cht/)
- [Test Suite](../tests/)