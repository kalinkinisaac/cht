# Graph Mapping Feature Documentation

## Overview

The CHT library now includes comprehensive dependency graph mapping functionality that discovers and visualizes the relationships between tables and materialized views across your ClickHouse databases.

## Key Features

- **Automatic Discovery**: Scans all databases to find tables and materialized views
- **Dependency Analysis**: Maps data flow pipelines through materialized views
- **Multiple Export Formats**: NetworkX, GraphML, DOT, and JSON serialization
- **Visualization Support**: Integration with Graphviz and matplotlib
- **Pipeline Analysis**: Cycle detection, impact analysis, and health checks
- **Table Integration**: Uses existing `Table` class as graph nodes

## Quick Start

```python
from cht import Cluster

# Create cluster connection
cluster = Cluster(name="production", host="localhost", user="developer", password="developer")

# Get dependency graph for all databases
graph = cluster.get_dependency_graph()

# Analyze the graph
print(f"Found {len(graph.nodes)} tables and {len(graph.edges)} dependencies")

# Check for cycles (problematic circular dependencies)
cycles = graph.find_cycles()
if cycles:
    print(f"⚠️  Found {len(cycles)} circular dependencies!")

# Find tables with most dependencies
influential_tables = graph.get_most_influential_tables(limit=5)
for table, score in influential_tables:
    print(f"{table.fqdn}: {score} connections")
```

## API Reference

### Cluster.get_dependency_graph()

```python
def get_dependency_graph(
    self,
    databases: Optional[List[str]] = None,
    include_system: bool = False,
    include_temp: bool = False
) -> DependencyGraph
```

**Parameters:**
- `databases`: List of specific databases to analyze (None = all databases)
- `include_system`: Include system databases in analysis
- `include_temp`: Include temporary tables

**Returns:** `DependencyGraph` object representing the complete dependency structure

### DependencyGraph Class

#### Core Methods

```python
# Export to different formats
graph.to_networkx() -> nx.DiGraph
graph.to_graphml(filepath: str) -> None
graph.to_dot(filepath: str) -> None
graph.to_json() -> str

# Visualization
graph.visualize(output_path: str) -> None
graph.create_interactive_html(output_path: str) -> None

# Analysis
graph.find_cycles() -> List[List[Table]]
graph.calculate_depths() -> Dict[Table, int]
graph.get_most_influential_tables(limit: int = 10) -> List[Tuple[Table, float]]
graph.analyze_impact(table: Table) -> Dict[str, Any]
```

#### Properties

```python
graph.nodes: List[GraphNode]  # All table nodes
graph.edges: List[GraphEdge]  # All materialized view edges
graph.tables: List[Table]     # List of Table instances
```

### Table Extensions

New methods added to the `Table` class for dependency discovery:

```python
# Find materialized views that depend on this table
table.get_dependent_views() -> List[Tuple[str, str]]

# Find source tables this table depends on
table.get_source_tables() -> List[Tuple[str, str]]
```

## Graph Structure

### Nodes (GraphNode)
- **Represent**: ClickHouse tables and materialized views
- **Contains**: Table instance, metadata (row count, size, last modified)
- **Types**: Regular tables, materialized views, temporary tables

### Edges (GraphEdge)  
- **Represent**: Data flow through materialized views
- **Direction**: From source tables TO target tables
- **Contains**: Materialized view definition, dependency type, SQL query

## Use Cases

### 1. Pipeline Impact Analysis

```python
# Find all tables affected by changes to a source table
source_table = Table("raw_events", "analytics")
impact = graph.analyze_impact(source_table)

print(f"Downstream tables: {len(impact['downstream_tables'])}")
print(f"Materialized views: {len(impact['affected_views'])}")
```

### 2. Circular Dependency Detection

```python
cycles = graph.find_cycles()
for cycle in cycles:
    print("Circular dependency found:")
    for i, table in enumerate(cycle):
        next_table = cycle[(i + 1) % len(cycle)]
        print(f"  {table.fqdn} -> {next_table.fqdn}")
```

### 3. Data Pipeline Visualization

```python
# Create visual diagram
graph.visualize("pipeline_diagram.png")

# Create interactive HTML with zoom/pan
graph.create_interactive_html("pipeline.html")

# Export for external tools
graph.to_graphml("pipeline.graphml")  # Gephi, yEd
graph.to_dot("pipeline.dot")         # Graphviz
```

### 4. Performance Optimization

```python
# Find tables with deepest dependency chains
depths = graph.calculate_depths()
deepest_tables = sorted(depths.items(), key=lambda x: x[1], reverse=True)

for table, depth in deepest_tables[:5]:
    print(f"{table.fqdn}: depth {depth} (consider optimization)")
```

### 5. Data Governance

```python
# Find tables with no dependencies (potential candidates for archival)
orphaned_tables = [node.table for node in graph.nodes 
                   if not node.outgoing_edges and not node.incoming_edges]

# Find most critical tables (many dependents)
influential = graph.get_most_influential_tables(limit=10)
for table, score in influential:
    print(f"Critical table: {table.fqdn} (score: {score:.2f})")
```

## Visualization Examples

### Network Diagram
```python
import matplotlib.pyplot as plt

# Create network visualization
graph.visualize("network.png", layout="spring", node_size=1000)
```

### DOT/Graphviz Export
```python
# Export to Graphviz DOT format
graph.to_dot("pipeline.dot")

# Render with Graphviz (requires graphviz installed)
# dot -Tpng pipeline.dot -o pipeline.png
```

### Interactive HTML
```python
# Create interactive web visualization
graph.create_interactive_html("interactive_pipeline.html")
# Open in browser for zoom, pan, and click interactions
```

## Integration with Existing CHT Features

The graph mapping integrates seamlessly with existing CHT functionality:

```python
# Use with backup flows
for table in graph.get_most_influential_tables(limit=5):
    table_obj, _ = table
    backup = table_obj.backup_to_suffix("_critical_backup")
    print(f"Backed up critical table: {backup}")

# Combine with DataFrame operations
source_table = Table("events", "raw")
df = source_table.to_df(limit=1000)
# Analyze sample data before full pipeline changes

# Use with materialized view replay
target_table = Table("aggregated_events", "analytics") 
dependencies = target_table.get_source_tables()
print(f"Rebuilding {target_table.fqdn} requires: {dependencies}")
```

## Performance Considerations

- **Caching**: Graph structure is cached until explicitly refreshed
- **Incremental Updates**: Can update specific databases without full rescan
- **Memory Usage**: Large clusters with 1000+ tables use ~10-50MB RAM
- **Query Performance**: Discovery takes 1-5 seconds on typical clusters

## Error Handling

The graph mapping handles common edge cases:

- **Missing Tables**: Logs warnings for referenced but missing tables
- **Circular Dependencies**: Detected and reported (but doesn't break graph)
- **Malformed Views**: Skips views with unparseable CREATE statements
- **Permission Issues**: Gracefully handles databases with access restrictions

## Dependencies

Optional dependencies for enhanced functionality:

```bash
# For NetworkX export and advanced analysis
pip install networkx

# For Graphviz visualization  
pip install graphviz

# For interactive HTML visualizations
pip install matplotlib plotly
```

## Examples

See the included example files:
- `demo_graph_mapping.py` - Basic usage with live ClickHouse
- `demo_graph_with_mock.py` - Functionality demo with mock data
- `tests/test_graph.py` - Comprehensive test suite

## Future Enhancements

Planned features for future releases:
- Real-time dependency tracking
- Integration with ClickHouse system.query_log
- Performance metrics correlation
- Automated dependency health monitoring
- Integration with external metadata catalogs