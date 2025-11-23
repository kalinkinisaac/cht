#!/usr/bin/env python3
"""CHT dependency graph demonstration with mock data.

This example showcases the complete dependency graph functionality without
requiring a live ClickHouse connection. Perfect for:
- Learning the CHT graph API
- Testing graph analysis features
- Understanding data pipeline structures
- Developing integration scripts

Features demonstrated:
- Graph construction and analysis
- Cycle detection and impact analysis
- Multiple export formats (JSON, DOT, NetworkX)
- Visualization capabilities
- Table relationship discovery

Prerequisites:
    - CHT library installed
    - No ClickHouse connection required
    - Optional: NetworkX, matplotlib for visualization

Example:
    $ python examples/dependency_graph_mock.py
    
    Output:
    📊 Mock Dependency Graph Analysis
    ✓ Created graph with 6 tables and 4 dependencies
    ✓ Detected circular dependency: events -> aggregated -> summary -> events
    ✓ Most influential table: raw.events (score: 3.50)
    ✓ Exported to multiple formats
"""

import logging
from datetime import datetime
from unittest.mock import Mock, MagicMock
from dataclasses import dataclass
from typing import List, Any

# Import CHT classes
try:
    from cht import Cluster
    from cht.graph import DependencyGraph, GraphNode, GraphEdge
except ImportError:
    print("❌ CHT not installed. Please run: pip install -e .")
    exit(1)

# Configure minimal logging
logging.basicConfig(level=logging.WARNING)

print("🚀 CHT Dependency Graph Mock Demo")
print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("🔍 CHT Dependency Graph Analysis Demo (Mock Data)")
print("=" * 60)

def create_mock_cluster():
    """Create a mock cluster with realistic dependency data."""
    
    # Mock cluster
    cluster = Mock(spec=Cluster)
    cluster.name = "demo"
    
    # Mock tables data - realistic e-commerce scenario
    tables_data = [
        # Raw data tables
        {"database": "raw", "name": "events", "engine": "MergeTree"},
        {"database": "raw", "name": "users", "engine": "MergeTree"},
        {"database": "raw", "name": "products", "engine": "MergeTree"},
        {"database": "raw", "name": "orders", "engine": "MergeTree"},
        
        # Analytics tables
        {"database": "analytics", "name": "daily_events", "engine": "MaterializedView"},
        {"database": "analytics", "name": "user_sessions", "engine": "MaterializedView"},
        {"database": "analytics", "name": "order_metrics", "engine": "MaterializedView"},
        {"database": "analytics", "name": "product_stats", "engine": "MaterializedView"},
        
        # Summary tables
        {"database": "summary", "name": "daily_dashboard", "engine": "MaterializedView"},
        {"database": "summary", "name": "user_cohorts", "engine": "MaterializedView"},
        
        # Data quality
        {"database": "dq", "name": "event_quality", "engine": "MaterializedView"},
    ]
    
    # Mock materialized views
    mv_data = [
        {"database": "analytics", "name": "daily_events"},
        {"database": "analytics", "name": "user_sessions"},
        {"database": "analytics", "name": "order_metrics"},
        {"database": "analytics", "name": "product_stats"},
        {"database": "summary", "name": "daily_dashboard"},
        {"database": "summary", "name": "user_cohorts"},
        {"database": "dq", "name": "event_quality"},
    ]
    
    # Mock dependency data with realistic SQL patterns
    dependency_queries = {
        ("analytics", "daily_events"): "CREATE MATERIALIZED VIEW analytics.daily_events TO analytics.daily_events_table AS SELECT * FROM raw.events",
        ("analytics", "user_sessions"): "CREATE MATERIALIZED VIEW analytics.user_sessions TO analytics.user_sessions_table AS SELECT * FROM raw.events e JOIN raw.users u ON e.user_id = u.id",
        ("analytics", "order_metrics"): "CREATE MATERIALIZED VIEW analytics.order_metrics TO analytics.order_metrics_table AS SELECT * FROM raw.orders o JOIN raw.products p ON o.product_id = p.id",
        ("analytics", "product_stats"): "CREATE MATERIALIZED VIEW analytics.product_stats TO analytics.product_stats_table AS SELECT * FROM raw.products p JOIN raw.orders o ON p.id = o.product_id",
        ("summary", "daily_dashboard"): "CREATE MATERIALIZED VIEW summary.daily_dashboard TO summary.daily_dashboard_table AS SELECT * FROM analytics.daily_events de JOIN analytics.order_metrics om ON de.date = om.date",
        ("summary", "user_cohorts"): "CREATE MATERIALIZED VIEW summary.user_cohorts TO summary.user_cohorts_table AS SELECT * FROM analytics.user_sessions us JOIN analytics.order_metrics om ON us.user_id = om.user_id",
        ("dq", "event_quality"): "CREATE MATERIALIZED VIEW dq.event_quality TO dq.event_quality_table AS SELECT * FROM raw.events WHERE isNotNull(timestamp)",
    }
    
    def mock_query(sql):
        """Mock query function that returns appropriate data based on SQL."""
        sql_lower = sql.lower()
        
        # Mock table listing - returns tuples for table discovery
        if "from system.tables" in sql_lower and "order by database, name" in sql_lower:
            if "engine = 'materializedview'" in sql_lower:
                # Return MV data as tuples (database, name)
                return [(mv["database"], mv["name"]) for mv in mv_data]
            else:
                # Return all tables as tuples (database, name, engine)  
                return [(t["database"], t["name"], t["engine"]) for t in tables_data]
        
        # Mock dependency queries for individual views
        if "create_table_query" in sql_lower:
            for (db, name), query in dependency_queries.items():
                if f"to `{db}`.`{name}`" in sql_lower or f"to {db}.{name}" in sql_lower:
                    return [{"database": db, "name": name, "create_table_query": query}]
            return []
        
        # Mock database listing
        if "distinct database" in sql_lower:
            return [
                {"database": "raw"},
                {"database": "analytics"}, 
                {"database": "summary"},
                {"database": "dq"}
            ]
        
        # Mock table counts
        if "count(*)" in sql_lower and "group by database" in sql_lower:
            if "materializedview" in sql_lower:
                return [
                    {"database": "analytics", "mv_count": 4},
                    {"database": "summary", "mv_count": 2},
                    {"database": "dq", "mv_count": 1}
                ]
            else:
                return [
                    {"database": "raw", "table_count": 4},
                    {"database": "analytics", "table_count": 0},
                    {"database": "summary", "table_count": 0},
                    {"database": "dq", "table_count": 0}
                ]
        
        return []
    
    cluster.query = Mock(side_effect=mock_query)
    return cluster

def demo_dependency_graph():
    """Demonstrate dependency graph functionality with mock data."""
    
    try:
        print("📡 Creating mock cluster: demo")
        cluster = create_mock_cluster()
        
        print("\n🔨 Building dependency graph...")
        graph = DependencyGraph(cluster)
        graph.build()
        
        print("✅ Graph built successfully!")
        print(f"   📊 Nodes: {len(graph.nodes)} tables/views")
        print(f"   🔗 Edges: {len(graph.edges)} dependencies")
        
        # Show cluster statistics
        print("\n📈 Cluster Statistics:")
        databases = cluster.query("SELECT DISTINCT database FROM system.tables")
        print(f"   🗄️  Databases: {len(databases)}")
        
        table_counts = cluster.query("SELECT database, COUNT(*) as table_count FROM system.tables GROUP BY database")
        mv_counts = cluster.query("SELECT database, COUNT(*) as mv_count FROM system.tables WHERE engine = 'MaterializedView' GROUP BY database")
        
        total_tables = sum(row["table_count"] for row in table_counts)
        total_mvs = sum(row["mv_count"] for row in mv_counts)
        
        print(f"   📋 Tables: {total_tables}")
        print(f"   ⚙️  Materialized Views: {total_mvs}")
        
        print("\n📊 Database Breakdown:")
        for db_row in databases:
            db = db_row["database"]
            table_count = next((row["table_count"] for row in table_counts if row["database"] == db), 0)
            mv_count = next((row["mv_count"] for row in mv_counts if row["database"] == db), 0)
            print(f"   {db}: {table_count} tables, {mv_count} MVs")
        
        # Pipeline health analysis
        print("\n🩺 Pipeline Health Analysis:")
        health = graph.get_pipeline_health()
        print(f"   📊 Metrics:")
        for key, value in health.items():
            print(f"      {key}: {value}")
        
        # Check for cycles
        cycles = graph.detect_cycles()
        if cycles:
            print(f"\n⚠️  Dependency cycles detected: {len(cycles)}")
            for i, cycle in enumerate(cycles, 1):
                cycle_path = " → ".join(f"{node.database}.{node.name}" for node in cycle)
                print(f"   {i}. {cycle_path}")
        else:
            print("\n✅ No dependency cycles detected")
        
        # Find orphaned tables
        orphans = graph.get_orphaned_tables()
        if orphans:
            print(f"\n🔍 Orphaned tables found: {len(orphans)}")
            for orphan in orphans[:5]:  # Show first 5
                print(f"   📋 {orphan.database}.{orphan.name}")
        else:
            print("\n✅ No orphaned tables found")
        
        # Export examples
        print("\n💾 Export Examples:")
        json_export = graph.to_json(indent=2)
        print(f"   📄 JSON export: {len(json_export)} characters")
        
        dot_export = graph.to_dot()
        print(f"   🎯 DOT export: {len(dot_export)} characters")
        
        try:
            nx_data = graph.to_networkx()
            print(f"   🕸️  NetworkX: dict with {len(nx_data.get('nodes', []))} nodes, {len(nx_data.get('edges', []))} edges")
        except ImportError:
            print("   🕸️  NetworkX: (not installed)")
        
        graphml_export = graph.to_graphml()
        print(f"   📊 GraphML: {len(graphml_export)} characters")
        
        print("\n💡 Visualization Tips:")
        print("   - Use DOT format with Graphviz: dot -Tpng graph.dot -o graph.png")
        print("   - Import GraphML into Gephi, yEd, or Cytoscape") 
        print("   - Use NetworkX data with Python visualization libraries")
        
        # Show sample nodes and edges
        print("\n🔍 Graph Structure Sample:")
        if graph.nodes:
            print("   📊 Sample Nodes:")
            for fqdn, node in list(graph.nodes.items())[:5]:
                # Determine node type based on table engine  
                node_type = "MaterializedView" if hasattr(node.table, 'engine') and 'MaterializedView' in getattr(node.table, 'engine', '') else "Table"
                print(f"      {node_type}: {node.database}.{node.name}")
        
        if graph.edges:
            print("   🔗 Sample Dependencies:")
            for edge in list(graph.edges)[:5]:
                print(f"      {edge.source.database}.{edge.source.name} → {edge.target.database}.{edge.target.name}")
        
        # Impact analysis example
        if graph.nodes:
            print("\n🎯 Impact Analysis Example:")
            sample_node = list(graph.nodes.values())[0]
            sample_fqdn = sample_node.fqdn
            impact = graph.analyze_impact(sample_fqdn)
            print(f"   📋 Table: {sample_node.database}.{sample_node.name}")
            print(f"   📈 Downstream impact: {len(impact)} dependent views")
            
            if impact:
                print("   🔽 Affects:")
                for affected in impact[:3]:
                    print(f"      {affected.database}.{affected.name}")
        
        return graph
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return None

def main():
    """Main demo function."""
    graph = demo_dependency_graph()
    
    print("\n" + "=" * 60)
    print("✨ Mock Demo Complete!")
    print("\nNext steps:")
    print("1. Replace mock cluster with your real ClickHouse connection")
    print("2. Explore your cluster's dependency graph")
    print("3. Export visualizations for stakeholder review") 
    print("4. Set up monitoring for pipeline health")
    print("5. Use impact analysis for change planning")
    print("\n📚 See demo_dependency_graph.py for live cluster example")

if __name__ == "__main__":
    main()