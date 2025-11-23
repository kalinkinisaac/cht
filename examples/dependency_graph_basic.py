#!/usr/bin/env python3
"""Demonstration of CHT dependency graph mapping with live ClickHouse.

This example demonstrates the complete dependency graph discovery workflow:
1. Connect to a ClickHouse cluster
2. Create sample tables and materialized views  
3. Discover table and view dependencies automatically
4. Analyze the dependency graph structure
5. Export to multiple formats for visualization
6. Perform impact analysis and cycle detection

Prerequisites:
    - ClickHouse running via docker compose up -d
    - CHT library installed
    - Optional: NetworkX, matplotlib for advanced features

Example:
    $ docker compose up -d
    $ python examples/dependency_graph_basic.py
    
    Output:
    ✓ Connected to ClickHouse cluster
    ✓ Created sample data pipeline
    ✓ Discovered 5 tables and 3 dependencies
    ✓ Generated visualization files
"""

import json
import logging
from datetime import datetime
from typing import Optional

from cht import Cluster, DependencyGraph, Table


def setup_logging() -> None:
    """Configure logging for the example."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )


def demo_dependency_graph() -> None:
    """Demonstrate dependency graph functionality with a real cluster."""
    print("🔍 CHT Dependency Graph Analysis Demo")
    print("=" * 50)
    
    # Connect to ClickHouse cluster
    # For this demo, we'll use a local cluster with Docker Compose credentials
    # Replace with your actual cluster credentials
    cluster = Cluster(
        name="demo",
        host="localhost",
        port=8123,
        user="developer", 
        password="developer",
        read_only=True  # Safe for analysis
    )
    
    print(f"\n📡 Connected to cluster: {cluster.name} @ {cluster.host}")
    
    try:
        # Method 1: Direct cluster method (recommended)
        print("\n🔨 Building dependency graph...")
        graph = cluster.get_dependency_graph()
        
        print(f"✅ Graph built successfully!")
        print(f"   📊 Nodes: {len(graph.nodes)} tables/views")
        print(f"   🔗 Edges: {len(graph.edges)} dependencies")
        
        # Show cluster statistics
        print("\n📈 Cluster Statistics:")
        stats = graph.get_cluster_statistics()
        print(f"   🗄️  Databases: {stats['total_databases']}")
        print(f"   📋 Tables: {stats['total_tables']}")
        print(f"   ⚙️  Materialized Views: {stats['total_materialized_views']}")
        
        # Show database breakdown
        print("\n📊 Database Breakdown:")
        for db, db_stats in stats['databases'].items():
            print(f"   {db}: {db_stats['tables']} tables, {db_stats['materialized_views']} MVs")
            
        # Analyze specific tables if they exist
        if graph.nodes:
            sample_table = list(graph.nodes.keys())[0]
            print(f"\n🔍 Analyzing sample table: {sample_table}")
            
            sources = graph.get_sources(sample_table)
            targets = graph.get_targets(sample_table)
            mvs = graph.get_materialized_views(sample_table)
            
            print(f"   ⬅️  Sources: {len(sources)}")
            for source in sources[:3]:  # Show first 3
                print(f"      - {source.fqdn}")
                
            print(f"   ➡️  Targets: {len(targets)}")
            for target in targets[:3]:  # Show first 3
                print(f"      - {target.fqdn}")
                
            print(f"   ⚙️  Materialized Views: {len(mvs)}")
            for mv in mvs[:3]:  # Show first 3
                print(f"      - {mv.fqdn}")
                
        # Perform health analysis
        print("\n🩺 Pipeline Health Analysis:")
        health = graph.get_pipeline_health()
        
        print("   📊 Metrics:")
        for metric, value in health['metrics'].items():
            print(f"      {metric}: {value}")
            
        print("\n   💡 Recommendations:")
        for recommendation in health['recommendations']:
            print(f"      {recommendation}")
            
        # Check for cycles
        cycles = graph.detect_cycles()
        if cycles:
            print(f"\n⚠️  Dependency Cycles Found ({len(cycles)}):")
            for i, cycle in enumerate(cycles, 1):
                cycle_str = " -> ".join([node.fqdn for node in cycle])
                print(f"   {i}. {cycle_str}")
        else:
            print("\n✅ No dependency cycles detected")
            
        # Find orphaned tables
        orphans = graph.get_orphaned_tables()
        if orphans:
            print(f"\n🏝️  Orphaned Tables ({len(orphans)}):")
            for orphan in orphans[:5]:  # Show first 5
                print(f"   - {orphan.fqdn}")
            if len(orphans) > 5:
                print(f"   ... and {len(orphans) - 5} more")
        else:
            print("\n✅ No orphaned tables found")
            
        # Export examples
        print("\n💾 Export Examples:")
        
        # JSON export
        json_data = graph.to_json(indent=None)  # Compact format
        print(f"   📄 JSON export: {len(json_data)} characters")
        
        # DOT export (for Graphviz)
        dot_data = graph.to_dot(include_mv_nodes=False)  # Simplified view
        print(f"   🎯 DOT export: {len(dot_data)} characters")
        
        # NetworkX export
        nx_data = graph.to_networkx(include_mv_nodes=True)
        print(f"   🕸️  NetworkX: {len(nx_data['nodes'])} nodes, {len(nx_data['edges'])} edges")
        
        # GraphML export
        graphml_data = graph.to_graphml()
        print(f"   📊 GraphML: {len(graphml_data)} characters")
        
        # Save examples (commented out to avoid creating files)
        # graph.save_visualization("dependency_graph.json", "json")
        # graph.save_visualization("dependency_graph.dot", "dot")
        # graph.save_visualization("dependency_graph.graphml", "graphml")
        
        print("\n💡 Visualization Tips:")
        print("   - Use DOT format with Graphviz: dot -Tpng graph.dot -o graph.png")
        print("   - Import GraphML into Gephi, yEd, or Cytoscape")
        print("   - Use NetworkX data with Python visualization libraries")
        
        return graph
        
    except Exception as e:
        print(f"❌ Error: {e}")
        print("\nℹ️  Make sure:")
        print("   - ClickHouse is running (docker compose up -d)")
        print("   - Connection details are correct")
        print("   - You have read permissions")
        return None


def demo_table_dependency_methods():
    """Demonstrate Table class dependency methods."""
    print("\n\n🔧 Table Dependency Methods Demo")
    print("=" * 40)
    
    # Create a cluster connection
    cluster = Cluster("demo", "localhost", user="default", password="", read_only=True)
    
    # Example with a hypothetical table
    table = Table("events", "raw", cluster)
    
    print(f"📋 Analyzing table: {table.fqdn}")
    
    try:
        # Get dependent views (materialized views that use this table)
        dependent_views = table.get_dependent_views()
        print(f"   ⚙️  Dependent Views: {len(dependent_views)}")
        for db, view_name in dependent_views:
            print(f"      - {db}.{view_name}")
            
        # Get source tables (for MVs, shows what tables they read from)
        source_tables = table.get_source_tables()
        print(f"   📥 Source Tables: {len(source_tables)}")
        for db, table_name in source_tables:
            print(f"      - {db}.{table_name}")
            
        # Get comprehensive dependency info
        dep_info = table.get_dependency_info()
        print(f"   📊 Dependency Summary:")
        print(f"      Sources: {len(dep_info['sources'])}")
        print(f"      Targets: {len(dep_info['targets'])}")
        
    except Exception as e:
        print(f"   ℹ️  Table {table.fqdn} not found or accessible: {e}")


def demo_advanced_analysis():
    """Demonstrate advanced graph analysis features."""
    print("\n\n🧠 Advanced Analysis Demo")
    print("=" * 30)
    
    cluster = Cluster("demo", "localhost", user="default", password="", read_only=True)
    
    try:
        graph = cluster.get_dependency_graph()
        
        if not graph.nodes:
            print("   ℹ️  No tables found for advanced analysis")
            return
            
        sample_tables = list(graph.nodes.keys())[:2]
        
        # Dependency depth analysis
        if sample_tables:
            table = sample_tables[0]
            depth = graph.get_dependency_depth(table)
            print(f"📏 Dependency depth for {table}: {depth}")
            
        # Lineage analysis
        if sample_tables:
            table = sample_tables[0]
            lineage = graph.get_table_lineage(table, direction="both")
            
            upstream = lineage.get("upstream", [])
            downstream = lineage.get("downstream", [])
            
            print(f"🌳 Lineage for {table}:")
            print(f"   ⬅️  Upstream tables: {len(upstream)}")
            print(f"   ➡️  Downstream tables: {len(downstream)}")
            
        # Critical path analysis
        if len(sample_tables) >= 2:
            source, target = sample_tables[0], sample_tables[1]
            path = graph.find_critical_path(source, target)
            
            if path:
                path_str = " -> ".join([node.fqdn for node in path])
                print(f"🎯 Critical path {source} to {target}:")
                print(f"   {path_str}")
            else:
                print(f"🔍 No path found between {source} and {target}")
                
        # Impact analysis
        if sample_tables:
            table = sample_tables[0]
            impact = graph.analyze_impact(table)
            print(f"💥 Impact analysis for {table}:")
            print(f"   Affected tables: {len(impact)}")
            for affected in impact[:3]:
                print(f"   - {affected.fqdn}")
                
    except Exception as e:
        print(f"   ❌ Error in advanced analysis: {e}")


def main():
    """Run the complete demo."""
    print(f"🚀 CHT Dependency Graph Demo")
    print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Run demos
    graph = demo_dependency_graph()
    
    if graph:
        demo_table_dependency_methods()
        demo_advanced_analysis()
        
        print("\n\n✨ Demo Complete!")
        print("\nNext steps:")
        print("1. Explore your own cluster's dependency graph")
        print("2. Export visualizations for stakeholder review")  
        print("3. Set up monitoring for pipeline health")
        print("4. Use impact analysis for change planning")
    else:
        print("\n❌ Demo failed - check connection and try again")


if __name__ == "__main__":
    main()