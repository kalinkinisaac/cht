"""
Dependency graph functionality for CHT.

This module provides tools for discovering and analyzing table/view dependencies
in ClickHouse clusters, representing them as directed graphs where:
- Nodes are tables (including materialized views)
- Edges represent data flow via materialized views

The graph can be exported to various formats for visualization and analysis.
"""

from __future__ import annotations

import json
import logging
import warnings
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set, Tuple, Union

from .cluster import Cluster
from .sql_utils import parse_from_table, parse_to_table
from .table import Table

_logger = logging.getLogger("cht.graph")


@dataclass
class GraphNode:
    """
    Represents a table or materialized view as a node in the dependency graph.

    Each node wraps a Table instance and provides graph-specific functionality.
    """

    table: Table

    @property
    def fqdn(self) -> str:
        """Return fully qualified name (database.table)."""
        return self.table.fqdn

    @property
    def database(self) -> str:
        """Return database name."""
        return self.table.database

    @property
    def name(self) -> str:
        """Return table name."""
        return self.table.name

    def __str__(self) -> str:
        return self.fqdn

    def __repr__(self) -> str:
        return f"GraphNode({self.fqdn})"

    def __hash__(self) -> int:
        return hash(self.fqdn)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, GraphNode) and self.fqdn == other.fqdn


@dataclass
class GraphEdge:
    """
    Represents a dependency relationship between tables via a materialized view.

    The edge indicates data flows from source -> target through the materialized view.
    """

    source: GraphNode  # Source table that feeds the MV
    target: GraphNode  # Target table populated by the MV
    materialized_view: GraphNode  # The MV that creates the dependency
    view_type: str = "MaterializedView"  # Type of view (extensible)

    def __str__(self) -> str:
        return f"{self.source.fqdn} -> {self.target.fqdn} (via {self.materialized_view.fqdn})"

    def __repr__(self) -> str:
        return (
            f"GraphEdge(source={self.source.fqdn}, target={self.target.fqdn}, "
            f"mv={self.materialized_view.fqdn})"
        )


class DependencyGraph:
    """
    Discovers and represents table/view dependencies in a ClickHouse cluster.

    The graph maps data flow pipelines where:
    - Tables are nodes
    - Materialized views create directed edges between source and target tables
    - Multiple export formats support visualization and analysis

    Example:
        >>> cluster = Cluster("prod", "clickhouse.company.com")
        >>> graph = DependencyGraph(cluster)
        >>> graph.build()
        >>> print(f"Found {len(graph.nodes)} tables, {len(graph.edges)} dependencies")
        >>>
        >>> # Export for visualization
        >>> nx_data = graph.to_networkx()
        >>> json_data = graph.to_json()
        >>> dot_graph = graph.to_dot()
        >>>
        >>> # Analyze dependencies
        >>> affected = graph.analyze_impact("raw.events")
        >>> cycles = graph.detect_cycles()
    """

    def __init__(self, cluster: Cluster):
        """
        Initialize dependency graph for a ClickHouse cluster.

        Args:
            cluster: ClickHouse cluster to analyze
        """
        self.cluster = cluster
        self.nodes: Dict[str, GraphNode] = {}  # fqdn -> GraphNode
        self.edges: List[GraphEdge] = []
        self._built = False

    def build(self) -> None:
        """
        Discover all tables and dependencies to build the complete graph.

        This method:
        1. Discovers all tables and materialized views
        2. Creates nodes for each table
        3. Analyzes MV dependencies to create edges
        4. Handles missing tables gracefully with warnings
        """
        _logger.info("Building dependency graph for cluster %s", self.cluster.name)

        # Step 1: Discover all tables and MVs
        all_tables = self._get_all_tables()
        materialized_views = self._get_materialized_views()

        _logger.info(
            "Found %d tables, %d materialized views", len(all_tables), len(materialized_views)
        )

        # Step 2: Create nodes for all tables (including MVs)
        for database, table_name, engine in all_tables:
            table_obj = Table(name=table_name, database=database, cluster=self.cluster)
            node = GraphNode(table_obj)
            self.nodes[node.fqdn] = node

        # Step 3: Analyze MV dependencies and create edges
        for mv_database, mv_name in materialized_views:
            self._process_materialized_view(mv_database, mv_name)

        self._built = True
        _logger.info("Graph built: %d nodes, %d edges", len(self.nodes), len(self.edges))

    def _get_all_tables(self) -> List[Tuple[str, str, str]]:
        """
        Get all tables and materialized views from the cluster.

        Returns:
            List of (database, table_name, engine) tuples
        """
        sql = """
        SELECT database, name, engine
        FROM system.tables 
        WHERE database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
        ORDER BY database, name
        """

        results = self.cluster.query(sql)
        return [(row[0], row[1], row[2]) for row in results] if results else []

    def _get_materialized_views(self) -> List[Tuple[str, str]]:
        """
        Get only materialized views from the cluster.

        Returns:
            List of (database, view_name) tuples
        """
        sql = """
        SELECT database, name
        FROM system.tables 
        WHERE engine = 'MaterializedView'
          AND database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
        ORDER BY database, name
        """

        results = self.cluster.query(sql)
        return [(row[0], row[1]) for row in results] if results else []

    def _process_materialized_view(self, mv_database: str, mv_name: str) -> None:
        """
        Process a single materialized view to extract dependencies.

        Args:
            mv_database: Database containing the MV
            mv_name: Name of the materialized view
        """
        dependencies = self._get_view_dependencies(mv_database, mv_name)

        if not dependencies:
            _logger.warning("No dependencies found for MV %s.%s", mv_database, mv_name)
            return

        # Parse dependencies to find source and target tables
        sources = []
        targets = []

        for dep_database, dep_table in dependencies:
            dep_fqdn = f"{dep_database}.{dep_table}"

            if dep_fqdn in self.nodes:
                node = self.nodes[dep_fqdn]

                # Heuristic: if it's the MV itself, skip
                if dep_database == mv_database and dep_table == mv_name:
                    continue

                # Check if this dependency is a target (TO clause) or source (FROM clause)
                if self._is_mv_target(mv_database, mv_name, dep_database, dep_table):
                    targets.append(node)
                else:
                    sources.append(node)
            else:
                if self._is_mv_target(mv_database, mv_name, dep_database, dep_table):
                    warnings.warn(
                        f"Target table {dep_fqdn} not found for MV {mv_database}.{mv_name}",
                        UserWarning,
                    )
                else:
                    warnings.warn(
                        f"Source table {dep_fqdn} not found for MV {mv_database}.{mv_name}",
                        UserWarning,
                    )

        # Create MV node
        mv_fqdn = f"{mv_database}.{mv_name}"
        mv_node = self.nodes.get(mv_fqdn)
        if not mv_node:
            _logger.warning("MV node %s not found in nodes", mv_fqdn)
            return

        # Create edges for each source->target combination
        for source in sources:
            for target in targets:
                edge = GraphEdge(
                    source=source,
                    target=target,
                    materialized_view=mv_node,
                    view_type="MaterializedView",
                )
                self.edges.append(edge)
                _logger.debug("Created edge: %s", edge)

    def _get_view_dependencies(self, mv_database: str, mv_name: str) -> List[Tuple[str, str]]:
        """
        Get dependencies for a materialized view using system.dependencies.

        Args:
            mv_database: Database containing the MV
            mv_name: Name of the materialized view

        Returns:
            List of (database, table) tuples that the MV depends on
        """
        sql = f"""
        SELECT depends_on_database, depends_on_table
        FROM system.dependencies  
        WHERE database = '{mv_database}' 
          AND table = '{mv_name}'
          AND depends_on_database != ''
          AND depends_on_table != ''
        """

        results = self.cluster.query(sql)
        return [(row[0], row[1]) for row in results] if results else []

    def _is_mv_target(
        self, mv_database: str, mv_name: str, dep_database: str, dep_table: str
    ) -> bool:
        """
        Determine if a dependency is a target table (TO clause) or source table.

        This uses heuristics based on the MV's CREATE statement to identify
        whether a dependent table is the target (populated by the MV) or
        a source (read by the MV).

        Args:
            mv_database: MV database
            mv_name: MV name
            dep_database: Dependent table database
            dep_table: Dependent table name

        Returns:
            True if dependency is a target table, False if source
        """
        try:
            # Get the CREATE statement for the MV
            sql = f"""
            SELECT create_table_query
            FROM system.tables
            WHERE database = '{mv_database}' AND name = '{mv_name}'
            """

            results = self.cluster.query(sql)
            if not results:
                return False

            create_query = results[0][0] if results[0] else ""

            # Parse TO clause to identify target
            to_database, to_table = parse_to_table(create_query, default_db=mv_database)

            if to_database == dep_database and to_table == dep_table:
                return True

            # If no TO clause found, use heuristics:
            # - Tables in same database as MV are more likely to be targets
            # - Tables with "agg", "summary", "mart" in name are likely targets
            if dep_database == mv_database:
                target_keywords = ["agg", "summary", "mart", "dim", "fact"]
                if any(keyword in dep_table.lower() for keyword in target_keywords):
                    return True

            return False

        except Exception as e:
            _logger.warning("Error determining MV target for %s.%s: %s", mv_database, mv_name, e)
            return False

    # ======================== Analysis Methods ========================

    def get_sources(self, table_fqdn: str) -> List[GraphNode]:
        """
        Get all source tables that feed into the specified table.

        Args:
            table_fqdn: Fully qualified table name (database.table)

        Returns:
            List of source table nodes
        """
        sources = []
        for edge in self.edges:
            if edge.target.fqdn == table_fqdn:
                sources.append(edge.source)
        return sources

    def get_targets(self, table_fqdn: str) -> List[GraphNode]:
        """
        Get all target tables that depend on the specified table.

        Args:
            table_fqdn: Fully qualified table name (database.table)

        Returns:
            List of target table nodes
        """
        targets = []
        for edge in self.edges:
            if edge.source.fqdn == table_fqdn:
                targets.append(edge.target)
        return targets

    def get_materialized_views(self, table_fqdn: str) -> List[GraphNode]:
        """
        Get all materialized views associated with the specified table.

        Args:
            table_fqdn: Fully qualified table name (database.table)

        Returns:
            List of materialized view nodes
        """
        mvs = []
        for edge in self.edges:
            if edge.source.fqdn == table_fqdn or edge.target.fqdn == table_fqdn:
                mvs.append(edge.materialized_view)
        return list(set(mvs))  # Remove duplicates

    def get_dependency_chain(self, source_fqdn: str, target_fqdn: str) -> List[GraphNode]:
        """
        Find dependency chain from source to target table.

        Args:
            source_fqdn: Source table FQDN
            target_fqdn: Target table FQDN

        Returns:
            List of nodes in the dependency chain, or empty if no path exists
        """
        # Simple implementation - could be enhanced with full path finding
        for edge in self.edges:
            if edge.source.fqdn == source_fqdn and edge.target.fqdn == target_fqdn:
                return [edge.source, edge.target]
        return []

    def analyze_impact(self, table_fqdn: str) -> List[GraphNode]:
        """
        Analyze impact of changes to a table - what would be affected.

        Args:
            table_fqdn: Table to analyze impact for

        Returns:
            List of nodes that would be affected by changes to the table
        """
        affected = set()

        # Add the table itself
        if table_fqdn in self.nodes:
            affected.add(self.nodes[table_fqdn])

        # Add directly dependent MVs and their targets
        for edge in self.edges:
            if edge.source.fqdn == table_fqdn:
                affected.add(edge.materialized_view)
                affected.add(edge.target)

        return list(affected)

    def get_dependency_depth(self, table_fqdn: str) -> int:
        """
        Calculate the maximum dependency depth for a table.

        Depth represents the longest chain of dependencies starting from this table.

        Args:
            table_fqdn: Table to calculate depth for

        Returns:
            Maximum dependency depth (0 for tables with no dependencies)
        """

        def calculate_depth(node_fqdn: str, visited: Set[str]) -> int:
            if node_fqdn in visited:
                return 0  # Avoid infinite recursion on cycles

            visited.add(node_fqdn)
            max_depth = 0

            # Find all targets of this node
            for edge in self.edges:
                if edge.source.fqdn == node_fqdn:
                    target_depth = calculate_depth(edge.target.fqdn, visited.copy())
                    max_depth = max(max_depth, target_depth + 1)

            return max_depth

        return calculate_depth(table_fqdn, set())

    def get_pipeline_health(self) -> Dict[str, Any]:
        """
        Analyze overall pipeline health and identify potential issues.

        Returns:
            Dictionary with health metrics and recommendations
        """
        # Basic metrics
        total_tables = len(
            [n for n in self.nodes.values() if not self._is_materialized_view_node(n)]
        )
        total_mvs = len([n for n in self.nodes.values() if self._is_materialized_view_node(n)])
        total_edges = len(self.edges)

        # Find problematic patterns
        cycles = self.detect_cycles()
        orphans = self.get_orphaned_tables()

        # Calculate complexity metrics
        max_depth = 0
        depth_distribution = {}

        for node in self.nodes.values():
            if not self._is_materialized_view_node(node):
                depth = self.get_dependency_depth(node.fqdn)
                max_depth = max(max_depth, depth)
                depth_distribution[depth] = depth_distribution.get(depth, 0) + 1

        # Find highly connected nodes (potential bottlenecks)
        node_connections = {}
        for node_fqdn in self.nodes:
            incoming = len(self.get_sources(node_fqdn))
            outgoing = len(self.get_targets(node_fqdn))
            node_connections[node_fqdn] = incoming + outgoing

        # Sort to find most connected
        highly_connected = sorted(node_connections.items(), key=lambda x: x[1], reverse=True)[:5]

        # Generate recommendations
        recommendations = []

        if len(cycles) > 0:
            recommendations.append(
                f"⚠️  Found {len(cycles)} dependency cycles - may cause processing deadlocks"
            )

        if len(orphans) > 10:
            recommendations.append(
                f"📋 {len(orphans)} orphaned tables - consider cleanup or documentation"
            )

        if max_depth > 5:
            recommendations.append(
                f"🔗 Maximum dependency depth is {max_depth} - consider simplifying pipelines"
            )

        if total_mvs > total_tables * 2:
            recommendations.append(
                f"⚙️  High MV to table ratio ({total_mvs}/{total_tables}) - may impact performance"
            )

        if not recommendations:
            recommendations.append("✅ No significant issues detected")

        return {
            "metrics": {
                "total_tables": total_tables,
                "total_materialized_views": total_mvs,
                "total_dependencies": total_edges,
                "dependency_cycles": len(cycles),
                "orphaned_tables": len(orphans),
                "max_dependency_depth": max_depth,
                "avg_connections_per_table": (
                    sum(node_connections.values()) / len(node_connections)
                    if node_connections
                    else 0
                ),
            },
            "depth_distribution": depth_distribution,
            "highly_connected_tables": highly_connected,
            "cycles": [{"tables": [n.fqdn for n in cycle]} for cycle in cycles],
            "orphaned_tables": [n.fqdn for n in orphans],
            "recommendations": recommendations,
        }

    def find_critical_path(self, source_fqdn: str, target_fqdn: str) -> List[GraphNode]:
        """
        Find the critical path between two tables in the dependency graph.

        Uses breadth-first search to find the shortest dependency path.

        Args:
            source_fqdn: Starting table FQDN
            target_fqdn: Target table FQDN

        Returns:
            List of nodes in the critical path, empty if no path exists
        """
        from collections import deque

        if source_fqdn not in self.nodes or target_fqdn not in self.nodes:
            return []

        # BFS to find shortest path
        queue = deque([(source_fqdn, [self.nodes[source_fqdn]])])
        visited = {source_fqdn}

        while queue:
            current_fqdn, path = queue.popleft()

            if current_fqdn == target_fqdn:
                return path

            # Explore neighbors
            for edge in self.edges:
                if edge.source.fqdn == current_fqdn:
                    neighbor_fqdn = edge.target.fqdn

                    if neighbor_fqdn not in visited:
                        visited.add(neighbor_fqdn)
                        new_path = path + [self.nodes[neighbor_fqdn]]
                        queue.append((neighbor_fqdn, new_path))

        return []  # No path found

    def get_table_lineage(
        self, table_fqdn: str, direction: str = "both"
    ) -> Dict[str, List[GraphNode]]:
        """
        Get complete lineage (upstream and/or downstream) for a table.

        Args:
            table_fqdn: Table to analyze lineage for
            direction: "upstream", "downstream", or "both"

        Returns:
            Dictionary with upstream and/or downstream node lists
        """
        result = {}

        if direction in ("upstream", "both"):
            upstream = set()

            def trace_upstream(node_fqdn: str, visited: Set[str]) -> None:
                if node_fqdn in visited:
                    return
                visited.add(node_fqdn)

                sources = self.get_sources(node_fqdn)
                for source in sources:
                    upstream.add(source)
                    trace_upstream(source.fqdn, visited)

            trace_upstream(table_fqdn, set())
            result["upstream"] = list(upstream)

        if direction in ("downstream", "both"):
            downstream = set()

            def trace_downstream(node_fqdn: str, visited: Set[str]) -> None:
                if node_fqdn in visited:
                    return
                visited.add(node_fqdn)

                targets = self.get_targets(node_fqdn)
                for target in targets:
                    downstream.add(target)
                    trace_downstream(target.fqdn, visited)

            trace_downstream(table_fqdn, set())
            result["downstream"] = list(downstream)

        return result

    def detect_cycles(self) -> List[List[GraphNode]]:
        """
        Detect cycles in the dependency graph.

        Returns:
            List of cycles, where each cycle is a list of nodes
        """
        # Build adjacency list for cycle detection
        adj_list: Dict[str, List[str]] = {}

        for edge in self.edges:
            source_fqdn = edge.source.fqdn
            target_fqdn = edge.target.fqdn

            if source_fqdn not in adj_list:
                adj_list[source_fqdn] = []
            adj_list[source_fqdn].append(target_fqdn)

        # DFS-based cycle detection
        visited = set()
        rec_stack = set()
        cycles = []

        def dfs(node: str, path: List[str]) -> None:
            visited.add(node)
            rec_stack.add(node)
            path.append(node)

            if node in adj_list:
                for neighbor in adj_list[node]:
                    if neighbor in rec_stack:
                        # Found cycle
                        cycle_start = path.index(neighbor)
                        cycle_nodes = [self.nodes[fqdn] for fqdn in path[cycle_start:]]
                        cycles.append(cycle_nodes)
                    elif neighbor not in visited:
                        dfs(neighbor, path.copy())

            rec_stack.remove(node)

        for node_fqdn in self.nodes:
            if node_fqdn not in visited:
                dfs(node_fqdn, [])

        return cycles

    def get_orphaned_tables(self) -> List[GraphNode]:
        """
        Find tables with no incoming or outgoing dependencies.

        Returns:
            List of orphaned table nodes
        """
        connected_nodes = set()

        for edge in self.edges:
            connected_nodes.add(edge.source.fqdn)
            connected_nodes.add(edge.target.fqdn)
            connected_nodes.add(edge.materialized_view.fqdn)

        orphans = []
        for fqdn, node in self.nodes.items():
            if fqdn not in connected_nodes:
                orphans.append(node)

        return orphans

    def filter_by_database(self, database: str) -> List[GraphNode]:
        """
        Filter graph nodes by database name.

        Args:
            database: Database name to filter by

        Returns:
            List of nodes in the specified database
        """
        return [node for node in self.nodes.values() if node.database == database]

    # ======================== Export Methods ========================

    def to_dict(self) -> Dict[str, Any]:
        """
        Export graph to dictionary format.

        Returns:
            Dictionary with nodes, edges, and metadata
        """
        nodes_data = []
        for node in self.nodes.values():
            nodes_data.append(
                {
                    "fqdn": node.fqdn,
                    "database": node.database,
                    "name": node.name,
                    "type": "table",  # Could be extended to distinguish table types
                }
            )

        edges_data = []
        for edge in self.edges:
            edges_data.append(
                {
                    "source": edge.source.fqdn,
                    "target": edge.target.fqdn,
                    "materialized_view": edge.materialized_view.fqdn,
                    "type": edge.view_type,
                }
            )

        return {
            "nodes": nodes_data,
            "edges": edges_data,
            "metadata": {
                "cluster": self.cluster.name,
                "total_nodes": len(self.nodes),
                "total_edges": len(self.edges),
                "built": self._built,
            },
        }

    def to_json(self, indent: Optional[int] = 2) -> str:
        """
        Export graph to JSON format.

        Args:
            indent: JSON indentation (None for compact format)

        Returns:
            JSON string representation
        """
        return json.dumps(self.to_dict(), indent=indent)

    def to_networkx(self, include_mv_nodes: bool = True) -> Dict[str, Any]:
        """
        Export graph in NetworkX-compatible format.

        Args:
            include_mv_nodes: Whether to include MV nodes or create direct table edges

        Returns:
            Dictionary with NetworkX-compatible nodes and edges
        """
        nodes_data = []
        edges_data = []

        if include_mv_nodes:
            # Include all nodes and preserve MV structure
            for node in self.nodes.values():
                nodes_data.append(
                    {
                        "id": node.fqdn,
                        "database": node.database,
                        "name": node.name,
                        "label": node.fqdn,
                    }
                )

            for edge in self.edges:
                # Source -> MV edge
                edges_data.append(
                    {
                        "source": edge.source.fqdn,
                        "target": edge.materialized_view.fqdn,
                        "type": "feeds",
                    }
                )
                # MV -> Target edge
                edges_data.append(
                    {
                        "source": edge.materialized_view.fqdn,
                        "target": edge.target.fqdn,
                        "type": "populates",
                    }
                )
        else:
            # Create direct edges between table nodes, exclude MVs
            table_nodes = {
                fqdn: node
                for fqdn, node in self.nodes.items()
                if not self._is_materialized_view_node(node)
            }

            for node in table_nodes.values():
                nodes_data.append(
                    {
                        "id": node.fqdn,
                        "database": node.database,
                        "name": node.name,
                        "label": node.fqdn,
                    }
                )

            for edge in self.edges:
                if edge.source.fqdn in table_nodes and edge.target.fqdn in table_nodes:
                    edges_data.append(
                        {
                            "source": edge.source.fqdn,
                            "target": edge.target.fqdn,
                            "materialized_view": edge.materialized_view.fqdn,
                            "type": edge.view_type,
                        }
                    )

        return {"nodes": nodes_data, "edges": edges_data}

    def to_dot(self, include_mv_nodes: bool = True) -> str:
        """
        Export graph to DOT (Graphviz) format.

        Args:
            include_mv_nodes: Whether to include MV nodes in the graph

        Returns:
            DOT format string
        """
        lines = ["digraph dependency_graph {"]
        lines.append("  rankdir=LR;")
        lines.append("  node [shape=box, style=filled];")
        lines.append("")

        if include_mv_nodes:
            # Add all nodes with different styles
            for node in self.nodes.values():
                node_id = node.fqdn.replace(".", "_").replace("-", "_")
                if self._is_materialized_view_node(node):
                    lines.append(f'  {node_id} [label="{node.fqdn}", fillcolor=lightblue];')
                else:
                    lines.append(f'  {node_id} [label="{node.fqdn}", fillcolor=lightgreen];')

            lines.append("")

            # Add edges
            for edge in self.edges:
                source_id = edge.source.fqdn.replace(".", "_").replace("-", "_")
                mv_id = edge.materialized_view.fqdn.replace(".", "_").replace("-", "_")
                target_id = edge.target.fqdn.replace(".", "_").replace("-", "_")

                lines.append(f'  {source_id} -> {mv_id} [label="feeds"];')
                lines.append(f'  {mv_id} -> {target_id} [label="populates"];')
        else:
            # Direct table-to-table edges
            table_nodes = {
                fqdn: node
                for fqdn, node in self.nodes.items()
                if not self._is_materialized_view_node(node)
            }

            for node in table_nodes.values():
                node_id = node.fqdn.replace(".", "_").replace("-", "_")
                lines.append(f'  {node_id} [label="{node.fqdn}", fillcolor=lightgreen];')

            lines.append("")

            for edge in self.edges:
                if edge.source.fqdn in table_nodes and edge.target.fqdn in table_nodes:
                    source_id = edge.source.fqdn.replace(".", "_").replace("-", "_")
                    target_id = edge.target.fqdn.replace(".", "_").replace("-", "_")
                    mv_label = edge.materialized_view.name

                    lines.append(f'  {source_id} -> {target_id} [label="{mv_label}"];')

        lines.append("}")
        return "\n".join(lines)

    def to_graphml(self) -> str:
        """
        Export graph to GraphML format (XML-based graph format).

        GraphML is supported by many graph analysis tools like Gephi, yEd, etc.

        Returns:
            GraphML XML string
        """
        lines = ['<?xml version="1.0" encoding="UTF-8"?>']
        lines.append('<graphml xmlns="http://graphml.graphdrawing.org/xmlns"')
        lines.append('         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"')
        lines.append('         xsi:schemaLocation="http://graphml.graphdrawing.org/xmlns')
        lines.append('         http://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd">')

        # Define attributes
        lines.append('  <key id="database" for="node" attr.name="database" attr.type="string"/>')
        lines.append(
            '  <key id="table_name" for="node" attr.name="table_name" attr.type="string"/>'
        )
        lines.append('  <key id="node_type" for="node" attr.name="node_type" attr.type="string"/>')
        lines.append('  <key id="edge_type" for="edge" attr.name="edge_type" attr.type="string"/>')
        lines.append(
            '  <key id="materialized_view" for="edge" attr.name="materialized_view" attr.type="string"/>'
        )
        lines.append("")

        lines.append('  <graph id="dependency_graph" edgedefault="directed">')

        # Add nodes
        for node in self.nodes.values():
            node_id = node.fqdn.replace(".", "_").replace("-", "_")
            node_type = "MaterializedView" if self._is_materialized_view_node(node) else "Table"

            lines.append(f'    <node id="{node_id}">')
            lines.append(f'      <data key="database">{node.database}</data>')
            lines.append(f'      <data key="table_name">{node.name}</data>')
            lines.append(f'      <data key="node_type">{node_type}</data>')
            lines.append("    </node>")

        # Add edges
        for i, edge in enumerate(self.edges):
            source_id = edge.source.fqdn.replace(".", "_").replace("-", "_")
            target_id = edge.target.fqdn.replace(".", "_").replace("-", "_")
            mv_fqdn = edge.materialized_view.fqdn

            lines.append(f'    <edge id="e{i}" source="{source_id}" target="{target_id}">')
            lines.append(f'      <data key="edge_type">{edge.view_type}</data>')
            lines.append(f'      <data key="materialized_view">{mv_fqdn}</data>')
            lines.append("    </edge>")

        lines.append("  </graph>")
        lines.append("</graphml>")

        return "\n".join(lines)

    def save_visualization(self, filepath: str, format_type: str = "json", **kwargs) -> None:
        """
        Save graph to file in specified format.

        Args:
            filepath: Output file path
            format_type: Export format ('json', 'dot', 'graphml')
            **kwargs: Additional arguments passed to format-specific methods
        """
        format_type = format_type.lower()

        if format_type == "json":
            content = self.to_json(**kwargs)
        elif format_type == "dot":
            content = self.to_dot(**kwargs)
        elif format_type == "graphml":
            content = self.to_graphml()
        else:
            raise ValueError(
                f"Unsupported format: {format_type}. " f"Supported formats: json, dot, graphml"
            )

        with open(filepath, "w", encoding="utf-8") as f:
            f.write(content)

        _logger.info("Graph saved to %s in %s format", filepath, format_type)

    def _is_materialized_view_node(self, node: GraphNode) -> bool:
        """Check if a node represents a materialized view."""
        # This could be enhanced by checking the engine type from system.tables
        # For now, we'll use a simple heuristic
        return any(edge.materialized_view.fqdn == node.fqdn for edge in self.edges)

    # ======================== Statistics Methods ========================

    def get_cluster_statistics(self) -> Dict[str, Any]:
        """
        Get comprehensive statistics about the cluster's table structure.

        Returns:
            Dictionary with cluster-wide statistics
        """
        # Get database list
        db_sql = """
        SELECT DISTINCT database 
        FROM system.tables 
        WHERE database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
        ORDER BY database
        """
        db_results = self.cluster.query(db_sql)
        databases = [row[0] for row in db_results] if db_results else []

        # Get table counts by database
        table_sql = """
        SELECT database, COUNT(*) as table_count
        FROM system.tables 
        WHERE database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
          AND engine != 'MaterializedView'
        GROUP BY database
        ORDER BY database
        """
        table_results = self.cluster.query(table_sql)
        table_counts = {row[0]: row[1] for row in table_results} if table_results else {}

        # Get MV counts by database
        mv_sql = """
        SELECT database, COUNT(*) as mv_count
        FROM system.tables 
        WHERE database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
          AND engine = 'MaterializedView'
        GROUP BY database
        ORDER BY database
        """
        mv_results = self.cluster.query(mv_sql)
        mv_counts = {row[0]: row[1] for row in mv_results} if mv_results else {}

        # Build statistics
        total_tables = sum(table_counts.values())
        total_mvs = sum(mv_counts.values())

        db_stats = {}
        for db in databases:
            db_stats[db] = {
                "tables": table_counts.get(db, 0),
                "materialized_views": mv_counts.get(db, 0),
            }

        return {
            "total_databases": len(databases),
            "total_tables": total_tables,
            "total_materialized_views": total_mvs,
            "databases": db_stats,
        }
