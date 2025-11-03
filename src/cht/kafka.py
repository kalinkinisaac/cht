from __future__ import annotations

import difflib
import re
from typing import Dict, List, Tuple

from .cluster import Cluster


def get_kafka_tables(cluster: Cluster) -> List[Tuple[str, str]]:
    """Return ``(database, table)`` tuples for all Kafka engine tables."""
    sql = """
    SELECT database, name
    FROM system.tables
    WHERE engine = 'Kafka'
    """
    return [tuple(row) for row in cluster.query(sql)]


def show_kafka_create_statements(cluster: Cluster) -> Dict[Tuple[str, str], str]:
    """Fetch ``SHOW CREATE TABLE`` statements for every Kafka table."""
    statements: Dict[Tuple[str, str], str] = {}
    for db, table in get_kafka_tables(cluster):
        fqdn = f"{db}.{table}"
        result = cluster.query(f"SHOW CREATE TABLE {fqdn}")
        statements[(db, table)] = result[0][0]
    return statements


def generate_kafka_consumer_group_update(
    create_statement: str,
    *,
    new_group: str,
) -> str:
    """
    Produce a new CREATE TABLE statement with the ``kafka_group_name`` replaced.

    Raises ``ValueError`` if the statement does not contain that setting.
    """
    updated = re.sub(
        r"(kafka_group_name\s*=\s*)'[^']+'",
        rf"\1'{new_group}'",
        create_statement,
    )
    if updated == create_statement:
        raise ValueError("Statement does not contain kafka_group_name setting")
    return updated


def batch_update_consumer_groups(
    cluster: Cluster,
    updates: Dict[Tuple[str, str], str],
) -> Dict[Tuple[str, str], str]:
    """
    Return updated DDL statements for the supplied Kafka tables without applying them.
    """
    original_statements = show_kafka_create_statements(cluster)
    result: Dict[Tuple[str, str], str] = {}
    for key, new_group in updates.items():
        create_stmt = original_statements[key]
        result[key] = generate_kafka_consumer_group_update(create_stmt, new_group=new_group)
    return result


def replace_kafka_consumer_groups(
    cluster: Cluster,
    *,
    new_group_name: str,
    test_run: bool = True,
) -> List[Tuple[str, str, str]]:
    """
    Replace ``kafka_group_name`` for all Kafka engine tables.

    Returns a log of operations ``(database, table, action)`` performed.
    """
    operations: List[Tuple[str, str, str]] = []
    for db, table in get_kafka_tables(cluster):
        fqdn = f"{db}.{table}"
        create_stmt = cluster.query(f"SHOW CREATE TABLE {fqdn}")[0][0]
        try:
            new_stmt = generate_kafka_consumer_group_update(create_stmt, new_group=new_group_name)
        except ValueError:
            operations.append((db, table, "skipped:no-group"))
            continue

        if test_run:
            operations.append((db, table, "test"))
            continue

        cluster.query(f"DROP TABLE {fqdn}")
        cluster.query(new_stmt)
        operations.append((db, table, "replaced"))
    return operations


def diff_line_chars(line1: str, line2: str) -> List[str]:
    """
    Highlight character-level differences between two lines.
    """
    diff = difflib.ndiff([line1], [line2])
    highlights: List[str] = []
    for entry in diff:
        if entry.startswith(("- ", "+ ", "? ")):
            highlights.append(entry)
    return highlights


def compare_kafka_tables_inline(
    cluster_a: Cluster, cluster_b: Cluster
) -> Dict[str, Dict[str, List[str]]]:
    """
    Compare Kafka tables between two clusters and return character-level diffs.

    The result dictionary contains keys ``only_in_a``, ``only_in_b`` and ``diffs``.
    """
    result: Dict[str, Dict[str, List[str]]] = {
        "only_in_a": {},
        "only_in_b": {},
        "diffs": {},
    }

    sql = "SELECT database, name FROM system.tables WHERE engine = 'Kafka'"
    tables_a = {tuple(row) for row in cluster_a.query(sql)}
    tables_b = {tuple(row) for row in cluster_b.query(sql)}

    for db, table in sorted(tables_a - tables_b):
        result["only_in_a"][f"{db}.{table}"] = []
    for db, table in sorted(tables_b - tables_a):
        result["only_in_b"][f"{db}.{table}"] = []

    for db, table in sorted(tables_a & tables_b):
        fqdn = f"{db}.{table}"
        stmt_a = cluster_a.query(f"SHOW CREATE TABLE {fqdn}")[0][0]
        stmt_b = cluster_b.query(f"SHOW CREATE TABLE {fqdn}")[0][0]

        if stmt_a == stmt_b:
            continue

        diffs: List[str] = []
        lines_a = stmt_a.splitlines()
        lines_b = stmt_b.splitlines()
        max_len = max(len(lines_a), len(lines_b))

        for idx in range(max_len):
            line_a = lines_a[idx] if idx < len(lines_a) else ""
            line_b = lines_b[idx] if idx < len(lines_b) else ""
            if line_a != line_b:
                diffs.append(f"- {line_a}")
                diffs.append(f"+ {line_b}")
                diffs.extend(diff_line_chars(line_a, line_b))

        result["diffs"][fqdn] = diffs

    return result
