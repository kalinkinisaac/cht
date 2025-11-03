"""
Temporary table management utilities for ClickHouse.

This module provides functions to create, manage, and clean up temporary tables
with TTL-based expiration using table comments.
"""

from __future__ import annotations

import re
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Optional, Union

import pandas as pd


def quote_identifier(name: str) -> str:
    """Quote ClickHouse identifier if needed."""
    # Always quote identifiers for consistency
    return f"`{name.replace('`', '``')}`"


# Regular expression to match expires_at timestamp in table comments
_EXPIRES_RE = re.compile(r"expires_at=(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)")


def parse_expires_at(comment: Optional[str]) -> Optional[datetime]:
    """
    Parse expires_at timestamp from table comment.

    Args:
        comment: Table comment string that may contain expires_at=YYYY-MM-DDTHH:MM:SSZ

    Returns:
        datetime object in UTC if found, None otherwise

    Examples:
        >>> parse_expires_at("expires_at=2023-12-25T10:30:00Z")
        datetime.datetime(2023, 12, 25, 10, 30, tzinfo=datetime.timezone.utc)

        >>> parse_expires_at("some other comment")
        None
    """
    if not comment:
        return None

    match = _EXPIRES_RE.search(comment)
    if not match:
        return None

    try:
        # Parse ISO format timestamp
        timestamp_str = match.group(1)
        return datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
    except ValueError:
        return None


def is_table_expired(comment: Optional[str], now: Optional[datetime] = None) -> bool:
    """
    Check if a table is expired based on its comment.

    Args:
        comment: Table comment string
        now: Current time (defaults to datetime.now(timezone.utc))

    Returns:
        True if table is expired, False otherwise

    Examples:
        >>> from datetime import datetime, timezone
        >>> now = datetime(2023, 12, 25, 12, 0, tzinfo=timezone.utc)
        >>> is_table_expired("expires_at=2023-12-25T10:30:00Z", now)
        True

        >>> is_table_expired("expires_at=2023-12-25T14:30:00Z", now)
        False
    """
    if now is None:
        now = datetime.now(timezone.utc)

    expires_at = parse_expires_at(comment)
    if expires_at is None:
        return False

    return now >= expires_at


def format_expires_at(expires_at: datetime) -> str:
    """
    Format datetime as expires_at comment string.

    Args:
        expires_at: Expiration datetime in UTC

    Returns:
        Formatted string for table comment

    Examples:
        >>> from datetime import datetime, timezone
        >>> dt = datetime(2023, 12, 25, 10, 30, tzinfo=timezone.utc)
        >>> format_expires_at(dt)
        'expires_at=2023-12-25T10:30:00Z'
    """
    # Ensure datetime is in UTC and remove microseconds
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)
    elif expires_at.tzinfo != timezone.utc:
        expires_at = expires_at.astimezone(timezone.utc)

    expires_at = expires_at.replace(microsecond=0)
    return f"expires_at={expires_at.isoformat().replace('+00:00', 'Z')}"


def generate_temp_table_name(prefix: str = "tmp_") -> str:
    """
    Generate unique temporary table name.

    Args:
        prefix: Table name prefix

    Returns:
        Unique table name

    Examples:
        >>> name = generate_temp_table_name("test_")
        >>> name.startswith("test_")
        True
        >>> len(name) == len("test_") + 8  # prefix + 8 hex chars
        True
    """
    suffix = uuid.uuid4().hex[:8]
    return f"{prefix}{suffix}"


def create_temp_table_sql(
    query: str,
    table_name: str,
    database: str = "temp",
    ttl: Optional[timedelta] = None,
    order_by: Optional[Union[str, Iterable[str]]] = None,
    on_cluster: Optional[str] = None,
) -> tuple[str, Optional[str]]:
    """
    Generate SQL to create temporary table with TTL.

    Args:
        query: SELECT query to populate the table
        table_name: Name of the temporary table
        database: Database name
        ttl: Time to live (None for no expiration)
        order_by: ORDER BY columns
        on_cluster: Cluster name for distributed tables

    Returns:
        Tuple of (CREATE SQL, ALTER SQL for comment) or (CREATE SQL, None)

    Raises:
        ValueError: If query is not a SELECT statement or TTL is invalid
    """
    # Validate query is SELECT-like
    query = query.strip()
    if query.endswith(";"):
        query = query[:-1].strip()

    forbidden_pattern = re.compile(
        r"^\s*(INSERT|ALTER|DROP|TRUNCATE|OPTIMIZE|SYSTEM|KILL|RENAME|DETACH|"
        r"ATTACH|UPDATE|DELETE|GRANT|REVOKE|CREATE)\b",
        re.IGNORECASE,
    )

    if forbidden_pattern.match(query):
        raise ValueError("Query must be a SELECT statement")

    if not re.match(r"^\s*(SELECT|WITH)\b", query, re.IGNORECASE):
        raise ValueError("Query must be a SELECT statement")

    # Validate TTL
    if ttl is not None and ttl <= timedelta(0):
        raise ValueError("TTL must be positive")

    # Build ORDER BY clause
    if order_by is None:
        order_clause = "ORDER BY tuple()"
    elif isinstance(order_by, str):
        order_clause = f"ORDER BY {quote_identifier(order_by)}"
    else:
        quoted_cols = [quote_identifier(col) for col in order_by]
        if len(quoted_cols) == 1:
            order_clause = f"ORDER BY {quoted_cols[0]}"
        else:
            order_clause = f"ORDER BY tuple({', '.join(quoted_cols)})"

    # Build ON CLUSTER clause
    cluster_clause = f"ON CLUSTER {on_cluster}" if on_cluster else ""

    # Build CREATE TABLE SQL
    quoted_db = quote_identifier(database)
    quoted_table = quote_identifier(table_name)

    create_sql = f"""CREATE TABLE {quoted_db}.{quoted_table}
{cluster_clause}
ENGINE = MergeTree
{order_clause}
AS
{query}""".strip()

    # Build ALTER SQL for TTL comment if needed
    alter_sql = None
    if ttl is not None:
        expires_at = datetime.now(timezone.utc) + ttl
        comment = format_expires_at(expires_at)
        full_table_name = f"{quoted_db}.{quoted_table}"
        alter_sql = f"ALTER TABLE {full_table_name} {cluster_clause} MODIFY COMMENT '{comment}'"

    return create_sql, alter_sql


def get_expired_tables(
    cluster,
    database: str = "temp",
    now: Optional[datetime] = None,
    table_pattern: Optional[str] = None,
) -> pd.DataFrame:
    """
    Get list of expired tables in database.

    Args:
        cluster: Cluster connection object
        database: Database to check
        now: Current time (defaults to datetime.now(timezone.utc))
        table_pattern: Optional LIKE pattern for table names

    Returns:
        DataFrame with columns: table, comment, expires_at, expired
    """
    if now is None:
        now = datetime.now(timezone.utc)

    # Build query to get tables with comments
    where_clause = f"database = '{database}'"
    if table_pattern:
        where_clause += f" AND name LIKE '{table_pattern}'"

    sql = f"""
    SELECT
        name as table,
        comment,
        create_table_query
    FROM system.tables
    WHERE {where_clause}
    AND comment != ''
    ORDER BY name
    """

    result = cluster.query_raw(sql)
    if result is None or not result.result_rows:
        return pd.DataFrame(columns=["table", "comment", "expires_at", "expired"])

    # Parse results and check expiration
    tables_data = []
    for row in result.result_rows:
        table_name, comment, _ = row
        expires_at = parse_expires_at(comment)
        expired = is_table_expired(comment, now)

        tables_data.append(
            {
                "table": table_name,
                "comment": comment,
                "expires_at": expires_at,
                "expired": expired,
            }
        )

    return pd.DataFrame(tables_data)


def cleanup_expired_tables(
    cluster,
    database: str = "temp",
    table_pattern: Optional[str] = None,
    dry_run: bool = False,
    now: Optional[datetime] = None,
) -> dict[str, Any]:
    """
    Clean up expired temporary tables.

    Args:
        cluster: Cluster connection object
        database: Database to clean
        table_pattern: Optional LIKE pattern for table names
        dry_run: If True, only return what would be deleted

    Returns:
        Dictionary with cleanup results
    """
    # Get expired tables
    expired_df = get_expired_tables(cluster, database, now=now, table_pattern=table_pattern)
    expired_tables = expired_df[expired_df["expired"]]["table"].tolist()  # Use boolean indexing

    results = {
        "database": database,
        "total_tables_checked": len(expired_df),
        "expired_tables_found": len(expired_tables),
        "tables_deleted": [],
        "errors": [],
        "dry_run": dry_run,
    }

    if not expired_tables:
        return results

    if dry_run:
        results["tables_to_delete"] = expired_tables
        return results

    # Delete expired tables
    quoted_db = quote_identifier(database)

    for table_name in expired_tables:
        try:
            quoted_table = quote_identifier(table_name)
            drop_sql = f"DROP TABLE IF EXISTS {quoted_db}.{quoted_table}"
            cluster.query(drop_sql)
            results["tables_deleted"].append(table_name)
        except Exception as e:
            results["errors"].append({"table": table_name, "error": str(e)})

    return results
