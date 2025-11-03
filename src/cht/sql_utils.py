from __future__ import annotations

import re
from typing import Iterable, Optional, Sequence, Tuple, Union, cast

try:  # Optional dependency for typing hints only
    from clickhouse_connect.driver.client import Client
except Exception:  # pragma: no cover - clickhouse_connect not installed in tests
    Client = object  # type: ignore

if True:  # pragma: no cover - typing imports
    from typing import TYPE_CHECKING

    if TYPE_CHECKING:  # pragma: no cover
        from .cluster import Cluster


def format_identifier(database: str, table: str) -> str:
    """Return a quoted identifier `` `db`.`table` `` suitable for SQL strings."""
    return f"`{database}`.`{table}`"


def rows_to_list(rows: Optional[Iterable[Sequence]]) -> list[Tuple]:
    """Coerce the result_rows iterable into a list of tuples."""
    if not rows:
        return []
    return [tuple(row) for row in rows]


def extract_from_tables(sql_query: str) -> list[str]:
    """
    Parse the SQL string and report table names referenced in ``FROM``/``JOIN`` clauses.

    The parser is intentionally regex-based to remain dependency-free and handle
    the common cases encountered when auditing ClickHouse pipelines.
    """
    pattern = re.compile(
        r"(?:FROM|JOIN)\s+`?([\w\d_]+)`?(?:\.`?([\w\d_]+)`?)?",
        re.IGNORECASE,
    )

    tables: set[str] = set()
    for match in pattern.finditer(sql_query or ""):
        first, second = match.group(1), match.group(2)
        tables.add(f"{first}.{second}" if second else first)
    return sorted(tables)


def parse_to_table(create_query: str, default_db: Optional[str] = None) -> tuple[Optional[str], Optional[str]]:
    """
    Extract the ``TO`` table (database, table) tuple from a ``CREATE MATERIALIZED VIEW`` statement.
    """
    match = re.search(r"\bTO\s+`?([\w\d_]+)`?\.`?([\w\d_]+)`?", create_query, re.IGNORECASE)
    if match:
        return match.group(1), match.group(2)

    match = re.search(r"\bTO\s+`?([\w\d_]+)`?", create_query, re.IGNORECASE)
    if match:
        return default_db, match.group(1)

    return None, None


def parse_from_table(create_query: str) -> Optional[str]:
    """
    Extract the ``FROM`` table (possibly qualified) from a CREATE statement.
    """
    match = re.search(r"\bFROM\s+`?([\w\d_]+)`?\.`?([\w\d_]+)`?", create_query, re.IGNORECASE)
    if match:
        return f"{match.group(1)}.{match.group(2)}"

    match = re.search(r"\bFROM\s+`?([\w\d_]+)`?", create_query, re.IGNORECASE)
    if match:
        return match.group(1)

    return None


def generate_cityhash_query(
    columns: Sequence[str],
    *,
    table_expression: str,
    where: Optional[str] = None,
    distinct: bool = True,
) -> str:
    """
    Build a ``SELECT`` statement that hashes rows with ``cityHash64`` on the supplied columns.

    Parameters
    ----------
    columns:
        Real (non-computed) column names to include in the hash expression.
    table_expression:
        SQL expression or table name to select from.
    where:
        Optional WHERE clause appended verbatim.
    distinct:
        Whether to deduplicate hash values (default) or keep duplicates for debugging.
    """
    if not columns:
        raise ValueError("columns must not be empty when building a hash query")

    column_list = ", ".join(columns)
    select_prefix = "SELECT distinct" if distinct else "SELECT"
    query = (
        f"{select_prefix} cityHash64({column_list}) AS row_hash\n"
        f"FROM {table_expression}"
    )
    if where:
        query += f"\nWHERE {where}"
    return query


ConnectionLike = Union["Cluster", Client]


def get_table_columns(connection: ConnectionLike, table_name: str, database: str = "default") -> list[str]:
    """
    Retrieve physical column names for a ClickHouse table.

    Accepts either a :class:`cht.cluster.Cluster` or a raw ``clickhouse_connect`` client.
    """
    query = f"""
    SELECT name
    FROM system.columns
    WHERE database = '{database}'
      AND table = '{table_name}'
    ORDER BY position
    """

    if hasattr(connection, "query"):
        rows = connection.query(query)
    else:
        result = cast(Client, connection).query(query)
        rows = result.result_rows
    return [row[0] for row in rows]


def remote_expression(
    *,
    host: str,
    database: str,
    table: str,
    user: str = "default",
    password: str = "",
    port: int = 9000,
) -> str:
    """
    Construct a ClickHouse ``remote()`` table function expression.
    """
    return (
        f"remote('{host}', {database}.{table}, '{user}', '{password}', {port})"
    )
