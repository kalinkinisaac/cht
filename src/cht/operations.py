from __future__ import annotations

from time import time
from typing import Dict, List, Optional, Sequence

from .cluster import Cluster
from .sql_utils import format_identifier
from .table import Table


def rebuild_table_via_mv(
    cluster: Cluster,
    *,
    db: str,
    table: str,
    backup_suffix: str = "_backup",
    recreate_backup: bool = False,
    mv_db: Optional[str] = None,
    mv_name: Optional[str] = None,
    replay_from_db: Optional[str] = None,
    replay_from_table: Optional[str] = None,
    replay_select_sql: Optional[str] = None,
    truncate_mv_source_first: bool = False,
    where: Optional[str] = None,
) -> Dict[str, object]:
    """
    Orchestrate a full rebuild of ``db.table`` by replaying data through a materialised view.

    Returns a summary dictionary describing the actions taken.
    """
    tbl = Table(name=table, database=db, cluster=cluster)

    backup_name = tbl.backup_to_suffix(
        backup_suffix=backup_suffix,
        recreate=recreate_backup,
    )
    tbl.verify_backup(backup_suffix=backup_suffix, check_rows=True, check_columns=True)
    tbl.truncate()

    replay_info = tbl.repopulate_through_mv(
        replay_from_db=replay_from_db,
        replay_from_table=replay_from_table,
        replay_select_sql=replay_select_sql,
        mv_db=mv_db,
        mv_name=mv_name,
        truncate_mv_source_first=truncate_mv_source_first,
        where=where,
    )

    try:
        rows_after = cluster.query(
            f"SELECT count() FROM {format_identifier(db, table)}"
        )[0][0]
    except Exception:  # pragma: no cover - defensive for flaky clusters
        rows_after = None

    return {
        "backup_table": f"{db}.{backup_name}",
        "replay": replay_info,
        "rows_after": rows_after,
    }


def restore_table_from_backup(
    cluster: Cluster,
    *,
    db: str,
    table: str,
    backup_suffix: str = "_backup",
    drop_backup: bool = False,
) -> None:
    """Restore ``db.table`` in-place from its backup table."""
    Table(name=table, database=db, cluster=cluster).restore_from_backup(
        backup_suffix=backup_suffix,
        drop_backup=drop_backup,
    )


def run_queries_with_status(client, queries: Sequence[str]) -> None:
    """
    Execute ``ALTER``/``SYSTEM`` style statements in sequence while printing timing information.

    Parameters
    ----------
    client:
        An object exposing ``command(sql: str)`` – typically ``clickhouse_connect``'s client.
    queries:
        Iterable of SQL statements to run.
    """
    total = len(queries)
    print(f"Starting to run {total} queries...\n")

    for idx, query in enumerate(queries, 1):
        print(f"[{idx}/{total}] Running query:\n{query.strip()}")
        start = time()
        try:
            client.command(query)
            elapsed = round(time() - start, 2)
            print(f"[{idx}/{total}] ✅ Success ({elapsed}s)\n")
        except Exception as exc:  # pragma: no cover - interactive feedback
            print(f"[{idx}/{total}] ❌ Failed: {exc}\n")

    print("🎉 All queries processed.\n")


def sync_missing_rows_by_date(
    origin_table: Table,
    remote_table: Table,
    *,
    date_filter: str,
    test_run: bool = False,
    delete_missing_rows: bool = True,
) -> None:
    """
    Synchronise rows from ``origin_table`` into ``remote_table`` for the supplied date window.

    The ``date_filter`` parameter may include a ``$time`` placeholder that is replaced with the
    detected time column on ``origin_table``.
    """
    time_col = origin_table.get_time_column()
    if not time_col:
        raise RuntimeError(f"No time column found in table `{origin_table.fqdn}`")

    effective_filter = date_filter.replace("$time", time_col)
    columns = origin_table.get_columns()
    column_list = ", ".join(f"`{col}`" for col in columns)
    hash_expr = f"cityHash64({', '.join(f'`{col}`' for col in columns)})"

    source_expr = origin_table.remote()
    target_cluster = remote_table.cluster
    if not target_cluster:
        raise RuntimeError("remote_table requires a bound Cluster instance")

    insert_sql = f"""
    INSERT INTO {remote_table.fqdn} ({column_list})
    SELECT {column_list}
    FROM {source_expr}
    WHERE {effective_filter}
      AND {hash_expr} GLOBAL NOT IN (
          SELECT {hash_expr}
          FROM {remote_table.fqdn}
          WHERE {effective_filter}
      )
    """

    print(
        f"Running sync for `{origin_table.fqdn}` → `{remote_table.fqdn}` with filter `{effective_filter}`"
    )
    if test_run:
        print("Test run: INSERT query:")
        print(insert_sql)
    else:
        target_cluster.query(insert_sql)
        print("Insert sync complete.")

    if delete_missing_rows:
        delete_sql = f"""
        ALTER TABLE {remote_table.fqdn}
        DELETE WHERE {effective_filter}
          AND {hash_expr} GLOBAL NOT IN (
              SELECT {hash_expr}
              FROM {origin_table.fqdn}
              WHERE {effective_filter}
          )
        """
        if test_run:
            print("Test run: DELETE query:")
            print(delete_sql)
        else:
            target_cluster.query(delete_sql)
            print("Delete sync complete.")


def analyze_and_remove_duplicates(
    table: Table,
    *,
    date: str,
    test_run: bool = False,
    remove_duplicates: bool = False,
) -> Dict[str, float]:
    """
    Inspect duplicate rows for a given date and optionally purge them using ``ALTER .. DELETE``.

    Returns statistics about total and duplicate counts to aid reporting.
    """
    time_col = table.get_time_column()
    if not time_col:
        raise RuntimeError(f"No time column found in table `{table.fqdn}`")

    cluster = table.cluster
    if not cluster:
        raise RuntimeError("Table requires a bound Cluster instance")
    columns = table.get_columns()
    hash_expr = f"cityHash64({', '.join(f'`{col}`' for col in columns)})"
    date_filter = f"toDate({time_col}) = toDate('{date}')"

    total_rows = cluster.query(
        f"SELECT count() FROM {table.fqdn} WHERE {date_filter}"
    )[0][0]
    unique_rows = cluster.query(
        f"SELECT count(DISTINCT {hash_expr}) FROM {table.fqdn} WHERE {date_filter}"
    )[0][0]
    duplicate_rows = total_rows - unique_rows
    duplicate_pct = (duplicate_rows / total_rows * 100) if total_rows else 0.0

    print(f"Date: {date}")
    print(f"Total rows: {total_rows}")
    print(f"Unique rows (by hash): {unique_rows}")
    print(f"Duplicate rows: {duplicate_rows}")
    print(f"Percentage duplicates: {duplicate_pct:.2f}%")

    if remove_duplicates and duplicate_rows > 0:
        if cluster.read_only:
            raise PermissionError("Cannot remove duplicates: cluster is read-only.")

        delete_sql = f"""
        ALTER TABLE {table.fqdn}
        DELETE WHERE {date_filter} AND
          {hash_expr} IN (
            SELECT hash_val FROM (
              SELECT {hash_expr} AS hash_val
              FROM {table.fqdn}
              WHERE {date_filter}
              GROUP BY hash_val
              HAVING count() > 1
            )
          )
          AND tuple({', '.join(f'`{col}`' for col in columns)}) NOT IN (
            SELECT tuple({', '.join(f'`{col}`' for col in columns)})
            FROM (
              SELECT *, row_number() OVER (PARTITION BY {hash_expr} ORDER BY {time_col}) AS rn
              FROM {table.fqdn}
              WHERE {date_filter}
            )
            WHERE rn = 1
          )
        """

        if test_run:
            print("Test run: DELETE duplicates query:")
            print(delete_sql)
        else:
            cluster.query(delete_sql)
            print("Duplicate rows removed.")

    return {
        "total_rows": total_rows,
        "unique_rows": unique_rows,
        "duplicate_rows": duplicate_rows,
        "duplicate_pct": duplicate_pct,
    }
