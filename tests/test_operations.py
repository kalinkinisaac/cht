from __future__ import annotations

from unittest.mock import MagicMock, patch

from cht.operations import analyze_and_remove_duplicates, sync_missing_rows_by_date
from cht.table import Table


def make_cluster():
    cluster = MagicMock()
    cluster.host = "host"
    cluster.user = "user"
    cluster.password = "pwd"
    cluster.read_only = False
    return cluster


def test_sync_missing_rows_by_date_builds_expected_sql():
    origin_cluster = make_cluster()
    remote_cluster = make_cluster()

    origin_table = Table("default", "origin", cluster=origin_cluster)
    remote_table = Table("default", "dest", cluster=remote_cluster)

    with patch.object(origin_table, "get_time_column", return_value="event_date"):
        with patch.object(origin_table, "get_columns", return_value=["id", "value"]):
            with patch.object(origin_table, "remote", return_value="remote_expr"):
                sync_missing_rows_by_date(
                    origin_table,
                    remote_table,
                    date_filter="$time >= '2024-01-01'",
                    test_run=False,
                    delete_missing_rows=True,
                )

    # Two statements should be executed: INSERT and DELETE
    assert remote_cluster.query.call_count == 2
    insert_sql = remote_cluster.query.call_args_list[0][0][0]
    assert "remote_expr" in insert_sql
    assert "event_date" in insert_sql


def test_analyze_and_remove_duplicates_returns_stats():
    cluster = make_cluster()
    table = Table("default", "events", cluster=cluster)

    with patch.object(table, "get_time_column", return_value="event_date"):
        with patch.object(table, "get_columns", return_value=["id", "value"]):
            cluster.query.side_effect = [
                [(10,)],  # total rows
                [(8,)],  # unique rows
                None,  # delete query (ignored)
            ]
            stats = analyze_and_remove_duplicates(
                table,
                date="2024-01-01",
                test_run=False,
                remove_duplicates=True,
            )

    assert stats["total_rows"] == 10
    assert stats["duplicate_rows"] == 2
    assert cluster.query.call_count == 3
