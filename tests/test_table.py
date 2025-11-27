from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from cht.table import Table


def make_cluster(responses):
    """
    Utility to create a MagicMock query method that consumes responses sequentially.
    """
    mock = MagicMock()
    mock.query.side_effect = list(responses)
    mock.read_only = False
    mock.host = "host"
    mock.user = "user"
    mock.password = "pwd"
    mock.port = 9000
    return mock


def test_table_exists_true():
    cluster = make_cluster([[(1,)]])
    table = Table("default", "events", cluster=cluster)
    assert table.exists() is True
    cluster.query.assert_called_with("EXISTS TABLE default.events")


def test_backup_to_suffix_recreates_when_exists():
    cluster = make_cluster(
        [
            [(1,)],  # exists check
            [],  # drop
            [],  # create
            [],  # insert
        ]
    )
    table = Table("default", "events", cluster=cluster)
    backup_name = table.backup_to_suffix(recreate=True)
    assert backup_name == "events_backup"
    drop_sql = cluster.query.call_args_list[1][0][0]
    assert "DROP TABLE" in drop_sql
    create_sql = cluster.query.call_args_list[2][0][0]
    assert "CREATE TABLE" in create_sql


def test_verify_backup_passes_when_matching():
    cluster = make_cluster(
        [
            [("col1", "UInt32")],  # describe original
            [("col1", "UInt32")],  # describe backup
            [(5,)],  # count original
            [(5,)],  # count backup
        ]
    )
    table = Table("default", "events", cluster=cluster)
    table.verify_backup()


def test_repopulate_through_mv_from_table():
    cluster = make_cluster(
        [
            [("id",)],  # describe mv source
            [("id",)],  # describe source table
            [(10,)],  # count from source
            [],  # insert
        ]
    )
    table = Table("default", "events", cluster=cluster)
    with patch.object(table, "find_targeting_mvs", return_value=[("raw", "mv_events")]):
        with patch.object(table, "find_mv_sources", return_value=[("raw", "source")]):
            result = table.repopulate_through_mv(
                replay_from_db="raw",
                replay_from_table="source",
            )

    assert result["mv"] == ("raw", "mv_events")
    assert result["estimated_rows_replayed"] == 10


def test_remote_expression_uses_cluster_credentials():
    cluster = make_cluster([])
    table = Table("default", "events", cluster=cluster)
    remote_expr = table.remote(port=9100)
    assert remote_expr == "remote('host', default.events, 'user', 'pwd', 9100)"


def test_table_to_df():
    """Test Table.to_df() method."""
    mock_client = MagicMock()
    expected_df = pd.DataFrame({"id": [1, 2, 3], "name": ["A", "B", "C"]})
    mock_client.query_df.return_value = expected_df

    cluster = make_cluster([])
    cluster.client = mock_client

    table = Table("test", "users", cluster=cluster)
    result_df = table.to_df()

    mock_client.query_df.assert_called_once_with("SELECT * FROM test.users")
    assert result_df.equals(expected_df)


def test_table_from_df_overwrite_mode():
    """Test Table.from_df() with overwrite mode."""
    df = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
        }
    )

    # Create cluster mock with sufficient responses
    cluster = MagicMock()
    cluster.query.return_value = None  # For all query calls

    # Mock the client for insert operations
    mock_client = MagicMock()
    cluster.client = mock_client

    # Mock table existence check
    with patch.object(Table, "exists", return_value=True):
        table = Table.from_df(
            df,
            database="temp",
            name="test_table",
            cluster=cluster,
            mode="overwrite",
            engine="MergeTree",
            order_by=["id"],
        )

    # Verify the table was created
    assert table.name == "test_table"
    assert table.database == "temp"
    assert table.cluster == cluster

    # Verify cluster.query was called (for DROP TABLE and CREATE TABLE)
    assert cluster.query.call_count >= 1


def test_table_from_df_append_mode():
    """Test Table.from_df() with append mode."""
    df = pd.DataFrame(
        {
            "id": [4, 5, 6],
            "name": ["Diana", "Eve", "Frank"],
        }
    )

    cluster = MagicMock()
    cluster.query.return_value = None
    mock_client = MagicMock()
    cluster.client = mock_client

    # Mock table existence check
    with patch.object(Table, "exists", return_value=False):
        table = Table.from_df(
            df,
            database="temp",
            name="existing_table",
            cluster=cluster,
            mode="append",
            create_if_not_exists=True,
        )

    assert table.name == "existing_table"
    assert table.database == "temp"


def test_table_from_df_auto_generated_name():
    """Test Table.from_df() with auto-generated table name."""
    df = pd.DataFrame({"id": [1, 2, 3]})
    cluster = MagicMock()
    cluster.query.return_value = None
    mock_client = MagicMock()
    cluster.client = mock_client

    with patch.object(Table, "exists", return_value=False):
        table = Table.from_df(df, cluster=cluster, mode="overwrite")

    # Should have generated a temp name
    assert table.name.startswith("temp_")
    assert table.database == "temp"  # default database


def test_table_from_df_requires_cluster():
    """Test that from_df raises error when cluster is None."""
    df = pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})

    # Clear any default cluster to ensure clean test
    from cht.table import Table

    Table.clear_default_cluster()

    with pytest.raises(RuntimeError, match="Table operation requires a cluster"):
        Table.from_df(df, cluster=None)


def test_table_from_df_invalid_mode():
    """Test that invalid mode raises ValueError."""
    df = pd.DataFrame({"id": [1, 2, 3]})
    cluster = make_cluster([])

    with pytest.raises(ValueError, match="mode must be 'overwrite' or 'append'"):
        Table.from_df(df, cluster=cluster, mode="invalid")
