from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from cht.cluster import Cluster, is_mutating


def test_is_mutating_detects_writes():
    assert is_mutating("INSERT INTO t VALUES (1)")
    assert is_mutating(" alter table foo add column x UInt8")
    assert not is_mutating("SELECT * FROM system.tables")
    assert not is_mutating("   -- comment\nSELECT 1")


def test_cluster_query_select_uses_client_query():
    fake_result = MagicMock(result_rows=[("value",)], column_names=["col"])
    client = MagicMock()
    client.query.return_value = fake_result

    cluster = Cluster(
        name="test",
        host="localhost",
        client_factory=lambda **_: client,
    )

    rows = cluster.query("SELECT 1")
    assert rows == [("value",)]
    client.query.assert_called_once_with("SELECT 1")
    client.command.assert_not_called()


def test_cluster_query_mutation_honours_read_only():
    cluster = Cluster(
        name="ro",
        host="localhost",
        read_only=True,
        client_factory=lambda **_: MagicMock(),
    )

    with pytest.raises(PermissionError):
        cluster.query("INSERT INTO foo VALUES (1)")


def test_cluster_query_mutation_executes():
    client = MagicMock()
    cluster = Cluster(
        name="rw",
        host="localhost",
        read_only=False,
        client_factory=lambda **_: client,
    )

    cluster.query("INSERT INTO foo VALUES (1)")
    client.command.assert_called_once_with("INSERT INTO foo VALUES (1)")
