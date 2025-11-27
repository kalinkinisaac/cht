"""Tests for Table class default cluster functionality."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from cht.cluster import Cluster
from cht.table import Table


class TestTableDefaultCluster:
    """Test default cluster functionality in Table class."""

    def setup_method(self):
        """Reset default cluster before each test."""
        Table.clear_default_cluster()

    def teardown_method(self):
        """Clean up default cluster after each test."""
        Table.clear_default_cluster()

    def test_no_default_cluster_raises_error(self):
        """Test that operations without cluster raise helpful error."""
        table = Table("test_table", "test_db")

        with pytest.raises(RuntimeError, match="Table operation requires a cluster"):
            table._require_cluster()

    def test_instance_cluster_takes_precedence(self):
        """Test that instance cluster takes precedence over default."""
        default_cluster = MagicMock(spec=Cluster)
        instance_cluster = MagicMock(spec=Cluster)

        Table.set_default_cluster(default_cluster)
        table = Table("test_db", "test_table", cluster=instance_cluster)

        result = table._require_cluster()
        assert result is instance_cluster
        assert result is not default_cluster

    def test_default_cluster_used_when_no_instance_cluster(self):
        """Test that default cluster is used when no instance cluster."""
        default_cluster = MagicMock(spec=Cluster)

        Table.set_default_cluster(default_cluster)
        table = Table("test_table", "test_db")  # No cluster specified

        result = table._require_cluster()
        assert result is default_cluster

    def test_set_and_get_default_cluster(self):
        """Test setting and getting default cluster."""
        cluster = MagicMock(spec=Cluster)

        # Initially no default cluster
        assert Table.get_default_cluster() is None

        # Set default cluster
        Table.set_default_cluster(cluster)
        assert Table.get_default_cluster() is cluster

        # Clear default cluster
        Table.clear_default_cluster()
        assert Table.get_default_cluster() is None

    def test_default_cluster_shared_across_instances(self):
        """Test that default cluster is shared across all Table instances."""
        cluster = MagicMock(spec=Cluster)

        Table.set_default_cluster(cluster)

        table1 = Table("table1", "db1")
        table2 = Table("table2", "db2")

        assert table1._require_cluster() is cluster
        assert table2._require_cluster() is cluster

    def test_with_cluster_method_overrides_default(self):
        """Test that with_cluster method works with default cluster."""
        default_cluster = MagicMock(spec=Cluster)
        other_cluster = MagicMock(spec=Cluster)

        Table.set_default_cluster(default_cluster)

        table = Table("test_table", "test_db")
        assert table._require_cluster() is default_cluster

        # Create new table with different cluster
        table_with_other = table.with_cluster(other_cluster)
        assert table_with_other._require_cluster() is other_cluster

        # Original table still uses default
        assert table._require_cluster() is default_cluster

    def test_exists_method_uses_default_cluster(self):
        """Test that exists() method works with default cluster."""
        mock_cluster = MagicMock(spec=Cluster)
        mock_cluster.query.return_value = [[1]]  # Table exists

        Table.set_default_cluster(mock_cluster)
        table = Table("test_table", "test_db")

        result = table.exists()

        # Verify cluster.query was called
        mock_cluster.query.assert_called_once()
        call_args = mock_cluster.query.call_args[0][0]
        assert "EXISTS TABLE test_table.test_db" in call_args
        assert result is True

    def test_get_columns_uses_default_cluster(self):
        """Test that get_columns() method works with default cluster."""
        mock_cluster = MagicMock(spec=Cluster)
        mock_cluster.query.return_value = [["col1"], ["col2"], ["col3"]]

        Table.set_default_cluster(mock_cluster)
        table = Table("test_table", "test_db")

        columns = table.get_columns()

        # Verify cluster.query was called
        mock_cluster.query.assert_called_once()
        call_args = mock_cluster.query.call_args[0][0]
        assert "DESCRIBE TABLE test_table.test_db" in call_args
        assert columns == ["col1", "col2", "col3"]

    def test_real_cluster_integration(self):
        """Test with real Cluster objects (no actual connection)."""
        cluster1 = Cluster("cluster1", "host1")
        cluster2 = Cluster("cluster2", "host2")

        # Set default cluster
        Table.set_default_cluster(cluster1)

        # Create table without explicit cluster
        table = Table("events", "analytics")
        assert table._require_cluster() is cluster1

        # Create table with explicit cluster
        table_explicit = Table("system", "logs", cluster=cluster2)
        assert table_explicit._require_cluster() is cluster2

        # with_cluster should work
        table_switched = table.with_cluster(cluster2)
        assert table_switched._require_cluster() is cluster2
        assert table._require_cluster() is cluster1  # Original unchanged

    def test_multiple_default_cluster_changes(self):
        """Test changing default cluster multiple times."""
        cluster1 = MagicMock(spec=Cluster)
        cluster2 = MagicMock(spec=Cluster)
        cluster3 = MagicMock(spec=Cluster)

        table = Table("test_table", "test_db")

        # Set first default
        Table.set_default_cluster(cluster1)
        assert table._require_cluster() is cluster1

        # Change default
        Table.set_default_cluster(cluster2)
        assert table._require_cluster() is cluster2

        # Change again
        Table.set_default_cluster(cluster3)
        assert table._require_cluster() is cluster3

        # Clear default
        Table.clear_default_cluster()
        with pytest.raises(RuntimeError):
            table._require_cluster()


class TestTableDefaultClusterDocumentation:
    """Test that the default cluster functionality works as documented."""

    def setup_method(self):
        """Reset default cluster before each test."""
        Table.clear_default_cluster()

    def teardown_method(self):
        """Clean up default cluster after each test."""
        Table.clear_default_cluster()

    def test_example_from_docstring(self):
        """Test the example from the docstring works."""
        cluster = MagicMock(spec=Cluster)
        cluster.query.return_value = [[1]]  # Mock exists() result

        # Example from docstring
        Table.set_default_cluster(cluster)
        table = Table("events", "analytics")  # No cluster needed
        result = table.exists()  # Uses default cluster

        # Verify it worked
        assert result is True
        cluster.query.assert_called_once()

    def test_error_message_helpful(self):
        """Test that error message guides users properly."""
        table = Table("test_table", "test_db")

        with pytest.raises(RuntimeError) as exc_info:
            table._require_cluster()

        error_msg = str(exc_info.value)
        assert "cluster=" in error_msg
        assert "Table.set_default_cluster()" in error_msg
        assert "global default" in error_msg
