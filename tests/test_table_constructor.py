"""
Test Table constructor enhancements supporting 'schema.table' syntax.
"""

from unittest.mock import Mock

import pytest

from cht import Cluster, Table


class TestTableConstructor:
    """Test enhanced Table constructor with flexible syntax support."""

    def test_simple_table_name(self):
        """Test simple table name uses default database."""
        table = Table("users")
        assert table.name == "users"
        assert table.database == "default"
        assert table.cluster is None

    def test_schema_dot_table_syntax(self):
        """Test 'database.table' syntax parses correctly."""
        table = Table("analytics.events")
        assert table.name == "events"
        assert table.database == "analytics"
        assert table.cluster is None

    def test_positional_arguments_backward_compatible(self):
        """Test backward compatibility with positional arguments."""
        table = Table("events", "analytics")
        assert table.name == "analytics"
        assert table.database == "events"
        assert table.cluster is None

    def test_keyword_arguments_backward_compatible(self):
        """Test backward compatibility with keyword arguments."""
        table = Table(database_or_fqdn="events", table_name="analytics")
        assert table.name == "analytics"
        assert table.database == "events"
        assert table.cluster is None

    def test_explicit_table_name_provided(self):
        """Test when both parameters are provided explicitly."""
        table = Table("events", "analytics")
        assert table.name == "analytics"
        assert table.database == "events"
        assert table.cluster is None

    def test_with_cluster_simple_name(self):
        """Test cluster parameter with simple table name."""
        cluster = Mock(spec=Cluster)
        table = Table("users", cluster=cluster)
        assert table.name == "users"
        assert table.database == "default"
        assert table.cluster is cluster

    def test_with_cluster_schema_table(self):
        """Test cluster parameter with database.table syntax."""
        cluster = Mock(spec=Cluster)
        table = Table("analytics.events", cluster=cluster)
        assert table.name == "events"
        assert table.database == "analytics"
        assert table.cluster is cluster

    def test_with_cluster_all_parameters(self):
        """Test cluster with all parameters specified."""
        cluster = Mock(spec=Cluster)
        table = Table(database_or_fqdn="events", table_name="analytics", cluster=cluster)
        assert table.name == "analytics"
        assert table.database == "events"
        assert table.cluster is cluster

    def test_complex_schema_names(self):
        """Test database.table syntax with complex names."""
        # Test underscore names
        table = Table("user_analytics.event_tracking")
        assert table.name == "event_tracking"
        assert table.database == "user_analytics"

        # Test mixed case
        table = Table("Analytics.Events")
        assert table.name == "Events"
        assert table.database == "Analytics"

    def test_multiple_dots_only_splits_once(self):
        """Test that only the first dot is used for database.table splitting."""
        table = Table("database.table.with.dots")
        assert table.name == "table.with.dots"
        assert table.database == "database"

    def test_empty_parts_edge_cases(self):
        """Test edge cases with empty parts (though invalid in practice)."""
        # Leading dot - should be handled gracefully
        table = Table(".events")
        assert table.name == "events"
        assert table.database == ""  # Empty but explicit

        # Trailing dot
        table = Table("analytics.")
        assert table.name == ""
        assert table.database == "analytics"

    def test_error_on_missing_name(self):
        """Test that ValueError is raised when database_or_fqdn is None."""
        with pytest.raises(ValueError, match="Database or table specification is required"):
            Table(database_or_fqdn=None)

        with pytest.raises(ValueError, match="Database or table specification is required"):
            Table()

    def test_fqdn_property_works_with_new_syntax(self):
        """Test that fqdn property works correctly with new constructor syntax."""
        # Simple name
        table = Table("users")
        assert table.fqdn == "default.users"

        # Schema.table syntax
        table = Table("analytics.events")
        assert table.fqdn == "analytics.events"

        # Backward compatible
        table = Table("events", "analytics")
        assert table.fqdn == "events.analytics"

    def test_repr_works_with_new_syntax(self):
        """Test that string representation works with new syntax."""
        # Simple name
        table = Table("users")
        expected = "Table(name='users', database='default', cluster=None)"
        assert repr(table) == expected

        # Database.table syntax
        table = Table("analytics.events")
        expected = "Table(name='events', database='analytics', cluster=None)"
        assert repr(table) == expected

    def test_multiple_constructor_patterns(self):
        """Test various constructor patterns produce same result."""
        cluster = Mock(spec=Cluster)

        # All these should create equivalent tables
        table1 = Table("analytics.events")
        table2 = Table("analytics", "events")
        table3 = Table(database_or_fqdn="analytics", table_name="events")

        # Verify they're equivalent
        for table in [table2, table3]:
            assert table.name == table1.name
            assert table.database == table1.database
            assert table.fqdn == table1.fqdn

        # With cluster
        table_c1 = Table("analytics.events", cluster=cluster)
        table_c2 = Table("analytics", "events", cluster)
        table_c3 = Table(database_or_fqdn="analytics", table_name="events", cluster=cluster)

        for table in [table_c2, table_c3]:
            assert table.name == table_c1.name
            assert table.database == table_c1.database
            assert table.cluster == table_c1.cluster


if __name__ == "__main__":
    pytest.main([__file__])
