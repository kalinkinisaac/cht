"""Tests for temporary table management functionality."""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pandas as pd
import pytest

from cht.temp_tables import (
    cleanup_expired_tables,
    create_temp_table_sql,
    format_expires_at,
    generate_temp_table_name,
    get_expired_tables,
    is_table_expired,
    parse_expires_at,
)


class TestParseExpiresAt:
    """Test parsing expires_at from table comments."""

    def test_valid_timestamp(self):
        """Test parsing valid expires_at timestamp."""
        comment = "expires_at=2023-12-25T10:30:00Z"
        result = parse_expires_at(comment)
        expected = datetime(2023, 12, 25, 10, 30, tzinfo=timezone.utc)
        assert result == expected

    def test_timestamp_in_middle_of_comment(self):
        """Test parsing timestamp embedded in longer comment."""
        comment = "temp table expires_at=2023-12-25T10:30:00Z created by job"
        result = parse_expires_at(comment)
        expected = datetime(2023, 12, 25, 10, 30, tzinfo=timezone.utc)
        assert result == expected

    def test_no_timestamp(self):
        """Test comment without expires_at."""
        comment = "just a regular comment"
        result = parse_expires_at(comment)
        assert result is None

    def test_empty_comment(self):
        """Test empty comment."""
        result = parse_expires_at("")
        assert result is None

    def test_none_comment(self):
        """Test None comment."""
        result = parse_expires_at("")  # Use empty string instead of None
        assert result is None

    def test_invalid_timestamp_format(self):
        """Test invalid timestamp format."""
        comment = "expires_at=2023-25-12T10:30:00Z"  # Invalid month
        result = parse_expires_at(comment)
        assert result is None


class TestIsTableExpired:
    """Test table expiration checking."""

    def test_expired_table(self):
        """Test table that is expired."""
        now = datetime(2023, 12, 25, 12, 0, tzinfo=timezone.utc)
        comment = "expires_at=2023-12-25T10:30:00Z"
        assert is_table_expired(comment, now) is True

    def test_not_expired_table(self):
        """Test table that is not expired."""
        now = datetime(2023, 12, 25, 12, 0, tzinfo=timezone.utc)
        comment = "expires_at=2023-12-25T14:30:00Z"
        assert is_table_expired(comment, now) is False

    def test_no_expiration_timestamp(self):
        """Test table without expiration timestamp."""
        now = datetime(2023, 12, 25, 12, 0, tzinfo=timezone.utc)
        comment = "regular comment without expiration"
        assert is_table_expired(comment, now) is False

    def test_exactly_at_expiration(self):
        """Test table at exact expiration time."""
        now = datetime(2023, 12, 25, 10, 30, tzinfo=timezone.utc)
        comment = "expires_at=2023-12-25T10:30:00Z"
        assert is_table_expired(comment, now) is True


class TestFormatExpiresAt:
    """Test formatting expires_at timestamps."""

    def test_utc_datetime(self):
        """Test formatting UTC datetime."""
        dt = datetime(2023, 12, 25, 10, 30, 45, tzinfo=timezone.utc)
        result = format_expires_at(dt)
        assert result == "expires_at=2023-12-25T10:30:45Z"

    def test_naive_datetime(self):
        """Test formatting naive datetime (assumes UTC)."""
        dt = datetime(2023, 12, 25, 10, 30, 45)
        result = format_expires_at(dt)
        assert result == "expires_at=2023-12-25T10:30:45Z"

    def test_microseconds_removed(self):
        """Test that microseconds are removed."""
        dt = datetime(2023, 12, 25, 10, 30, 45, 123456, tzinfo=timezone.utc)
        result = format_expires_at(dt)
        assert result == "expires_at=2023-12-25T10:30:45Z"


class TestGenerateTempTableName:
    """Test temporary table name generation."""

    def test_default_prefix(self):
        """Test default prefix."""
        name = generate_temp_table_name()
        assert name.startswith("tmp_")
        assert len(name) == len("tmp_") + 8  # prefix + 8 hex chars
        assert re.match(r"^tmp_[0-9a-f]{8}$", name)

    def test_custom_prefix(self):
        """Test custom prefix."""
        name = generate_temp_table_name("test_")
        assert name.startswith("test_")
        assert len(name) == len("test_") + 8
        assert re.match(r"^test_[0-9a-f]{8}$", name)

    def test_uniqueness(self):
        """Test that generated names are unique."""
        names = [generate_temp_table_name() for _ in range(100)]
        assert len(set(names)) == 100  # All unique


class TestCreateTempTableSQL:
    """Test SQL generation for temporary tables."""

    def test_basic_select(self):
        """Test basic SELECT query."""
        query = "SELECT * FROM events"
        create_sql, alter_sql = create_temp_table_sql(
            query=query,
            table_name="test_table",
            database="temp",
            ttl=timedelta(hours=1),
        )

        assert "CREATE TABLE `temp`.`test_table`" in create_sql
        assert "ENGINE = MergeTree" in create_sql
        assert "ORDER BY tuple()" in create_sql
        assert "SELECT * FROM events" in create_sql

        assert alter_sql is not None
        assert "ALTER TABLE `temp`.`test_table`" in alter_sql
        assert "MODIFY COMMENT" in alter_sql
        assert "expires_at=" in alter_sql

    def test_with_order_by(self):
        """Test with ORDER BY clause."""
        query = "SELECT id, name FROM users"
        create_sql, _ = create_temp_table_sql(
            query=query,
            table_name="test_table",
            database="temp",
            order_by="id",
        )

        assert "ORDER BY `id`" in create_sql

    def test_with_multiple_order_by(self):
        """Test with multiple ORDER BY columns."""
        query = "SELECT id, name FROM users"
        create_sql, _ = create_temp_table_sql(
            query=query,
            table_name="test_table",
            database="temp",
            order_by=["id", "name"],
        )

        assert "ORDER BY tuple(`id`, `name`)" in create_sql

    def test_with_cluster(self):
        """Test with ON CLUSTER clause."""
        query = "SELECT * FROM events"
        create_sql, alter_sql = create_temp_table_sql(
            query=query,
            table_name="test_table",
            database="temp",
            on_cluster="test_cluster",
            ttl=timedelta(hours=1),
        )

        assert "ON CLUSTER test_cluster" in create_sql
        assert alter_sql is not None
        assert "ON CLUSTER test_cluster" in alter_sql

    def test_no_ttl(self):
        """Test without TTL (no expiration)."""
        query = "SELECT * FROM events"
        create_sql, alter_sql = create_temp_table_sql(
            query=query,
            table_name="test_table",
            database="temp",
            ttl=None,
        )

        assert "CREATE TABLE `temp`.`test_table`" in create_sql
        assert alter_sql is None

    def test_query_validation_insert(self):
        """Test that INSERT queries are rejected."""
        query = "INSERT INTO table VALUES (1, 2, 3)"
        with pytest.raises(ValueError, match="Query must be a SELECT statement"):
            create_temp_table_sql(query, "test", "temp")

    def test_query_validation_drop(self):
        """Test that DROP queries are rejected."""
        query = "DROP TABLE test"
        with pytest.raises(ValueError, match="Query must be a SELECT statement"):
            create_temp_table_sql(query, "test", "temp")

    def test_query_validation_random_text(self):
        """Test that non-SQL text is rejected."""
        query = "this is not sql"
        with pytest.raises(ValueError, match="Query must be a SELECT statement"):
            create_temp_table_sql(query, "test", "temp")

    def test_with_cte(self):
        """Test WITH (CTE) queries are accepted."""
        query = "WITH cte AS (SELECT 1) SELECT * FROM cte"
        create_sql, _ = create_temp_table_sql(query, "test", "temp")
        assert "CREATE TABLE" in create_sql

    def test_negative_ttl(self):
        """Test negative TTL is rejected."""
        query = "SELECT 1"
        with pytest.raises(ValueError, match="TTL must be positive"):
            create_temp_table_sql(query, "test", "temp", ttl=timedelta(seconds=-1))

    def test_zero_ttl(self):
        """Test zero TTL is rejected."""
        query = "SELECT 1"
        with pytest.raises(ValueError, match="TTL must be positive"):
            create_temp_table_sql(query, "test", "temp", ttl=timedelta(0))


class TestGetExpiredTables:
    """Test getting expired tables from database."""

    def test_no_tables(self):
        """Test database with no tables."""
        mock_cluster = MagicMock()
        mock_result = MagicMock()
        mock_result.result_rows = []
        mock_cluster.query_raw.return_value = mock_result

        result = get_expired_tables(mock_cluster, "temp")
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0
        assert list(result.columns) == ["table", "comment", "expires_at", "expired"]

    def test_mixed_tables(self):
        """Test database with mixed expired and non-expired tables."""
        mock_cluster = MagicMock()
        mock_result = MagicMock()

        # Mock table data: name, comment, create_query
        mock_result.result_rows = [
            ("expired_table", "expires_at=2023-01-01T10:00:00Z", "CREATE TABLE..."),
            ("active_table", "expires_at=2025-01-01T10:00:00Z", "CREATE TABLE..."),
            ("no_expiry", "regular comment", "CREATE TABLE..."),
        ]
        mock_cluster.query_raw.return_value = mock_result

        now = datetime(2024, 1, 1, tzinfo=timezone.utc)
        result = get_expired_tables(mock_cluster, "temp", now=now)

        assert len(result) == 3
        assert result.iloc[0]["table"] == "expired_table"
        assert bool(result.iloc[0]["expired"]) is True  # Convert numpy bool to Python bool
        assert result.iloc[1]["table"] == "active_table"
        assert bool(result.iloc[1]["expired"]) is False
        assert result.iloc[2]["table"] == "no_expiry"
        assert bool(result.iloc[2]["expired"]) is False

    def test_with_pattern(self):
        """Test filtering tables by pattern."""
        mock_cluster = MagicMock()
        get_expired_tables(mock_cluster, "temp", table_pattern="tmp_%")

        # Verify SQL contains LIKE pattern
        call_args = mock_cluster.query_raw.call_args[0][0]
        assert "name LIKE 'tmp_%'" in call_args


class TestCleanupExpiredTables:
    """Test cleanup of expired tables."""

    def test_no_expired_tables(self):
        """Test cleanup when no tables are expired."""
        mock_cluster = MagicMock()
        mock_result = MagicMock()
        mock_result.result_rows = [
            ("active_table", "expires_at=2025-01-01T10:00:00Z", "CREATE TABLE..."),
        ]
        mock_cluster.query_raw.return_value = mock_result

        # Use a fixed time that makes the table not expired (2024, before 2025)
        now = datetime(2024, 1, 1, tzinfo=timezone.utc)
        result = cleanup_expired_tables(mock_cluster, "temp", now=now)

        assert result["database"] == "temp"
        assert result["total_tables_checked"] == 1
        assert result["expired_tables_found"] == 0
        assert result["tables_deleted"] == []
        assert result["errors"] == []
        assert result["dry_run"] is False

    def test_dry_run(self):
        """Test dry run mode."""
        mock_cluster = MagicMock()
        mock_result = MagicMock()
        mock_result.result_rows = [
            ("expired_table", "expires_at=2023-01-01T10:00:00Z", "CREATE TABLE..."),
        ]
        mock_cluster.query_raw.return_value = mock_result

        # Use a time that makes the table expired
        now = datetime(2024, 1, 1, tzinfo=timezone.utc)
        result = cleanup_expired_tables(mock_cluster, "temp", dry_run=True, now=now)

        assert result["dry_run"] is True
        assert result["expired_tables_found"] == 1
        assert result["tables_to_delete"] == ["expired_table"]
        # Verify no DROP queries were executed
        mock_cluster.query.assert_not_called()

    def test_actual_cleanup(self):
        """Test actual table deletion."""
        mock_cluster = MagicMock()
        mock_result = MagicMock()
        mock_result.result_rows = [
            ("expired_table1", "expires_at=2023-01-01T10:00:00Z", "CREATE TABLE..."),
            ("expired_table2", "expires_at=2023-01-01T11:00:00Z", "CREATE TABLE..."),
        ]
        mock_cluster.query_raw.return_value = mock_result

        # Use a time that makes the tables expired
        now = datetime(2024, 1, 1, tzinfo=timezone.utc)
        result = cleanup_expired_tables(mock_cluster, "temp", now=now)

        assert result["expired_tables_found"] == 2
        assert result["tables_deleted"] == ["expired_table1", "expired_table2"]
        assert result["errors"] == []

        # Verify DROP queries were called
        assert mock_cluster.query.call_count == 2
        drop_calls = [call[0][0] for call in mock_cluster.query.call_args_list]
        assert "DROP TABLE IF EXISTS `temp`.`expired_table1`" in drop_calls
        assert "DROP TABLE IF EXISTS `temp`.`expired_table2`" in drop_calls

    def test_cleanup_with_errors(self):
        """Test cleanup with some deletion errors."""
        mock_cluster = MagicMock()
        mock_result = MagicMock()
        mock_result.result_rows = [
            ("expired_table1", "expires_at=2023-01-01T10:00:00Z", "CREATE TABLE..."),
            ("expired_table2", "expires_at=2023-01-01T11:00:00Z", "CREATE TABLE..."),
        ]
        mock_cluster.query_raw.return_value = mock_result

        # Make second deletion fail
        def side_effect(sql):
            if "expired_table2" in sql:
                raise Exception("Permission denied")

        mock_cluster.query.side_effect = side_effect

        result = cleanup_expired_tables(mock_cluster, "temp")

        assert result["expired_tables_found"] == 2
        assert result["tables_deleted"] == ["expired_table1"]
        assert len(result["errors"]) == 1
        assert result["errors"][0]["table"] == "expired_table2"
        assert "Permission denied" in result["errors"][0]["error"]
