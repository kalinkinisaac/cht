from __future__ import annotations

from unittest.mock import MagicMock

import pandas as pd
import pytest

from cht.dataframe import (
    build_create_table_sql,
    create_table_from_dataframe,
    generate_temp_table_name,
    insert_dataframe,
    pandas_dtype_to_clickhouse,
    resolve_column_types,
)


def test_pandas_dtype_to_clickhouse_bool():
    """Test boolean dtype mapping."""
    df = pd.DataFrame({"active": [True, False, True]})
    assert pandas_dtype_to_clickhouse(df["active"].dtype) == "UInt8"


def test_pandas_dtype_to_clickhouse_integers():
    """Test integer dtype mappings."""
    df = pd.DataFrame(
        {
            "int8_col": pd.array([1, 2, 3], dtype="int8"),
            "int16_col": pd.array([1, 2, 3], dtype="int16"),
            "int32_col": pd.array([1, 2, 3], dtype="int32"),
            "int64_col": pd.array([1, 2, 3], dtype="int64"),
            "uint8_col": pd.array([1, 2, 3], dtype="uint8"),
            "uint16_col": pd.array([1, 2, 3], dtype="uint16"),
            "uint32_col": pd.array([1, 2, 3], dtype="uint32"),
            "uint64_col": pd.array([1, 2, 3], dtype="uint64"),
        }
    )

    assert pandas_dtype_to_clickhouse(df["int8_col"].dtype) == "Int8"
    assert pandas_dtype_to_clickhouse(df["int16_col"].dtype) == "Int16"
    assert pandas_dtype_to_clickhouse(df["int32_col"].dtype) == "Int32"
    assert pandas_dtype_to_clickhouse(df["int64_col"].dtype) == "Int64"
    assert pandas_dtype_to_clickhouse(df["uint8_col"].dtype) == "UInt8"
    assert pandas_dtype_to_clickhouse(df["uint16_col"].dtype) == "UInt16"
    assert pandas_dtype_to_clickhouse(df["uint32_col"].dtype) == "UInt32"
    assert pandas_dtype_to_clickhouse(df["uint64_col"].dtype) == "UInt64"


def test_pandas_dtype_to_clickhouse_floats():
    """Test float dtype mappings."""
    df = pd.DataFrame(
        {
            "float32_col": pd.array([1.0, 2.0, 3.0], dtype="float32"),
            "float64_col": pd.array([1.0, 2.0, 3.0], dtype="float64"),
        }
    )

    assert pandas_dtype_to_clickhouse(df["float32_col"].dtype) == "Float32"
    assert pandas_dtype_to_clickhouse(df["float64_col"].dtype) == "Float64"


def test_pandas_dtype_to_clickhouse_datetime():
    """Test datetime dtype mapping."""
    df = pd.DataFrame({"timestamp": pd.to_datetime(["2023-01-01", "2023-01-02"])})
    assert pandas_dtype_to_clickhouse(df["timestamp"].dtype) == "DateTime64(3)"


def test_pandas_dtype_to_clickhouse_category():
    """Test category dtype mapping."""
    df = pd.DataFrame({"category": pd.Categorical(["A", "B", "A"])})
    assert pandas_dtype_to_clickhouse(df["category"].dtype) == "String"


def test_pandas_dtype_to_clickhouse_object():
    """Test object/string dtype mapping."""
    df = pd.DataFrame({"text": ["hello", "world", "test"]})
    assert pandas_dtype_to_clickhouse(df["text"].dtype) == "String"


def test_resolve_column_types_default():
    """Test column type resolution without overrides."""
    df = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "active": [True, False, True],
            "score": [85.5, 92.0, 78.3],
        }
    )

    resolved = resolve_column_types(df)
    expected = [
        ("id", "Int64"),
        ("name", "String"),
        ("active", "UInt8"),
        ("score", "Float64"),
    ]
    assert resolved == expected


def test_resolve_column_types_with_overrides():
    """Test column type resolution with custom overrides."""
    df = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "email": ["a@test.com", "b@test.com", "c@test.com"],
        }
    )

    overrides = {"email": "LowCardinality(String)", "id": "UInt32"}
    resolved = resolve_column_types(df, overrides)
    expected = [
        ("id", "UInt32"),
        ("email", "LowCardinality(String)"),
    ]
    assert resolved == expected


def test_build_create_table_sql_basic():
    """Test basic CREATE TABLE SQL generation."""
    df = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
        }
    )

    sql = build_create_table_sql(df, "users", "test")

    assert "CREATE TABLE IF NOT EXISTS `test`.`users`" in sql
    assert "`id` Int64" in sql
    assert "`name` String" in sql
    assert "ENGINE = MergeTree" in sql
    assert "ORDER BY (tuple())" in sql  # Default order by tuple()


def test_build_create_table_sql_custom_engine():
    """Test CREATE TABLE SQL with custom engine and clauses."""
    df = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "timestamp": pd.to_datetime(["2023-01-01", "2023-01-02", "2023-01-03"]),
            "value": [10.0, 20.0, 30.0],
        }
    )

    sql = build_create_table_sql(
        df,
        "events",
        "analytics",
        engine="ReplacingMergeTree",
        order_by=["id", "timestamp"],
        partition_by=["toYYYYMM(timestamp)"],
        primary_key=["id"],
        settings={"index_granularity": 8192},
        if_not_exists=False,
    )

    assert "CREATE TABLE `analytics`.`events`" in sql
    assert "ENGINE = ReplacingMergeTree" in sql
    assert "ORDER BY (`id`, `timestamp`)" in sql
    assert "PARTITION BY (`toYYYYMM(timestamp)`)" in sql
    assert "PRIMARY KEY (`id`)" in sql
    assert "SETTINGS index_granularity=8192" in sql


def test_build_create_table_sql_empty_dataframe():
    """Test that empty DataFrame raises ValueError."""
    df = pd.DataFrame()

    with pytest.raises(ValueError, match="DataFrame is empty"):
        build_create_table_sql(df, "test", "test")


def test_build_create_table_sql_no_table_name():
    """Test that missing table name raises ValueError."""
    df = pd.DataFrame({"id": [1, 2, 3]})

    with pytest.raises(ValueError, match="table_name must be provided"):
        build_create_table_sql(df, "", "test")


def test_create_table_from_dataframe():
    """Test table creation from DataFrame."""
    df = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
        }
    )

    mock_cluster = MagicMock()

    sql = create_table_from_dataframe(
        cluster=mock_cluster,
        df=df,
        table_name="users",
        database="test",
        engine="MergeTree",
        order_by=["id"],
    )

    # Verify cluster.query was called
    mock_cluster.query.assert_called_once()

    # Verify the SQL contains expected elements
    assert "CREATE TABLE IF NOT EXISTS `test`.`users`" in sql
    assert "`id` Int64" in sql
    assert "`name` String" in sql
    assert "ENGINE = MergeTree" in sql
    assert "ORDER BY (`id`)" in sql


def test_insert_dataframe():
    """Test DataFrame insertion."""
    df = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "description": ["", None, "Test"],  # Test NaN handling
        }
    )

    mock_cluster = MagicMock()
    mock_client = MagicMock()
    mock_cluster.client = mock_client

    insert_dataframe(cluster=mock_cluster, df=df, table_name="users", database="test")

    # Verify client.insert_df was called
    mock_client.insert_df.assert_called_once()

    # Get the DataFrame that was passed to insert_df
    call_args = mock_client.insert_df.call_args
    inserted_df = call_args[1]["df"]

    # Verify NaN values in string columns were replaced with empty strings
    assert inserted_df["description"].iloc[1] == ""


def test_insert_dataframe_empty():
    """Test that empty DataFrame insertion is skipped."""
    df = pd.DataFrame()
    mock_cluster = MagicMock()

    insert_dataframe(cluster=mock_cluster, df=df, table_name="users", database="test")

    # Verify no client operations were performed
    mock_cluster.client.assert_not_called()


def test_generate_temp_table_name():
    """Test temporary table name generation."""
    name1 = generate_temp_table_name()
    name2 = generate_temp_table_name()

    # Should start with temp_
    assert name1.startswith("temp_")
    assert name2.startswith("temp_")

    # Should be unique
    assert name1 != name2

    # Should be reasonable length
    assert len(name1) == 13  # "temp_" + 8 hex chars


def test_resolve_column_types_empty_dataframe():
    """Test column type resolution with empty DataFrame."""
    df = pd.DataFrame()
    resolved = resolve_column_types(df)
    assert resolved == []
