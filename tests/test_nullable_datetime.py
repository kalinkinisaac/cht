#!/usr/bin/env python3
"""
Test for nullable datetime handling in Table.from_df()

This test reproduces the issue where pandas DataFrames with NaT (Not-a-Time)
values in datetime columns cause errors when inserted into ClickHouse tables.

The error occurs because ClickHouse expects proper datetime values or NULL
for nullable datetime columns, but pandas NaT values don't serialize correctly.
"""

from unittest.mock import MagicMock, patch

import pandas as pd

from cht.dataframe import insert_dataframe, pandas_dtype_to_clickhouse, resolve_column_types
from cht.table import Table


def create_test_dataframe_with_nat():
    """Create a DataFrame similar to the user's bidders table with NaT values."""
    # Create the DataFrame first with string dates
    df = pd.DataFrame(
        {
            "bidder_id": [79, 67, 90, 93, 82, 1571, 1578, 1583, 1585, 1576],
            "bidder_start_date": [
                "2025-07-06",
                "2025-07-02",
                "2025-07-09",
                "2025-07-09",
                "2025-07-08",
                "2025-11-22",
                "2025-11-22",
                "2025-11-22",
                "2025-11-22",
                "2025-11-22",
            ],
            "bidder_end_date": [
                "2025-09-17",
                "2025-08-06",
                "2025-09-10",
                "2025-09-10",
                "2025-09-10",
                None,
                None,
                None,
                None,
                None,  # These become NaT
            ],
            "nm_id": [
                184287359,
                200188077,
                155706305,
                162922347,
                162922350,
                565816825,
                613976578,
                536759773,
                559750842,
                568777428,
            ],
            "shop_id": [21, 22, 24, 24, 24, 393, 394, 396, 396, 397],
            "cur_duration_days": [73, 35, 63, 63, 64, 1, 1, 1, 1, 1],
        }
    )

    # Convert to datetime with proper handling of None values
    df["bidder_start_date"] = pd.to_datetime(df["bidder_start_date"])
    df["bidder_end_date"] = pd.to_datetime(df["bidder_end_date"])  # None becomes NaT

    return df


def test_dataframe_with_nat_values():
    """Test that we can create the DataFrame and identify NaT values."""
    df = create_test_dataframe_with_nat()

    # Verify the DataFrame structure matches the user's data
    assert len(df) == 10
    assert "bidder_end_date" in df.columns

    # Check that we have NaT values
    nat_count = df["bidder_end_date"].isna().sum()
    assert nat_count == 5, f"Expected 5 NaT values, got {nat_count}"

    # Verify pandas detects this as datetime
    assert pd.api.types.is_datetime64_any_dtype(df["bidder_end_date"])


def test_pandas_dtype_mapping_for_datetime():
    """Test that datetime columns map to correct ClickHouse types."""
    df = create_test_dataframe_with_nat()

    # Test the dtype mapping
    ch_type = pandas_dtype_to_clickhouse(df["bidder_end_date"].dtype)
    assert ch_type == "DateTime64(3)"


def test_resolve_column_types_with_nullable_override():
    """Test that we can override datetime columns to be nullable."""
    df = create_test_dataframe_with_nat()

    # Without overrides - non-nullable by default
    default_types = resolve_column_types(df)
    bidder_end_date_type = next(
        (col_type for col_name, col_type in default_types if col_name == "bidder_end_date"), None
    )
    assert bidder_end_date_type == "DateTime64(3)"  # Non-nullable

    # With nullable override
    overrides = {"bidder_end_date": "Nullable(DateTime64(3))"}
    nullable_types = resolve_column_types(df, overrides)
    bidder_end_date_type = next(
        (col_type for col_name, col_type in nullable_types if col_name == "bidder_end_date"), None
    )
    assert bidder_end_date_type == "Nullable(DateTime64(3))"


def test_insert_dataframe_handles_nat_values():
    """Test that insert_dataframe properly handles NaT values."""
    df = create_test_dataframe_with_nat()

    # Create a mock cluster
    mock_cluster = MagicMock()
    mock_client = MagicMock()
    mock_cluster.client = mock_client

    # Test with nullable datetime column type
    column_types = {"bidder_end_date": "Nullable(DateTime64(3))"}

    # This should not raise an exception
    insert_dataframe(
        cluster=mock_cluster,
        df=df,
        table_name="test_bidders",
        database="test",
        column_types=column_types,
    )

    # Verify the client was called
    mock_client.insert_df.assert_called_once()

    # Get the DataFrame that was passed to insert_df
    call_args = mock_client.insert_df.call_args
    inserted_df = call_args[1]["df"]

    # The NaT values should still be present (not converted to strings)
    assert inserted_df["bidder_end_date"].isna().sum() == 5


def test_from_df_with_nullable_datetime_columns():
    """Test Table.from_df() with nullable datetime column specification."""
    df = create_test_dataframe_with_nat()

    # Create a mock cluster
    mock_cluster = MagicMock()
    mock_cluster.query.return_value = None
    mock_client = MagicMock()
    mock_cluster.client = mock_client

    # Mock the table existence check
    with patch.object(Table, "exists", return_value=False):
        # This should work with proper column type override
        table = Table.from_df(
            df,
            cluster=mock_cluster,
            name="test_bidders",
            database="test",
            column_types={"bidder_end_date": "Nullable(DateTime64(3))"},
            mode="overwrite",
        )

    assert table.name == "test_bidders"
    assert table.database == "test"


def test_from_df_with_auto_nullable():
    """Test Table.from_df() with auto_nullable=True (default behavior)."""
    df = create_test_dataframe_with_nat()

    # Create a mock cluster
    mock_cluster = MagicMock()
    mock_cluster.query.return_value = None
    mock_client = MagicMock()
    mock_cluster.client = mock_client

    # Mock the table existence check
    with patch.object(Table, "exists", return_value=False):
        # This should work with auto_nullable=True (default)
        table = Table.from_df(
            df, cluster=mock_cluster, name="test_bidders_auto", database="test", mode="overwrite"
        )

    assert table.name == "test_bidders_auto"
    assert table.database == "test"

    # Verify that create_table_from_dataframe was called with auto_nullable=True

    with patch("cht.dataframe.create_table_from_dataframe") as mock_create:
        with patch("cht.dataframe.insert_dataframe"):
            with patch.object(Table, "exists", return_value=False):
                Table.from_df(
                    df, cluster=mock_cluster, name="test_auto", database="test", mode="overwrite"
                )

        mock_create.assert_called_once()
        call_kwargs = mock_create.call_args[1]
        assert call_kwargs.get("auto_nullable", False) is True


def test_from_df_with_auto_nullable_disabled():
    """Test Table.from_df() with auto_nullable=False."""
    df = create_test_dataframe_with_nat()

    # Create a mock cluster
    mock_cluster = MagicMock()
    mock_cluster.query.return_value = None
    mock_client = MagicMock()
    mock_cluster.client = mock_client

    # Mock the table existence check
    with patch.object(Table, "exists", return_value=False):
        # This should work with auto_nullable=False and manual column_types
        table = Table.from_df(
            df,
            cluster=mock_cluster,
            name="test_bidders_manual",
            database="test",
            column_types={"bidder_end_date": "Nullable(DateTime64(3))"},
            auto_nullable=False,
            mode="overwrite",
        )

    assert table.name == "test_bidders_manual"

    # Verify that auto_nullable=False was passed correctly

    with patch("cht.dataframe.create_table_from_dataframe") as mock_create:
        with patch("cht.dataframe.insert_dataframe"):
            with patch.object(Table, "exists", return_value=False):
                Table.from_df(
                    df,
                    cluster=mock_cluster,
                    name="test_manual",
                    database="test",
                    auto_nullable=False,
                    mode="overwrite",
                )

        mock_create.assert_called_once()


def test_auto_detect_nullable_datetime_columns():
    """Test automatic detection and handling of nullable datetime columns."""
    df = create_test_dataframe_with_nat()

    # Import the function from our updated module
    from cht.dataframe import detect_nullable_columns

    overrides = detect_nullable_columns(df)
    assert "bidder_end_date" in overrides
    assert overrides["bidder_end_date"] == "Nullable(DateTime64(3))"

    # bidder_start_date should not be in overrides (no NaT values)
    assert "bidder_start_date" not in overrides

    # Other non-datetime columns with no nulls should not be in overrides
    assert "bidder_id" not in overrides
    assert "nm_id" not in overrides


def test_detect_nullable_columns_mixed_types():
    """Test detection with mixed column types including nulls."""
    df = pd.DataFrame(
        {
            "int_col": [1, 2, None, 4],  # Integer with None (becomes Float64 in pandas)
            "str_col": ["A", None, "C", "D"],  # String with None
            "datetime_col": [
                "2023-01-01",
                None,
                "2023-01-03",
                "2023-01-04",
            ],  # Will convert to datetime
            "clean_col": [1.0, 2.0, 3.0, 4.0],  # Float without nulls
        }
    )

    # Convert to datetime properly
    df["datetime_col"] = pd.to_datetime(df["datetime_col"])  # None becomes NaT

    from cht.dataframe import detect_nullable_columns

    overrides = detect_nullable_columns(df)

    # Should detect all columns with nulls
    assert "int_col" in overrides
    assert "str_col" in overrides
    assert "datetime_col" in overrides

    # Should not detect clean column
    assert "clean_col" not in overrides

    # Check correct types (int with None becomes Float64 in pandas)
    assert overrides["int_col"] == "Nullable(Float64)"  # pandas converts int+None to float64
    assert overrides["str_col"] == "Nullable(String)"
    assert overrides["datetime_col"] == "Nullable(DateTime64(3))"


def test_build_create_table_sql_with_auto_nullable():
    """Test that CREATE TABLE SQL is generated correctly with auto_nullable."""
    df = create_test_dataframe_with_nat()

    from cht.dataframe import build_create_table_sql

    # Test with auto_nullable=True
    sql_with_auto = build_create_table_sql(
        df, "test_table", "test_db", auto_nullable=True, if_not_exists=False
    )

    # Should contain Nullable for the column with NaT values
    assert "Nullable(DateTime64(3))" in sql_with_auto
    assert "`bidder_end_date` Nullable(DateTime64(3))" in sql_with_auto

    # Non-nullable datetime column should remain non-nullable
    assert "`bidder_start_date` DateTime64(3)" in sql_with_auto

    # Test with auto_nullable=False
    sql_without_auto = build_create_table_sql(
        df, "test_table", "test_db", auto_nullable=False, if_not_exists=False
    )

    # Should NOT contain Nullable types when auto_nullable=False
    assert "Nullable(DateTime64(3))" not in sql_without_auto
    assert "`bidder_end_date` DateTime64(3)" in sql_without_auto


if __name__ == "__main__":
    # Run a simple test to demonstrate the issue and solution
    print("🧪 Testing nullable datetime handling...")

    # Create test DataFrame
    df = create_test_dataframe_with_nat()
    print(f"📊 DataFrame shape: {df.shape}")
    print(f"❌ NaT values in bidder_end_date: {df['bidder_end_date'].isna().sum()}")

    # Show the issue
    print("\n🔍 Column type mapping:")
    types = resolve_column_types(df)
    for col, ch_type in types:
        has_na = df[col].isna().any() if col in df.columns else False
        na_marker = " (has NaN/NaT)" if has_na else ""
        print(f"  {col}: {ch_type}{na_marker}")

    # Show the solution
    print("\n✅ With nullable overrides:")

    def detect_nullable_columns(df):
        nullable_overrides = {}
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]) and df[col].isna().any():
                nullable_overrides[col] = f"Nullable({pandas_dtype_to_clickhouse(df[col].dtype)})"
        return nullable_overrides

    overrides = detect_nullable_columns(df)
    types_with_overrides = resolve_column_types(df, overrides)
    for col, ch_type in types_with_overrides:
        has_na = df[col].isna().any() if col in df.columns else False
        na_marker = " (has NaN/NaT)" if has_na else ""
        print(f"  {col}: {ch_type}{na_marker}")

    print(
        "\n🎯 Solution: Use column_types parameter with Nullable() for datetime columns containing NaT"
    )
    print("Example:")
    print("  bidders_ch = t.from_df(")
    print("      bidders, ")
    print("      name='bidders_from_08_01_to_11_22',")
    print("      column_types={'bidder_end_date': 'Nullable(DateTime64(3))'},")
    print("      ttl=None")
    print("  )")
