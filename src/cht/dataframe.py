"""DataFrame utilities for ClickHouse operations."""

from __future__ import annotations

import uuid
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, Optional

import pandas as pd
from pandas.api.types import (
    is_bool_dtype,
    is_datetime64_any_dtype,
    is_float_dtype,
    is_integer_dtype,
)

if TYPE_CHECKING:
    from .cluster import Cluster


def pandas_dtype_to_clickhouse(dtype: Any) -> str:
    """Map a pandas dtype to a reasonable ClickHouse column type."""
    if is_bool_dtype(dtype):
        return "UInt8"
    if is_integer_dtype(dtype):
        name = str(dtype).lower()
        # Check unsigned types first
        if "uint8" in name:
            return "UInt8"
        if "uint16" in name:
            return "UInt16"
        if "uint32" in name:
            return "UInt32"
        if "uint64" in name:
            return "UInt64"
        # Then check signed types
        if "int8" in name:
            return "Int8"
        if "int16" in name:
            return "Int16"
        if "int32" in name:
            return "Int32"
        return "Int64"
    if is_float_dtype(dtype):
        name = str(dtype).lower()
        if "float32" in name or "float16" in name:
            return "Float32"
        return "Float64"
    if is_datetime64_any_dtype(dtype):
        return "DateTime64(3)"
    if str(dtype) == "category":
        return "String"
    return "String"


def detect_nullable_columns(df: pd.DataFrame) -> dict[str, str]:
    """
    Auto-detect columns that should be nullable in ClickHouse.

    This function identifies columns that contain null/NaN/NaT values and
    returns appropriate nullable ClickHouse types for them.

    Args:
        df: DataFrame to analyze

    Returns:
        Dictionary mapping column names to nullable ClickHouse types

    Example:
        >>> df = pd.DataFrame({
        ...     'id': [1, 2, 3],
        ...     'name': ['A', None, 'C'],  # Has None
        ...     'date': pd.to_datetime(['2023-01-01', None, '2023-01-03'])  # Has NaT
        ... })
        >>> detect_nullable_columns(df)
        {'name': 'Nullable(String)', 'date': 'Nullable(DateTime64(3))'}
    """
    nullable_overrides = {}

    for column in df.columns:
        if df[column].isna().any():
            base_type = pandas_dtype_to_clickhouse(df[column].dtype)
            nullable_overrides[column] = f"Nullable({base_type})"

    return nullable_overrides


def resolve_column_types(
    df: pd.DataFrame,
    column_types: Optional[Mapping[str, str]] = None,
    auto_nullable: bool = False,
) -> list[tuple[str, str]]:
    """
    Resolve column types for a DataFrame, applying overrides where specified.

    Args:
        df: DataFrame to analyze
        column_types: Manual column type overrides
        auto_nullable: Automatically detect and apply nullable types for columns with nulls

    Returns:
        List of (column_name, clickhouse_type) tuples
    """
    resolved: list[tuple[str, str]] = []
    overrides = dict(column_types or {})

    # Apply auto-nullable detection if requested
    if auto_nullable:
        auto_detected = detect_nullable_columns(df)
        # Only add auto-detected types if not manually overridden
        for col, nullable_type in auto_detected.items():
            if col not in overrides:
                overrides[col] = nullable_type

    for column in df.columns:
        resolved_type = overrides.get(column, pandas_dtype_to_clickhouse(df[column].dtype))
        resolved.append((column, resolved_type))

    return resolved


def build_create_table_sql(
    df: pd.DataFrame,
    table_name: str,
    database: str = "default",
    *,
    engine: str = "MergeTree",
    order_by: Optional[Sequence[str]] = None,
    partition_by: Optional[Sequence[str]] = None,
    primary_key: Optional[Sequence[str]] = None,
    settings: Optional[Mapping[str, Any]] = None,
    if_not_exists: bool = True,
    column_types: Optional[Mapping[str, str]] = None,
    auto_nullable: bool = False,
) -> str:
    """
    Build a CREATE TABLE statement for ClickHouse based on a pandas DataFrame schema.

    Args:
        df: DataFrame to create table for
        table_name: Name of the table to create
        database: Database name
        engine: ClickHouse table engine
        order_by: ORDER BY clause columns
        partition_by: PARTITION BY clause columns
        primary_key: PRIMARY KEY clause columns
        settings: Table settings
        if_not_exists: Whether to use IF NOT EXISTS
        column_types: Manual column type overrides
        auto_nullable: Automatically detect nullable columns

    Returns:
        CREATE TABLE SQL statement
    """
    if df.empty:
        raise ValueError("DataFrame is empty; cannot infer schema.")
    if not table_name:
        raise ValueError("table_name must be provided.")

    resolved_types = resolve_column_types(df, column_types, auto_nullable=auto_nullable)

    def _format_identifier(name: str) -> str:
        """Format a single identifier with backticks."""
        return f"`{name}`"

    columns_sql = ",\n    ".join(
        f"{_format_identifier(column)} {column_type}" for column, column_type in resolved_types
    )

    db_prefix = f"{_format_identifier(database)}." if database else ""
    clause_prefix = "IF NOT EXISTS " if if_not_exists else ""

    def format_clause(keyword: str, value: Optional[Sequence[str]]) -> str:
        if not value:
            return ""
        if isinstance(value, str):
            expression = value
        else:
            expression = ", ".join(_format_identifier(v) for v in value)
        return f"\n{keyword} ({expression})"

    if engine.lower() == "mergetree" and not order_by:
        order_by = (df.columns[0],)

    settings_clause = ""
    if settings:
        formatted_settings = ", ".join(f"{key}={value}" for key, value in settings.items())
        settings_clause = f"\nSETTINGS {formatted_settings}"

    sql = (
        f"CREATE TABLE {clause_prefix}{db_prefix}{_format_identifier(table_name)} (\n"
        f"    {columns_sql}\n)"
        f"\nENGINE = {engine}"
        f"{format_clause('ORDER BY', order_by)}"
        f"{format_clause('PRIMARY KEY', primary_key)}"
        f"{format_clause('PARTITION BY', partition_by)}"
        f"{settings_clause}"
    )
    return sql


def create_table_from_dataframe(
    cluster: "Cluster",
    df: pd.DataFrame,
    table_name: str,
    database: str = "default",
    *,
    engine: str = "MergeTree",
    order_by: Optional[Sequence[str]] = None,
    partition_by: Optional[Sequence[str]] = None,
    primary_key: Optional[Sequence[str]] = None,
    settings: Optional[Mapping[str, Any]] = None,
    if_not_exists: bool = True,
    column_types: Optional[Mapping[str, str]] = None,
    auto_nullable: bool = False,
) -> str:
    """
    Create a ClickHouse table based on the schema of the provided DataFrame.

    Args:
        cluster: ClickHouse cluster connection
        df: DataFrame to create table for
        table_name: Name of the table to create
        database: Database name
        engine: ClickHouse table engine
        order_by: ORDER BY clause columns
        partition_by: PARTITION BY clause columns
        primary_key: PRIMARY KEY clause columns
        settings: Table settings
        if_not_exists: Whether to use IF NOT EXISTS
        column_types: Manual column type overrides
        auto_nullable: Automatically detect nullable columns

    Returns:
        CREATE TABLE SQL statement that was executed
    """
    create_sql = build_create_table_sql(
        df,
        table_name,
        database=database,
        engine=engine,
        order_by=order_by,
        partition_by=partition_by,
        primary_key=primary_key,
        settings=settings,
        if_not_exists=if_not_exists,
        column_types=column_types,
        auto_nullable=auto_nullable,
    )
    cluster.query(create_sql)
    return create_sql


def insert_dataframe(
    cluster: "Cluster",
    df: pd.DataFrame,
    table_name: str,
    database: str = "default",
    *,
    column_types: Optional[Mapping[str, str]] = None,
    auto_nullable: bool = False,
) -> None:
    """
    Insert a pandas DataFrame into an existing ClickHouse table.

    Args:
        cluster: ClickHouse cluster connection
        df: DataFrame to insert
        table_name: Name of target table
        database: Database name
        column_types: Manual column type overrides
        auto_nullable: Automatically detect nullable columns (used for data processing)
    """
    if df.empty:
        return

    df_to_insert = df.copy()
    resolved_types = resolve_column_types(df_to_insert, column_types, auto_nullable=auto_nullable)

    # Handle string columns - fill NaN with empty strings
    for column, ch_type in resolved_types:
        lower_type = ch_type.lower()
        if lower_type.startswith("string") or lower_type.startswith("fixedstring"):
            df_to_insert[column] = df_to_insert[column].fillna("").astype(str)

    # Use the cluster's client to insert
    client = cluster.client
    client.insert_df(
        table=table_name,
        df=df_to_insert,
        database=database,
    )


def generate_temp_table_name() -> str:
    """Generate a unique temporary table name."""
    return f"temp_{uuid.uuid4().hex[:8]}"
