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
        if "int8" in name:
            return "Int8"
        if "int16" in name:
            return "Int16"
        if "int32" in name:
            return "Int32"
        if "uint8" in name:
            return "UInt8"
        if "uint16" in name:
            return "UInt16"
        if "uint32" in name:
            return "UInt32"
        if "uint64" in name:
            return "UInt64"
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


def resolve_column_types(
    df: pd.DataFrame,
    column_types: Optional[Mapping[str, str]] = None,
) -> list[tuple[str, str]]:
    """Resolve column types for a DataFrame, applying overrides where specified."""
    resolved: list[tuple[str, str]] = []
    overrides = column_types or {}
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
) -> str:
    """Build a CREATE TABLE statement for ClickHouse based on a pandas DataFrame schema."""
    if df.empty:
        raise ValueError("DataFrame is empty; cannot infer schema.")
    if not table_name:
        raise ValueError("table_name must be provided.")
    
    resolved_types = resolve_column_types(df, column_types)

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
) -> str:
    """Create a ClickHouse table based on the schema of the provided DataFrame."""
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
) -> None:
    """Insert a pandas DataFrame into an existing ClickHouse table."""
    if df.empty:
        return
    
    df_to_insert = df.copy()
    resolved_types = resolve_column_types(df_to_insert, column_types)
    
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