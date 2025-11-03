from .cluster import Cluster, is_mutating
from .dataframe import (
    build_create_table_sql,
    create_table_from_dataframe,
    insert_dataframe,
    pandas_dtype_to_clickhouse,
    resolve_column_types,
)
from .kafka import (
    batch_update_consumer_groups,
    compare_kafka_tables_inline,
    diff_line_chars,
    generate_kafka_consumer_group_update,
    get_kafka_tables,
    replace_kafka_consumer_groups,
    show_kafka_create_statements,
)
from .operations import (
    analyze_and_remove_duplicates,
    rebuild_table_via_mv,
    restore_table_from_backup,
    run_queries_with_status,
    sync_missing_rows_by_date,
)
from .sql_utils import (
    extract_from_tables,
    format_identifier,
    generate_cityhash_query,
    get_table_columns,
    parse_from_table,
    parse_to_table,
    remote_expression,
    rows_to_list,
)
from .table import Table

__version__ = "0.1.0"

__all__ = [
    "Cluster",
    "Table",
    "is_mutating",
    # DataFrame utilities
    "build_create_table_sql",
    "create_table_from_dataframe", 
    "insert_dataframe",
    "pandas_dtype_to_clickhouse",
    "resolve_column_types",
    # Operations
    "rebuild_table_via_mv",
    "restore_table_from_backup",
    "analyze_and_remove_duplicates",
    "sync_missing_rows_by_date",
    "run_queries_with_status",
    # Kafka utilities
    "get_kafka_tables",
    "show_kafka_create_statements",
    "batch_update_consumer_groups",
    "generate_kafka_consumer_group_update",
    "replace_kafka_consumer_groups",
    "compare_kafka_tables_inline",
    "diff_line_chars",
    # SQL utilities
    "generate_cityhash_query",
    "remote_expression",
    "format_identifier",
    "rows_to_list",
    "extract_from_tables",
    "parse_from_table",
    "parse_to_table",
    "get_table_columns",
]
