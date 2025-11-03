from __future__ import annotations

from types import SimpleNamespace

import pytest

from cht.sql_utils import (
    extract_from_tables,
    format_identifier,
    generate_cityhash_query,
    get_table_columns,
    parse_from_table,
    parse_to_table,
    remote_expression,
)


def test_extract_from_tables_handles_db_and_table():
    sql = "SELECT * FROM db.table_a JOIN table_b USING id"
    assert extract_from_tables(sql) == ["db.table_a", "table_b"]


def test_parse_to_table_variants():
    query = "CREATE MATERIALIZED VIEW mv TO `analytics`.`fact` AS SELECT 1"
    assert parse_to_table(query) == ("analytics", "fact")
    query_single = "CREATE MATERIALIZED VIEW mv TO fact AS SELECT 1"
    assert parse_to_table(query_single, default_db="analytics") == ("analytics", "fact")


def test_parse_from_table_variants():
    query = "CREATE TABLE foo AS SELECT * FROM `raw`.`events`"
    assert parse_from_table(query) == "raw.events"
    query_single = "CREATE TABLE foo AS SELECT * FROM events"
    assert parse_from_table(query_single) == "events"


def test_generate_cityhash_query_requires_columns():
    with pytest.raises(ValueError):
        generate_cityhash_query([], table_expression="db.table")

    query = generate_cityhash_query(
        ["id", "value"], table_expression="db.table", where="date = today()"
    )
    assert "cityHash64(id, value)" in query
    assert "WHERE date = today()" in query


def test_remote_expression_formatting():
    expr = remote_expression(
        host="localhost",
        database="default",
        table="events",
        user="user",
        password="pwd",
        port=9100,
    )
    assert expr == "remote('localhost', default.events, 'user', 'pwd', 9100)"


def test_get_table_columns_works_with_cluster_like_object():
    obj = SimpleNamespace(
        query=lambda sql: [("col_a",), ("col_b",)],
    )
    assert get_table_columns(obj, "foo") == ["col_a", "col_b"]
