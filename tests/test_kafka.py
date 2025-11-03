from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from cht.kafka import (
    compare_kafka_tables_inline,
    generate_kafka_consumer_group_update,
)


def test_generate_kafka_consumer_group_update_replaces_value():
    original = "CREATE TABLE t ENGINE = Kafka SETTINGS kafka_group_name = 'old'"
    updated = generate_kafka_consumer_group_update(original, new_group="new")
    assert "new" in updated
    assert "old" not in updated

    with pytest.raises(ValueError):
        generate_kafka_consumer_group_update("CREATE TABLE t ENGINE = Kafka", new_group="new")


def test_compare_kafka_tables_inline_detects_differences():
    def make_cluster(tables, create_map):
        mock = MagicMock()

        def side_effect(sql):
            sql = sql.strip()
            if sql.startswith("SELECT database"):
                return tables
            if sql.startswith("SHOW CREATE TABLE"):
                fqdn = sql.split("SHOW CREATE TABLE ", 1)[1]
                return [[create_map[fqdn]]]
            raise AssertionError(f"Unexpected SQL: {sql}")

        mock.query.side_effect = side_effect
        return mock

    cluster_a = make_cluster(
        tables=[("default", "kafka_events")],
        create_map={"default.kafka_events": "CREATE TABLE ... kafka_group_name = 'a'"},
    )
    cluster_b = make_cluster(
        tables=[("default", "kafka_events")],
        create_map={"default.kafka_events": "CREATE TABLE ... kafka_group_name = 'b'"},
    )

    diffs = compare_kafka_tables_inline(cluster_a, cluster_b)
    assert "default.kafka_events" in diffs["diffs"]
    entries = diffs["diffs"]["default.kafka_events"]
    assert any("kafka_group_name" in line for line in entries)
