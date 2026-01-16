from __future__ import annotations

import shutil
import subprocess
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from cht.api.app import create_app
from cht.api.cluster_store import ClusterSettings, ClusterStore
from cht.api.services import ClickHouseMetadataService
from cht.cluster import Cluster

COMPOSE_FILE = Path(__file__).resolve().parent.parent / "docker-compose.yml"
CLICKHOUSE_SERVICE = "clickhouse"


def _compose_command() -> list[str]:
    if shutil.which("docker"):
        return ["docker", "compose", "-f", str(COMPOSE_FILE)]
    if shutil.which("docker-compose"):
        return ["docker-compose", "-f", str(COMPOSE_FILE)]
    return []


def _wait_for_clickhouse(host: str = "localhost", port: int = 8123) -> Cluster:
    cluster = Cluster(
        name="docker",
        host=host,
        port=port,
        user="developer",
        password="developer",
    )
    for _ in range(30):
        try:
            cluster.client.ping()
            return cluster
        except Exception:
            time.sleep(1)
    raise RuntimeError("ClickHouse did not become ready in time")


@pytest.fixture(scope="session")
def clickhouse_cluster():
    compose = _compose_command()
    if not compose:
        pytest.skip("docker compose not available")

    subprocess.run([*compose, "up", "-d", CLICKHOUSE_SERVICE], check=True)
    cluster = _wait_for_clickhouse()
    yield cluster
    subprocess.run([*compose, "stop", CLICKHOUSE_SERVICE], check=False)


@pytest.fixture(scope="session")
def api_client(clickhouse_cluster: Cluster) -> TestClient:
    store = ClusterStore()
    store.add_cluster(
        "default",
        ClusterSettings(
            host=clickhouse_cluster.host,
            port=clickhouse_cluster.port,
            user=clickhouse_cluster.user,
            password=clickhouse_cluster.password,
            secure=clickhouse_cluster.secure,
            verify=clickhouse_cluster.verify,
            read_only=clickhouse_cluster.read_only,
        ),
        make_active=True,
    )
    service = ClickHouseMetadataService(store)
    app = create_app(service, cluster_store=store)
    return TestClient(app)


@pytest.fixture
def prepared_table(clickhouse_cluster: Cluster):
    clickhouse_cluster.query("DROP TABLE IF EXISTS default.metadata_api_demo")
    clickhouse_cluster.query(
        """
        CREATE TABLE default.metadata_api_demo (
            id UInt64,
            ts DateTime COMMENT 'event time',
            user_id UInt64 COMMENT 'user identifier'
        )
        ENGINE = MergeTree
        ORDER BY id
        COMMENT 'demo table'
        """
    )
    yield "metadata_api_demo"
    clickhouse_cluster.query("DROP TABLE IF EXISTS default.metadata_api_demo")


def _print_call(path: str, response) -> None:
    """Small helper to display route calls and payloads for human readability."""
    print(
        f"\n[CALL] GET {path}"
        if response.request.method == "GET"
        else f"\n[CALL] {response.request.method} {path}"
    )
    print(f"status: {response.status_code}")
    try:
        print(f"json: {response.json()}")
    except Exception:
        print("no json payload")


def test_list_metadata_against_real_clickhouse(api_client: TestClient, prepared_table: str):
    db_response = api_client.get("/databases")
    _print_call("/databases", db_response)
    assert db_response.status_code == 200
    assert "default" in db_response.json()

    tables_response = api_client.get("/databases/default/tables")
    _print_call("/databases/default/tables", tables_response)
    assert tables_response.status_code == 200
    assert {"name": prepared_table, "comment": "demo table"} in tables_response.json()

    columns_response = api_client.get(f"/databases/default/tables/{prepared_table}/columns")
    _print_call(f"/databases/default/tables/{prepared_table}/columns", columns_response)
    assert columns_response.status_code == 200
    assert columns_response.json() == [
        {"name": "id", "type": "UInt64", "comment": None},
        {"name": "ts", "type": "DateTime", "comment": "event time"},
        {"name": "user_id", "type": "UInt64", "comment": "user identifier"},
    ]


def test_comment_updates_apply(api_client: TestClient, prepared_table: str):
    table_comment = "updated table comment"
    column_comment = "updated column comment"

    update_table = api_client.patch(
        f"/databases/default/tables/{prepared_table}/comment",
        json={"comment": table_comment},
    )
    _print_call(f"/databases/default/tables/{prepared_table}/comment", update_table)
    assert update_table.status_code == 204

    update_column = api_client.patch(
        f"/databases/default/tables/{prepared_table}/columns/user_id/comment",
        json={"comment": column_comment},
    )
    _print_call(
        f"/databases/default/tables/{prepared_table}/columns/user_id/comment",
        update_column,
    )
    assert update_column.status_code == 204

    tables_response = api_client.get("/databases/default/tables")
    _print_call("/databases/default/tables", tables_response)
    assert tables_response.status_code == 200
    assert {"name": prepared_table, "comment": table_comment} in tables_response.json()

    columns_response = api_client.get(f"/databases/default/tables/{prepared_table}/columns")
    _print_call(f"/databases/default/tables/{prepared_table}/columns", columns_response)
    assert columns_response.status_code == 200
    assert columns_response.json() == [
        {"name": "id", "type": "UInt64", "comment": None},
        {"name": "ts", "type": "DateTime", "comment": "event time"},
        {"name": "user_id", "type": "UInt64", "comment": column_comment},
    ]


def test_cluster_routes_and_selection(
    api_client: TestClient, clickhouse_cluster: Cluster, prepared_table: str
):
    list_resp = api_client.get("/clusters")
    _print_call("/clusters", list_resp)
    assert list_resp.status_code == 200
    clusters = list_resp.json()
    assert any(c["name"] == "default" and c["active"] for c in clusters)

    add_resp = api_client.post(
        "/clusters",
        json={
            "name": "secondary",
            "host": clickhouse_cluster.host,
            "port": clickhouse_cluster.port,
            "user": clickhouse_cluster.user,
            "password": clickhouse_cluster.password,
            "make_active": False,
        },
    )
    _print_call("/clusters (add secondary)", add_resp)
    assert add_resp.status_code == 201

    select_resp = api_client.post("/clusters/secondary/select")
    _print_call("/clusters/secondary/select", select_resp)
    assert select_resp.status_code == 204

    list_resp_after = api_client.get("/clusters")
    _print_call("/clusters (after select)", list_resp_after)
    assert list_resp_after.status_code == 200
    clusters_after = list_resp_after.json()
    assert any(c["name"] == "secondary" and c["active"] for c in clusters_after)

    tables_response = api_client.get(f"/databases/default/tables?cluster=secondary")
    _print_call("/databases/default/tables?cluster=secondary", tables_response)
    assert tables_response.status_code == 200
    assert {"name": prepared_table, "comment": "demo table"} in tables_response.json()
