from __future__ import annotations

from fastapi.testclient import TestClient

from cht.api.app import create_app
from cht.api.cluster_store import ClusterSettings, ClusterStore
from cht.api.services import ClickHouseMetadataService


def _build_app() -> TestClient:
    store = ClusterStore()
    # Seed one cluster to keep metadata endpoints usable
    store.add_cluster(
        "primary",
        settings=ClusterSettings(
            host="localhost",
            port=9000,
            user="default",
            password="",
            secure=False,
            verify=False,
            read_only=True,
        ),
        make_active=True,
    )
    # ClickHouseMetadataService won't be exercised in these tests, but it needs a store
    service = ClickHouseMetadataService(store)
    app = create_app(service, cluster_store=store)
    return TestClient(app)


def test_test_cluster_connection_success(monkeypatch):
    store = ClusterStore()
    fake_cluster = type("C", (), {"client": type("CC", (), {"ping": lambda self: None})()})
    settings = ClusterSettings(
        host="h", port=1, user="u", password="", secure=False, verify=False, read_only=False
    )
    store.add_cluster_instance("mock", settings, cluster=fake_cluster, make_active=True)
    service = ClickHouseMetadataService(store)
    app = create_app(service, cluster_store=store)
    client = TestClient(app)

    resp = client.post("/clusters/mock/test")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


def test_test_cluster_connection_failure(monkeypatch):
    store = ClusterStore()

    class BadClient:
        def ping(self):
            raise RuntimeError("boom")

    bad_cluster = type("C", (), {"client": BadClient()})
    settings = ClusterSettings(
        host="h", port=1, user="u", password="", secure=False, verify=False, read_only=False
    )
    store.add_cluster_instance("bad", settings, cluster=bad_cluster, make_active=True)
    service = ClickHouseMetadataService(store)
    app = create_app(service, cluster_store=store)
    client = TestClient(app)

    resp = client.post("/clusters/bad/test")
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["status"] == "error"
    assert "boom" in payload["error"]


def test_add_and_list_clusters():
    client = _build_app()
    payload = {
        "name": "analytics",
        "host": "ch-host",
        "port": 8123,
        "user": "developer",
        "password": "dev",
        "read_only": False,
        "make_active": True,
    }

    resp = client.post("/clusters", json=payload)
    assert resp.status_code == 201
    data = resp.json()
    assert data["name"] == "analytics"
    assert data["active"] is True

    list_resp = client.get("/clusters")
    assert list_resp.status_code == 200
    clusters = list_resp.json()
    assert any(c["name"] == "analytics" and c["active"] for c in clusters)


def test_select_cluster_marks_active():
    client = _build_app()
    client.post(
        "/clusters",
        json={
            "name": "secondary",
            "host": "other-host",
            "port": 8123,
            "user": "dev",
            "password": "dev",
        },
    )

    resp = client.post("/clusters/secondary/select")
    assert resp.status_code == 204

    clusters = client.get("/clusters").json()
    active_names = [c["name"] for c in clusters if c["active"]]
    assert "secondary" in active_names
