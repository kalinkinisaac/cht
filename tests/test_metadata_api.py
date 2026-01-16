from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from cht.api.app import create_app
from cht.api.services import MetadataService


class FakeMetadataService(MetadataService):
    def __init__(self):
        self.updated_table_comment = None
        self.updated_column_comment = None
        self.last_tables_request = None
        self.last_columns_request = None
        self.last_cluster = None

    def list_databases(self, *, cluster: str | None = None):
        self.last_cluster = cluster
        return ["default", "analytics"]

    def list_tables(self, database: str, *, cluster: str | None = None):
        self.last_tables_request = (database, cluster)
        return [{"name": "events", "comment": "event log"}]

    def list_columns(self, database: str, table: str, *, cluster: str | None = None):
        self.last_columns_request = (database, table, cluster)
        return [
            {"name": "ts", "type": "DateTime", "comment": "event time"},
            {"name": "user_id", "type": "UInt64", "comment": None},
        ]

    def update_table_comment(
        self, database: str, table: str, comment: str, *, cluster: str | None = None
    ) -> None:
        self.updated_table_comment = (database, table, comment, cluster)

    def update_column_comment(
        self,
        database: str,
        table: str,
        column: str,
        comment: str,
        *,
        cluster: str | None = None,
    ) -> None:
        self.updated_column_comment = (database, table, column, comment, cluster)


@pytest.fixture
def fake_service() -> FakeMetadataService:
    return FakeMetadataService()


@pytest.fixture
def client(fake_service: FakeMetadataService) -> TestClient:
    app = create_app(fake_service)
    return TestClient(app)


def test_create_app_requires_service() -> None:
    with pytest.raises(ValueError):
        create_app(None)  # type: ignore[arg-type]


def test_list_databases_returns_simple_names(
    client: TestClient, fake_service: FakeMetadataService
) -> None:
    response = client.get("/databases")
    assert response.status_code == 200
    assert response.json() == ["default", "analytics"]
    assert fake_service.last_cluster is None


def test_list_tables_uses_database_param(
    client: TestClient, fake_service: FakeMetadataService
) -> None:
    response = client.get("/databases/analytics/tables")
    assert response.status_code == 200
    assert response.json() == [{"name": "events", "comment": "event log"}]
    assert fake_service.last_tables_request == ("analytics", None)


def test_list_columns_returns_type_and_comment(
    client: TestClient, fake_service: FakeMetadataService
) -> None:
    response = client.get("/databases/analytics/tables/events/columns")
    assert response.status_code == 200
    assert response.json() == [
        {"name": "ts", "type": "DateTime", "comment": "event time"},
        {"name": "user_id", "type": "UInt64", "comment": None},
    ]
    assert fake_service.last_columns_request == ("analytics", "events", None)


def test_update_table_comment_invokes_service(
    client: TestClient, fake_service: FakeMetadataService
) -> None:
    response = client.patch(
        "/databases/analytics/tables/events/comment",
        json={"comment": "new table comment"},
    )
    assert response.status_code == 204
    assert fake_service.updated_table_comment == (
        "analytics",
        "events",
        "new table comment",
        None,
    )


def test_update_column_comment_invokes_service(
    client: TestClient, fake_service: FakeMetadataService
) -> None:
    response = client.patch(
        "/databases/analytics/tables/events/columns/ts/comment",
        json={"comment": "updated column comment"},
    )
    assert response.status_code == 204
    assert fake_service.updated_column_comment == (
        "analytics",
        "events",
        "ts",
        "updated column comment",
        None,
    )


def test_cluster_query_param_is_forwarded(
    client: TestClient, fake_service: FakeMetadataService
) -> None:
    response = client.get("/databases/analytics/tables?cluster=secondary")
    assert response.status_code == 200
    assert fake_service.last_tables_request == ("analytics", "secondary")
