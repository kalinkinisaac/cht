# CHT metadata API design

Goals:
- Expose a lightweight FastAPI service for ClickHouse metadata tasks.
- List databases, list tables with comments, and list columns with type and comment.
- Allow updating table comments and individual column comments.
- Rely on existing `Cluster` and `Table` primitives to avoid duplicating ClickHouse access logic.

Architecture overview:
- `cht.api.app:create_app(metadata_service)` builds the FastAPI instance and stores the service on
  `app.state.metadata_service`.
- `cht.api.services.MetadataService` defines the contract (list + comment mutation methods); a
  concrete implementation will wrap `Cluster`/`Table`.
- `cht.api.dependencies.get_metadata_service` retrieves the service from application state to use as
  a FastAPI dependency.
- `cht.api.schemas` contains the response/request models shared by the router and future frontend.
- `cht.api.routers.metadata` hosts the HTTP surface. Endpoints are grouped under `/databases`.

Planned endpoints:
- `GET /databases` → `["default", "analytics", ...]`
- `GET /databases/{database}/tables` → `[{"name": "...", "comment": "..."}]`
- `GET /databases/{database}/tables/{table}/columns` →
  `[{"name": "ts", "type": "DateTime", "comment": "event time"}, ...]`
- `PATCH /databases/{database}/tables/{table}/comment` with body `{"comment": "..."}` → 204
- `PATCH /databases/{database}/tables/{table}/columns/{column}/comment` with body
  `{"comment": "..."}` → 204

Service implementation notes:
- Listing uses `Cluster` helpers (`SHOW DATABASES`, `system.tables`, `system.columns`) to avoid
  per-endpoint SQL in the router.
- Column listing reuses `Table` to resolve fully qualified names and to keep consistency with future
  table helpers.
- Comment updates will rely on small additions to `Table` (table comment) and a helper that reads a
  column type before issuing `ALTER TABLE ... COMMENT COLUMN ...`.

Testing approach:
- Unit tests use `fastapi.testclient.TestClient` with a fake `MetadataService` to keep the contract
  easy to read and free of ClickHouse dependencies.
- Follow-up work will add integration coverage against a mocked or local ClickHouse once the
  service logic is implemented.
