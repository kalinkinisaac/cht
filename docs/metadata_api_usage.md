# CHT metadata API usage guide

## Local stack with Docker
1. Start ClickHouse:
   ```bash
   docker compose -f docker-compose.yml up -d clickhouse
   ```
2. Wait for readiness (same defaults as tests):
   - Host: `localhost`
   - HTTP port: `8123`
   - User/password: `developer` / `developer`

## Run the API locally
The API uses FastAPI and exposes Swagger at `/docs`. 

### Quick Start (Recommended)
After installing CHT (`pip install -e .` or `pip install git+https://github.com/kalinkinisaac/cht.git`):

```bash
# Simple command - uses default localhost ClickHouse
python -m cht.web

# Or with custom ClickHouse settings
python -m cht.web --ch-host production.clickhouse.com --ch-user admin --ch-password secret

# Or use the installed command (after pip install)
cht-web --port 9000 --reload
```

### Manual Setup (Advanced)
Make sure the code is importable (either `pip install -e .` or set `PYTHONPATH=src`).

```bash
# From repo root
pip install -e .            # or ensure PYTHONPATH=src when running uvicorn

export CH_HOST=localhost
export CH_PORT=8123
export CH_USER=developer
export CH_PASSWORD=developer

PYTHONPATH=src python3 -m uvicorn --app-dir src cht.api.main:app --reload --port 8000
```

Navigate to http://localhost:8000/docs to explore and try requests interactively (Swagger UI).
For a friendly UI, open http://localhost:8000/ui (no build step needed).

## Cluster management endpoints
- Add a cluster and optionally make it active (used when `cluster` query param is omitted):
  ```bash
  curl -X POST http://localhost:8000/clusters \
    -H 'Content-Type: application/json' \
    -d '{"name":"analytics","host":"ch-host","port":8123,"user":"dev","password":"dev","make_active":true}'
  ```
- List configured clusters and which one is active:
  ```bash
  curl http://localhost:8000/clusters
  ```
- Switch active cluster:
  ```bash
  curl -X POST http://localhost:8000/clusters/analytics/select
  ```

You can also target a specific cluster per request via `?cluster=analytics` on metadata routes.

## Example calls (using curl)
- List databases:
  ```bash
  curl http://localhost:8000/databases
  ```
- List tables with comments:
  ```bash
  curl http://localhost:8000/databases/default/tables
  ```
- List columns with types/comments:
  ```bash
  curl http://localhost:8000/databases/default/tables/metadata_api_demo/columns
  ```
- Update comments:
  ```bash
  curl -X PATCH http://localhost:8000/databases/default/tables/metadata_api_demo/comment \
    -H 'Content-Type: application/json' \
    -d '{"comment":"new table comment"}'

  curl -X PATCH http://localhost:8000/databases/default/tables/metadata_api_demo/columns/user_id/comment \
    -H 'Content-Type: application/json' \
    -d '{"comment":"updated column comment"}'
  ```

## Running the Docker-backed tests
```bash
python3 -m pytest tests/test_metadata_api_docker.py -q
```
They will bring up the ClickHouse container, hit the API routes via `TestClient`, print the call/response pairs, and assert the metadata and comment updates round-trip.
