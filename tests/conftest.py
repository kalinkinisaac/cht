"""
Test utilities and fixtures for CHT web testing.
Shared utilities, mock objects, and helper functions.
"""

from __future__ import annotations

import json
import subprocess
import time
from pathlib import Path
from typing import Any, Dict, Generator

import pytest
from fastapi.testclient import TestClient

from cht.api.app import create_app
from cht.api.cluster_store import ClusterSettings, ClusterStore
from cht.api.services import ClickHouseMetadataService
from cht.cluster import Cluster


class MockClickHouseClient:
    """Mock ClickHouse client for testing without real database."""

    def __init__(self, responses: Dict[str, Any] | None = None):
        self.responses = responses or {}
        self.queries = []  # Track executed queries

    def ping(self):
        """Mock ping - always succeeds."""
        return True

    def query(self, sql: str, parameters=None):
        """Mock query execution."""
        self.queries.append({"sql": sql, "parameters": parameters})

        # Return predefined responses based on SQL pattern
        if "SHOW DATABASES" in sql:
            return self.responses.get("databases", ["default", "system"])
        elif "SHOW TABLES" in sql:
            return self.responses.get("tables", [])
        elif "DESCRIBE" in sql:
            return self.responses.get("describe", [])
        else:
            return self.responses.get("default", [])


class MockCluster:
    """Mock cluster for testing."""

    def __init__(self, name: str = "mock", responses: Dict[str, Any] | None = None):
        self.name = name
        self.host = "localhost"
        self.port = 8123
        self.user = "mock_user"
        self.password = "mock_pass"
        self.secure = False
        self.verify = False
        self.read_only = False
        self.client = MockClickHouseClient(responses)

    def query(self, sql: str):
        """Execute query through mock client."""
        return self.client.query(sql)

    def query_with_fresh_client(self, sql: str):
        """Mock fresh client query."""
        return self.query(sql)

    def create_fresh_client(self):
        """Mock fresh client creation."""
        return self.client


def create_test_cluster_store() -> ClusterStore:
    """Create cluster store with mock clusters for testing."""
    store = ClusterStore()

    # Add mock clusters
    mock_responses = {
        "databases": ["default", "system", "test_db"],
        "tables": [
            {"name": "users", "comment": "User accounts"},
            {"name": "events", "comment": "User events"},
        ],
        "describe": [
            {"name": "id", "type": "UInt64", "comment": "User ID"},
            {"name": "name", "type": "String", "comment": "User name"},
        ],
    }

    mock_cluster = MockCluster("test_cluster", mock_responses)
    store.add_cluster_instance(
        "test_cluster",
        ClusterSettings(
            host="localhost",
            port=8123,
            user="test_user",
            password="test_pass",
            secure=False,
            verify=False,
            read_only=False,
        ),
        cluster=mock_cluster,
        make_active=True,
    )

    return store


@pytest.fixture
def mock_cluster_store() -> ClusterStore:
    """Fixture providing mock cluster store."""
    return create_test_cluster_store()


@pytest.fixture
def mock_api_client(mock_cluster_store: ClusterStore) -> TestClient:
    """Fixture providing mock API client."""
    service = ClickHouseMetadataService(mock_cluster_store)
    app = create_app(service, cluster_store=mock_cluster_store)
    return TestClient(app)


class DockerClickHouseManager:
    """Manager for Docker ClickHouse instances in tests."""

    def __init__(self, compose_file: Path):
        self.compose_file = compose_file
        self.service_name = "clickhouse"

    def _compose_command(self) -> list[str]:
        """Get docker compose command."""
        import shutil

        if shutil.which("docker"):
            return ["docker", "compose", "-f", str(self.compose_file)]
        if shutil.which("docker-compose"):
            return ["docker-compose", "-f", str(self.compose_file)]
        return []

    def start(self) -> Cluster:
        """Start ClickHouse container and return cluster."""
        compose = self._compose_command()
        if not compose:
            raise RuntimeError("Docker compose not available")

        # Stop any existing container
        subprocess.run([*compose, "stop", self.service_name], check=False)

        # Start fresh container
        subprocess.run([*compose, "up", "-d", self.service_name], check=True)

        # Wait for readiness
        return self._wait_for_ready()

    def stop(self):
        """Stop ClickHouse container."""
        compose = self._compose_command()
        if compose:
            subprocess.run([*compose, "stop", self.service_name], check=False)

    def _wait_for_ready(self, timeout: int = 30) -> Cluster:
        """Wait for ClickHouse to become ready."""
        cluster = Cluster(
            name="docker_test",
            host="localhost",
            port=8123,
            user="developer",
            password="developer",
        )

        for i in range(timeout):
            try:
                cluster.client.ping()
                return cluster
            except Exception as e:
                if i == timeout - 1:
                    raise RuntimeError(f"ClickHouse not ready after {timeout}s: {e}")
                time.sleep(1)

        raise RuntimeError("ClickHouse did not become ready in time")


@pytest.fixture(scope="session")
def docker_manager():
    """Fixture providing Docker ClickHouse manager."""
    compose_file = Path(__file__).resolve().parent.parent / "docker-compose.yml"
    return DockerClickHouseManager(compose_file)


class TestDataBuilder:
    """Builder for creating test data in ClickHouse."""

    def __init__(self, cluster: Cluster):
        self.cluster = cluster
        self.created_databases = []
        self.created_tables = []

    def create_database(self, name: str) -> "TestDataBuilder":
        """Create test database."""
        self.cluster.query(f"CREATE DATABASE IF NOT EXISTS {name}")
        self.created_databases.append(name)
        return self

    def create_table(
        self, database: str, table: str, schema: str, comment: str = ""
    ) -> "TestDataBuilder":
        """Create test table with schema."""
        full_name = f"{database}.{table}"
        comment_clause = f"COMMENT '{comment}'" if comment else ""

        self.cluster.query(
            f"""
            CREATE TABLE IF NOT EXISTS {full_name} (
                {schema}
            ) ENGINE = MergeTree
            ORDER BY tuple()
            {comment_clause}
        """
        )

        self.created_tables.append(full_name)
        return self

    def insert_data(self, database: str, table: str, data: list) -> "TestDataBuilder":
        """Insert test data."""
        full_name = f"{database}.{table}"

        if data:
            # Convert data to VALUES format
            values = []
            for row in data:
                if isinstance(row, (list, tuple)):
                    formatted_values = []
                    for value in row:
                        if isinstance(value, str):
                            formatted_values.append(f"'{value}'")
                        else:
                            formatted_values.append(str(value))
                    values.append(f"({', '.join(formatted_values)})")
                else:
                    values.append(str(row))

            values_clause = ", ".join(values)
            self.cluster.query(f"INSERT INTO {full_name} VALUES {values_clause}")

        return self

    def cleanup(self):
        """Clean up created test data."""
        # Drop tables
        for table in reversed(self.created_tables):
            try:
                self.cluster.query(f"DROP TABLE IF EXISTS {table}")
            except Exception:
                pass  # Ignore cleanup errors

        # Drop databases
        for database in reversed(self.created_databases):
            try:
                self.cluster.query(f"DROP DATABASE IF EXISTS {database}")
            except Exception:
                pass  # Ignore cleanup errors

        self.created_databases.clear()
        self.created_tables.clear()


@pytest.fixture
def test_data_builder(clickhouse_cluster: Cluster):
    """Fixture providing test data builder."""
    builder = TestDataBuilder(clickhouse_cluster)
    yield builder
    builder.cleanup()


class APITestHelper:
    """Helper class for API testing."""

    def __init__(self, client: TestClient):
        self.client = client

    def add_cluster(self, name: str, **kwargs) -> dict:
        """Add cluster via API."""
        cluster_data = {
            "name": name,
            "host": kwargs.get("host", "localhost"),
            "port": kwargs.get("port", 8123),
            "user": kwargs.get("user", "default"),
            "password": kwargs.get("password", ""),
            "secure": kwargs.get("secure", False),
            "verify": kwargs.get("verify", False),
            "read_only": kwargs.get("read_only", True),
        }

        response = self.client.post("/clusters", json=cluster_data)
        assert response.status_code == 200
        return response.json()

    def test_cluster_connection(self, name: str) -> dict:
        """Test cluster connection via API."""
        response = self.client.post(f"/clusters/{name}/test")
        return response.json()

    def get_databases(self, cluster: str = "default") -> list:
        """Get databases via API."""
        response = self.client.get(f"/databases?cluster={cluster}")
        assert response.status_code == 200
        return response.json()

    def get_tables(self, database: str, cluster: str = "default") -> list:
        """Get tables via API."""
        response = self.client.get(f"/tables?cluster={cluster}&database={database}")
        assert response.status_code == 200
        return response.json()

    def get_table_schema(self, table_name: str, cluster: str = "default") -> dict:
        """Get table schema via API."""
        response = self.client.get(f"/tables/{table_name}/schema?cluster={cluster}")
        assert response.status_code == 200
        return response.json()

    def update_table_comment(self, table_name: str, comment: str, cluster: str = "default") -> dict:
        """Update table comment via API."""
        response = self.client.post(
            f"/tables/{table_name}/comment", params={"cluster": cluster}, json={"comment": comment}
        )
        assert response.status_code == 200
        return response.json()


@pytest.fixture
def api_helper(api_client: TestClient):
    """Fixture providing API test helper."""
    return APITestHelper(api_client)


class WebServerManager:
    """Manager for starting CHT web server in tests."""

    def __init__(self, port: int = 8765):
        self.port = port
        self.process = None
        self.base_url = f"http://localhost:{port}"

    def start(self, **kwargs):
        """Start web server."""
        import subprocess

        project_root = Path(__file__).resolve().parent.parent

        cmd = [
            "python",
            "-m",
            "cht.web",
            "--port",
            str(self.port),
            "--ch-host",
            kwargs.get("ch_host", "localhost"),
            "--ch-port",
            str(kwargs.get("ch_port", 8123)),
            "--ch-user",
            kwargs.get("ch_user", "developer"),
            "--ch-password",
            kwargs.get("ch_password", "developer"),
        ]

        self.process = subprocess.Popen(cmd, cwd=project_root)

        # Wait for server to start
        time.sleep(3)

        return self.base_url

    def stop(self):
        """Stop web server."""
        if self.process:
            self.process.terminate()
            self.process.wait()
            self.process = None


@pytest.fixture(scope="session")
def web_server_manager():
    """Fixture providing web server manager."""
    manager = WebServerManager()
    yield manager
    manager.stop()


def assert_response_success(response, expected_status: int = 200):
    """Assert API response is successful."""
    assert (
        response.status_code == expected_status
    ), f"Expected {expected_status}, got {response.status_code}: {response.text}"


def assert_response_error(response, expected_status: int | None = None):
    """Assert API response is an error."""
    if expected_status:
        assert response.status_code == expected_status
    else:
        assert response.status_code >= 400


def load_test_config() -> dict:
    """Load test configuration from file or environment."""
    config = {
        "clickhouse": {
            "host": "localhost",
            "port": 8123,
            "user": "developer",
            "password": "developer",
        },
        "web_server": {
            "port": 8765,
            "timeout": 30,
        },
        "timeouts": {
            "startup": 30,
            "request": 10,
            "ui_wait": 15,
        },
    }

    # Override with environment variables if present
    import os

    if os.getenv("CHT_TEST_CH_HOST"):
        config["clickhouse"]["host"] = os.getenv("CHT_TEST_CH_HOST")
    if port_env := os.getenv("CHT_TEST_CH_PORT"):
        config["clickhouse"]["port"] = int(port_env)

    return config


@pytest.fixture(scope="session")
def test_config():
    """Fixture providing test configuration."""
    return load_test_config()


def skip_if_no_docker():
    """Skip test if Docker is not available."""
    import shutil

    if not shutil.which("docker"):
        pytest.skip("Docker not available")


def skip_if_no_selenium():
    """Skip test if Selenium/Chrome is not available."""
    try:
        import shutil

        import selenium

        if not shutil.which("chromedriver"):
            pytest.skip("ChromeDriver not available")
    except ImportError:
        pytest.skip("Selenium not installed")


# Pytest marks for test categorization
pytest_docker = pytest.mark.docker
pytest_integration = pytest.mark.integration
pytest_ui = pytest.mark.ui
pytest_slow = pytest.mark.slow
pytest_api = pytest.mark.api
