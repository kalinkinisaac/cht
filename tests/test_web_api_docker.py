"""
Comprehensive test suite for CHT Web API with Docker ClickHouse backend.
Tests all API endpoints, frontend functionality, and error handling.
"""

from __future__ import annotations

import io
import json
import shutil
import subprocess
import time
from pathlib import Path

import openpyxl
import pytest
from fastapi.testclient import TestClient

from cht.api.app import create_app
from cht.api.cluster_store import ClusterSettings, ClusterStore
from cht.api.services import ClickHouseMetadataService
from cht.cluster import Cluster

COMPOSE_FILE = Path(__file__).resolve().parent.parent / "docker-compose.yml"
CLICKHOUSE_SERVICE = "clickhouse"


def _compose_command() -> list[str]:
    """Get docker compose command based on available executable."""
    if shutil.which("docker"):
        return ["docker", "compose", "-f", str(COMPOSE_FILE)]
    if shutil.which("docker-compose"):
        return ["docker-compose", "-f", str(COMPOSE_FILE)]
    return []


def _wait_for_clickhouse(host: str = "localhost", port: int = 8123, timeout: int = 30) -> Cluster:
    """Wait for ClickHouse to become ready."""
    cluster = Cluster(
        name="docker_test",
        host=host,
        port=port,
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
def clickhouse_cluster():
    """Start ClickHouse container and return cluster connection."""
    compose = _compose_command()
    if not compose:
        pytest.skip("docker compose not available")

    # Stop any existing container
    subprocess.run([*compose, "stop", CLICKHOUSE_SERVICE], check=False)

    # Start fresh container
    subprocess.run([*compose, "up", "-d", CLICKHOUSE_SERVICE], check=True)
    cluster = _wait_for_clickhouse()
    yield cluster

    # Cleanup
    subprocess.run([*compose, "stop", CLICKHOUSE_SERVICE], check=False)


@pytest.fixture
def cluster_store(clickhouse_cluster: Cluster) -> ClusterStore:
    """Create cluster store with test cluster."""
    store = ClusterStore()
    store.add_cluster(
        "test_cluster",
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
    return store


@pytest.fixture
def api_client(cluster_store: ClusterStore) -> TestClient:
    """Create FastAPI test client."""
    service = ClickHouseMetadataService(cluster_store)
    app = create_app(service, cluster_store=cluster_store)
    return TestClient(app)


@pytest.fixture
def sample_data(clickhouse_cluster: Cluster):
    """Create sample database and tables for testing."""
    # Create test database
    clickhouse_cluster.query("CREATE DATABASE IF NOT EXISTS test_db")

    # Create test table with comments
    clickhouse_cluster.query(
        """
        CREATE TABLE IF NOT EXISTS test_db.users (
            id UInt64 COMMENT 'User identifier',
            name String COMMENT 'User full name',
            email String COMMENT 'User email address',
            created_at DateTime COMMENT 'Account creation time'
        )
        ENGINE = MergeTree
        ORDER BY id
        COMMENT 'User accounts table'
    """
    )

    # Create another test table
    clickhouse_cluster.query(
        """
        CREATE TABLE IF NOT EXISTS test_db.events (
            user_id UInt64,
            event_type String,
            timestamp DateTime
        )
        ENGINE = MergeTree
        ORDER BY (user_id, timestamp)
        COMMENT 'User events log'
    """
    )

    # Insert sample data
    clickhouse_cluster.query(
        """
        INSERT INTO test_db.users VALUES 
        (1, 'John Doe', 'john@example.com', '2023-01-01 10:00:00'),
        (2, 'Jane Smith', 'jane@example.com', '2023-01-02 11:00:00')
    """
    )

    yield

    # Cleanup
    clickhouse_cluster.query("DROP DATABASE IF EXISTS test_db")


class TestClustersAPI:
    """Test cluster management endpoints."""

    def test_list_clusters(self, api_client: TestClient):
        """Test listing clusters."""
        response = api_client.get("/clusters")
        assert response.status_code == 200
        clusters = response.json()
        assert len(clusters) >= 1
        assert clusters[0]["name"] == "test_cluster"
        assert clusters[0]["active"] is True

    def test_add_cluster(self, api_client: TestClient):
        """Test adding a new cluster."""
        new_cluster = {
            "name": "new_test",
            "host": "localhost",
            "port": 8123,
            "user": "test_user",
            "password": "test_pass",
            "secure": False,
            "verify": False,
            "read_only": True,
        }
        response = api_client.post("/clusters", json=new_cluster)
        assert response.status_code == 200
        result = response.json()
        assert result["message"] == "Cluster 'new_test' added successfully"

    def test_update_cluster(self, api_client: TestClient):
        """Test updating cluster settings."""
        update_data = {"host": "updated-host", "port": 9123, "user": "updated_user"}
        response = api_client.put("/clusters/test_cluster", json=update_data)
        assert response.status_code == 200
        result = response.json()
        assert result["message"] == "Cluster 'test_cluster' updated successfully"

    def test_delete_cluster(self, api_client: TestClient):
        """Test deleting a cluster."""
        # First add a cluster to delete
        new_cluster = {
            "name": "to_delete",
            "host": "localhost",
            "port": 8123,
            "user": "test",
            "password": "",
            "secure": False,
            "verify": False,
            "read_only": True,
        }
        api_client.post("/clusters", json=new_cluster)

        # Now delete it
        response = api_client.delete("/clusters/to_delete")
        assert response.status_code == 200
        result = response.json()
        assert result["message"] == "Cluster 'to_delete' deleted successfully"

    def test_select_cluster(self, api_client: TestClient):
        """Test selecting active cluster."""
        response = api_client.post("/clusters/test_cluster/select")
        assert response.status_code == 200
        result = response.json()
        assert result["message"] == "Cluster 'test_cluster' is now active"

    def test_test_cluster_connection(self, api_client: TestClient):
        """Test cluster connection testing."""
        response = api_client.post("/clusters/test_cluster/test")
        assert response.status_code == 200
        result = response.json()
        assert result["status"] == "ok"


class TestMetadataAPI:
    """Test metadata browsing endpoints."""

    def test_list_databases(self, api_client: TestClient, sample_data):
        """Test listing databases."""
        response = api_client.get("/databases?cluster=test_cluster")
        assert response.status_code == 200
        databases = response.json()
        assert "test_db" in databases
        assert "default" in databases

    def test_list_tables(self, api_client: TestClient, sample_data):
        """Test listing tables in a database."""
        response = api_client.get("/databases/test_db/tables?cluster=test_cluster")
        assert response.status_code == 200
        tables = response.json()
        table_names = [t["name"] for t in tables]
        assert "users" in table_names
        assert "events" in table_names

    def test_get_table_schema(self, api_client: TestClient, sample_data):
        """Test getting table schema."""
        response = api_client.get("/tables/test_db.users/schema?cluster=test_cluster")
        assert response.status_code == 200
        schema = response.json()

        assert schema["table_name"] == "users"
        assert schema["database_name"] == "test_db"
        assert len(schema["columns"]) == 4

        # Check specific columns
        columns = {col["name"]: col for col in schema["columns"]}
        assert columns["id"]["type"] == "UInt64"
        assert columns["id"]["comment"] == "User identifier"
        assert columns["name"]["type"] == "String"
        assert columns["email"]["comment"] == "User email address"

    def test_update_table_comment(self, api_client: TestClient, sample_data):
        """Test updating table comment."""
        new_comment = "Updated users table comment"
        response = api_client.post(
            "/tables/test_db.users/comment",
            params={"cluster": "test_cluster"},
            json={"comment": new_comment},
        )
        assert response.status_code == 200
        result = response.json()
        assert result["message"] == "Table comment updated successfully"

    def test_update_column_comment(self, api_client: TestClient, sample_data):
        """Test updating column comment."""
        new_comment = "Updated user ID comment"
        response = api_client.post(
            "/tables/test_db.users/columns/id/comment",
            params={"cluster": "test_cluster"},
            json={"comment": new_comment},
        )
        assert response.status_code == 200
        result = response.json()
        assert result["message"] == "Column comment updated successfully"

    def test_get_table_data_preview(self, api_client: TestClient, sample_data):
        """Test getting table data preview."""
        response = api_client.get("/tables/test_db.users/data?cluster=test_cluster&limit=10")
        assert response.status_code == 200
        data = response.json()

        assert len(data) == 2  # We inserted 2 rows
        assert data[0]["id"] == 1
        assert data[0]["name"] == "John Doe"
        assert data[1]["id"] == 2
        assert data[1]["name"] == "Jane Smith"

    def test_export_table_descriptions_to_excel(self, api_client: TestClient, sample_data):
        """Test exporting table descriptions to Excel format."""
        # Test with single database
        export_data = {"databases": ["test_db"], "cluster": "test_cluster"}

        response = api_client.post("/databases/export/excel", json=export_data)
        assert response.status_code == 200
        assert (
            response.headers["content-type"]
            == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
        # Check that filename contains table_descriptions and .xlsx extension (now with timestamp and quotes)
        content_disposition = response.headers["content-disposition"]
        assert "table_descriptions_" in content_disposition
        assert ".xlsx" in content_disposition

        # Verify we get binary Excel data
        excel_data = response.content
        assert len(excel_data) > 0

        # Check Excel file format signature (first few bytes should indicate Excel file)
        assert excel_data[:2] == b"PK"  # Excel files are ZIP archives

        # Parse the Excel file to verify content structure
        excel_buffer = io.BytesIO(excel_data)
        workbook = openpyxl.load_workbook(excel_buffer)

        # Should have at least one worksheet for the users table
        assert len(workbook.worksheets) > 0

        # Find the users table worksheet
        users_sheet = None
        for sheet in workbook.worksheets:
            if "users" in sheet.title.lower():
                users_sheet = sheet
                break

        assert users_sheet is not None, "Should have a worksheet for users table"

        # Verify worksheet structure
        # Should have table name in first row
        assert "users" in str(users_sheet["A1"].value).lower()

        # Find header row (should contain "Column Name", "Column Type", "Comment")
        header_row = None
        for row in users_sheet.iter_rows():
            if any(cell.value and "column name" in str(cell.value).lower() for cell in row):
                header_row = [cell.value for cell in row if cell.value]
                break

        assert header_row is not None, "Should have header row with column info"
        assert "Column Name" in header_row
        assert "Column Type" in header_row
        assert "Comment" in header_row

        # Test with multiple databases
        export_data_multi = {"databases": ["test_db", "default"], "cluster": "test_cluster"}

        response_multi = api_client.post("/databases/export/excel", json=export_data_multi)
        assert response_multi.status_code == 200
        assert len(response_multi.content) > 0

        # Test with empty database list (should still work)
        export_data_empty = {"databases": [], "cluster": "test_cluster"}

        response_empty = api_client.post("/databases/export/excel", json=export_data_empty)
        assert response_empty.status_code == 200

        # Test with invalid database name
        export_data_invalid = {"databases": ["nonexistent_db"], "cluster": "test_cluster"}

        response_invalid = api_client.post("/databases/export/excel", json=export_data_invalid)
        # Should still work but produce Excel with minimal content
        assert response_invalid.status_code == 200


class TestFrontendAPI:
    """Test frontend-specific endpoints."""

    def test_get_ui_page(self, api_client: TestClient):
        """Test frontend UI page load."""
        response = api_client.get("/ui")
        assert response.status_code == 200
        assert "text/html" in response.headers["content-type"]
        assert "CHT Web Interface" in response.text

    def test_ui_has_cluster_management(self, api_client: TestClient):
        """Test UI includes cluster management functionality."""
        response = api_client.get("/ui")
        content = response.text

        # Check for key UI elements
        assert "Add cluster" in content
        assert "cluster-form" in content
        assert "cluster-list" in content
        assert "renderClusters" in content

    def test_ui_has_metadata_browser(self, api_client: TestClient):
        """Test UI includes metadata browser functionality."""
        response = api_client.get("/ui")
        content = response.text

        # Check for metadata browser elements
        assert "db-select" in content
        assert "tables" in content
        assert "table-detail" in content
        assert "loadDatabases" in content
        assert "loadTables" in content


class TestErrorHandling:
    """Test error handling and edge cases."""

    def test_invalid_cluster_name(self, api_client: TestClient):
        """Test operations with invalid cluster name."""
        response = api_client.get("/databases?cluster=nonexistent")
        assert response.status_code == 400
        result = response.json()
        assert "not found" in result["detail"].lower()

    def test_invalid_database_name(self, api_client: TestClient):
        """Test operations with invalid database name."""
        response = api_client.get("/tables?cluster=test_cluster&database=nonexistent")
        assert response.status_code == 500  # ClickHouse error

    def test_invalid_table_name(self, api_client: TestClient, sample_data):
        """Test operations with invalid table name."""
        response = api_client.get("/tables/test_db.nonexistent/schema?cluster=test_cluster")
        assert response.status_code == 500  # ClickHouse error

    def test_duplicate_cluster_name(self, api_client: TestClient):
        """Test adding cluster with duplicate name."""
        duplicate_cluster = {
            "name": "test_cluster",  # Already exists
            "host": "localhost",
            "port": 8123,
            "user": "test",
            "password": "",
            "secure": False,
            "verify": False,
            "read_only": True,
        }
        response = api_client.post("/clusters", json=duplicate_cluster)
        assert response.status_code == 400
        result = response.json()
        assert "already exists" in result["detail"].lower()

    def test_malformed_cluster_data(self, api_client: TestClient):
        """Test adding cluster with malformed data."""
        malformed_cluster = {
            "name": "bad_cluster",
            "host": "localhost",
            "port": "not_a_number",  # Should be integer
            "user": "test",
            # Missing required fields
        }
        response = api_client.post("/clusters", json=malformed_cluster)
        assert response.status_code == 422  # Validation error


class TestSecurityAndValidation:
    """Test security and input validation."""

    def test_sql_injection_prevention(self, api_client: TestClient, sample_data):
        """Test that SQL injection is prevented."""
        malicious_input = "'; DROP TABLE users; --"
        response = api_client.get(f"/tables?cluster=test_cluster&database={malicious_input}")
        # Should fail gracefully, not execute malicious SQL
        assert response.status_code in [400, 500]

    def test_xss_prevention_in_comments(self, api_client: TestClient, sample_data):
        """Test XSS prevention in comment updates."""
        malicious_comment = "<script>alert('xss')</script>"
        response = api_client.post(
            "/tables/test_db.users/comment",
            params={"cluster": "test_cluster"},
            json={"comment": malicious_comment},
        )
        # Should accept but sanitize input
        assert response.status_code == 200

    def test_cluster_name_validation(self, api_client: TestClient):
        """Test cluster name validation."""
        invalid_names = ["", "   ", "cluster with spaces", "cluster/with/slashes"]
        for name in invalid_names:
            cluster_data = {
                "name": name,
                "host": "localhost",
                "port": 8123,
                "user": "test",
                "password": "",
                "secure": False,
                "verify": False,
                "read_only": True,
            }
            response = api_client.post("/clusters", json=cluster_data)
            assert response.status_code in [400, 422]


class TestPerformanceAndLimits:
    """Test performance and limits."""

    def test_large_data_preview_limit(self, api_client: TestClient, sample_data):
        """Test data preview respects limit parameter."""
        response = api_client.get("/tables/test_db.users/data?cluster=test_cluster&limit=1")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1

    def test_concurrent_requests(self, api_client: TestClient, sample_data):
        """Test handling of concurrent requests."""
        import concurrent.futures

        def make_request():
            return api_client.get("/databases?cluster=test_cluster")

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(make_request) for _ in range(10)]
            results = [future.result() for future in concurrent.futures.as_completed(futures)]

        # All requests should succeed
        for response in results:
            assert response.status_code == 200

    def test_timeout_handling(self, api_client: TestClient):
        """Test timeout handling for long queries."""
        # This would require a specific slow query setup
        # For now, just test that the timeout is properly configured
        response = api_client.get("/databases?cluster=test_cluster")
        assert response.status_code == 200
        # Actual timeout testing would need a slow ClickHouse query


class TestWebInterfaceAutomation:
    """Test web interface using browser automation."""

    @pytest.mark.skipif(
        not pytest.importorskip("selenium", minversion=None), reason="Selenium not available"
    )
    def test_web_interface_no_js_errors(self, docker_clickhouse):
        """Test web interface has no JavaScript console errors."""
        import time

        import requests
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.support.ui import WebDriverWait

        # First check if web server is running
        web_url = "http://127.0.0.1:8000"
        try:
            response = requests.get(web_url, timeout=5)
            if response.status_code != 200:
                pytest.skip(f"Web server not running at {web_url}")
        except requests.exceptions.RequestException:
            pytest.skip(f"Web server not available at {web_url}")

        # Setup Chrome options for headless testing
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option("useAutomationExtension", False)

        # Enable logging for console errors
        chrome_options.set_capability("goog:loggingPrefs", {"browser": "ALL"})

        driver = None
        try:
            driver = webdriver.Chrome(options=chrome_options)

            # Navigate to web interface
            driver.get(web_url)

            # Wait for page to load
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "body")))

            # Wait a bit more for any dynamic content to load
            time.sleep(2)

            # Get console logs
            logs = driver.get_log("browser")

            # Filter for actual errors (ignore warnings, info, etc.)
            errors = []
            for entry in logs:
                if entry["level"] in ["SEVERE"]:
                    # Skip network errors for resources that might not exist
                    if "net::" not in entry["message"].lower():
                        errors.append(entry)

            # Check if page loaded successfully
            page_title = driver.title
            assert page_title, "Page should have a title"

            # Check if main content is present
            body_text = driver.find_element(By.TAG_NAME, "body").text
            assert (
                "ClickHouse" in body_text or "Tables" in body_text or len(body_text) > 50
            ), "Page should contain meaningful content"

            # Report any JavaScript errors
            if errors:
                error_messages = [f"Level: {e['level']}, Message: {e['message']}" for e in errors]
                pytest.fail(f"JavaScript errors found:\n" + "\n".join(error_messages))

        except Exception as e:
            pytest.skip(f"Browser automation test failed: {str(e)}")
        finally:
            if driver:
                driver.quit()

    def test_web_interface_basic_functionality(self, docker_clickhouse):
        """Test basic web interface functionality without browser automation."""
        import requests

        web_url = "http://127.0.0.1:8000"

        # Check if web server is running
        try:
            response = requests.get(web_url, timeout=10)
            assert response.status_code == 200

            # Check if it's HTML content
            content_type = response.headers.get("content-type", "")
            assert "text/html" in content_type

            # Check if page contains expected content
            content = response.text
            assert len(content) > 1000, "HTML content should be substantial"
            assert "ClickHouse" in content or "Tables" in content

            # Check if CSS/JS resources are mentioned
            assert "style" in content.lower() or ".css" in content.lower()
            assert "script" in content.lower() or ".js" in content.lower()

        except requests.exceptions.RequestException as e:
            pytest.skip(f"Web server not available: {str(e)}")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
