"""
Frontend integration tests using Selenium WebDriver.
Tests the full web interface functionality including user interactions.
"""

from __future__ import annotations

import shutil
import subprocess
import time
from pathlib import Path
from typing import Generator

import pytest
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait

from cht.cluster import Cluster

COMPOSE_FILE = Path(__file__).resolve().parent.parent / "docker-compose.yml"
CLICKHOUSE_SERVICE = "clickhouse"


def _compose_command() -> list[str]:
    """Get docker compose command."""
    if shutil.which("docker"):
        return ["docker", "compose", "-f", str(COMPOSE_FILE)]
    if shutil.which("docker-compose"):
        return ["docker-compose", "-f", str(COMPOSE_FILE)]
    return []


def _wait_for_clickhouse(timeout: int = 30) -> Cluster:
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
def clickhouse_cluster():
    """Start ClickHouse container."""
    compose = _compose_command()
    if not compose:
        pytest.skip("docker compose not available")

    subprocess.run([*compose, "stop", CLICKHOUSE_SERVICE], check=False)
    subprocess.run([*compose, "up", "-d", CLICKHOUSE_SERVICE], check=True)
    cluster = _wait_for_clickhouse()
    yield cluster
    subprocess.run([*compose, "stop", CLICKHOUSE_SERVICE], check=False)


@pytest.fixture(scope="session")
def web_server(clickhouse_cluster):
    """Start CHT web server."""
    import subprocess
    import time
    from pathlib import Path

    # Get project root
    project_root = Path(__file__).resolve().parent.parent

    # Start web server
    proc = subprocess.Popen(
        [
            "python",
            "-m",
            "cht.web",
            "--port",
            "8765",
            "--ch-host",
            "localhost",
            "--ch-port",
            "8123",
            "--ch-user",
            "developer",
            "--ch-password",
            "developer",
        ],
        cwd=project_root,
    )

    # Wait for server to start
    time.sleep(3)

    yield "http://localhost:8765"

    # Cleanup
    proc.terminate()
    proc.wait()


@pytest.fixture
def chrome_driver() -> Generator[webdriver.Chrome, None, None]:
    """Create Chrome WebDriver instance."""
    if not shutil.which("chromedriver"):
        pytest.skip("chromedriver not available")

    options = Options()
    options.add_argument("--headless")  # Run in headless mode
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")

    driver = webdriver.Chrome(options=options)
    driver.implicitly_wait(10)
    yield driver
    driver.quit()


@pytest.fixture
def sample_data(clickhouse_cluster: Cluster):
    """Create sample data for frontend testing."""
    # Create test database
    clickhouse_cluster.query("CREATE DATABASE IF NOT EXISTS frontend_test")

    # Create test table
    clickhouse_cluster.query(
        """
        CREATE TABLE IF NOT EXISTS frontend_test.products (
            id UInt64 COMMENT 'Product ID',
            name String COMMENT 'Product name',
            price Decimal(10,2) COMMENT 'Product price',
            category String COMMENT 'Product category',
            created_at DateTime COMMENT 'Creation timestamp'
        )
        ENGINE = MergeTree
        ORDER BY id
        COMMENT 'Products catalog'
    """
    )

    # Insert sample data
    clickhouse_cluster.query(
        """
        INSERT INTO frontend_test.products VALUES 
        (1, 'Laptop', 999.99, 'Electronics', '2023-01-01 10:00:00'),
        (2, 'Mouse', 29.99, 'Electronics', '2023-01-01 11:00:00'),
        (3, 'Book', 19.99, 'Education', '2023-01-01 12:00:00')
    """
    )

    yield

    # Cleanup
    clickhouse_cluster.query("DROP DATABASE IF EXISTS frontend_test")


class TestUIBasics:
    """Test basic UI functionality."""

    def test_page_loads(self, chrome_driver, web_server):
        """Test that the main page loads correctly."""
        chrome_driver.get(f"{web_server}/ui")

        # Check page title
        assert "CHT Web Interface" in chrome_driver.title

        # Check main elements are present
        assert chrome_driver.find_element(By.ID, "cluster-list")
        assert chrome_driver.find_element(By.ID, "cluster-form")
        assert chrome_driver.find_element(By.ID, "db-select")

    def test_page_has_correct_structure(self, chrome_driver, web_server):
        """Test page has expected structure."""
        chrome_driver.get(f"{web_server}/ui")

        # Check headers
        headers = chrome_driver.find_elements(By.TAG_NAME, "h2")
        header_texts = [h.text for h in headers]
        assert "Cluster Management" in header_texts
        assert "Metadata Browser" in header_texts

        # Check form elements
        assert chrome_driver.find_element(By.ID, "cluster-name")
        assert chrome_driver.find_element(By.ID, "cluster-host")
        assert chrome_driver.find_element(By.ID, "cluster-port")


class TestClusterManagement:
    """Test cluster management functionality."""

    def test_add_new_cluster(self, chrome_driver, web_server):
        """Test adding a new cluster through the UI."""
        chrome_driver.get(f"{web_server}/ui")

        # Fill cluster form
        chrome_driver.find_element(By.ID, "cluster-name").send_keys("test_ui_cluster")
        chrome_driver.find_element(By.ID, "cluster-host").send_keys("test.example.com")
        chrome_driver.find_element(By.ID, "cluster-port").clear()
        chrome_driver.find_element(By.ID, "cluster-port").send_keys("9000")
        chrome_driver.find_element(By.ID, "cluster-user").send_keys("testuser")
        chrome_driver.find_element(By.ID, "cluster-password").send_keys("testpass")

        # Submit form
        submit_button = chrome_driver.find_element(
            By.CSS_SELECTOR, "#cluster-form button[type='submit']"
        )
        submit_button.click()

        # Wait for success message
        WebDriverWait(chrome_driver, 10).until(EC.presence_of_element_located((By.ID, "status")))

        # Check if cluster appears in list
        cluster_list = chrome_driver.find_element(By.ID, "cluster-list")
        assert "test_ui_cluster" in cluster_list.text

    def test_cluster_connection_test(self, chrome_driver, web_server):
        """Test cluster connection testing."""
        chrome_driver.get(f"{web_server}/ui")

        # Wait for default cluster to load
        WebDriverWait(chrome_driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "[data-test]"))
        )

        # Click test button for default cluster
        test_button = chrome_driver.find_element(By.CSS_SELECTOR, "[data-test='default']")
        test_button.click()

        # Wait for test result
        WebDriverWait(chrome_driver, 15).until(
            EC.text_to_be_present_in_element((By.ID, "status"), "Connection")
        )

        status_text = chrome_driver.find_element(By.ID, "status").text
        assert "Connection" in status_text

    def test_cluster_switching(self, chrome_driver, web_server):
        """Test switching between clusters."""
        chrome_driver.get(f"{web_server}/ui")

        # Add a second cluster first
        chrome_driver.find_element(By.ID, "cluster-name").send_keys("second_cluster")
        chrome_driver.find_element(By.ID, "cluster-host").send_keys("localhost")
        chrome_driver.find_element(By.CSS_SELECTOR, "#cluster-form button[type='submit']").click()

        # Wait for cluster to be added
        time.sleep(2)

        # Switch to the new cluster
        WebDriverWait(chrome_driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "[data-select='second_cluster']"))
        )
        select_button = chrome_driver.find_element(
            By.CSS_SELECTOR, "[data-select='second_cluster']"
        )
        select_button.click()

        # Verify switch
        WebDriverWait(chrome_driver, 10).until(
            EC.text_to_be_present_in_element((By.ID, "status"), "Switched")
        )


class TestMetadataBrowser:
    """Test metadata browsing functionality."""

    def test_database_loading(self, chrome_driver, web_server, sample_data):
        """Test that databases load correctly."""
        chrome_driver.get(f"{web_server}/ui")

        # Wait for databases to load
        WebDriverWait(chrome_driver, 15).until(EC.element_to_be_clickable((By.ID, "db-select")))

        # Check database dropdown
        db_select = Select(chrome_driver.find_element(By.ID, "db-select"))
        options = [option.text for option in db_select.options]
        assert "frontend_test" in options
        assert "default" in options

    def test_table_browsing(self, chrome_driver, web_server, sample_data):
        """Test table browsing functionality."""
        chrome_driver.get(f"{web_server}/ui")

        # Wait for databases to load
        WebDriverWait(chrome_driver, 15).until(EC.element_to_be_clickable((By.ID, "db-select")))

        # Select test database
        db_select = Select(chrome_driver.find_element(By.ID, "db-select"))
        db_select.select_by_value("frontend_test")

        # Wait for tables to load
        WebDriverWait(chrome_driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "[data-table='products']"))
        )

        # Check that products table is visible
        tables_section = chrome_driver.find_element(By.ID, "tables")
        assert "products" in tables_section.text

    def test_table_detail_view(self, chrome_driver, web_server, sample_data):
        """Test table detail view functionality."""
        chrome_driver.get(f"{web_server}/ui")

        # Navigate to test database and table
        WebDriverWait(chrome_driver, 15).until(EC.element_to_be_clickable((By.ID, "db-select")))

        db_select = Select(chrome_driver.find_element(By.ID, "db-select"))
        db_select.select_by_value("frontend_test")

        # Wait for tables and click on products
        WebDriverWait(chrome_driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "[data-table='products']"))
        )
        products_button = chrome_driver.find_element(By.CSS_SELECTOR, "[data-table='products']")
        products_button.click()

        # Wait for table details to load
        WebDriverWait(chrome_driver, 10).until(
            EC.text_to_be_present_in_element((By.ID, "table-detail"), "id")
        )

        # Check table details
        table_detail = chrome_driver.find_element(By.ID, "table-detail")
        detail_text = table_detail.text
        assert "id" in detail_text
        assert "name" in detail_text
        assert "price" in detail_text
        assert "Product ID" in detail_text  # Comment

    def test_comment_editing(self, chrome_driver, web_server, sample_data):
        """Test comment editing functionality."""
        chrome_driver.get(f"{web_server}/ui")

        # Navigate to test table
        WebDriverWait(chrome_driver, 15).until(EC.element_to_be_clickable((By.ID, "db-select")))

        db_select = Select(chrome_driver.find_element(By.ID, "db-select"))
        db_select.select_by_value("frontend_test")

        WebDriverWait(chrome_driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "[data-table='products']"))
        )
        chrome_driver.find_element(By.CSS_SELECTOR, "[data-table='products']").click()

        # Wait for table details to load
        WebDriverWait(chrome_driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "[data-edit-table]"))
        )

        # Click edit table comment button
        edit_button = chrome_driver.find_element(By.CSS_SELECTOR, "[data-edit-table]")
        edit_button.click()

        # Wait for prompt and handle it
        WebDriverWait(chrome_driver, 5).until(EC.alert_is_present())
        alert = chrome_driver.switch_to.alert
        alert.send_keys("Updated products catalog comment")
        alert.accept()

        # Wait for update confirmation
        WebDriverWait(chrome_driver, 10).until(
            EC.text_to_be_present_in_element((By.ID, "status"), "comment updated")
        )

    def test_excel_export_functionality(self, chrome_driver, web_server, sample_data):
        """Test Excel export dialog and functionality."""
        chrome_driver.get(f"{web_server}/ui")

        # Wait for page to load
        WebDriverWait(chrome_driver, 10).until(
            EC.presence_of_element_located((By.ID, "cluster-select"))
        )

        # Set up cluster connection (assuming we have the test cluster available)
        cluster_select = Select(chrome_driver.find_element(By.ID, "cluster-select"))
        try:
            cluster_select.select_by_visible_text("test_cluster")
        except:
            # Add cluster if not available
            host_input = chrome_driver.find_element(By.NAME, "host")
            host_input.clear()
            host_input.send_keys("localhost")

            port_input = chrome_driver.find_element(By.NAME, "port")
            port_input.clear()
            port_input.send_keys("8123")

            user_input = chrome_driver.find_element(By.NAME, "user")
            user_input.clear()
            user_input.send_keys("developer")

            password_input = chrome_driver.find_element(By.NAME, "password")
            password_input.clear()
            password_input.send_keys("developer")

            cluster_name_input = chrome_driver.find_element(By.NAME, "cluster_name")
            cluster_name_input.clear()
            cluster_name_input.send_keys("test_cluster")

            add_button = chrome_driver.find_element(
                By.XPATH, "//button[contains(text(), 'Add cluster')]"
            )
            add_button.click()

            # Wait for cluster to be added and selected
            WebDriverWait(chrome_driver, 10).until(
                EC.text_to_be_present_in_element((By.ID, "cluster-select"), "test_cluster")
            )

        # Wait for databases to load
        WebDriverWait(chrome_driver, 10).until(EC.presence_of_element_located((By.ID, "db-select")))

        # Find and click the export button
        export_button = chrome_driver.find_element(
            By.XPATH, "//button[contains(text(), 'Export to Excel')]"
        )
        assert export_button.is_displayed()
        export_button.click()

        # Wait for export dialog to appear
        WebDriverWait(chrome_driver, 5).until(
            EC.presence_of_element_located((By.ID, "export-modal"))
        )

        # Verify dialog is visible
        export_modal = chrome_driver.find_element(By.ID, "export-modal")
        assert export_modal.is_displayed()

        # Verify dialog has database checkboxes
        database_checkboxes = chrome_driver.find_elements(
            By.CSS_SELECTOR, "#database-checkboxes input[type='checkbox']"
        )
        assert len(database_checkboxes) > 0, "Should have at least one database checkbox"

        # Test select all button
        select_all_button = chrome_driver.find_element(
            By.XPATH, "//button[contains(text(), 'Select All')]"
        )
        select_all_button.click()

        # Verify all checkboxes are checked
        for checkbox in database_checkboxes:
            assert checkbox.is_selected()

        # Test clear all button
        clear_all_button = chrome_driver.find_element(
            By.XPATH, "//button[contains(text(), 'Clear All')]"
        )
        clear_all_button.click()

        # Verify all checkboxes are unchecked
        for checkbox in database_checkboxes:
            assert not checkbox.is_selected()

        # Select test_db for export
        test_db_checkbox = None
        for checkbox in database_checkboxes:
            if checkbox.get_attribute("value") == "test_db":
                test_db_checkbox = checkbox
                break

        assert test_db_checkbox is not None, "Should have test_db checkbox"
        test_db_checkbox.click()
        assert test_db_checkbox.is_selected()

        # Test cancel button
        cancel_button = chrome_driver.find_element(By.XPATH, "//button[contains(text(), 'Cancel')]")
        cancel_button.click()

        # Verify dialog is hidden
        WebDriverWait(chrome_driver, 5).until(EC.invisibility_of_element((By.ID, "export-modal")))

        # Re-open dialog and test export
        export_button.click()
        WebDriverWait(chrome_driver, 5).until(
            EC.presence_of_element_located((By.ID, "export-modal"))
        )

        # Select test_db again
        test_db_checkbox = chrome_driver.find_element(By.CSS_SELECTOR, "input[value='test_db']")
        test_db_checkbox.click()

        # Click export button (Note: actual file download testing is complex in Selenium)
        export_submit_button = chrome_driver.find_element(
            By.XPATH, "//button[contains(text(), 'Export')]"
        )
        assert export_submit_button.is_enabled()
        # We won't actually click it as file download testing requires more complex setup


class TestErrorHandling:
    """Test error handling in the UI."""

    def test_connection_error_display(self, chrome_driver, web_server):
        """Test that connection errors are properly displayed."""
        chrome_driver.get(f"{web_server}/ui")

        # Add cluster with bad connection
        chrome_driver.find_element(By.ID, "cluster-name").send_keys("bad_cluster")
        chrome_driver.find_element(By.ID, "cluster-host").send_keys("nonexistent.host")
        chrome_driver.find_element(By.CSS_SELECTOR, "#cluster-form button[type='submit']").click()

        # Wait for cluster to be added
        time.sleep(2)

        # Try to test the bad connection
        WebDriverWait(chrome_driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "[data-test='bad_cluster']"))
        )
        test_button = chrome_driver.find_element(By.CSS_SELECTOR, "[data-test='bad_cluster']")
        test_button.click()

        # Wait for error message
        WebDriverWait(chrome_driver, 15).until(
            EC.text_to_be_present_in_element((By.ID, "status"), "failed")
        )

        status_text = chrome_driver.find_element(By.ID, "status").text.lower()
        assert "failed" in status_text or "error" in status_text

    def test_form_validation(self, chrome_driver, web_server):
        """Test form validation."""
        chrome_driver.get(f"{web_server}/ui")

        # Try to submit form with empty name
        submit_button = chrome_driver.find_element(
            By.CSS_SELECTOR, "#cluster-form button[type='submit']"
        )
        submit_button.click()

        # Check for validation error
        cluster_name_field = chrome_driver.find_element(By.ID, "cluster-name")
        validation_message = cluster_name_field.get_attribute("validationMessage")
        assert validation_message  # Should have validation message

    def test_loading_states(self, chrome_driver, web_server):
        """Test loading state indicators."""
        chrome_driver.get(f"{web_server}/ui")

        # Check that loader exists
        loader = chrome_driver.find_element(By.ID, "loader")
        assert loader

        # Initially should be hidden
        assert not loader.is_displayed()


class TestResponsiveDesign:
    """Test responsive design and mobile compatibility."""

    def test_mobile_viewport(self, chrome_driver, web_server):
        """Test mobile viewport rendering."""
        chrome_driver.get(f"{web_server}/ui")

        # Resize to mobile viewport
        chrome_driver.set_window_size(375, 667)  # iPhone size

        # Check that main elements are still visible
        assert chrome_driver.find_element(By.ID, "cluster-list").is_displayed()
        assert chrome_driver.find_element(By.ID, "cluster-form").is_displayed()
        assert chrome_driver.find_element(By.ID, "db-select").is_displayed()

    def test_tablet_viewport(self, chrome_driver, web_server):
        """Test tablet viewport rendering."""
        chrome_driver.get(f"{web_server}/ui")

        # Resize to tablet viewport
        chrome_driver.set_window_size(768, 1024)  # iPad size

        # Check layout remains functional
        assert chrome_driver.find_element(By.ID, "cluster-list").is_displayed()
        assert chrome_driver.find_element(By.ID, "metadata-browser").is_displayed()


class TestAccessibility:
    """Test accessibility features."""

    def test_keyboard_navigation(self, chrome_driver, web_server):
        """Test keyboard navigation."""
        chrome_driver.get(f"{web_server}/ui")

        # Check that form elements can be focused
        cluster_name = chrome_driver.find_element(By.ID, "cluster-name")
        cluster_name.send_keys("test")
        assert chrome_driver.switch_to.active_element == cluster_name

    def test_aria_labels(self, chrome_driver, web_server):
        """Test ARIA labels and accessibility attributes."""
        chrome_driver.get(f"{web_server}/ui")

        # Check for important accessibility attributes
        form = chrome_driver.find_element(By.ID, "cluster-form")
        assert form.get_attribute("role") or form.tag_name == "form"

        # Check button accessibility
        buttons = chrome_driver.find_elements(By.TAG_NAME, "button")
        for button in buttons:
            assert (
                button.text or button.get_attribute("aria-label") or button.get_attribute("title")
            )

    def test_javascript_console_errors(self, chrome_driver: webdriver.Chrome, api_server):
        """Test that the web interface loads without JavaScript console errors."""
        # Navigate to the main page
        chrome_driver.get(f"http://127.0.0.1:8000/ui")

        # Wait for page to load
        wait = WebDriverWait(chrome_driver, 10)
        wait.until(EC.presence_of_element_located((By.ID, "cluster-select")))

        # Allow time for any async operations to complete
        time.sleep(2)

        # Get browser console logs
        logs = chrome_driver.get_log("browser")

        # Filter for actual JavaScript errors (ignore favicon 404 which is expected)
        js_errors = [
            log
            for log in logs
            if log["level"] in ["SEVERE", "ERROR"]
            and "favicon.ico" not in log.get("message", "")
            and "javascript" in log.get("source", "").lower()
        ]

        # Print any errors for debugging
        if js_errors:
            print("JavaScript errors found:")
            for error in js_errors:
                print(f"  {error['level']}: {error['message']}")

        # Assert no JavaScript errors
        assert len(js_errors) == 0, f"Found {len(js_errors)} JavaScript errors"

        # Verify page loaded correctly
        assert "CHT Web Interface" in chrome_driver.title

        # Check that key elements are present
        assert chrome_driver.find_element(By.ID, "cluster-select")
        assert chrome_driver.find_element(By.ID, "status")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
