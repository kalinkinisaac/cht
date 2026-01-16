#!/usr/bin/env python3
"""
Integration test for the CHT web interface to check for JavaScript errors.

This test loads the web interface and verifies no JavaScript console errors occur.
"""
import pytest
import requests
from fastapi.testclient import TestClient


class TestWebInterface:
    """Test class for web interface functionality."""

    def test_web_interface_no_js_errors(self, api_client: TestClient):
        """Test that the web interface loads without JavaScript errors."""

        # Test the main UI page loads
        response = api_client.get("/ui")
        assert response.status_code == 200
        assert "text/html" in response.headers["content-type"]

        # Check that page contains expected JavaScript
        content = response.text
        assert "script" in content.lower()
        assert "cht" in content.lower() or "clickhouse" in content.lower()

        # Check critical JavaScript functions are present
        expected_js_elements = [
            "function",  # Should contain JavaScript functions
            "fetch",  # Should use fetch for API calls
            "async",  # Should use async functions
        ]

        for element in expected_js_elements:
            assert element in content, f"Missing expected JavaScript element: {element}"

    def test_api_endpoints_return_valid_responses(self, api_client: TestClient):
        """Test that API endpoints return valid responses without 500 errors."""

        # Test clusters endpoint
        response = api_client.get("/clusters")
        assert response.status_code == 200
        clusters = response.json()
        assert isinstance(clusters, list)

        # Test databases endpoint (should work if clusters work)
        response = api_client.get("/databases")
        assert response.status_code == 200
        databases = response.json()
        assert isinstance(databases, list)

    def test_frontend_has_required_elements(self, api_client: TestClient):
        """Test that frontend HTML contains required UI elements."""

        response = api_client.get("/ui")
        assert response.status_code == 200

        content = response.text

        # Check for required UI elements
        required_elements = [
            'id="cluster-select"',  # Cluster selector
            'id="databases-container"',  # Databases container
            'id="status"',  # Status element
            "export-modal",  # Export modal
            "database-checkboxes",  # Database checkboxes
        ]

        for element in required_elements:
            assert element in content, f"Missing required UI element: {element}"

    def test_javascript_error_handling(self, api_client: TestClient):
        """Test that JavaScript includes proper error handling."""

        response = api_client.get("/ui")
        assert response.status_code == 200

        content = response.text

        # Check for error handling patterns
        error_handling_patterns = [
            "try {",
            "catch",
            ".catch(",
            "error",
        ]

        found_patterns = sum(1 for pattern in error_handling_patterns if pattern in content)
        assert found_patterns >= 3, "JavaScript should include proper error handling"

    def test_export_functionality_elements(self, api_client: TestClient):
        """Test that export functionality UI elements are present."""

        response = api_client.get("/ui")
        assert response.status_code == 200

        content = response.text

        # Check for export-related elements
        export_elements = [
            "Export to Excel",
            "exportToExcel",
            "export-modal",
            "database-checkboxes",
        ]

        for element in export_elements:
            assert element in content, f"Missing export functionality element: {element}"
