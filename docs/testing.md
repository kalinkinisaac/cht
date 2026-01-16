# CHT Testing Guide

This document describes the comprehensive testing setup for CHT (ClickHouse Operations Toolkit), organized by testing layers with clear instructions for setup and execution.

## Overview

CHT testing is organized into three main layers:
1. **Core Library Tests** - Unit tests for core functionality
2. **Web API Tests** - Integration tests for REST API endpoints
3. **Web Frontend Tests** - Browser automation tests for UI functionality

## Environment Setup

### Virtual Environment
Always use the project's virtual environment for consistent dependencies:

```bash
# Activate virtual environment
source .venv/bin/activate

# Or use full path directly
/Users/i.kalinkin/dev/my/cht/.venv/bin/python -m pytest
```

### Dependencies

**Production requirements** (`requirements.txt`):
- Core dependencies for running CHT
- ClickHouse client libraries
- FastAPI and web framework dependencies

**Development requirements** (`requirements-dev.txt`):
- All production dependencies (`-r requirements.txt`)
- Testing frameworks (pytest, pytest-mock)
- Browser automation (selenium) 
- Code quality tools (black, isort, flake8, pylint)
- Build tools (build, twine)

```bash
# Install development dependencies (includes production deps)
/Users/i.kalinkin/dev/my/cht/.venv/bin/pip install -r requirements-dev.txt

# Or install only production dependencies
/Users/i.kalinkin/dev/my/cht/.venv/bin/pip install -r requirements.txt
```

## Testing Layers

## 1. Core Library Tests

**Purpose**: Test core business logic, data models, and utility functions

**Files**:
- `tests/test_cluster.py` - Cluster management and connections
- `tests/test_dataframe.py` - DataFrame operations
- `tests/test_table.py` - Table operations and metadata
- `tests/test_operations.py` - Database operations
- `tests/test_sql_utils.py` - SQL utilities and query building

**Run**:
```bash
# All unit tests
/Users/i.kalinkin/dev/my/cht/.venv/bin/python -m pytest tests/test_*.py -v

# Specific module
/Users/i.kalinkin/dev/my/cht/.venv/bin/python -m pytest tests/test_cluster.py -v

# Fast tests only (no Docker required)
/Users/i.kalinkin/dev/my/cht/.venv/bin/python -m pytest tests/ -v -k "not docker"
```

**No external dependencies required** - these tests use mocks and fixtures.

## 2. Web API Tests

**Purpose**: Test REST API endpoints with real ClickHouse integration

**Files**:
- `tests/test_web_api_docker.py` - Complete API integration tests
- `tests/test_metadata_api_docker.py` - Metadata API tests
- `tests/test_clusters_api.py` - Cluster management API tests

**Prerequisites**:
```bash
# Start ClickHouse with Docker
docker-compose up -d

# Verify ClickHouse is running
curl http://localhost:8123/ping
```

**Run**:
```bash
# All API tests (requires ClickHouse)
/Users/i.kalinkin/dev/my/cht/.venv/bin/python -m pytest tests/test_*docker*.py -v

# Specific API test
/Users/i.kalinkin/dev/my/cht/.venv/bin/python -m pytest tests/test_web_api_docker.py::TestMetadataAPI::test_export_table_descriptions_to_excel -v

# Test with coverage
/Users/i.kalinkin/dev/my/cht/.venv/bin/python -m pytest tests/test_web_api_docker.py --cov=cht.api --cov-report=html
```

**Coverage**:
- ✅ Cluster management (CRUD operations)
- ✅ Database and table listing
- ✅ Table schema inspection
- ✅ Comment editing (table and column)
- ✅ Excel export functionality
- ✅ Error handling and validation
- ✅ Security and input sanitization

## 3. Web Frontend Tests

**Purpose**: Test browser-based UI functionality and JavaScript console errors

**Files**:
- `tests/test_frontend_integration.py` - Selenium browser automation tests
- Browser console error detection and validation

**Prerequisites**:
```bash
# Install selenium (included in requirements-dev.txt)
/Users/i.kalinkin/dev/my/cht/.venv/bin/pip install selenium

# Start ClickHouse and web server
docker-compose up -d
/Users/i.kalinkin/dev/my/cht/.venv/bin/python -m cht.web --port 8000
```

**Run**:
```bash
# Frontend integration tests
/Users/i.kalinkin/dev/my/cht/.venv/bin/python -m pytest tests/test_frontend_integration.py -v

# Specific frontend test
/Users/i.kalinkin/dev/my/cht/.venv/bin/python -m pytest tests/test_frontend_integration.py::TestFrontendIntegration::test_javascript_console_errors -v

# Run with visible browser (for debugging)
# Edit test to remove --headless from Chrome options
```

**Browser Console Error Testing**:

Create a simple test script for quick console error checking:
```python
#!/usr/bin/env python3
"""Browser automation test for JavaScript console errors."""

import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def test_console_errors():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    
    driver = webdriver.Chrome(options=chrome_options)
    try:
        # Load the page
        driver.get("http://127.0.0.1:8000/ui")
        
        # Wait for page load
        wait = WebDriverWait(driver, 10)
        wait.until(EC.presence_of_element_located((By.ID, "cluster-select")))
        time.sleep(2)  # Allow async operations
        
        # Check console logs
        logs = driver.get_log('browser')
        js_errors = [
            log for log in logs 
            if log['level'] in ['SEVERE', 'ERROR'] 
            and 'favicon.ico' not in log.get('message', '')
        ]
        
        if js_errors:
            print("❌ JavaScript errors found:")
            for error in js_errors:
                print(f"  {error['level']}: {error['message']}")
            return False
        else:
            print("✅ No JavaScript errors found")
            return True
            
    finally:
        driver.quit()

if __name__ == "__main__":
    success = test_console_errors()
    exit(0 if success else 1)
```

**Coverage**:
- ✅ Page loading without JavaScript errors
- ✅ UI element presence and functionality
- ✅ User interaction flows (clicks, form submissions)
- ✅ Database selection and export functionality
- ✅ Error handling and user feedback
- ✅ Cross-browser compatibility

## Complete Test Workflow

### Development Testing
```bash
# 1. Start development environment
source .venv/bin/activate
docker-compose up -d

# 2. Run tests in layers
# Fast unit tests first
/Users/i.kalinkin/dev/my/cht/.venv/bin/python -m pytest tests/ -k "not docker" -v

# API integration tests  
/Users/i.kalinkin/dev/my/cht/.venv/bin/python -m pytest tests/test_*docker*.py -v

# Frontend tests (start web server first)
/Users/i.kalinkin/dev/my/cht/.venv/bin/python -m cht.web --port 8000 &
/Users/i.kalinkin/dev/my/cht/.venv/bin/python -m pytest tests/test_frontend_integration.py -v

# 3. Clean up
docker-compose down
```

### Continuous Integration
```bash
# Full test suite for CI/CD
/Users/i.kalinkin/dev/my/cht/.venv/bin/python -m pytest tests/ -v --cov=cht --cov-report=xml
```

### Test-Driven Development
```bash
# Watch mode for development
/Users/i.kalinkin/dev/my/cht/.venv/bin/python -m pytest tests/ -f --tb=short
```

## Test Organization

**Fixtures** (`tests/conftest.py`):
- Mock ClickHouse clients and clusters
- Docker container management
- Test data generation
- API client setup
- Browser driver configuration

**Test Marks**:
```python
@pytest.mark.unit       # Unit tests (no external deps)
@pytest.mark.integration # Integration tests (requires ClickHouse)
@pytest.mark.frontend   # Frontend tests (requires browser)
@pytest.mark.slow       # Slow tests (for CI filtering)
```

**Run by marks**:
```bash
# Only unit tests
/Users/i.kalinkin/dev/my/cht/.venv/bin/python -m pytest -m "unit" -v

# Only integration tests  
/Users/i.kalinkin/dev/my/cht/.venv/bin/python -m pytest -m "integration" -v
```

## Troubleshooting

### ClickHouse Issues
```bash
# Check ClickHouse status
docker-compose ps
curl http://localhost:8123/ping

# View logs
docker-compose logs clickhouse

# Restart if needed
docker-compose restart clickhouse
```

### Selenium Issues
```bash
# Check ChromeDriver
chromedriver --version

# Install/update ChromeDriver (macOS)
brew install chromedriver
brew upgrade chromedriver

# Run browser tests visibly (remove --headless for debugging)
```

### Virtual Environment Issues
```bash
# Recreate virtual environment
rm -rf .venv
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
```

### Port Conflicts
```bash
# Check what's using port 8000
lsof -i :8000

# Kill process if needed
kill -9 $(lsof -t -i:8000)
```

## Best Practices

1. **Always use .venv**: Use full paths to virtual environment python executable
2. **Layer testing**: Start with unit tests, then integration, then frontend
3. **Clean state**: Each test should be independent and clean up after itself
4. **Realistic data**: Use realistic test data that matches production patterns
5. **Error scenarios**: Test both success and failure paths
6. **Performance**: Include timeout and concurrency tests
7. **Security**: Test input validation and SQL injection prevention
8. **Documentation**: Document complex test scenarios and expected behaviors

## GitHub Actions Integration

```yaml
name: Test Suite
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v4
        with:
          python-version: '3.12'
      - name: Install dependencies
        run: |
          python -m venv .venv
          .venv/bin/pip install -r requirements-dev.txt
      - name: Start ClickHouse
        run: docker-compose up -d
      - name: Run tests
        run: |
          .venv/bin/python -m pytest tests/ -v --cov=cht --cov-report=xml
      - name: Upload coverage
        uses: codecov/codecov-action@v3
```

### External Dependencies
- **Docker/Docker Compose**: For ClickHouse container
- **Chrome/ChromeDriver**: For frontend tests (optional)

## Running Tests

### Quick Test Run (API only)
```bash
# Run API tests with Docker ClickHouse
pytest tests/test_web_api_docker.py -v
```

### Full Test Suite
```bash
# Run all tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=cht --cov-report=html

# Run specific test categories
pytest tests/ -m "not ui" -v  # Skip UI tests
pytest tests/ -m "api" -v     # Only API tests
```

### Frontend Tests (Requires Selenium)
```bash
# Install UI test dependencies first
pip install selenium

# Install ChromeDriver (macOS)
brew install chromedriver

# Run frontend tests
pytest tests/test_frontend_integration.py -v
```

### Environment Variables
```bash
# Override test ClickHouse connection
export CHT_TEST_CH_HOST=localhost
export CHT_TEST_CH_PORT=8123

# Run tests with custom settings
pytest tests/test_web_api_docker.py -v
```

## Test Classes and Methods

### TestClustersAPI
Tests cluster management endpoints:
- `test_list_clusters()` - List configured clusters
- `test_add_cluster()` - Add new cluster
- `test_update_cluster()` - Update cluster settings
- `test_delete_cluster()` - Delete cluster
- `test_select_cluster()` - Switch active cluster
- `test_test_cluster_connection()` - Test connection

### TestMetadataAPI  
Tests metadata browsing:
- `test_list_databases()` - List databases
- `test_list_tables()` - List tables in database
- `test_get_table_schema()` - Get table column info
- `test_update_table_comment()` - Update table comments
- `test_update_column_comment()` - Update column comments
- `test_get_table_data_preview()` - Preview table data

### TestFrontendAPI
Tests web interface:
- `test_get_ui_page()` - Load UI page
- `test_ui_has_cluster_management()` - Cluster UI elements
- `test_ui_has_metadata_browser()` - Metadata browser UI

### TestErrorHandling
Tests error scenarios:
- `test_invalid_cluster_name()` - Nonexistent cluster
- `test_invalid_database_name()` - Nonexistent database
- `test_duplicate_cluster_name()` - Duplicate cluster
- `test_malformed_cluster_data()` - Invalid input data

### TestSecurityAndValidation
Tests security measures:
- `test_sql_injection_prevention()` - SQL injection protection
- `test_xss_prevention_in_comments()` - XSS prevention
- `test_cluster_name_validation()` - Input validation

## Mock Objects

### MockClickHouseClient
```python
# Create mock client with predefined responses
mock_client = MockClickHouseClient({
    "databases": ["test_db", "default"],
    "tables": [{"name": "users", "comment": "User data"}]
})
```

### MockCluster
```python
# Create mock cluster for testing
mock_cluster = MockCluster("test", {
    "databases": ["default", "system"]
})
```

### TestDataBuilder
```python
# Build test data in real ClickHouse
builder = TestDataBuilder(cluster)
builder.create_database("test_db")
       .create_table("test_db", "users", "id UInt64, name String")
       .insert_data("test_db", "users", [(1, "John"), (2, "Jane")])
```

## Test Fixtures

### Session-scoped Fixtures
- `clickhouse_cluster` - Docker ClickHouse instance
- `web_server` - CHT web server instance
- `test_config` - Test configuration

### Function-scoped Fixtures  
- `api_client` - FastAPI test client
- `sample_data` - Sample test data in ClickHouse
- `test_data_builder` - Helper for creating test data
- `chrome_driver` - Selenium WebDriver (for UI tests)

## Docker Integration

Tests use Docker Compose to start ClickHouse:

```yaml
# docker-compose.yml
version: "3.9"
services:
  clickhouse:
    image: clickhouse/clickhouse-server:23.8
    container_name: cht-clickhouse
    ports:
      - "8123:8123"
    environment:
      CLICKHOUSE_USER: developer
      CLICKHOUSE_PASSWORD: developer
```

The `DockerClickHouseManager` handles container lifecycle:
- Starts fresh container for each test session
- Waits for ClickHouse to become ready
- Provides clean database state
- Handles cleanup after tests

## Test Data Management

### Automatic Cleanup
- Test databases and tables are automatically cleaned up
- `TestDataBuilder` tracks created objects and removes them
- Docker container provides clean state between sessions

### Sample Data Patterns
```python
# Create realistic test data
def create_ecommerce_data(builder):
    builder.create_database("ecommerce")
           .create_table("ecommerce", "products", """
               id UInt64 COMMENT 'Product ID',
               name String COMMENT 'Product name', 
               price Decimal(10,2) COMMENT 'Price in USD'
           """, "Product catalog")
           .insert_data("ecommerce", "products", [
               (1, "Laptop", 999.99),
               (2, "Mouse", 29.99)
           ])
```

## Performance Testing

### Concurrent Request Testing
```python
def test_concurrent_requests(api_client, sample_data):
    """Test handling of concurrent API requests."""
    import concurrent.futures
    
    def make_request():
        return api_client.get("/databases?cluster=test_cluster")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(make_request) for _ in range(10)]
        results = [future.result() for future in futures]
    
    for response in results:
        assert response.status_code == 200
```

## Security Testing

### Input Validation
```python
def test_sql_injection_prevention(api_client, sample_data):
    """Test SQL injection prevention."""
    malicious_input = "'; DROP TABLE users; --"
    response = api_client.get(f"/tables?database={malicious_input}")
    assert response.status_code in [400, 500]  # Should fail safely
```

### XSS Prevention
```python
def test_xss_prevention_in_comments(api_client, sample_data):
    """Test XSS prevention in comment updates."""
    malicious_comment = "<script>alert('xss')</script>"
    response = api_client.post("/tables/test.users/comment", 
                              json={"comment": malicious_comment})
    assert response.status_code == 200  # Should accept but sanitize
```

## Debugging Tests

### Verbose Output
```bash
# Run with verbose output and logging
pytest tests/ -v -s --log-cli-level=DEBUG
```

### Test Selection
```bash
# Run specific test
pytest tests/test_web_api_docker.py::TestClustersAPI::test_add_cluster -v

# Run tests matching pattern
pytest tests/ -k "cluster" -v
```

### Interactive Debugging
```python
# Add breakpoint in test
def test_cluster_operation(api_client):
    import pdb; pdb.set_trace()
    response = api_client.get("/clusters")
    assert response.status_code == 200
```

## CI/CD Integration

### GitHub Actions Example
```yaml
name: Tests
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      - run: pip install -e ".[test]"
      - run: pytest tests/test_web_api_docker.py -v
```

### Test Marks
```python
# Mark tests for selective execution
@pytest.mark.api
@pytest.mark.slow
def test_large_dataset_processing():
    pass

# Run only API tests
pytest -m "api" tests/
```

## Best Practices

1. **Isolation**: Each test should be independent and not rely on other tests
2. **Cleanup**: Always clean up test data, use fixtures for automatic cleanup
3. **Realistic Data**: Use realistic test data that matches production patterns
4. **Error Testing**: Test both success and failure scenarios
5. **Performance**: Include tests for concurrent access and timeouts
6. **Security**: Test input validation and injection prevention
7. **Documentation**: Document complex test scenarios and data setup

## Troubleshooting

### Docker Issues
```bash
# Check if ClickHouse is running
docker compose ps

# View ClickHouse logs
docker compose logs clickhouse

# Restart ClickHouse
docker compose restart clickhouse
```

### Selenium Issues
```bash
# Check ChromeDriver version
chromedriver --version

# Update ChromeDriver
brew upgrade chromedriver

# Run tests with visible browser (debugging)
# Modify chrome_driver fixture to remove --headless
```

### Connection Issues
```bash
# Test ClickHouse connection manually
curl http://localhost:8123/ping

# Check if port is open
lsof -i :8123
```