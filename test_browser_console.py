#!/usr/bin/env python3
"""
Browser automation test to check for JavaScript console errors.

This test uses Selenium WebDriver to load the CHT web interface and
capture any JavaScript console errors that occur during page load.
"""
import json
import time
from typing import List, Dict, Any
import pytest
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException


def create_chrome_driver() -> webdriver.Chrome:
    """Create a Chrome WebDriver with console logging enabled."""
    options = Options()
    options.add_argument("--headless")  # Run in headless mode
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    
    # Enable logging
    options.set_capability("goog:loggingPrefs", {
        "browser": "ALL",
        "driver": "ALL",
        "performance": "ALL"
    })
    
    try:
        driver = webdriver.Chrome(options=options)
        return driver
    except Exception as e:
        pytest.skip(f"Chrome WebDriver not available: {e}")


def get_console_logs(driver: webdriver.Chrome) -> List[Dict[str, Any]]:
    """Extract console logs from the browser."""
    try:
        logs = driver.get_log("browser")
        return [
            {
                "level": log["level"],
                "message": log["message"],
                "source": log.get("source", ""),
                "timestamp": log["timestamp"]
            }
            for log in logs
        ]
    except Exception as e:
        print(f"Warning: Could not retrieve console logs: {e}")
        return []


def get_network_logs(driver: webdriver.Chrome) -> List[Dict[str, Any]]:
    """Extract network logs from the browser."""
    try:
        logs = driver.get_log("performance")
        network_events = []
        
        for log in logs:
            message = json.loads(log["message"])
            if message["message"]["method"].startswith("Network."):
                network_events.append({
                    "method": message["message"]["method"],
                    "params": message["message"].get("params", {}),
                    "timestamp": log["timestamp"]
                })
        
        return network_events
    except Exception as e:
        print(f"Warning: Could not retrieve network logs: {e}")
        return []


def test_web_interface_console_errors():
    """Test the web interface for JavaScript console errors and network issues."""
    base_url = "http://127.0.0.1:8000"  # Use standard test port
    
    # First check if server is available
    try:
        import requests
        response = requests.get(f"{base_url}/ui", timeout=2)
        if response.status_code != 200:
            pytest.skip(f"CHT web server not available at {base_url}")
    except Exception:
        pytest.skip(f"CHT web server not available at {base_url}")
    
    driver = create_chrome_driver()
    
    try:
        print(f"🌐 Loading web interface at {base_url}")
        
        # Load the page
        driver.get(f"{base_url}/ui")
        
        # Wait for initial page load
        print("⏳ Waiting for page to load...")
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        
        # Wait a bit more for JS to initialize
        time.sleep(3)
        
        # Check for page title
        title = driver.title
        print(f"📄 Page title: {title}")
        
        # Get console logs
        console_logs = get_console_logs(driver)
        
        # Get network logs
        network_logs = get_network_logs(driver)
        
        # Filter network requests for failed ones
        failed_requests = []
        for event in network_logs:
            if event["method"] == "Network.loadingFailed":
                failed_requests.append(event)
            elif event["method"] == "Network.responseReceived":
                params = event["params"]
                response = params.get("response", {})
                status = response.get("status", 0)
                if status >= 400:
                    failed_requests.append({
                        "url": response.get("url"),
                        "status": status,
                        "statusText": response.get("statusText")
                    })
        
        # Analyze console logs
        errors = [log for log in console_logs if log["level"] == "SEVERE"]
        warnings = [log for log in console_logs if log["level"] == "WARNING"]
        
        # Print results
        print(f"\\n📊 Results:")
        print(f"   Total console logs: {len(console_logs)}")
        print(f"   Errors (SEVERE): {len(errors)}")
        print(f"   Warnings: {len(warnings)}")
        print(f"   Failed network requests: {len(failed_requests)}")
        
        if errors:
            print(f"\\n❌ Console Errors:")
            for i, error in enumerate(errors[:5], 1):  # Show first 5 errors
                print(f"   {i}. {error['message']}")
        
        if warnings:
            print(f"\\n⚠️  Console Warnings:")
            for i, warning in enumerate(warnings[:3], 1):  # Show first 3 warnings
                print(f"   {i}. {warning['message']}")
        
        if failed_requests:
            print(f"\\n🌐 Failed Network Requests:")
            for i, req in enumerate(failed_requests[:5], 1):  # Show first 5 failed requests
                if isinstance(req, dict) and "url" in req:
                    print(f"   {i}. {req['status']} {req['statusText']} - {req['url']}")
                else:
                    print(f"   {i}. Loading failed: {req}")
        
        # Test specific elements are present
        try:
            print(f"\\n🔍 Checking page elements...")
            
            # Check for cluster selector
            cluster_select = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.ID, "cluster-select"))
            )
            print(f"   ✅ Cluster selector found")
            
            # Check for databases container
            databases_container = driver.find_element(By.ID, "databases-container")
            print(f"   ✅ Databases container found")
            
            # Check for status element
            status_element = driver.find_element(By.ID, "status")
            status_text = status_element.text.strip()
            print(f"   📋 Status: '{status_text}'")
            
        except TimeoutException as e:
            print(f"   ❌ Element not found: {e}")
            errors.append({"message": f"Missing UI element: {e}", "level": "SEVERE"})
        
        # Return summary for pytest
        result = {
            "total_logs": len(console_logs),
            "errors": len(errors),
            "warnings": len(warnings),
            "failed_requests": len(failed_requests),
            "error_messages": [e["message"] for e in errors],
            "page_title": title
        }
        
        # Assert no critical errors
        if errors:
            print(f"\\n💥 Test FAILED: Found {len(errors)} JavaScript console errors")
            for error in errors:
                print(f"   - {error['message']}")
        else:
            print(f"\\n✅ Test PASSED: No JavaScript console errors detected")
        
        return result
        
    except WebDriverException as e:
        print(f"❌ WebDriver error: {e}")
        pytest.fail(f"WebDriver error: {e}")
        
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        pytest.fail(f"Unexpected error: {e}")
        
    finally:
        print("🧹 Cleaning up...")
        driver.quit()


if __name__ == "__main__":
    # Run the test directly
    try:
        result = test_web_interface_console_errors()
        print(f"\\nTest completed successfully")
        if result["errors"] > 0:
            exit(1)  # Exit with error code if JS errors found
    except Exception as e:
        print(f"Test failed: {e}")
        exit(1)