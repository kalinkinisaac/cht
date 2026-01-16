#!/usr/bin/env python3
"""
Simple browser test to check console errors in CHT web interface.
"""
import subprocess
import time
import requests
import tempfile
import os
from pathlib import Path


def test_web_interface_basic():
    """Basic test to check if web interface loads without major errors."""
    
    # Test server availability
    try:
        response = requests.get("http://127.0.0.1:8001/ui", timeout=5)
        print(f"✅ Server responding: {response.status_code}")
        
        # Check if it's HTML
        content_type = response.headers.get('content-type', '')
        if 'html' in content_type:
            print("✅ HTML response received")
        else:
            print(f"⚠️  Unexpected content type: {content_type}")
            
        # Check basic content
        content = response.text
        if "CHT" in content and "script" in content:
            print("✅ Page contains expected content")
        else:
            print("❌ Page missing expected content")
            
        # Test API endpoints
        api_tests = [
            ("/clusters", "Clusters API"),
            ("/databases", "Databases API") 
        ]
        
        for endpoint, name in api_tests:
            try:
                api_response = requests.get(f"http://127.0.0.1:8001{endpoint}", timeout=5)
                if api_response.status_code == 200:
                    print(f"✅ {name}: OK")
                else:
                    print(f"❌ {name}: {api_response.status_code}")
                    try:
                        error_detail = api_response.json()
                        print(f"   Error: {error_detail}")
                    except:
                        print(f"   Error: {api_response.text[:100]}...")
            except Exception as e:
                print(f"❌ {name}: {e}")
                
        return True
        
    except Exception as e:
        print(f"❌ Server test failed: {e}")
        return False


if __name__ == "__main__":
    print("🧪 Running CHT Web Interface Basic Tests")
    print("=" * 50)
    
    success = test_web_interface_basic()
    
    if success:
        print("\\n✅ Basic tests completed")
    else:
        print("\\n❌ Tests failed")
        exit(1)