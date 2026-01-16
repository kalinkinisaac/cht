#!/usr/bin/env python3
"""
Test runner script for CHT project.
Provides convenient commands for running different test suites.
"""
import argparse
import subprocess
import sys
from pathlib import Path


def run_command(cmd: list[str], description: str) -> int:
    """Run command and return exit code."""
    print(f"🚀 {description}")
    print(f"Running: {' '.join(cmd)}")
    print("-" * 50)
    
    result = subprocess.run(cmd, cwd=Path(__file__).parent)
    
    if result.returncode == 0:
        print(f"✅ {description} - PASSED")
    else:
        print(f"❌ {description} - FAILED")
    
    return result.returncode


def main():
    parser = argparse.ArgumentParser(description="CHT Test Runner")
    parser.add_argument(
        "suite", 
        choices=["api", "frontend", "unit", "all", "quick"],
        help="Test suite to run"
    )
    parser.add_argument(
        "--verbose", "-v", 
        action="store_true", 
        help="Verbose output"
    )
    parser.add_argument(
        "--coverage", "-c",
        action="store_true",
        help="Generate coverage report"
    )
    parser.add_argument(
        "--parallel", "-p",
        action="store_true", 
        help="Run tests in parallel"
    )
    parser.add_argument(
        "--docker", "-d",
        action="store_true",
        help="Use Docker ClickHouse (default for api/frontend)"
    )
    
    args = parser.parse_args()
    
    # Base pytest command
    base_cmd = ["python", "-m", "pytest"]
    
    if args.verbose:
        base_cmd.append("-v")
    
    if args.coverage:
        base_cmd.extend(["--cov=cht", "--cov-report=html", "--cov-report=term"])
    
    if args.parallel:
        base_cmd.extend(["-n", "auto"])
    
    # Define test suites
    test_commands = {
        "api": {
            "cmd": base_cmd + ["tests/test_web_api_docker.py", "tests/test_clusters_api.py", "tests/test_metadata_api.py"],
            "desc": "Running API tests with Docker ClickHouse"
        },
        "frontend": {
            "cmd": base_cmd + ["tests/test_frontend_integration.py"],
            "desc": "Running frontend integration tests"
        },
        "unit": {
            "cmd": base_cmd + ["tests/", "-m", "not integration"],
            "desc": "Running unit tests"
        },
        "quick": {
            "cmd": base_cmd + ["tests/test_clusters_api.py", "tests/test_metadata_api.py"],
            "desc": "Running quick tests (no Docker required)"
        },
        "all": {
            "cmd": base_cmd + ["tests/"],
            "desc": "Running all tests"
        }
    }
    
    # Run selected test suite
    suite_config = test_commands[args.suite]
    exit_code = run_command(suite_config["cmd"], suite_config["desc"])
    
    # Print summary
    if exit_code == 0:
        print(f"\n🎉 All tests passed!")
    else:
        print(f"\n💥 Some tests failed (exit code: {exit_code})")
    
    return exit_code


if __name__ == "__main__":
    sys.exit(main())