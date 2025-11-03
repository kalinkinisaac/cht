# Contributing to CHT (ClickHouse Toolkit)

Welcome to the CHT project! This guide will help you set up your development environment and understand our contribution workflow.

## Quick Start

### Prerequisites

- **Python 3.10+** (required)
- **Git** for version control
- **Docker** (optional, for local ClickHouse testing)

### Development Setup

#### 1. Clone the Repository
```bash
git clone https://github.com/kalinkinisaac/cht.git
cd cht
```

#### 2. Set Up Development Environment

**Option A: Automated Setup (Recommended)**
```bash
# Run the setup script to create venv and install dependencies
./scripts/setup-dev.sh

# Activate the environment
source .venv/bin/activate
```

**Option B: Manual Setup**
```bash
# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Upgrade pip and install dependencies
pip install --upgrade pip
pip install -r requirements-dev.txt

# Install package in development mode
pip install -e .
```

#### 3. Verify Installation
```bash
# Check that the package is installed
python -c "import cht; print(f'CHT version: {cht.__version__}')"

# Run tests to ensure everything works
pytest
```

## Development Workflow

### Code Quality Standards

We maintain high code quality through automated tools:

#### **Formatting with Black**
```bash
# Format all code
black src/ tests/

# Check formatting without making changes
black --check src/ tests/
```

#### **Import Sorting with isort**
```bash
# Sort imports
isort src/ tests/

# Check import sorting
isort --check-only src/ tests/
```

#### **Linting with flake8**
```bash
# Check code style
flake8 src/ tests/
```

#### **Type Checking with Pylint**
```bash
# Run pylint on source code
pylint src/cht/

# Run pylint on specific file
pylint src/cht/cluster.py
```

#### **All Quality Checks**
```bash
# Run all quality checks at once
black --check src/ tests/ && \
isort --check-only src/ tests/ && \
flake8 src/ tests/ && \
pylint src/cht/
```

### Running Tests

#### **Unit Tests**
```bash
# Run all tests
pytest

# Run tests with verbose output
pytest -v

# Run specific test file
pytest tests/test_cluster.py

# Run with coverage
pytest --cov=cht --cov-report=html
```

#### **Integration Tests**
```bash
# Start Docker ClickHouse (required for integration tests)
docker compose up -d

# Run integration test
python test_docker_integration.py

# Stop Docker when done
docker compose down
```

### Local ClickHouse Testing

For testing DataFrame integration and other features:

```bash
# Start ClickHouse server
docker compose up -d

# ClickHouse will be available at:
# - HTTP interface: http://localhost:8123
# - Native TCP: localhost:9000
# - Credentials: developer/developer

# Test connection
python -c "
from cht import Cluster
cluster = Cluster(name='test', host='localhost', user='developer', password='developer')
print('✓ Connected to ClickHouse')
"

# Stop when done
docker compose down
```

### **Comprehensive Local Testing Guide**

#### **Pre-Commit Testing Workflow** 

Before making any commit, run this complete testing sequence:

```bash
# 1. Activate virtual environment
source .venv/bin/activate

# 2. Install package in development mode (if not already done)
pip install -e .

# 3. Run full test suite
pytest -v

# 4. Check code formatting
black --check src/ tests/

# 5. Check import sorting
isort --check-only src/ tests/

# 6. Check code style
flake8 src/ tests/ --max-line-length=100 --extend-ignore=E203,W503

# 7. Verify version consistency (before version bumps)
python -c "
import re
with open('pyproject.toml') as f:
    content = f.read()
    match = re.search(r'version = \"([^\"]+)\"', content)
    pyproject_version = match.group(1) if match else 'NOT_FOUND'

from src.cht import __version__ as code_version
print(f'pyproject.toml: {pyproject_version}')
print(f'__init__.py: {code_version}')
print(f'Versions match: {pyproject_version == code_version}')
"
```

#### **Quick Test Commands**

```bash
# Fast: Run only unit tests (no Docker required)
pytest tests/ -v

# Medium: Run with coverage report
pytest --cov=cht --cov-report=term-missing

# Detailed: Generate HTML coverage report
pytest --cov=cht --cov-report=html
# View at: htmlcov/index.html

# Specific: Run single test file
pytest tests/test_table.py -v

# Debug: Run specific test with detailed output
pytest tests/test_table.py::test_table_from_df_overwrite_mode -v -s

# Performance: Run tests with timing
pytest --durations=10
```

#### **Code Quality Checks**

```bash
# Auto-fix formatting issues
black src/ tests/

# Auto-fix import sorting
isort src/ tests/

# Check for style issues (manual fixes required)
flake8 src/ tests/ --max-line-length=100 --extend-ignore=E203,W503

# Run all quality checks in one command
black --check src/ tests/ && \
isort --check-only src/ tests/ && \
flake8 src/ tests/ --max-line-length=100 --extend-ignore=E203,W503 && \
echo "✅ All quality checks passed!"
```

#### **Feature Testing**

When adding new features, test them comprehensively:

```bash
# Test new Table functionality
python -c "
from src.cht import Table, Cluster

# Test string representation
table = Table('events', 'analytics')
print(f'str(table): {str(table)}')
print(f'repr(table): {repr(table)}')

# Test FQDN consistency
assert str(table) == table.fqdn
print('✅ String representation working correctly')

# Test available methods
print('Available methods:')
print(f'- from_df: {hasattr(Table, \"from_df\")}')
print(f'- from_query: {hasattr(Table, \"from_query\")}')
print(f'- to_df with limit: {\"limit\" in table.to_df.__code__.co_varnames}')
"

# Test version consistency
python -c "
from src.cht import __version__
print(f'CHT version: {__version__}')
"
```

#### **Integration Testing with Docker**

```bash
# 1. Start ClickHouse (required for integration tests)
docker compose up -d

# Wait for ClickHouse to be ready
sleep 10

# 2. Test connection
python -c "
from src.cht import Cluster
try:
    cluster = Cluster(name='test', host='localhost', user='developer', password='developer')
    result = cluster.query('SELECT 1')
    print('✅ ClickHouse connection successful')
    print(f'Test query result: {result}')
except Exception as e:
    print(f'❌ ClickHouse connection failed: {e}')
    exit(1)
"

# 3. Run integration tests
python test_docker_integration.py

# 4. Test DataFrame integration
python example_complete_workflow.py

# 5. Cleanup
docker compose down
```

#### **Performance Testing**

```bash
# Test with different dataset sizes
python -c "
import pandas as pd
import time
from src.cht import Table

# Create test DataFrames of different sizes
sizes = [100, 1000, 10000]
for size in sizes:
    df = pd.DataFrame({
        'id': range(size),
        'value': [f'test_{i}' for i in range(size)]
    })
    
    start = time.time()
    # Test type resolution performance
    from src.cht.dataframe import resolve_column_types
    types = resolve_column_types(df)
    duration = time.time() - start
    
    print(f'Size {size:5d}: {duration:.4f}s - Types: {types}')
"
```

#### **Release Testing Checklist**

Before creating a release, verify:

```bash
# ✅ All tests pass
pytest -v --tb=short

# ✅ Code quality
black --check src/ tests/
isort --check-only src/ tests/
flake8 src/ tests/ --max-line-length=100 --extend-ignore=E203,W503

# ✅ Version consistency
python -c "
import re
with open('pyproject.toml') as f:
    content = f.read()
    match = re.search(r'version = \"([^\"]+)\"', content)
    pyproject_version = match.group(1) if match else 'NOT_FOUND'

from src.cht import __version__ as code_version
assert pyproject_version == code_version, f'Version mismatch: {pyproject_version} != {code_version}'
print(f'✅ Version consistency verified: {code_version}')
"

# ✅ Package builds correctly
python -m build --wheel
echo "✅ Package builds successfully"

# ✅ Installation works
pip install git+https://github.com/kalinkinisaac/cht.git --force-reinstall --quiet
python -c "import cht; print(f'✅ Installation successful: CHT v{cht.__version__}')"

# ✅ All examples work
python example_complete_workflow.py
python demo_unified_api.py
echo "✅ Examples work correctly"
```

## Project Structure

```
cht/
├── src/cht/              # Main package source
│   ├── __init__.py       # Package initialization
│   ├── cluster.py        # Cluster connection management
│   ├── table.py          # Table operations and DataFrame integration
│   ├── dataframe.py      # DataFrame utilities and type mapping
│   ├── operations.py     # High-level operations
│   ├── kafka.py          # Kafka engine utilities
│   └── sql_utils.py      # SQL helper functions
├── tests/                # Unit tests
│   ├── test_cluster.py
│   ├── test_table.py
│   ├── test_dataframe.py
│   └── ...
├── docker/               # Docker configuration
│   └── init/             # ClickHouse initialization scripts
├── scripts/              # Development scripts
│   └── setup-dev.sh
├── .github/              # GitHub Actions workflows
│   └── workflows/
├── pyproject.toml        # Project configuration
├── requirements.txt      # Production dependencies
├── requirements-dev.txt  # Development dependencies
├── .pylintrc            # Pylint configuration
└── README.md            # Project documentation
```

## Key Components

### **Cluster Class** (`src/cht/cluster.py`)
- Connection management with lazy initialization
- Structured logging for all operations
- Read-only mode support
- Introspection utilities (disk usage, table statistics)

### **Table Class** (`src/cht/table.py`)
- High-level table operations
- DataFrame integration with `to_df()` and `from_df()`
- Backup/restore workflows
- Materialized view management

### **DataFrame Integration** (`src/cht/dataframe.py`)
- Automatic pandas ↔ ClickHouse type mapping
- Table creation from DataFrames
- Bulk data insertion utilities

## Contribution Guidelines

### **Making Changes**

1. **Create a Feature Branch**
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Write Code**
   - Follow existing code style and patterns
   - Add type annotations for all functions
   - Include docstrings for public methods

3. **Write Tests**
   ```bash
   # Add tests for new functionality
   # Tests should be in tests/ directory
   # Follow existing test patterns
   ```

4. **Run Quality Checks**
   ```bash
   # Ensure all checks pass before committing
   black src/ tests/
   isort src/ tests/
   flake8 src/ tests/
   pylint src/cht/
   pytest
   ```

5. **Commit Changes**
   ```bash
   git add .
   git commit -m "feat: add DataFrame filtering support"
   ```

### **Code Style**

- **Line Length**: 100 characters maximum
- **Formatting**: Use Black formatter
- **Import Sorting**: Use isort with Black profile
- **Type Hints**: Required for all public functions
- **Docstrings**: Required for all public classes and methods

#### **Example Code Style**
```python
from typing import Optional, List
import pandas as pd

def create_table_from_dataframe(
    cluster: Cluster,
    df: pd.DataFrame,
    table_name: str,
    *,
    database: str = "default",
    engine: str = "MergeTree",
    order_by: Optional[List[str]] = None,
) -> None:
    """Create a ClickHouse table from a pandas DataFrame.
    
    Args:
        cluster: ClickHouse cluster connection
        df: Source DataFrame
        table_name: Target table name
        database: Target database name
        engine: ClickHouse table engine
        order_by: Columns for ORDER BY clause
    """
    # Implementation here
    pass
```

### **Testing**

- **Unit Tests**: Test individual components in isolation
- **Integration Tests**: Test with real ClickHouse instance
- **Mock External Dependencies**: Use pytest-mock for testing
- **Coverage**: Aim for >90% test coverage

#### **Example Test**
```python
import pytest
from unittest.mock import Mock
from cht.cluster import Cluster

def test_cluster_connection():
    """Test that cluster connects properly."""
    cluster = Cluster(name="test", host="localhost")
    
    # Mock the connection
    cluster._client = Mock()
    cluster._client.ping.return_value = True
    
    assert cluster.client.ping() is True
```

### **Commit Messages**

Use conventional commit format:
- `feat:` New features
- `fix:` Bug fixes
- `docs:` Documentation changes
- `test:` Test additions/changes
- `refactor:` Code refactoring
- `chore:` Maintenance tasks

Examples:
```
feat: add DataFrame filtering support to Table.to_df()
fix: handle empty DataFrames in type mapping
docs: update README with DataFrame examples
test: add integration tests for Docker setup
```

## Release Process & Deployment

### **Pre-Release Checklist**

Before creating any release, ensure all quality checks pass:

```bash
# 1. Run complete test suite
pytest -v

# 2. Run all code quality checks
black --check src/ tests/
isort --check-only src/ tests/
flake8 src/ tests/ --max-line-length=100 --extend-ignore=E203,W503
pylint src/cht/

# 3. Build package locally to verify
python -m build --wheel
```

### **Version Management**

#### **1. Update Version Numbers**

The version must be updated in **both** files to ensure consistency:

```bash
# Update pyproject.toml
vim pyproject.toml
# Change: version = "0.4.0" → version = "0.4.1"

# Update src/cht/__init__.py
vim src/cht/__init__.py
# Change: __version__ = "0.4.0" → __version__ = "0.4.1"
```

⚠️ **Critical**: Both files must have the same version number. Mismatched versions will cause pip installations to report incorrect versions.

#### **2. Verify Version Consistency**

```bash
# Check that both sources report the same version
python -c "
import toml
with open('pyproject.toml') as f:
    pyproject_version = toml.load(f)['project']['version']

from src.cht import __version__ as code_version

print(f'pyproject.toml: {pyproject_version}')
print(f'__init__.py: {code_version}')
print(f'Match: {pyproject_version == code_version}')
"
```

### **Complete Local Deployment Procedure**

#### **Step 0: Pre-Deployment Verification**

Run the full testing suite locally before any deployment:

```bash
# Activate environment
source .venv/bin/activate

# Complete quality assurance check
echo "🔍 Running complete pre-deployment checks..."

# 1. Full test suite
echo "📋 Running tests..."
pytest -v || (echo "❌ Tests failed!" && exit 1)

# 2. Code quality
echo "🎨 Checking code formatting..."
black --check src/ tests/ || (echo "❌ Black formatting failed!" && exit 1)

echo "📦 Checking import sorting..."
isort --check-only src/ tests/ || (echo "❌ isort failed!" && exit 1)

echo "🔍 Checking code style..."
flake8 src/ tests/ --max-line-length=100 --extend-ignore=E203,W503 || (echo "❌ flake8 failed!" && exit 1)

# 3. Version consistency check (before version bump)
echo "🏷️  Checking current version consistency..."
python -c "
import re
with open('pyproject.toml') as f:
    content = f.read()
    match = re.search(r'version = \"([^\"]+)\"', content)
    pyproject_version = match.group(1) if match else 'NOT_FOUND'

from src.cht import __version__ as code_version
print(f'Current pyproject.toml: {pyproject_version}')
print(f'Current __init__.py: {code_version}')
assert pyproject_version == code_version, f'Version mismatch: {pyproject_version} != {code_version}'
print('✅ Current versions are consistent')
"

echo "✅ All pre-deployment checks passed!"
```

### **Release Workflow**

#### **Step 1: Version Update and Testing**

```bash
# 1. Determine new version (example: 0.4.2 -> 0.4.3)
CURRENT_VERSION="0.4.2"
NEW_VERSION="0.4.3"

echo "🚀 Preparing release ${NEW_VERSION}"

# 2. Update version in pyproject.toml
sed -i '' "s/version = \"${CURRENT_VERSION}\"/version = \"${NEW_VERSION}\"/" pyproject.toml

# 3. Update version in __init__.py
sed -i '' "s/__version__ = \"${CURRENT_VERSION}\"/__version__ = \"${NEW_VERSION}\"/" src/cht/__init__.py

# 4. Verify version update
echo "🔍 Verifying version update..."
python -c "
import re
with open('pyproject.toml') as f:
    content = f.read()
    match = re.search(r'version = \"([^\"]+)\"', content)
    pyproject_version = match.group(1) if match else 'NOT_FOUND'

from src.cht import __version__ as code_version
print(f'Updated pyproject.toml: {pyproject_version}')
print(f'Updated __init__.py: {code_version}')
assert pyproject_version == code_version == '${NEW_VERSION}', f'Version update failed'
print('✅ Version update successful')
"

# 5. Test with new version
echo "🧪 Testing with new version..."
pytest -v || (echo "❌ Tests failed with new version!" && exit 1)

# 6. Build and test package
echo "📦 Building package..."
python -m build --wheel || (echo "❌ Package build failed!" && exit 1)
echo "✅ Package built successfully"
```

#### **Step 2: Commit and Tag Creation**

```bash
# 1. Commit version changes
git add pyproject.toml src/cht/__init__.py
git commit -m "bump: Update version to ${NEW_VERSION}

- Version consistency verified across all files
- All tests passing (88/88)
- Code quality checks passed
- Package build successful"

# 2. Push version commit
git push origin main

# 3. Create comprehensive release tag
git tag -a v${NEW_VERSION} -m "🚀 CHT v${NEW_VERSION} - [RELEASE TITLE]

✨ New Features:
- [List new features here]
- [Example: Enhanced Table string representation]

🔧 Improvements:
- [List improvements here]
- [Example: Better logging integration]

🐛 Bug Fixes:
- [List bug fixes here]

📋 Quality Assurance:
- ✅ All 88 tests passing (100% success rate)
- ✅ Black formatting compliance
- ✅ isort import sorting compliance  
- ✅ flake8 linting compliance
- ✅ Version consistency verified
- ✅ Package build successful

📦 Installation:
pip install git+https://github.com/kalinkinisaac/cht.git

🧪 Verification:
python -c \"import cht; print(f'CHT version: {cht.__version__}')\"

📚 Full API:
- Table.from_df() - Create table from DataFrame
- Table.from_query() - Create table from SQL query  
- Table.to_df(limit=N) - Load data with row limits
- Enhanced functionality as described above"

echo "✅ Tag v${NEW_VERSION} created with comprehensive description"
```

#### **Step 3: Deployment and Verification**

```bash
# 1. Push tag to trigger GitHub Actions
echo "🚀 Deploying v${NEW_VERSION}..."
git push origin v${NEW_VERSION}

# 2. Monitor GitHub Actions
echo "👀 Monitoring GitHub Actions..."
echo "Visit: https://github.com/kalinkinisaac/cht/actions"
echo "Expected workflow: Build and test on tag push"

# 3. Wait for GitHub Actions to complete (optional: use GitHub CLI)
# gh run list --limit 5
# gh run watch

# 4. Verify deployment success
echo "🔍 Verifying deployment..."

# Wait a moment for tag to propagate
sleep 30

# Test installation from GitHub
echo "📦 Testing installation from GitHub..."
pip install git+https://github.com/kalinkinisaac/cht.git --force-reinstall --quiet

# Verify correct version installed
python -c "
import cht
installed_version = cht.__version__
expected_version = '${NEW_VERSION}'
print(f'Installed version: {installed_version}')
print(f'Expected version: {expected_version}')
assert installed_version == expected_version, f'Version mismatch: {installed_version} != {expected_version}'
print('✅ Correct version installed from GitHub')
"

# Test core functionality
echo "🧪 Testing core functionality..."
python -c "
import cht

# Test string representation
table = cht.Table('test', 'analytics')
assert str(table) == 'analytics.test'
print('✅ Table string representation working')

# Test available methods
assert hasattr(cht.Table, 'from_df')
assert hasattr(cht.Table, 'from_query')
print('✅ All expected methods available')

print('🎉 Deployment verification successful!')
"

echo "✅ Release v${NEW_VERSION} deployed and verified successfully!"
echo "📋 Next steps:"
echo "   - Check GitHub release page: https://github.com/kalinkinisaac/cht/releases"
echo "   - Update documentation if needed"
echo "   - Communicate release to users"
```

#### **Quick Deployment Script**

For experienced developers, here's a complete deployment script:

```bash
#!/bin/bash
# deploy.sh - Complete CHT deployment script

set -e  # Exit on any error

# Configuration
CURRENT_VERSION="0.4.2"
NEW_VERSION="0.4.3"
RELEASE_TITLE="Enhanced Features"

echo "🚀 CHT Deployment Script - v${CURRENT_VERSION} → v${NEW_VERSION}"

# Pre-deployment checks
echo "🔍 Pre-deployment verification..."
source .venv/bin/activate
pytest -v
black --check src/ tests/
isort --check-only src/ tests/
flake8 src/ tests/ --max-line-length=100 --extend-ignore=E203,W503

# Version update
echo "🏷️  Updating version..."
sed -i '' "s/version = \"${CURRENT_VERSION}\"/version = \"${NEW_VERSION}\"/" pyproject.toml
sed -i '' "s/__version__ = \"${CURRENT_VERSION}\"/__version__ = \"${NEW_VERSION}\"/" src/cht/__init__.py

# Post-update testing
echo "🧪 Testing with new version..."
pytest -v
python -m build --wheel

# Git operations
echo "📝 Creating release commit and tag..."
git add pyproject.toml src/cht/__init__.py
git commit -m "bump: Update version to ${NEW_VERSION}"
git push origin main

git tag -a v${NEW_VERSION} -m "🚀 CHT v${NEW_VERSION} - ${RELEASE_TITLE}

✨ New Features:
- [Add your features here]

🔧 Improvements:
- [Add your improvements here]

📋 Quality Assurance:
- ✅ All tests passing
- ✅ Code quality verified
- ✅ Version consistency confirmed

📦 Installation:
pip install git+https://github.com/kalinkinisaac/cht.git"

git push origin v${NEW_VERSION}

# Verification
echo "🔍 Verifying deployment..."
sleep 30
pip install git+https://github.com/kalinkinisaac/cht.git --force-reinstall --quiet
python -c "import cht; assert cht.__version__ == '${NEW_VERSION}'; print('✅ Deployment successful!')"

echo "🎉 CHT v${NEW_VERSION} deployed successfully!"
```

Make the script executable:
```bash
chmod +x deploy.sh
```

#### **Step 2: Create and Push Git Tag**

```bash
# 1. Create annotated tag
git tag -a v0.4.1 -m "Release version 0.4.1

Features:
- Add Table.from_query() method for SQL-based table creation
- Add limit parameter to Table.to_df() for memory efficiency
- Improve code organization with shared utility methods"

# 2. Push tag to trigger GitHub Actions
git push origin v0.4.1
```

#### **Step 3: Monitor GitHub Actions**

```bash
# Check GitHub Actions status
# Go to: https://github.com/kalinkinisaac/cht/actions

# Or use GitHub CLI if installed:
gh run list --limit 5
```

**Expected GitHub Actions Workflow:**
1. **Linting & Testing**: Runs Black, flake8, isort, pylint, pytest
2. **Build**: Creates wheel and source distributions
3. **Release**: Creates GitHub release with artifacts

### **Common Deployment Issues & Solutions**

#### **Issue 1: GitHub Actions Failing on Linting**

**Symptoms**: "11 successful and 2 failing checks" - typically flake8 and Black failures

**Solution**:
```bash
# Fix all linting issues locally first
black src/ tests/
isort src/ tests/
flake8 src/ tests/ --max-line-length=100 --extend-ignore=E203,W503

# Common fixes needed:
# - Line length > 100 chars
# - Missing trailing commas in multiline structures
# - Import ordering issues
# - Boolean comparisons (use `bool(x) is True` for numpy booleans)

# Commit fixes and re-tag
git add -A
git commit -m "fix: resolve linting issues for GitHub Actions"

# Move tag to new commit
git tag -d v0.4.1
git tag v0.4.1
git push origin :refs/tags/v0.4.1  # Delete remote tag
git push origin v0.4.1             # Push new tag
```

#### **Issue 2: Pip Installation Shows Wrong Version**

**Symptoms**: `pip install git+https://github.com/kalinkinisaac/cht.git` installs old version

**Root Cause**: Version mismatch between `pyproject.toml` and `__init__.py`, or stale package metadata

**Solution**:
```bash
# 1. Verify both version files are updated (see Version Management above)

# 2. Test local build
python -m build --wheel
# Check output: "Successfully built cht-0.4.1-py3-none-any.whl"

# 3. If versions are correct, force pip cache clear
pip cache purge
pip install git+https://github.com/kalinkinisaac/cht.git --force-reinstall

# 4. Verify installation
python -c "import cht; print(f'Installed version: {cht.__version__}')"
```

#### **Issue 3: Tests Pass Locally but Fail in CI**

**Common Causes**:
- Different Python versions
- Missing environment variables
- Network-dependent tests
- Platform-specific code

**Solution**:
```bash
# Run tests in clean environment (like CI)
python -m venv clean_test_env
source clean_test_env/bin/activate
pip install -e .
pytest

# Check Python version compatibility
python --version  # Should match CI (3.10+)

# Review failed test logs in GitHub Actions
```

### **Deployment Verification**

#### **After Successful Deployment**

```bash
# 1. Verify GitHub release was created
# Visit: https://github.com/kalinkinisaac/cht/releases

# 2. Test installation from GitHub
pip install git+https://github.com/kalinkinisaac/cht.git --force-reinstall

# 3. Verify version and functionality
python -c "
import cht
print(f'Version: {cht.__version__}')

# Test new features work
print('Testing new features...')
print('- Table.from_df:', hasattr(cht.Table, 'from_df'))
print('- Table.from_query:', hasattr(cht.Table, 'from_query'))

# Test instance methods
table = cht.Table('test')
import inspect
to_df_params = inspect.signature(table.to_df).parameters
print('- Table.to_df limit param:', 'limit' in to_df_params)
print('✓ All features available')
"

# 4. Run integration test
python example_complete_workflow.py  # Should work with new version
```

### **Hotfix Deployment**

For critical bugs that need immediate release:

```bash
# 1. Create hotfix branch from latest release tag
git checkout v0.4.1
git checkout -b hotfix/v0.4.2

# 2. Make minimal fix
# 3. Update version to patch level (0.4.1 → 0.4.2)
# 4. Follow normal release process with expedited testing

# 5. Merge hotfix back to main
git checkout main
git merge hotfix/v0.4.2
```

### **Release Notes Template**

When creating GitHub releases, use this template:

```markdown
## 🚀 CHT v0.4.1

### ✨ New Features
- **Table.from_query()**: Create ClickHouse tables directly from SQL queries
- **Table.to_df(limit=N)**: Memory-efficient DataFrame loading with row limits

### 🔧 Improvements
- Refactored common logic to eliminate code duplication
- Enhanced TTL management for temporary tables
- Improved error handling and logging

### 🐛 Bug Fixes
- Fixed boolean comparison issues in test suite
- Resolved linting violations for CI/CD compatibility

### 📦 Installation
```bash
pip install git+https://github.com/kalinkinisaac/cht.git
```

### 🧪 Verification
```python
import cht
print(f"CHT version: {cht.__version__}")  # Should show 0.4.1
```

**Full Changelog**: https://github.com/kalinkinisaac/cht/compare/v0.4.0...v0.4.1
```

### **Rollback Procedure**

If a release needs to be rolled back:

```bash
# 1. Delete problematic tag
git tag -d v0.4.1
git push origin :refs/tags/v0.4.1

# 2. Delete GitHub release (via web interface)

# 3. Revert commits if needed
git revert <commit-hash>

# 4. Re-tag previous stable version as latest
git tag v0.4.1 v0.4.0  # Point v0.4.1 to v0.4.0 commit
git push origin v0.4.1
```

## Getting Help

### **Common Issues**

**ClickHouse Connection Failed**
```bash
# Make sure Docker is running
docker compose up -d

# Check logs
docker compose logs clickhouse

# Verify credentials (developer/developer)
```

**Import Errors**
```bash
# Reinstall in development mode
pip install -e .

# Check Python path
python -c "import sys; print(sys.path)"
```

**Test Failures**
```bash
# Run specific failing test
pytest tests/test_cluster.py::test_specific_function -v

# Run with debug output
pytest -s --tb=long
```

### **Documentation**

- **README.md**: Project overview and usage examples
- **Docstrings**: Inline code documentation
- **Type Hints**: Function signatures and return types
- **Tests**: Living examples of how code should work

### **Community**

- **Issues**: Report bugs and request features on GitHub
- **Discussions**: Ask questions in GitHub Discussions
- **Pull Requests**: Submit code contributions

## Architecture Notes for Newcomers

### **Design Principles**

1. **High-Level API First**: Users should work with `Table.from_df()`, not low-level clients
2. **Type Safety**: Strict type annotations throughout
3. **Logging**: Structured logging for all operations
4. **Error Handling**: Clear error messages with context
5. **Testing**: Comprehensive unit and integration tests

### **Key Design Patterns**

- **Lazy Initialization**: Connections created only when needed
- **Builder Pattern**: Fluent APIs for complex operations
- **Dependency Injection**: Easy testing with mock objects
- **Configuration Over Convention**: Explicit parameters preferred

### **Performance Considerations**

- **Connection Pooling**: Single connection per Cluster instance
- **Bulk Operations**: DataFrame operations handle large datasets efficiently
- **Memory Management**: Streaming for large query results
- **Type Mapping**: Optimized pandas ↔ ClickHouse conversions

Welcome to the project! If you have questions, don't hesitate to open an issue or start a discussion.