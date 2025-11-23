# CHT Single-File Distribution

CHT provides two single-file distribution options that work without pip installation, inspired by [copyparty](https://github.com/9001/copyparty)'s approach.

## 📦 Distribution Options

### 1. Self-Extracting File (cht-sfx.py)

**Best for most users** - Complete standalone distribution with all dependencies.

- **Size**: ~27MB
- **Dependencies**: None - everything included
- **Python**: Requires Python 3.10+
- **Extraction**: Auto-extracts to `~/.cache/cht-sfx/` on first run
- **Updates**: Re-extracts when new version is used

#### Usage
```bash
# Download (example URL - check releases)
curl -O https://github.com/kalinkinisaac/cht/releases/latest/download/cht-sfx.py

# Run directly
python cht-sfx.py --version
python cht-sfx.py  # Interactive mode with CHT loaded

# Use in scripts
python -c "exec(open('cht-sfx.py').read()); from cht import Cluster"
```

### 2. Zipapp Distribution (cht.pyz)

**Best for environments with existing dependencies** - Lightweight Python zipapp.

- **Size**: ~250KB  
- **Dependencies**: Requires `clickhouse-connect>=0.6.8` and `pandas>=1.5`
- **Python**: Requires Python 3.10+
- **Format**: Standard Python zipapp (PEP 441)
- **Execution**: Runs directly from zipfile

#### Usage
```bash
# Download (example URL - check releases)
curl -O https://github.com/kalinkinisaac/cht/releases/latest/download/cht.pyz

# Install dependencies first
pip install clickhouse-connect>=0.6.8 pandas>=1.5

# Run directly
python cht.pyz --version
python cht.pyz  # Interactive mode

# Use in scripts  
python -c "import sys; sys.path.insert(0, 'cht.pyz'); from cht import Cluster"
```

## 🛠️ Building Single-File Distributions

### Build Both Distributions
```bash
# Build both SFX and zipapp
./scripts/build-all.sh
```

### Build Individual Distributions
```bash
# Build self-extracting file only
python scripts/build-sfx.py

# Build zipapp only  
python scripts/build-zipapp.py
```

## 📝 Technical Details

### Self-Extracting File Architecture

The SFX uses a similar approach to copyparty:

1. **Header**: Python script with embedded base64 data
2. **Archive**: Compressed tar.gz containing CHT + dependencies  
3. **Extraction**: First run extracts to user cache directory
4. **Caching**: Subsequent runs check version and reuse cache
5. **Import**: Adds cache directory to sys.path for imports

```python
# SFX structure
#!/usr/bin/env python3
"""CHT Self-Extracting File"""

# ... metadata and imports ...

ARCHIVE_DATA = """
H4sIAKnVGWkC/+y9a3MbybEoOJ/xK9qtnQtgBoRISqJmYGOOZY...
"""  # Base64-encoded tar.gz

def extract_and_setup():
    """Extract embedded archive and set up Python path."""
    # Cache in ~/.cache/cht-sfx/
    # Check version, extract if needed
    # Add to sys.path
    
def main():
    """Main entry point."""
    extract_and_setup()
    import cht  # Now available
```

### Zipapp Architecture

Standard Python zipapp following PEP 441:

```
cht.pyz
├── cht/           # CHT source code
│   ├── __init__.py
│   ├── cluster.py
│   ├── table.py
│   └── ...
└── __main__.py    # Entry point
```

## 🔍 Comparison

| Feature | Self-Extracting (SFX) | Zipapp (PYZ) |
|---------|----------------------|---------------|
| **Size** | ~27MB | ~250KB |
| **Dependencies** | None (all included) | Must install separately |
| **First run** | Extracts to cache | Runs immediately |
| **Updates** | Re-extracts new versions | Replace file only |
| **Disk usage** | ~20MB cache + 27MB file | 250KB file only |
| **Offline use** | ✅ Fully offline | ❌ Need pip for deps |
| **Enterprise** | ✅ No external downloads | ⚠️ Requires package manager |

## 🎯 Use Cases

### Self-Extracting File (SFX)
- **Air-gapped environments**: No internet for pip installs
- **Quick demos**: Share single file for instant CHT access  
- **CI/CD**: Avoid dependency management in pipelines
- **Emergency tools**: Reliable standalone utility
- **Customer deployments**: Single file to ship

### Zipapp (PYZ)  
- **Docker containers**: Dependencies already in base image
- **Python environments**: pandas/clickhouse-connect already installed
- **Development**: Lightweight testing of CHT changes
- **Bandwidth-limited**: Minimal download size
- **Version pinning**: Control exact dependency versions

## 🚀 Integration Examples

### CI/CD Pipeline
```yaml
# GitHub Actions example
- name: Download CHT SFX
  run: curl -O https://github.com/kalinkinisaac/cht/releases/latest/download/cht-sfx.py

- name: Use CHT
  run: |
    python -c "
    exec(open('cht-sfx.py').read())
    from cht import Cluster
    cluster = Cluster('prod', 'clickhouse.example.com')
    # Use cluster for CI tasks...
    "
```

### Docker Container
```dockerfile
# Using zipapp in container with pre-installed deps
FROM python:3.12-slim
RUN pip install clickhouse-connect>=0.6.8 pandas>=1.5
COPY cht.pyz /usr/local/bin/
RUN python -c "import sys; sys.path.insert(0, '/usr/local/bin/cht.pyz'); import cht"
```

### Jupyter Notebook
```python
# Download SFX to notebook environment
import urllib.request
urllib.request.urlretrieve(
    'https://github.com/kalinkinisaac/cht/releases/latest/download/cht-sfx.py',
    'cht-sfx.py'
)

# Load CHT
exec(open('cht-sfx.py').read())
from cht import Cluster, Table, DependencyGraph

# Now use CHT normally
cluster = Cluster('local', 'localhost')
```

## 📊 Performance Comparison

| Metric | pip install | SFX | Zipapp |
|--------|-------------|-----|---------|
| **Download time** | ~30s (deps) | ~10s (single file) | <1s (tiny file) |
| **First import** | Instant | ~2s (extraction) | Instant |
| **Subsequent imports** | Instant | ~0.1s (cache hit) | Instant |
| **Disk space** | ~20MB | ~47MB (file+cache) | ~250KB |
| **Network deps** | Yes (pip/PyPI) | No | Yes (pip for deps) |

## 🔧 Development Notes

### Building Process
1. **SFX**: Install CHT+deps to temp dir → compress → base64 encode → embed in Python script
2. **Zipapp**: Copy CHT source → create __main__.py → zip with python -m zipapp

### Cache Management  
The SFX cache can be managed manually:
```bash
# Clear cache (forces re-extraction)
rm -rf ~/.cache/cht-sfx/

# Check cache
ls -la ~/.cache/cht-sfx/
```

### Version Handling
- SFX checks embedded version vs cached version
- Re-extracts automatically on version mismatch
- Zipapp version is in cht/__init__.py inside the zip

This single-file distribution approach makes CHT as accessible as copyparty - just download and run!