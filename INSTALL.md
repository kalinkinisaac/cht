# CHT Installation Guide

## 🚀 Quick Installation (Recommended)

### Option 1: Always Latest (Git)
```bash
pip install git+https://github.com/kalinkinisaac/cht.git
```
**Benefits:**
- ✅ Always gets the latest features and fixes
- ✅ No version numbers to maintain
- ✅ Perfect for development and production

### Option 2: Specific Release (Wheel)
```bash
pip install https://github.com/kalinkinisaac/cht/releases/latest/download/cht-0.2.2-py3-none-any.whl
```
**Benefits:**
- ✅ Faster installation (pre-built wheel)
- ✅ Specific version pinning
- ✅ Good for reproducible environments

## 🧪 Test Installation

After installation, test with:

```python
# Quick test
import cht
print(f"cht version: {cht.__version__}")

# Or run the test script
python test_install.py
```

## 🔄 Upgrade

```bash
# For git installation
pip install --upgrade git+https://github.com/kalinkinisaac/cht.git

# For wheel installation  
pip install --upgrade https://github.com/kalinkinisaac/cht/releases/latest/download/cht-latest-py3-none-any.whl
```

## 📝 Example Usage

```python
from cht import Cluster, Table
import pandas as pd

# Connect to ClickHouse
cluster = Cluster(name="local", host="localhost", user="default", password="")

# DataFrame integration
df = pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
table = Table.from_df(df, cluster=cluster, database="test", name="demo")
result_df = table.to_df()
```

## ⚡ Performance Tips

- Use git installation for latest features
- Use wheel installation for faster CI/CD pipelines
- Both methods support offline installation if cached