#!/bin/bash
# Build all CHT single-file distributions.
#
# This script builds both the self-extracting file (SFX) and zipapp versions of CHT.
#
# Usage:
#     ./scripts/build-all.sh
#     
# Output:
#     cht-sfx.py  - Self-extracting with all dependencies (~27MB)  
#     cht.pyz     - Lightweight zipapp (~250KB, deps required)

set -e  # Exit on any error

echo "🚀 Building CHT Single-File Distributions"
echo "=========================================="
echo

# Get to project root
cd "$(dirname "$0")/.."

# Build self-extracting file
echo "📦 Building Self-Extracting File (SFX)..."
python scripts/build-sfx.py
echo

# Build zipapp  
echo "📦 Building Zipapp (.pyz)..."
python scripts/build-zipapp.py
echo

# Show results
echo "✅ Build Complete!"
echo "=================="

if [[ -f "cht-sfx.py" && -f "cht.pyz" ]]; then
    echo "📁 Files created:"
    echo "  cht-sfx.py  - $(du -h cht-sfx.py | cut -f1) (self-extracting, all deps included)"
    echo "  cht.pyz     - $(du -h cht.pyz | cut -f1) (zipapp, deps required)"
    echo
    echo "🧪 Quick test:"
    echo "  python cht-sfx.py --version"
    echo "  python cht.pyz --version" 
    echo
    echo "📋 Usage examples:"
    echo "  # Self-extracting (no deps needed):"
    echo "  python cht-sfx.py"
    echo "  python -c \"exec(open('cht-sfx.py').read()); from cht import Cluster\""
    echo
    echo "  # Zipapp (install deps first):"
    echo "  pip install clickhouse-connect>=0.6.8 pandas>=1.5"
    echo "  python -c \"import sys; sys.path.insert(0, 'cht.pyz'); from cht import Cluster\""
else
    echo "❌ Some files missing!"
    exit 1
fi