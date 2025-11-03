#!/bin/bash
set -e

echo "🚀 Setting up cht development environment..."

# Create virtual environment if it doesn't exist
if [ ! -d ".venv" ]; then
    echo "📦 Creating virtual environment..."
    python -m venv .venv
fi

# Activate virtual environment
echo "🔧 Activating virtual environment..."
source .venv/bin/activate

# Upgrade pip
echo "⬆️  Upgrading pip..."
python -m pip install --upgrade pip

# Install development dependencies
echo "📚 Installing development dependencies..."
pip install -r requirements-dev.txt

# Install package in editable mode
echo "🔗 Installing package in editable mode..."
pip install -e .

echo "✅ Setup complete!"
echo ""
echo "To activate the environment:"
echo "  source .venv/bin/activate"
echo ""
echo "To run tests:"
echo "  pytest"
echo ""
echo "To format code:"
echo "  black src tests"
echo "  isort src tests"
echo ""
echo "To lint code:"
echo "  flake8 src tests"
echo ""
echo "To build wheels:"
echo "  python -m build"