#!/bin/bash
# Setup script to ensure Python virtual environment has all required dependencies
# This script ensures numpy and pandas are properly installed in the venv

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
VENV_DIR="$PROJECT_DIR/.venv"

echo "=========================================="
echo "Setting up Python virtual environment"
echo "=========================================="
echo ""

# Check if venv exists
if [ ! -d "$VENV_DIR" ]; then
    echo "Creating virtual environment..."
    python3 -m venv "$VENV_DIR"
fi

# Activate venv
echo "Activating virtual environment..."
source "$VENV_DIR/bin/activate"

# Ensure pip is installed and up to date
echo "Ensuring pip is installed..."
python -m ensurepip --upgrade || python -m pip install --upgrade pip

# Install build dependencies first (needed for compiling extensions)
echo "Installing build dependencies..."
python -m pip install --upgrade setuptools wheel cython

# Install core dependencies with proper versions
echo "Installing core dependencies..."
python -m pip install --upgrade \
    "numpy>=1.26.4,<2.0.0" \
    "pandas>=2.2.3,<3.0.0" \
    "msgspec>=0.19.0,<1.0.0" \
    "pyarrow>=19.0.1" \
    "pytz>=2025.1.0" \
    "fsspec>=2025.2.0,<2026.0.0" \
    "click>=8.1.8,<9.0.0" \
    "tqdm>=4.67.1,<5.0.0"

# Install uvloop on non-Windows systems
if [[ "$OSTYPE" != "msys" && "$OSTYPE" != "win32" ]]; then
    echo "Installing uvloop..."
    python -m pip install --upgrade "uvloop>=0.21.0,<1.0.0"
fi

# Verify installations
echo ""
echo "Verifying installations..."
python -c "import numpy; print(f'✓ NumPy {numpy.__version__}')" || { echo "✗ NumPy import failed"; exit 1; }
python -c "import pandas; print(f'✓ Pandas {pandas.__version__}')" || { echo "✗ Pandas import failed"; exit 1; }
python -c "import msgspec; print(f'✓ msgspec {msgspec.__version__}')" || { echo "✗ msgspec import failed"; exit 1; }
python -c "import pyarrow; print(f'✓ pyarrow {pyarrow.__version__}')" || { echo "✗ pyarrow import failed"; exit 1; }

echo ""
echo "=========================================="
echo "Virtual environment setup complete!"
echo "=========================================="
echo ""
echo "To activate the venv in the future, run:"
echo "  source $VENV_DIR/bin/activate"
echo ""

