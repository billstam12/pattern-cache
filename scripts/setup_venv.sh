#!/usr/bin/env bash
# Create a Python venv with the analysis dependencies and register it as a
# Jupyter kernel so the notebooks under analysis/ can run against it.
#
# Usage (run from repo root or anywhere):
#   bash analysis/setup_venv.sh          # create .venv at repo root
#   PYTHON=python3.11 bash analysis/setup_venv.sh
#   VENV_DIR=.venv-other bash analysis/setup_venv.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
VENV_DIR="${VENV_DIR:-$ROOT/.venv}"
PYTHON="${PYTHON:-python3}"
KERNEL_NAME="${KERNEL_NAME:-pattern-cache-analysis}"
KERNEL_DISPLAY="${KERNEL_DISPLAY:-Python (pattern-cache-analysis)}"

if ! command -v "$PYTHON" >/dev/null 2>&1; then
    echo "error: $PYTHON not found. Set PYTHON=… to a working interpreter." >&2
    exit 1
fi

# pycairo needs the cairo C library + pkg-config at build time. On macOS this
# usually means `brew install cairo pkg-config`; on Debian/Ubuntu it's
# `apt-get install libcairo2-dev pkg-config`. Warn early but don't hard-fail —
# the user might already have it installed elsewhere.
if ! pkg-config --exists cairo 2>/dev/null; then
    echo "warn: pkg-config cannot find cairo. pycairo will likely fail to build." >&2
    echo "      macOS:  brew install cairo pkg-config" >&2
    echo "      Debian: sudo apt-get install libcairo2-dev pkg-config" >&2
fi

echo "==> creating venv at $VENV_DIR (python: $($PYTHON --version 2>&1))"
"$PYTHON" -m venv "$VENV_DIR"

# shellcheck disable=SC1091
source "$VENV_DIR/bin/activate"

echo "==> upgrading pip / setuptools / wheel"
python -m pip install --upgrade pip setuptools wheel

echo "==> installing requirements"
python -m pip install -r "$ROOT/requirements.txt"

echo "==> registering Jupyter kernel '$KERNEL_NAME'"
python -m ipykernel install --user \
    --name "$KERNEL_NAME" \
    --display-name "$KERNEL_DISPLAY"

cat <<EOF

Done.

  Activate:        source "$VENV_DIR/bin/activate"
  Run notebooks:   jupyter lab analysis/
  Kernel name:     $KERNEL_NAME  (select "$KERNEL_DISPLAY" in Jupyter)
EOF
