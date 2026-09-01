#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENGINE_BIN="$SCRIPT_DIR/native_bin/cortex_1brc_silicon_engine"

DATA_FILE="${1:-measurements.txt}"
exec "$ENGINE_BIN" "$DATA_FILE"
