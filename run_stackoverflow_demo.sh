#!/usr/bin/env bash
# Run the real Stack Overflow revision-history CDC demo from any directory.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

exec bash "$SCRIPT_DIR/demo.sh" --stackoverflow
