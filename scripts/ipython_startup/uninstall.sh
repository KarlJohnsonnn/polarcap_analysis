#!/usr/bin/env bash
set -euo pipefail

STARTUP_FILE="${IPYTHONDIR:-$HOME/.ipython}/profile_default/startup/10-polarcap-startup.py"

if [[ -f "$STARTUP_FILE" ]]; then
    rm -f "$STARTUP_FILE"
    echo "Removed: $STARTUP_FILE"
else
    echo "Not found: $STARTUP_FILE"
fi
