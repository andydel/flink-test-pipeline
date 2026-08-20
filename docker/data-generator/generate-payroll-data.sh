#!/usr/bin/env bash

set -euo pipefail

echo "[payroll-data-generator] Starting payroll data generation"

trap 'echo "[payroll-data-generator] Caught termination signal, exiting"; exit 0' INT TERM

exec python3 /app/scripts/generate-payroll-data.py
