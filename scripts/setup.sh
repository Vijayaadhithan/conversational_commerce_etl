#!/usr/bin/env bash
set -euo pipefail
project_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
python_bin="${PYTHON_BIN:-python3}"
"$python_bin" -c 'import sys; raise SystemExit(0 if sys.version_info >= (3,12) else "Python 3.12+ is required")'
if [[ ! -e "$project_root/.venv" ]]; then
    "$python_bin" -m venv "$project_root/.venv"
fi
"$project_root/.venv/bin/python" -m pip install -r "$project_root/requirements-dev.txt"
if [[ ! -e "$project_root/.env" && ! -L "$project_root/.env" ]]; then
    (umask 077; set -o noclobber; cat "$project_root/.env.example" > "$project_root/.env")
fi
printf '\nReady. Run:\n  .venv/bin/python scripts/run_etl.py\n'
