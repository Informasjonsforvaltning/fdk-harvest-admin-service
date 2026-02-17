#!/usr/bin/env bash
# Run MongoDB → PostgreSQL migration. Use from repo root or from this directory.
#
# Env vars (set in shell or in .env in this directory):
#   Option A - component vars (passwords are URL-encoded automatically):
#     MONGO_USER, MONGO_PASSWORD, MONGO_HOST, MONGO_DATABASE, MONGO_AUTH_SOURCE
#     POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB
#   Option B - full URIs (encode special chars in password yourself):
#     MONGO_URI, POSTGRES_URI, MONGO_DATABASE
#
# Pass-through args: --dry-run, --skip-reports, --mongo-uri URI, --postgres-uri URI, etc.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if [[ -f .env ]]; then
  set -a
  source .env
  set +a
fi

VENV_DIR=".venv"
if [[ ! -d "$VENV_DIR" ]]; then
  echo "Creating venv in $SCRIPT_DIR/$VENV_DIR"
  python3 -m venv "$VENV_DIR"
fi
source "$VENV_DIR/bin/activate"

if ! python -c "import pymongo, psycopg" 2>/dev/null; then
  echo "Installing requirements..."
  pip install -q -r requirements.txt
fi

exec python migrate.py "$@"
