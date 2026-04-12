#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"

if ! command -v uv >/dev/null 2>&1; then
  echo "uv が見つかりません。https://docs.astral.sh/uv/ からインストールしてください。"
  exit 1
fi

if ! command -v npm >/dev/null 2>&1; then
  echo "npm が見つかりません。Node.js をインストールしてください。"
  exit 1
fi

echo "Installing backend dependencies..."
(cd "$ROOT_DIR/backend" && uv sync --extra dev)

echo "Installing frontend dependencies..."
(cd "$ROOT_DIR/frontend" && npm ci)

echo "Building frontend..."
(cd "$ROOT_DIR/frontend" && npm run build)

echo "Starting app on http://127.0.0.1:8000"
cd "$ROOT_DIR/backend"
exec uv run uvicorn app.main:app --host 127.0.0.1 --port 8000
