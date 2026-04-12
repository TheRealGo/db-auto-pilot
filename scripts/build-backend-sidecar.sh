#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
BACKEND_DIR="$ROOT_DIR/backend"
OUTPUT_DIR="$ROOT_DIR/frontend/src-tauri/binaries"
BUILD_DIR="$ROOT_DIR/.build/pyinstaller"
TARGET_TRIPLE="${TAURI_TARGET_TRIPLE:-$(rustc -vV | sed -n 's/^host: //p')}"
EXECUTABLE_NAME="db-auto-pilot-backend-$TARGET_TRIPLE"

mkdir -p "$OUTPUT_DIR" "$BUILD_DIR"

cd "$BACKEND_DIR"
uv sync --extra dev --extra desktop
uv run pyinstaller \
  --noconfirm \
  --clean \
  --onefile \
  --name "$EXECUTABLE_NAME" \
  --distpath "$OUTPUT_DIR" \
  --workpath "$BUILD_DIR/work" \
  --specpath "$BUILD_DIR/spec" \
  --hidden-import uvicorn.lifespan.on \
  --hidden-import uvicorn.loops.auto \
  --hidden-import uvicorn.protocols.http.auto \
  --hidden-import uvicorn.protocols.websockets.auto \
  app/desktop.py
