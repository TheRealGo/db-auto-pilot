from __future__ import annotations

import os

import uvicorn


def main() -> None:
    host = os.environ.get("DB_AUTO_PILOT_HOST", "127.0.0.1")
    port = int(os.environ.get("DB_AUTO_PILOT_PORT", "8000"))
    uvicorn.run("app.main:app", host=host, port=port, log_level="info")


if __name__ == "__main__":
    main()
