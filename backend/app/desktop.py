from __future__ import annotations

import os

import uvicorn


def main() -> None:
    uvicorn.run("app.main:app", host="127.0.0.1", port=int(os.environ.get("DB_AUTO_PILOT_PORT", "8765")))


if __name__ == "__main__":
    main()
