from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class EnvTemplateTests(unittest.TestCase):
    def test_env_example_lists_runtime_knobs(self) -> None:
        env_example = (ROOT / "backend" / ".env.example").read_text(encoding="utf-8")
        for key in [
            "DB_AUTO_PILOT_DATA_DIR",
            "DB_AUTO_PILOT_DATABASE_PATH",
            "DB_AUTO_PILOT_UPLOADS_DIR",
            "DB_AUTO_PILOT_APP_SETTINGS_PATH",
            "DB_AUTO_PILOT_LLM_ENABLED",
            "DB_AUTO_PILOT_LLM_DATA_POLICY",
            "DB_AUTO_PILOT_OPENAI_MODEL",
            "DB_AUTO_PILOT_MAX_UPLOAD_MB",
            "DB_AUTO_PILOT_MAX_MATERIALIZATION_ROWS",
            "DB_AUTO_PILOT_MAX_MATERIALIZATION_COLUMNS",
            "DB_AUTO_PILOT_QUERY_ROW_LIMIT",
            "DB_AUTO_PILOT_CORS_ALLOW_ORIGINS",
        ]:
            with self.subTest(key=key):
                self.assertIn(f"{key}=", env_example)


if __name__ == "__main__":
    unittest.main()
