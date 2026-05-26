from __future__ import annotations

from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]


class BackendRequirementsLockTests(unittest.TestCase):
    def test_lockfile_pins_declared_backend_requirements(self) -> None:
        declared = {
            line.strip().lower()
            for line in (ROOT / "backend" / "requirements.txt").read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.strip().startswith("#")
        }
        locked = {
            line.split("==", 1)[0].lower()
            for line in (ROOT / "backend" / "requirements.lock.txt").read_text(encoding="utf-8").splitlines()
            if "==" in line
        }
        self.assertLessEqual(declared, locked)


if __name__ == "__main__":
    unittest.main()
