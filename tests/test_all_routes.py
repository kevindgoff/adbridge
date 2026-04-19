"""Verify all route files use correct psycopg2 patterns:
   1. No conn.execute() — must use cursor
   2. No ? placeholders — must use %s for PostgreSQL
"""
import re

ROUTE_FILES = [
    "app/routes/basis.py",
    "app/routes/dv360.py",
    "app/routes/triton.py",
    "app/routes/triton_booking.py",
    "app/routes/hivestack.py",
]


def _read(path):
    with open(path, encoding="utf-8") as f:
        return f.read()


def test_no_conn_execute_in_any_route():
    """psycopg2 connections don't have .execute() — must use cursor."""
    for path in ROUTE_FILES:
        source = _read(path)
        matches = re.findall(r"conn\.execute\(", source)
        assert not matches, (
            f"{path} has {len(matches)} conn.execute() calls. "
            "Use conn.cursor().execute() instead."
        )


def test_no_sqlite_placeholders_in_any_route():
    """SQL params must use %s (PostgreSQL), not ? (SQLite)."""
    for path in ROUTE_FILES:
        source = _read(path)
        lines = source.splitlines()
        violations = []
        for i, line in enumerate(lines, 1):
            s = line.strip()
            if s.startswith("#"):
                continue
            # Look for ? in execute/SQL contexts
            if re.search(r'execute\(.*\?', s) or re.search(r"= \?", s):
                violations.append((i, s))
        assert not violations, (
            f"{path} has SQLite '?' placeholders:\n"
            + "\n".join(f"  L{n}: {t}" for n, t in violations)
        )
