"""
Database backend abstraction — supports PostgreSQL (psycopg2) and SQLite.

The active backend is chosen by the DB_BACKEND env var:
  - "postgres" (default) — requires POSTGRES_* env vars
  - "sqlite"            — uses SQLITE_PATH (default ./adbridge.db)

All route files continue to call conn.cursor(), cur.execute(sql, params),
cur.fetchone(), cur.fetchall() etc.  The wrappers transparently translate
psycopg2 conventions (%s placeholders, ILIKE, RETURNING, NOW(), SERIAL)
into SQLite equivalents when the sqlite backend is active.
"""

import os
import re
import sqlite3
from datetime import datetime

from dotenv import load_dotenv

load_dotenv()

BACKEND = os.environ.get("DB_BACKEND", "postgres").lower()
SQLITE_PATH = os.environ.get("SQLITE_PATH", "adbridge.db")

# ─────────────────────────── SQL rewriting ───────────────────────────────────

_PLACEHOLDER_RE = re.compile(r"(?<!%)%s")
_RETURNING_RE = re.compile(r"\s+RETURNING\s+(\*|\w[\w,\s]*)", re.IGNORECASE)
_SERIAL_RE = re.compile(r"\bSERIAL\b", re.IGNORECASE)
_ILIKE_RE = re.compile(r"\bILIKE\b", re.IGNORECASE)
_NOW_RE = re.compile(r"\bNOW\(\)", re.IGNORECASE)


def _rewrite_sql_for_sqlite(sql):
    """Translate Postgres SQL conventions to SQLite equivalents.

    Returns (rewritten_sql, returning_clause_or_None).
    """
    # Extract and strip RETURNING clause
    returning = None
    m = _RETURNING_RE.search(sql)
    if m:
        returning = m.group(1).strip()
        sql = sql[:m.start()] + sql[m.end():]

    # %s  →  ?
    sql = _PLACEHOLDER_RE.sub("?", sql)

    # ILIKE  →  LIKE  (SQLite LIKE is case-insensitive for ASCII)
    sql = _ILIKE_RE.sub("LIKE", sql)

    # NOW()  →  current timestamp string
    sql = _NOW_RE.sub(f"'{datetime.utcnow().isoformat()}Z'", sql)

    return sql, returning


def _rewrite_ddl_for_sqlite(sql):
    """Extra DDL-level rewrites (SERIAL → INTEGER PRIMARY KEY AUTOINCREMENT)."""
    sql, returning = _rewrite_sql_for_sqlite(sql)
    # SERIAL PRIMARY KEY → INTEGER PRIMARY KEY AUTOINCREMENT
    sql = re.sub(
        r"\bSERIAL\s+PRIMARY\s+KEY\b",
        "INTEGER PRIMARY KEY AUTOINCREMENT",
        sql,
        flags=re.IGNORECASE,
    )
    # Standalone SERIAL (not followed by PRIMARY KEY) → INTEGER
    sql = _SERIAL_RE.sub("INTEGER", sql)
    return sql


# ─────────────────────────── SQLite wrappers ─────────────────────────────────

class SqliteCursorWrapper:
    """Wraps sqlite3.Cursor to behave like a psycopg2 RealDictCursor."""

    def __init__(self, raw_cursor, conn_wrapper):
        self._cur = raw_cursor
        self._conn = conn_wrapper
        self._returning = None  # set after execute if RETURNING was stripped
        self._table = None      # table name for RETURNING re-SELECT

    def execute(self, sql, params=()):
        rewritten, returning = _rewrite_sql_for_sqlite(sql)
        self._returning = returning

        # Detect table name for potential RETURNING re-SELECT
        if returning:
            m = re.search(r"INSERT\s+INTO\s+(\w+)", sql, re.IGNORECASE)
            self._table = m.group(1) if m else None

        # Convert params: psycopg2 accepts both tuples and lists
        if isinstance(params, dict):
            self._cur.execute(rewritten, params)
        else:
            self._cur.execute(rewritten, tuple(params))

        # If there was a RETURNING clause, synthesise the result row(s)
        if returning and self._table:
            rowid = self._cur.lastrowid
            if rowid and rowid > 0:
                if returning == "*":
                    self._cur.execute(
                        f"SELECT * FROM {self._table} WHERE rowid = ?", (rowid,)
                    )
                else:
                    # RETURNING id (or similar single column)
                    col = returning.strip()
                    self._cur.execute(
                        f"SELECT {col} FROM {self._table} WHERE rowid = ?", (rowid,)
                    )

        return self

    @staticmethod
    def _normalise(d):
        """Ensure COUNT(*) is accessible as 'count' to match psycopg2 convention."""
        if "count" not in d and "COUNT(*)" in d:
            d["count"] = d.pop("COUNT(*)")
        return d

    def fetchone(self):
        row = self._cur.fetchone()
        if row is None:
            return None
        return self._normalise(dict(row))

    def fetchall(self):
        return [self._normalise(dict(r)) for r in self._cur.fetchall()]

    @property
    def description(self):
        return self._cur.description

    @property
    def lastrowid(self):
        return self._cur.lastrowid

    @property
    def rowcount(self):
        return self._cur.rowcount

    def close(self):
        self._cur.close()


class SqliteConnectionWrapper:
    """Wraps sqlite3.Connection to match the psycopg2 interface used by routes."""

    def __init__(self, path=None):
        p = path or SQLITE_PATH
        self._conn = sqlite3.connect(p)
        self._conn.row_factory = sqlite3.Row
        # Enable foreign keys
        self._conn.execute("PRAGMA foreign_keys = ON")
        # WAL mode for better concurrency
        self._conn.execute("PRAGMA journal_mode = WAL")

    def cursor(self):
        return SqliteCursorWrapper(self._conn.cursor(), self)

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def close(self):
        self._conn.close()


# ─────────────────────────── Postgres wrappers (passthrough) ─────────────────

class PgCursorWrapper:
    """Thin wrapper around psycopg2 RealDictCursor — mostly passthrough.

    Only patches COUNT(*) results to always use the key "count".
    """

    def __init__(self, raw_cursor):
        self._cur = raw_cursor

    def execute(self, sql, params=()):
        self._cur.execute(sql, params)
        return self

    def fetchone(self):
        row = self._cur.fetchone()
        if row is None:
            return None
        d = dict(row)
        # Normalise COUNT(*) key
        if "count" not in d and "COUNT(*)" in d:
            d["count"] = d.pop("COUNT(*)")
        return d

    def fetchall(self):
        rows = self._cur.fetchall()
        result = []
        for r in rows:
            d = dict(r)
            if "count" not in d and "COUNT(*)" in d:
                d["count"] = d.pop("COUNT(*)")
            result.append(d)
        return result

    @property
    def description(self):
        return self._cur.description

    @property
    def lastrowid(self):
        return getattr(self._cur, "lastrowid", None)

    @property
    def rowcount(self):
        return self._cur.rowcount

    def close(self):
        self._cur.close()


class PgConnectionWrapper:
    """Thin wrapper around psycopg2 connection."""

    def __init__(self, raw_conn):
        self._conn = raw_conn

    def cursor(self):
        return PgCursorWrapper(self._conn.cursor())

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def close(self):
        self._conn.close()


# ─────────────────────────── Public API ──────────────────────────────────────

def get_connection():
    """Return a wrapped connection for the active backend."""
    if BACKEND == "sqlite":
        return SqliteConnectionWrapper()
    else:
        import psycopg2
        import psycopg2.extras
        dsn = (
            f"host={os.environ['POSTGRES_HOST']} "
            f"port={os.environ.get('POSTGRES_PORT', 5432)} "
            f"dbname={os.environ['POSTGRES_DB']} "
            f"user={os.environ['POSTGRES_USER']} "
            f"password={os.environ['POSTGRES_PASSWORD']}"
        )
        raw = psycopg2.connect(dsn, cursor_factory=psycopg2.extras.RealDictCursor)
        return PgConnectionWrapper(raw)


def get_db():
    """FastAPI dependency — yields a connection and closes it after the request."""
    conn = get_connection()
    try:
        yield conn
    finally:
        conn.close()


def init_db_schema(conn):
    """Create all tables using the SCHEMA string from database module."""
    from app.database import SCHEMA

    cur = conn.cursor()
    for statement in SCHEMA.split(";"):
        stmt = statement.strip()
        if not stmt:
            continue
        if BACKEND == "sqlite":
            stmt = _rewrite_ddl_for_sqlite(stmt)
        cur.execute(stmt)
    conn.commit()


def init_db():
    """Full database initialisation: schema + seed."""
    from app.database import (
        _seed_core, _seed_dv360, _seed_triton_booking,
        _seed_triton, _seed_hivestack, _seed_adswizz,
        _seed_thetradedesk, _now,
    )

    conn = get_connection()
    cur = conn.cursor()

    # Create tables
    init_db_schema(conn)

    # Check if already seeded
    cur.execute("SELECT COUNT(*) AS count FROM users")
    count = cur.fetchone()["count"]
    if count == 0:
        _seed_core(cur)

    now = _now()
    _seed_dv360(cur, now)
    _seed_triton_booking(cur, now)
    _seed_triton(cur, now)
    _seed_hivestack(cur, now)
    _seed_adswizz(cur, now)
    _seed_thetradedesk(cur, now)

    conn.commit()
    cur.close()
    conn.close()
    print(f"Database initialized and seeded (backend={BACKEND}).")
