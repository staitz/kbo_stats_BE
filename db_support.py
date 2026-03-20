from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import Any, Iterable, Sequence
from urllib.parse import urlparse

import pandas as pd
import psycopg
from psycopg.rows import dict_row


ROOT = Path(__file__).resolve().parent
DEFAULT_SQLITE_PATH = ROOT / "kbo_stats.db"
ENV_PATH = ROOT / "be" / ".env"


def load_env_file(env_path: Path = ENV_PATH) -> None:
    if not env_path.exists():
        return
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


def should_use_postgres() -> bool:
    load_env_file()
    use_postgres = os.getenv("USE_POSTGRES", "").strip().lower()
    if use_postgres in {"1", "true", "yes", "on"}:
        return True
    return bool(os.getenv("DATABASE_URL", "").strip() or os.getenv("POSTGRES_DB", "").strip())


def sqlite_db_path() -> str:
    load_env_file()
    return os.getenv("SQLITE_DB_PATH", str(DEFAULT_SQLITE_PATH)).strip() or str(DEFAULT_SQLITE_PATH)


def postgres_kwargs() -> dict[str, Any]:
    load_env_file()
    database_url = os.getenv("DATABASE_URL", "").strip()
    if database_url:
        parsed = urlparse(database_url)
        if parsed.scheme not in {"postgres", "postgresql"}:
            raise ValueError(f"Unsupported DATABASE_URL scheme: {parsed.scheme}")
        return {
            "dbname": parsed.path.lstrip("/"),
            "user": parsed.username or "",
            "password": parsed.password or "",
            "host": parsed.hostname or "localhost",
            "port": parsed.port or 5432,
        }
    return {
        "dbname": os.getenv("POSTGRES_DB", "").strip(),
        "user": os.getenv("POSTGRES_USER", "").strip(),
        "password": os.getenv("POSTGRES_PASSWORD", "").strip(),
        "host": os.getenv("POSTGRES_HOST", "localhost").strip(),
        "port": int(os.getenv("POSTGRES_PORT", "5432").strip() or "5432"),
    }


def connect(db_path: str | Path | None = None):
    load_env_file()
    if should_use_postgres():
        kwargs = postgres_kwargs()
        conn = psycopg.connect(**kwargs, row_factory=dict_row)
        conn.autocommit = False
        return conn
    sqlite_path = str(db_path or sqlite_db_path())
    conn = sqlite3.connect(sqlite_path)
    conn.row_factory = sqlite3.Row
    return conn


def connect_for_path(db_path: str | Path | None = None):
    path = str(db_path or "")
    suffix = Path(path).suffix.lower()
    normalized = Path(path).name.lower() if path else ""
    if should_use_postgres() and normalized in {"kbo_stats.db", "db.sqlite3"}:
        return connect()
    if path and suffix in {".db", ".sqlite", ".sqlite3"}:
        conn = sqlite3.connect(path)
        conn.row_factory = sqlite3.Row
        return conn
    return connect(db_path=db_path)


def is_postgres(conn: Any) -> bool:
    return conn.__class__.__module__.startswith("psycopg")


def placeholder(conn: Any) -> str:
    return "%s" if is_postgres(conn) else "?"


def qmarks(sql: str, conn: Any) -> str:
    if not is_postgres(conn):
        return sql
    parts = sql.split("?")
    if len(parts) == 1:
        return sql
    return "%s".join(parts)


def execute(conn: Any, sql: str, params: Sequence[Any] | None = None):
    return conn.execute(qmarks(sql, conn), list(params or []))


def executemany(conn: Any, sql: str, rows: Iterable[Sequence[Any]]):
    if is_postgres(conn):
        with conn.cursor() as cur:
            return cur.executemany(qmarks(sql, conn), rows)
    return conn.executemany(qmarks(sql, conn), rows)


def fetchall(conn: Any, sql: str, params: Sequence[Any] | None = None) -> list[Any]:
    return execute(conn, sql, params).fetchall()


def fetchone(conn: Any, sql: str, params: Sequence[Any] | None = None):
    return execute(conn, sql, params).fetchone()


def table_exists(conn: Any, table_name: str) -> bool:
    if is_postgres(conn):
        row = fetchone(
            conn,
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = ?
            LIMIT 1
            """,
            [table_name.lower()],
        )
        return bool(row)
    row = fetchone(
        conn,
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        [table_name],
    )
    return bool(row)


def table_columns(conn: Any, table_name: str) -> list[str]:
    if is_postgres(conn):
        rows = fetchall(
            conn,
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = ?
            ORDER BY ordinal_position
            """,
            [table_name.lower()],
        )
        return [str(row["column_name"]) for row in rows]
    rows = conn.execute(f'PRAGMA table_info("{table_name}")').fetchall()
    return [str(row[1]) for row in rows]


def read_sql_query(sql: str, conn: Any, params: Sequence[Any] | None = None) -> pd.DataFrame:
    if is_postgres(conn):
        cur = execute(conn, sql, params)
        rows = cur.fetchall()
        if not rows:
            column_names = [desc[0] for desc in cur.description] if cur.description else []
            return pd.DataFrame(columns=column_names)
        if isinstance(rows[0], dict):
            return pd.DataFrame(rows)
        column_names = [desc[0] for desc in cur.description] if cur.description else []
        return pd.DataFrame(rows, columns=column_names)
    return pd.read_sql_query(qmarks(sql, conn), conn, params=list(params or []))


def row_value(row: Any, key: str, default: Any = None) -> Any:
    if row is None:
        return default
    if isinstance(row, dict):
        if key in row:
            return row.get(key, default)
        lowered = key.lower()
        for existing_key, value in row.items():
            if str(existing_key).lower() == lowered:
                return value
        return default
    try:
        return row[key]
    except Exception:
        return default
