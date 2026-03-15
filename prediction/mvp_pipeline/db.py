"""Shared database utilities for the MVP prediction pipeline.

Centralises the hitter game-log loader that was previously duplicated in
train.py and predict.py, and wires in WAL mode for SQLite so that concurrent
reads from the Django API server do not collide with the nightly upsert writer.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd

from .mock_data import make_mock_hitter_game_logs

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_DEFAULT_DB_RELATIVE_PATH = Path(__file__).resolve().parents[2] / "kbo_stats.db"

# ---------------------------------------------------------------------------
# Low-level helpers
# ---------------------------------------------------------------------------


def open_db(db_path: str | Path, wal: bool = True) -> sqlite3.Connection:
    """Open a SQLite connection.

    Args:
        db_path: Path to the SQLite database file.
        wal:     If True (default) switch the journal mode to WAL so that
                 concurrent readers from the Django API server and the nightly
                 writer do not block each other.

    Returns:
        An open :class:`sqlite3.Connection`.
    """
    conn = sqlite3.connect(str(db_path))
    if wal:
        conn.execute("PRAGMA journal_mode=WAL")
    return conn


# ---------------------------------------------------------------------------
# Game-log loader (single canonical implementation)
# ---------------------------------------------------------------------------

_GAME_LOG_QUERY = """
    SELECT
        CAST(substr(game_date, 1, 4) AS INTEGER) AS season,
        substr(game_date, 1, 4) || '-' || substr(game_date, 5, 2) || '-' || substr(game_date, 7, 2) AS game_date,
        game_id,
        team,
        player_name,
        AB,
        H,
        HR,
        BB,
        SO,
        "2B" AS "2B",
        "3B" AS "3B",
        HBP,
        SF,
        R,
        RBI,
        TB,
        PA,
        SB,
        CS,
        GDP,
        SH
    FROM hitter_game_logs
    WHERE substr(game_date, 1, 4) = ?
    ORDER BY game_date ASC, game_id ASC, team ASC, player_name ASC
"""


def load_hitter_game_logs_from_db(db_path: str | Path, season: int) -> pd.DataFrame:
    """Load a full season of hitter game logs from the SQLite database.

    Args:
        db_path: Path to the SQLite database file.
        season:  Four-digit season year (e.g. 2025).

    Returns:
        A :class:`pandas.DataFrame` with the raw game-log rows for *season*.
    """
    conn = open_db(db_path)
    try:
        df = pd.read_sql_query(_GAME_LOG_QUERY, conn, params=[str(season)])
    finally:
        conn.close()
    return df


def load_hitter_game_logs(
    input_path: str | None,
    season: int = 2025,
) -> pd.DataFrame:
    """Resolve the data source and return hitter game logs.

    Resolution order:
    1. ``input_path`` supplied explicitly → SQLite / Parquet / CSV from that path.
    2. No ``input_path`` → look for the default project SQLite DB.
    3. Default DB not found → fall back to mock data (unit-test / CI friendly).

    Args:
        input_path: Explicit path to a data file, or ``None``.
        season:     Season year used when reading from SQLite.

    Returns:
        A :class:`pandas.DataFrame` with hitter game logs.
    """
    if input_path is None:
        if _DEFAULT_DB_RELATIVE_PATH.exists():
            return load_hitter_game_logs_from_db(_DEFAULT_DB_RELATIVE_PATH, season)
        return make_mock_hitter_game_logs()

    path = Path(input_path)
    if path.suffix.lower() in {".db", ".sqlite", ".sqlite3"}:
        return load_hitter_game_logs_from_db(path, season)
    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path)
    return pd.read_csv(path)
