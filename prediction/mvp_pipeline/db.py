"""Shared database utilities for the MVP prediction pipeline.

Centralises the hitter game-log loader that was previously duplicated in
train.py and predict.py, and wires in WAL mode for SQLite so that concurrent
reads from the Django API server do not collide with the nightly upsert writer.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from db_support import connect_for_path, read_sql_query
from .mock_data import make_mock_hitter_game_logs

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_DEFAULT_DB_RELATIVE_PATH = Path(__file__).resolve().parents[2] / "kbo_stats.db"

# ---------------------------------------------------------------------------
# Low-level helpers
# ---------------------------------------------------------------------------


def open_db(db_path: str | Path, wal: bool = True):
    """Open a database connection.

    Args:
        db_path: Path to the database file when using SQLite.
        wal:     Kept for backward compatibility. Ignored for PostgreSQL.

    Returns:
        An open DB-API connection.
    """
    return connect_for_path(db_path)


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

_HITTER_COLUMN_ALIASES = {
    "ab": "AB",
    "h": "H",
    "hr": "HR",
    "bb": "BB",
    "so": "SO",
    "hbp": "HBP",
    "sf": "SF",
    "r": "R",
    "rbi": "RBI",
    "tb": "TB",
    "pa": "PA",
    "sb": "SB",
    "cs": "CS",
    "gdp": "GDP",
    "sh": "SH",
}


def load_hitter_game_logs_from_db(db_path: str | Path, season: int) -> pd.DataFrame:
    """Load a full season of hitter game logs from the database.

    Args:
        db_path: Path to the SQLite database file.
        season:  Four-digit season year (e.g. 2025).

    Returns:
        A :class:`pandas.DataFrame` with the raw game-log rows for *season*.
    """
    conn = open_db(db_path)
    try:
        df = read_sql_query(_GAME_LOG_QUERY, conn, params=[str(season)])
    finally:
        conn.close()
    df = df.rename(columns={k: v for k, v in _HITTER_COLUMN_ALIASES.items() if k in df.columns})
    for special in ("2b", "3b"):
        if special in df.columns:
            df.rename(columns={special: special.upper()}, inplace=True)
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
