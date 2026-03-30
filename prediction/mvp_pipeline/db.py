"""Shared database utilities for the MVP prediction pipeline.

Centralises the hitter game-log loader that was previously duplicated in
train.py and predict.py, and wires in WAL mode for SQLite so that concurrent
reads from the Django API server do not collide with the nightly upsert writer.
"""

from __future__ import annotations

from collections.abc import Iterable
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

_GAME_LOG_QUERY_BASE = """
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


def list_available_hitter_log_seasons(db_path: str | Path) -> list[int]:
    """Return hitter_game_logs seasons currently stored in the DB."""
    conn = open_db(db_path)
    try:
        df = read_sql_query(
            """
            SELECT DISTINCT CAST(substr(game_date, 1, 4) AS INTEGER) AS season
            FROM hitter_game_logs
            ORDER BY season ASC
            """,
            conn,
        )
    finally:
        conn.close()
    if "season" not in df.columns:
        return []
    return [int(v) for v in df["season"].dropna().tolist()]


def resolve_training_seasons(db_path: str | Path, target_season: int) -> list[int]:
    """Resolve all available seasons up to and including target_season."""
    path = Path(db_path)
    if path.suffix.lower() not in {".db", ".sqlite", ".sqlite3"} or not path.exists():
        return [int(target_season)]
    seasons = [season for season in list_available_hitter_log_seasons(db_path) if season <= int(target_season)]
    return seasons or [int(target_season)]


def _normalize_seasons(season: int | Iterable[int]) -> list[int]:
    if isinstance(season, int):
        return [int(season)]
    return sorted({int(value) for value in season})


def load_hitter_game_logs_from_db(db_path: str | Path, season: int | Iterable[int]) -> pd.DataFrame:
    """Load one or more seasons of hitter game logs from the database.

    Args:
        db_path: Path to the SQLite database file.
        season:  Four-digit season year or iterable of seasons.

    Returns:
        A :class:`pandas.DataFrame` with the raw game-log rows.
    """
    seasons = _normalize_seasons(season)
    season_placeholders = ", ".join(["?"] * len(seasons))
    query = (
        _GAME_LOG_QUERY_BASE
        + f"\n    WHERE CAST(substr(game_date, 1, 4) AS INTEGER) IN ({season_placeholders})"
        + "\n    ORDER BY game_date ASC, game_id ASC, team ASC, player_name ASC"
    )
    conn = open_db(db_path)
    try:
        df = read_sql_query(query, conn, params=seasons)
    finally:
        conn.close()
    df = df.rename(columns={k: v for k, v in _HITTER_COLUMN_ALIASES.items() if k in df.columns})
    for special in ("2b", "3b"):
        if special in df.columns:
            df.rename(columns={special: special.upper()}, inplace=True)
    return df


def load_hitter_game_logs(
    input_path: str | None,
    season: int | Iterable[int] = 2025,
) -> pd.DataFrame:
    """Resolve the data source and return hitter game logs.

    Resolution order:
    1. ``input_path`` supplied explicitly → SQLite / Parquet / CSV from that path.
    2. No ``input_path`` → look for the default project SQLite DB.
    3. Default DB not found → fall back to mock data (unit-test / CI friendly).

    Args:
        input_path: Explicit path to a data file, or ``None``.
        season:     Season year or iterable of seasons used when reading from SQLite.

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
