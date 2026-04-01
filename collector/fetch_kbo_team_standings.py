from __future__ import annotations

import argparse
import datetime as dt
import re
from io import StringIO
from typing import Any

import pandas as pd
import requests

from db_support import connect_for_path, execute

KBO_KR_STANDINGS_URL = "https://www.koreabaseball.com/record/teamrank/teamrankdaily.aspx"
KBO_EN_STANDINGS_URL = "https://eng.koreabaseball.com/Standings/TeamStandings.aspx"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36"
)

KR_COLUMN_ALIASES: dict[str, tuple[str, ...]] = {
    "rank": ("순위",),
    "team": ("팀명", "팀", "구단"),
    "games": ("경기",),
    "wins": ("승",),
    "losses": ("패",),
    "draws": ("무",),
    "win_pct": ("승률",),
    "gb": ("게임차",),
    "recent_10": ("최근10경기", "최근10", "최근 10경기"),
    "streak": ("연속",),
    "home_record": ("홈",),
    "away_record": ("방문", "원정"),
}

EN_COLUMN_ALIASES: dict[str, tuple[str, ...]] = {
    "rank": ("rk", "rank"),
    "team": ("team",),
    "games": ("games", "g"),
    "wins": ("w",),
    "losses": ("l",),
    "draws": ("d",),
    "win_pct": ("pct",),
    "gb": ("gb",),
    "streak": ("streak",),
    "home_record": ("home",),
    "away_record": ("away",),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch KBO official team standings and upsert into team_standings")
    parser.add_argument("--db", default="kbo_stats.db")
    parser.add_argument("--season", type=int, help="target season for validation/fallback label")
    parser.add_argument("--source", choices=["kr", "en", "auto"], default="auto")
    return parser.parse_args()


def _init_table(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS team_standings (
            season INTEGER NOT NULL,
            as_of_date TEXT NOT NULL,
            rank INTEGER NOT NULL,
            team TEXT NOT NULL,
            games INTEGER NOT NULL DEFAULT 0,
            wins INTEGER NOT NULL DEFAULT 0,
            losses INTEGER NOT NULL DEFAULT 0,
            draws INTEGER NOT NULL DEFAULT 0,
            win_pct REAL NOT NULL DEFAULT 0,
            gb REAL NOT NULL DEFAULT 0,
            recent_10 TEXT,
            streak TEXT,
            home_record TEXT,
            away_record TEXT,
            source TEXT NOT NULL DEFAULT 'KBO_OFFICIAL',
            source_url TEXT,
            collected_at TEXT NOT NULL,
            PRIMARY KEY (season, as_of_date, team)
        )
        """
    )
    conn.commit()


def _fetch_html(url: str) -> str:
    resp = requests.get(url, timeout=20, headers={"User-Agent": USER_AGENT})
    resp.raise_for_status()
    return resp.text


def _parse_tables(html: str, source_label: str) -> list[pd.DataFrame]:
    try:
        return pd.read_html(StringIO(html))
    except ValueError as exc:
        raise RuntimeError(f"Failed to parse {source_label} standings tables: {exc}") from exc
    except Exception as exc:  # pragma: no cover - defensive against parser-specific failures
        raise RuntimeError(f"Unexpected {source_label} standings parse failure: {exc.__class__.__name__}") from exc


def _normalize_name(value: Any) -> str:
    text = str(value or "").strip().lower()
    return re.sub(r"[^0-9a-z가-힣]+", "", text)


def _resolve_columns(df: pd.DataFrame, aliases: dict[str, tuple[str, ...]]) -> dict[str, str] | None:
    normalized_columns = {_normalize_name(column): str(column) for column in df.columns}
    resolved: dict[str, str] = {}
    for canonical, choices in aliases.items():
        matched = next((normalized_columns.get(_normalize_name(choice)) for choice in choices), None)
        if matched is not None:
            resolved[canonical] = matched
    return resolved


def _pick_table(
    tables: list[pd.DataFrame],
    aliases: dict[str, tuple[str, ...]],
    required: tuple[str, ...],
) -> tuple[pd.DataFrame, dict[str, str]] | None:
    for df in tables:
        columns = _resolve_columns(df, aliases)
        if columns and all(key in columns for key in required):
            return df, columns
    return None


def _extract_as_of_date(html: str) -> str:
    match = re.search(r"(20\d{2})\.(\d{2})\.(\d{2})", html)
    if match:
        return f"{match.group(1)}{match.group(2)}{match.group(3)}"
    return dt.datetime.now().strftime("%Y%m%d")


def _to_int(value: Any) -> int:
    if value is None:
        return 0
    text = str(value).strip().replace(",", "")
    if text in {"", "-", "nan", "None"}:
        return 0
    try:
        return int(float(text))
    except Exception:
        return 0


def _to_float(value: Any) -> float:
    if value is None:
        return 0.0
    text = str(value).strip().replace(",", "")
    if text in {"", "-", "nan", "None"}:
        return 0.0
    try:
        return float(text)
    except Exception:
        return 0.0


def _optional_text(row: pd.Series, columns: dict[str, str], key: str) -> str:
    column = columns.get(key)
    if not column:
        return ""
    return str(row.get(column, "")).strip()


def _rows_from_table(
    df: pd.DataFrame,
    columns: dict[str, str],
    source_label: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        team = str(row.get(columns["team"], "")).strip()
        if not team:
            continue
        rows.append(
            {
                "rank": _to_int(row.get(columns["rank"])),
                "team": team,
                "games": _to_int(row.get(columns.get("games", ""), 0)),
                "wins": _to_int(row.get(columns.get("wins", ""), 0)),
                "losses": _to_int(row.get(columns.get("losses", ""), 0)),
                "draws": _to_int(row.get(columns.get("draws", ""), 0)),
                "win_pct": _to_float(row.get(columns.get("win_pct", ""), 0)),
                "gb": _to_float(row.get(columns.get("gb", ""), 0)),
                "recent_10": _optional_text(row, columns, "recent_10") if source_label.endswith("KR") else "",
                "streak": _optional_text(row, columns, "streak"),
                "home_record": _optional_text(row, columns, "home_record"),
                "away_record": _optional_text(row, columns, "away_record"),
            }
        )
    return rows


def _fetch_rows_kr() -> tuple[str, str, list[dict[str, Any]]]:
    html = _fetch_html(KBO_KR_STANDINGS_URL)
    tables = _parse_tables(html, "KR")
    picked = _pick_table(
        tables,
        KR_COLUMN_ALIASES,
        ("rank", "team", "wins", "losses", "draws"),
    )
    if picked is None:
        raise RuntimeError("Failed to locate KR standings table")
    df, columns = picked
    return ("KBO_OFFICIAL_KR", _extract_as_of_date(html), _rows_from_table(df, columns, "KBO_OFFICIAL_KR"))


def _fetch_rows_en() -> tuple[str, str, list[dict[str, Any]]]:
    html = _fetch_html(KBO_EN_STANDINGS_URL)
    tables = _parse_tables(html, "EN")
    picked = _pick_table(
        tables,
        EN_COLUMN_ALIASES,
        ("rank", "team", "wins", "losses", "draws"),
    )
    if picked is None:
        raise RuntimeError("Failed to locate EN standings table")
    df, columns = picked
    as_of_date = dt.datetime.now().strftime("%Y%m%d")
    return ("KBO_OFFICIAL_EN", as_of_date, _rows_from_table(df, columns, "KBO_OFFICIAL_EN"))


def _fetch_rows(source: str) -> tuple[str, str, list[dict[str, Any]]]:
    errors: list[str] = []

    if source in {"kr", "auto"}:
        try:
            return _fetch_rows_kr()
        except Exception as exc:
            if source == "kr":
                raise
            errors.append(f"KR={exc}")

    try:
        return _fetch_rows_en()
    except Exception as exc:
        errors.append(f"EN={exc}")
        raise RuntimeError(" / ".join(errors)) from exc


def main() -> None:
    args = parse_args()
    source_label, as_of_date, rows = _fetch_rows(args.source)
    if not rows:
        raise SystemExit("No standings rows parsed")

    season = int(args.season) if args.season else int(as_of_date[:4])
    now = dt.datetime.utcnow().isoformat() + "Z"

    conn = connect_for_path(args.db)
    try:
        _init_table(conn)
        for row in rows:
            execute(
                conn,
                """
                INSERT INTO team_standings
                (season, as_of_date, rank, team, games, wins, losses, draws, win_pct, gb,
                 recent_10, streak, home_record, away_record, source, source_url, collected_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(season, as_of_date, team) DO UPDATE SET
                  rank=excluded.rank,
                  games=excluded.games,
                  wins=excluded.wins,
                  losses=excluded.losses,
                  draws=excluded.draws,
                  win_pct=excluded.win_pct,
                  gb=excluded.gb,
                  recent_10=excluded.recent_10,
                  streak=excluded.streak,
                  home_record=excluded.home_record,
                  away_record=excluded.away_record,
                  source=excluded.source,
                  source_url=excluded.source_url,
                  collected_at=excluded.collected_at
                """,
                (
                    season,
                    as_of_date,
                    row["rank"],
                    row["team"],
                    row["games"],
                    row["wins"],
                    row["losses"],
                    row["draws"],
                    row["win_pct"],
                    row["gb"],
                    row["recent_10"],
                    row["streak"],
                    row["home_record"],
                    row["away_record"],
                    source_label,
                    KBO_KR_STANDINGS_URL if source_label.endswith("KR") else KBO_EN_STANDINGS_URL,
                    now,
                ),
            )
        conn.commit()
        print(f"[ok] team_standings upserted={len(rows)} season={season} as_of_date={as_of_date} source={source_label}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
