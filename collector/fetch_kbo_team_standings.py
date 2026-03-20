import argparse
import datetime as dt
import re
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
    resp.encoding = resp.apparent_encoding or resp.encoding
    return resp.text


def _normalize_columns(df: pd.DataFrame) -> dict[str, str]:
    mapping = {}
    for c in df.columns:
        key = str(c).strip().lower().replace(" ", "")
        mapping[key] = c
    return mapping


def _pick_kr_table(tables: list[pd.DataFrame]) -> pd.DataFrame | None:
    for df in tables:
        cols = _normalize_columns(df)
        if "순위" in cols and "팀명" in cols and "승" in cols and "패" in cols and "무" in cols:
            return df
    return None


def _pick_en_table(tables: list[pd.DataFrame]) -> pd.DataFrame | None:
    for df in tables:
        cols = _normalize_columns(df)
        if "rk" in cols and "team" in cols and "w" in cols and "l" in cols and "d" in cols:
            return df
    return None


def _extract_as_of_date(html: str) -> str:
    # KR page format: 2025.10.04
    m = re.search(r"(20\d{2})\.(\d{2})\.(\d{2})", html)
    if m:
        return f"{m.group(1)}{m.group(2)}{m.group(3)}"
    # fallback today
    return dt.datetime.now().strftime("%Y%m%d")


def _to_int(v: Any) -> int:
    if v is None:
        return 0
    s = str(v).strip().replace(",", "")
    if s in {"", "-", "nan", "None"}:
        return 0
    try:
        return int(float(s))
    except Exception:
        return 0


def _to_float(v: Any) -> float:
    if v is None:
        return 0.0
    s = str(v).strip().replace(",", "")
    if s in {"", "-", "nan", "None"}:
        return 0.0
    try:
        return float(s)
    except Exception:
        return 0.0


def _rows_from_kr(df: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for _, r in df.iterrows():
        team = str(r.get("팀명", "")).strip()
        if not team:
            continue
        rows.append(
            {
                "rank": _to_int(r.get("순위")),
                "team": team,
                "games": _to_int(r.get("경기")),
                "wins": _to_int(r.get("승")),
                "losses": _to_int(r.get("패")),
                "draws": _to_int(r.get("무")),
                "win_pct": _to_float(r.get("승률")),
                "gb": _to_float(r.get("게임차")),
                "recent_10": str(r.get("최근10경기", "")).strip(),
                "streak": str(r.get("연속", "")).strip(),
                "home_record": str(r.get("홈", "")).strip(),
                "away_record": str(r.get("방문", "")).strip(),
            }
        )
    return rows


def _rows_from_en(df: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for _, r in df.iterrows():
        team = str(r.get("TEAM", "")).strip()
        if not team:
            continue
        rows.append(
            {
                "rank": _to_int(r.get("RK")),
                "team": team,
                "games": _to_int(r.get("GAMES")),
                "wins": _to_int(r.get("W")),
                "losses": _to_int(r.get("L")),
                "draws": _to_int(r.get("D")),
                "win_pct": _to_float(r.get("PCT")),
                "gb": _to_float(r.get("GB")),
                "recent_10": "",
                "streak": str(r.get("STREAK", "")).strip(),
                "home_record": str(r.get("HOME", "")).strip(),
                "away_record": str(r.get("AWAY", "")).strip(),
            }
        )
    return rows


def _fetch_rows(source: str) -> tuple[str, str, list[dict[str, Any]]]:
    if source in {"kr", "auto"}:
        html = _fetch_html(KBO_KR_STANDINGS_URL)
        tables = pd.read_html(html)
        table = _pick_kr_table(tables)
        if table is not None:
            return ("KBO_OFFICIAL_KR", _extract_as_of_date(html), _rows_from_kr(table))
        if source == "kr":
            raise RuntimeError("Failed to parse KR standings table")

    html = _fetch_html(KBO_EN_STANDINGS_URL)
    tables = pd.read_html(html)
    table = _pick_en_table(tables)
    if table is None:
        raise RuntimeError("Failed to parse EN standings table")
    # EN page doesn't expose explicit date in plain text; use today as_of and season inferred by top nav first year
    as_of_date = dt.datetime.now().strftime("%Y%m%d")
    return ("KBO_OFFICIAL_EN", as_of_date, _rows_from_en(table))


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
        values = [
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
            )
            for row in rows
        ]
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
            values[0],
        )
        for value in values[1:]:
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
                value,
            )
        conn.commit()
        print(f"[ok] team_standings upserted={len(rows)} season={season} as_of_date={as_of_date} source={source_label}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
