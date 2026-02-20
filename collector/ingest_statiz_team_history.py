import argparse
import datetime as dt
import sqlite3
from typing import Any

import pandas as pd

from collector.statiz_common import (
    fetch_html,
    norm_col,
    pick_table_by_keywords,
    read_html_tables,
    stable_player_id,
)
from collector.statiz_db import init_statiz_tables


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest player-team-season history from STATIZ table URL")
    parser.add_argument("--db", default="kbo_stats.db")
    parser.add_argument("--source-url", required=True, help="STATIZ URL containing history table")
    parser.add_argument("--preview", type=int, default=5)
    return parser.parse_args()


def _col(df: pd.DataFrame, aliases: list[str]) -> str | None:
    cols = list(df.columns)
    normalized = {norm_col(c): c for c in cols}
    for alias in aliases:
        na = norm_col(alias)
        if na in normalized:
            return normalized[na]
        for k, raw in normalized.items():
            if na and (na in k or k in na):
                return raw
    return None


def _to_season(value: Any) -> int | None:
    if value is None:
        return None
    text = str(value).strip()
    if len(text) >= 4 and text[:4].isdigit():
        return int(text[:4])
    return None


def main() -> None:
    args = parse_args()
    html = fetch_html(args.source_url)
    tables = read_html_tables(html)
    table = pick_table_by_keywords(tables, ["선수", "팀", "연도"])
    if table is None:
        table = pick_table_by_keywords(tables, ["player", "team", "season"])
    if table is None:
        raise SystemExit("No suitable history table found. Provide a URL with season/team history.")

    c_name = _col(table, ["선수", "선수명", "이름", "player"])
    c_team = _col(table, ["팀", "구단", "team"])
    c_season = _col(table, ["연도", "시즌", "year", "season"])
    c_birth = _col(table, ["생년월일", "출생", "birth"])

    if c_name is None or c_team is None or c_season is None:
        raise SystemExit("Required columns missing: player_name/team/season")

    now = dt.datetime.utcnow().isoformat() + "Z"
    conn = sqlite3.connect(args.db)
    try:
        init_statiz_tables(conn)
        written = 0
        for _, row in table.iterrows():
            name = str(row.get(c_name, "")).strip()
            team = str(row.get(c_team, "")).strip()
            season = _to_season(row.get(c_season))
            birth = str(row.get(c_birth, "")).strip() if c_birth else ""
            if not name or not team or season is None:
                continue
            player_id = stable_player_id(name, birth)

            conn.execute(
                """
                INSERT INTO statiz_player_team_history
                (player_id, player_name, season, team, source, source_url, collected_at)
                VALUES (?, ?, ?, ?, 'STATIZ', ?, ?)
                ON CONFLICT(player_id, season, team) DO UPDATE SET
                  player_name=excluded.player_name,
                  source=excluded.source,
                  source_url=excluded.source_url,
                  collected_at=excluded.collected_at
                """,
                (player_id, name, season, team, args.source_url, now),
            )
            written += 1

            conn.execute(
                """
                INSERT INTO statiz_players
                (player_id, player_name, birth_date, source, source_url, collected_at)
                VALUES (?, ?, ?, 'STATIZ', ?, ?)
                ON CONFLICT(player_id) DO UPDATE SET
                  player_name=excluded.player_name,
                  birth_date=CASE
                    WHEN COALESCE(excluded.birth_date, '') <> '' THEN excluded.birth_date
                    ELSE statiz_players.birth_date
                  END,
                  source=excluded.source,
                  source_url=excluded.source_url,
                  collected_at=excluded.collected_at
                """,
                (player_id, name, birth, args.source_url, now),
            )

        conn.commit()
        print(f"[ok] statiz_player_team_history upserted={written}")
        preview_rows = conn.execute(
            """
            SELECT player_name, season, team
            FROM statiz_player_team_history
            ORDER BY collected_at DESC
            LIMIT ?
            """,
            (max(1, args.preview),),
        ).fetchall()
        for r in preview_rows:
            print(r)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
