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
    parser = argparse.ArgumentParser(
        description="Ingest player split/matchup-like table from STATIZ into statiz_player_splits"
    )
    parser.add_argument("--db", default="kbo_stats.db")
    parser.add_argument("--source-url", required=True, help="STATIZ URL containing split table")
    parser.add_argument("--season", type=int, required=True, help="target season, e.g. 2025")
    parser.add_argument("--split-group", default="general_split", help="e.g. lr_home_away, monthly, vs_team")
    parser.add_argument("--player-name", help="fallback player name when table has no player column")
    parser.add_argument("--birth-date", default="", help="optional for stable player_id")
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


def _to_int(value: Any) -> int | None:
    if value is None:
        return None
    text = str(value).strip().replace(",", "")
    if text in {"", "-", "nan", "None"}:
        return None
    try:
        return int(float(text))
    except Exception:
        return None


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip().replace(",", "")
    if text in {"", "-", "nan", "None"}:
        return None
    try:
        return float(text)
    except Exception:
        return None


def main() -> None:
    args = parse_args()
    html = fetch_html(args.source_url)
    tables = read_html_tables(html)

    table = pick_table_by_keywords(tables, ["PA", "AB", "H"])
    if table is None:
        raise SystemExit("No split-like table found. Provide a URL containing PA/AB/H columns.")

    c_name = _col(table, ["선수", "선수명", "이름", "player"])
    c_split = _col(table, ["구분", "분할", "split", "상황", "type"])
    c_g = _col(table, ["G", "경기", "games"])
    c_pa = _col(table, ["PA", "타석"])
    c_ab = _col(table, ["AB", "타수"])
    c_h = _col(table, ["H", "안타"])
    c_hr = _col(table, ["HR", "홈런"])
    c_bb = _col(table, ["BB", "볼넷"])
    c_so = _col(table, ["SO", "삼진"])
    c_avg = _col(table, ["AVG", "타율"])
    c_obp = _col(table, ["OBP", "출루율"])
    c_slg = _col(table, ["SLG", "장타율"])
    c_ops = _col(table, ["OPS"])

    if c_split is None:
        # Fallback: first column as split key
        c_split = list(table.columns)[0]

    now = dt.datetime.utcnow().isoformat() + "Z"
    conn = sqlite3.connect(args.db)
    try:
        init_statiz_tables(conn)
        written = 0
        for _, row in table.iterrows():
            player_name = ""
            if c_name:
                player_name = str(row.get(c_name, "")).strip()
            if not player_name:
                player_name = (args.player_name or "").strip()
            if not player_name:
                continue

            split_key = str(row.get(c_split, "")).strip()
            if not split_key or split_key.lower() in {"합계", "total", "nan"}:
                continue

            player_id = stable_player_id(player_name, args.birth_date)

            conn.execute(
                """
                INSERT INTO statiz_player_splits
                (player_id, player_name, season, split_group, split_key, games, PA, AB, H, HR, BB, SO,
                 AVG, OBP, SLG, OPS, source, source_url, collected_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'STATIZ', ?, ?)
                ON CONFLICT(player_id, season, split_group, split_key) DO UPDATE SET
                  player_name=excluded.player_name,
                  games=excluded.games,
                  PA=excluded.PA,
                  AB=excluded.AB,
                  H=excluded.H,
                  HR=excluded.HR,
                  BB=excluded.BB,
                  SO=excluded.SO,
                  AVG=excluded.AVG,
                  OBP=excluded.OBP,
                  SLG=excluded.SLG,
                  OPS=excluded.OPS,
                  source=excluded.source,
                  source_url=excluded.source_url,
                  collected_at=excluded.collected_at
                """,
                (
                    player_id,
                    player_name,
                    args.season,
                    args.split_group,
                    split_key,
                    _to_int(row.get(c_g)) if c_g else None,
                    _to_int(row.get(c_pa)) if c_pa else None,
                    _to_int(row.get(c_ab)) if c_ab else None,
                    _to_int(row.get(c_h)) if c_h else None,
                    _to_int(row.get(c_hr)) if c_hr else None,
                    _to_int(row.get(c_bb)) if c_bb else None,
                    _to_int(row.get(c_so)) if c_so else None,
                    _to_float(row.get(c_avg)) if c_avg else None,
                    _to_float(row.get(c_obp)) if c_obp else None,
                    _to_float(row.get(c_slg)) if c_slg else None,
                    _to_float(row.get(c_ops)) if c_ops else None,
                    args.source_url,
                    now,
                ),
            )
            written += 1

        conn.commit()
        print(f"[ok] statiz_player_splits upserted={written}")
        preview_rows = conn.execute(
            """
            SELECT player_name, season, split_group, split_key, PA, AB, H, HR, OPS
            FROM statiz_player_splits
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
