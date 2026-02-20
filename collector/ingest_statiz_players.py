import argparse
import datetime as dt
import sqlite3

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
    parser = argparse.ArgumentParser(description="Ingest players master from STATIZ table URL")
    parser.add_argument("--db", default="kbo_stats.db")
    parser.add_argument("--source-url", required=True, help="STATIZ URL containing player table")
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


def main() -> None:
    args = parse_args()
    html = fetch_html(args.source_url)
    tables = read_html_tables(html)
    table = pick_table_by_keywords(tables, ["선수", "팀"])
    if table is None:
        raise SystemExit("No suitable players table found. Provide a URL containing a players list table.")

    c_name = _col(table, ["선수", "선수명", "이름", "player"])
    c_birth = _col(table, ["생년월일", "출생", "birth"])
    c_pos = _col(table, ["포지션", "position"])
    c_bt = _col(table, ["투타", "타/투", "bats", "throws"])
    c_debut = _col(table, ["데뷔", "debut"])
    c_salary = _col(table, ["연봉", "salary"])
    c_fa = _col(table, ["fa"])

    if c_name is None:
        raise SystemExit("Required column missing: player_name")

    now = dt.datetime.utcnow().isoformat() + "Z"
    conn = sqlite3.connect(args.db)
    try:
        init_statiz_tables(conn)
        written = 0
        for _, row in table.iterrows():
            name = str(row.get(c_name, "")).strip()
            if not name or name.lower() in {"nan", "none"}:
                continue
            birth = str(row.get(c_birth, "")).strip() if c_birth else ""
            player_id = stable_player_id(name, birth)
            pos = str(row.get(c_pos, "")).strip() if c_pos else ""
            bt = str(row.get(c_bt, "")).strip() if c_bt else ""
            debut = str(row.get(c_debut, "")).strip() if c_debut else ""
            salary = str(row.get(c_salary, "")).strip() if c_salary else ""
            fa_info = str(row.get(c_fa, "")).strip() if c_fa else ""

            conn.execute(
                """
                INSERT INTO statiz_players
                (player_id, player_name, birth_date, position, bats_throws, debut_year, salary_info, fa_info,
                 source, source_url, collected_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'STATIZ', ?, ?)
                ON CONFLICT(player_id) DO UPDATE SET
                  player_name=excluded.player_name,
                  birth_date=excluded.birth_date,
                  position=excluded.position,
                  bats_throws=excluded.bats_throws,
                  debut_year=excluded.debut_year,
                  salary_info=excluded.salary_info,
                  fa_info=excluded.fa_info,
                  source=excluded.source,
                  source_url=excluded.source_url,
                  collected_at=excluded.collected_at
                """,
                (player_id, name, birth, pos, bt, debut, salary, fa_info, args.source_url, now),
            )
            written += 1

        conn.commit()
        print(f"[ok] statiz_players upserted={written}")
        preview_rows = conn.execute(
            "SELECT player_id, player_name, birth_date, position FROM statiz_players ORDER BY collected_at DESC LIMIT ?",
            (max(1, args.preview),),
        ).fetchall()
        for r in preview_rows:
            print(r)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
