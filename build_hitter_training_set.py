import argparse
import sqlite3
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Dict, List, Tuple


def safe_col(name: str) -> str:
    if not name:
        return name
    if name[0].isdigit() or "-" in name or " " in name:
        return f'"{name}"'
    return name


def table_columns(conn: sqlite3.Connection, table: str) -> Dict[str, str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {row[1]: (row[2] or "") for row in rows}


def is_numeric(col_type: str) -> bool:
    t = (col_type or "").upper()
    if not t:
        return True
    return "INT" in t or "REAL" in t or "NUM" in t or "DEC" in t or "FLOAT" in t


def ensure_training_table(
    conn: sqlite3.Connection, feature_cols: List[Tuple[str, str]]
) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS hitter_training_rows (
            train_season INTEGER NOT NULL,
            as_of_date TEXT NOT NULL,
            team TEXT NOT NULL,
            player_name TEXT NOT NULL,
            y_hr_final INTEGER NOT NULL DEFAULT 0,
            y_ops_final REAL NOT NULL DEFAULT 0,
            UNIQUE (train_season, as_of_date, team, player_name)
        )
        """
    )
    existing = table_columns(conn, "hitter_training_rows")
    for col_name, col_type in feature_cols:
        if col_name in existing:
            continue
        col_sql = safe_col(col_name)
        type_sql = "REAL"
        if col_type and "INT" in col_type.upper():
            type_sql = "INTEGER"
        conn.execute(f"ALTER TABLE hitter_training_rows ADD COLUMN {col_sql} {type_sql}")
    conn.commit()


def pick_sample_dates(
    dates: List[str], start: str | None, end: str | None
) -> List[str]:
    if not dates:
        return []
    parsed = [(d, datetime.strptime(d, "%Y%m%d").date()) for d in dates]
    parsed.sort(key=lambda x: x[1])
    if start:
        anchor_date = datetime.strptime(start, "%Y%m%d").date()
    else:
        anchor_date = parsed[0][1]
    selected = []
    for date_str, date_val in parsed:
        if start and date_val < anchor_date:
            continue
        if end and date_val > datetime.strptime(end, "%Y%m%d").date():
            continue
        delta = (date_val - anchor_date).days
        if delta % 7 == 0:
            selected.append(date_str)
    return selected


def build_upsert_sql(columns: List[str]) -> str:
    updates = []
    for col in columns:
        if col in {"train_season", "as_of_date", "team", "player_name"}:
            continue
        updates.append(f"{safe_col(col)}=excluded.{safe_col(col)}")
    return ", ".join(updates)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build hitter training rows.")
    parser.add_argument("--db", default="db.sqlite3")
    parser.add_argument("--train-season", type=int)
    parser.add_argument("--start")
    parser.add_argument("--end")
    parser.add_argument("--team")
    parser.add_argument("--upsert", action="store_true")
    args = parser.parse_args()

    if args.train_season is None:
        kst_year = datetime.now(ZoneInfo("Asia/Seoul")).year
        args.train_season = kst_year - 1
        print(f"train_season not provided; using {args.train_season} (KST year-1)")

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row

    if not conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='hitter_daily_snapshots'"
    ).fetchone():
        raise SystemExit("Missing table: hitter_daily_snapshots")
    if not conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='hitter_season_totals'"
    ).fetchone():
        raise SystemExit("Missing table: hitter_season_totals")

    snapshot_cols = table_columns(conn, "hitter_daily_snapshots")
    total_cols = table_columns(conn, "hitter_season_totals")

    key_cols = {"season", "as_of_date", "team", "player_name"}
    feature_cols: List[Tuple[str, str]] = []
    skipped = []
    for col, col_type in snapshot_cols.items():
        if col in key_cols:
            continue
        if not is_numeric(col_type):
            skipped.append(col)
            continue
        feature_cols.append((col, col_type))
    if skipped:
        print(f"Skipped non-numeric snapshot columns: {', '.join(skipped)}")
    if not feature_cols:
        raise SystemExit("No numeric feature columns found in snapshots.")

    if "HR" not in total_cols or "OPS" not in total_cols:
        raise SystemExit("Missing HR or OPS in hitter_season_totals")

    ensure_training_table(conn, feature_cols)

    filters = ["season = ?"]
    params: List = [args.train_season]
    if args.team:
        filters.append("team = ?")
        params.append(args.team)
    if args.start:
        filters.append("as_of_date >= ?")
        params.append(args.start)
    if args.end:
        filters.append("as_of_date <= ?")
        params.append(args.end)
    date_rows = conn.execute(
        f"""
        SELECT DISTINCT as_of_date
        FROM hitter_daily_snapshots
        WHERE {' AND '.join(filters)}
        """,
        params,
    ).fetchall()
    distinct_dates = [row[0] for row in date_rows]
    sample_dates = pick_sample_dates(distinct_dates, args.start, args.end)
    if not sample_dates:
        print("No sample dates found.")
        return

    pa_filter = ""
    if "PA_to_date" in snapshot_cols:
        pa_filter = "AND s.PA_to_date >= 30"
    else:
        print("PA_to_date missing in snapshots; sampling without PA filter.")

    date_placeholders = ", ".join(["?"] * len(sample_dates))
    feature_select = ", ".join(f"s.{safe_col(c)} AS {safe_col(c)}" for c, _ in feature_cols)
    sql = f"""
        SELECT
            ? AS train_season,
            s.as_of_date,
            s.team,
            s.player_name,
            {feature_select},
            t.HR AS y_hr_final,
            t.OPS AS y_ops_final
        FROM hitter_daily_snapshots s
        JOIN hitter_season_totals t
            ON t.season = ?
            AND s.team = t.team
            AND s.player_name = t.player_name
        WHERE s.season = ?
            AND s.as_of_date IN ({date_placeholders})
            {pa_filter}
    """
    select_params: List = [args.train_season, args.train_season, args.train_season]
    select_params.extend(sample_dates)

    rows = conn.execute(sql, select_params).fetchall()
    if not rows:
        print("No training rows matched.")
        return

    insert_cols = (
        ["train_season", "as_of_date", "team", "player_name"]
        + [c for c, _ in feature_cols]
        + ["y_hr_final", "y_ops_final"]
    )
    insert_cols_sql = ", ".join(safe_col(c) for c in insert_cols)
    placeholders = ", ".join(["?"] * len(insert_cols))
    if args.upsert:
        update_sql = build_upsert_sql(insert_cols)
        insert_sql = f"""
            INSERT INTO hitter_training_rows ({insert_cols_sql})
            VALUES ({placeholders})
            ON CONFLICT(train_season, as_of_date, team, player_name) DO UPDATE SET
                {update_sql}
        """
    else:
        insert_sql = f"""
            INSERT OR IGNORE INTO hitter_training_rows ({insert_cols_sql})
            VALUES ({placeholders})
        """

    values = []
    for row in rows:
        values.append([row[col] for col in insert_cols])

    cursor = conn.cursor()
    cursor.executemany(insert_sql, values)
    conn.commit()

    print(
        f"Training rows built for train_season={args.train_season}, "
        f"dates={len(sample_dates)}, team={args.team or 'ALL'}"
    )
    print(f"Rows inserted: {cursor.rowcount}")

    summary = conn.execute(
        """
        SELECT COUNT(*) AS cnt,
               AVG(y_hr_final) AS avg_hr,
               AVG(y_ops_final) AS avg_ops
        FROM hitter_training_rows
        WHERE train_season = ?
        """,
        (args.train_season,),
    ).fetchone()
    if summary:
        print(
            f"Target summary: rows={summary['cnt']} avg_hr={summary['avg_hr']:.3f} "
            f"avg_ops={summary['avg_ops']:.4f}"
        )

    conn.close()


if __name__ == "__main__":
    main()
