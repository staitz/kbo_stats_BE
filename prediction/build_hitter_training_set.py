# =============================================================================
# DEPRECATED — 이 스크립트는 legacy pipeline에서 사용되었으며
# 현재 운영 파이프라인(mvp_pipeline)에서 호출되지 않습니다.
#
# 운영 학습은 아래 명령을 사용하세요:
#   python -m prediction.mvp_pipeline.train --season YYYY
#
# 이 파일은 참조 목적으로만 보존됩니다. 직접 실행하지 마세요.
# =============================================================================
raise SystemExit(
    "[DEPRECATED] prediction.build_hitter_training_set is no longer part of the "
    "active prediction pipeline.\n"
    "Hitter model training now uses:\n"
    "  python -m prediction.mvp_pipeline.train --season YYYY\n"
    "or run the weekly schedule script:\n"
    "  .\\scripts\\run_weekly_train.ps1"
)

import argparse
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Dict, List, Tuple

from db_support import connect_for_path, execute, executemany, row_value, table_columns as shared_table_columns, table_exists

def safe_col(name: str) -> str:
    if not name:
        return name
    if name[0].isdigit() or "-" in name or " " in name:
        return f'"{name}"'
    return name


def table_columns(conn, table: str) -> Dict[str, str]:
    return {col: "" for col in shared_table_columns(conn, table)}


def is_numeric(col_type: str) -> bool:
    t = (col_type or "").upper()
    if not t:
        return True
    return "INT" in t or "REAL" in t or "NUM" in t or "DEC" in t or "FLOAT" in t


def ensure_training_table(conn, feature_cols: List[Tuple[str, str]]) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS hitter_training_rows (
            train_season INTEGER NOT NULL,
            as_of_date TEXT NOT NULL,
            team TEXT NOT NULL,
            player_name TEXT NOT NULL,
            y_hr_final INTEGER NOT NULL DEFAULT 0,
            y_ops_final REAL NOT NULL DEFAULT 0,
            y_hr_ros REAL NOT NULL DEFAULT 0,
            y_ops_ros REAL NOT NULL DEFAULT 0,
            UNIQUE (train_season, as_of_date, team, player_name)
        )
        """
    )
    existing = table_columns(conn, "hitter_training_rows")
    if "y_hr_ros" not in existing:
        conn.execute("ALTER TABLE hitter_training_rows ADD COLUMN y_hr_ros REAL NOT NULL DEFAULT 0")
    if "y_ops_ros" not in existing:
        conn.execute("ALTER TABLE hitter_training_rows ADD COLUMN y_ops_ros REAL NOT NULL DEFAULT 0")
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

    conn = connect_for_path(args.db)

    if not table_exists(conn, "hitter_daily_snapshots"):
        raise SystemExit("Missing table: hitter_daily_snapshots")
    if not table_exists(conn, "hitter_season_totals"):
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

    prior_feature_cols: List[Tuple[str, str]] = [
        ("prev_season_pa", "INTEGER"),
        ("prev_season_hr", "INTEGER"),
        ("prev_season_ops", "REAL"),
    ]
    for col_name, col_type in prior_feature_cols:
        if col_name not in {c for c, _ in feature_cols}:
            feature_cols.append((col_name, col_type))

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
    date_rows = execute(
        conn,
        f"""
        SELECT DISTINCT as_of_date
        FROM hitter_daily_snapshots
        WHERE {' AND '.join(filters)}
        """,
        params,
    ).fetchall()
    distinct_dates = [str(row_value(row, "as_of_date", row[0] if not isinstance(row, dict) else "")) for row in date_rows]
    sample_dates = pick_sample_dates(distinct_dates, args.start, args.end)
    if not sample_dates:
        print("No sample dates found.")
        return

    pa_col = "PA_to_date" if "PA_to_date" in snapshot_cols else ("PA" if "PA" in snapshot_cols else "")
    pa_filter = f"AND s.{safe_col(pa_col)} >= 30" if pa_col else ""
    if not pa_col:
        print("PA/PA_to_date missing in snapshots; sampling without PA filter.")

    date_placeholders = ", ".join(["?"] * len(sample_dates))
    snapshot_feature_cols = [c for c, _ in feature_cols if c not in {"prev_season_pa", "prev_season_hr", "prev_season_ops"}]
    feature_select_snapshot = ", ".join(f"s.{safe_col(c)} AS {safe_col(c)}" for c in snapshot_feature_cols)
    feature_select_prior = ", ".join(
        [
            "COALESCE(p.PA, 0) AS prev_season_pa",
            "COALESCE(p.HR, 0) AS prev_season_hr",
            "COALESCE(p.OPS, 0.0) AS prev_season_ops",
        ]
    )
    feature_select = ", ".join([feature_select_snapshot, feature_select_prior]).strip(", ")
    sql = f"""
        SELECT
            ? AS train_season,
            s.as_of_date,
            s.team,
            s.player_name,
            {feature_select},
            t.HR AS y_hr_final,
            t.OPS AS y_ops_final,
            (t.HR - COALESCE(s.HR, 0)) AS y_hr_ros,
            (t.OPS - COALESCE(s.OPS, 0.0)) AS y_ops_ros
        FROM hitter_daily_snapshots s
        JOIN hitter_season_totals t
            ON t.season = ?
            AND s.team = t.team
            AND s.player_name = t.player_name
        LEFT JOIN hitter_season_totals p
            ON p.season = (? - 1)
            AND s.team = p.team
            AND s.player_name = p.player_name
        WHERE s.season = ?
            AND s.as_of_date IN ({date_placeholders})
            {pa_filter}
    """
    select_params: List = [args.train_season, args.train_season, args.train_season, args.train_season]
    select_params.extend(sample_dates)

    rows = execute(conn, sql, select_params).fetchall()
    if not rows:
        print("No training rows matched.")
        return

    insert_cols = (
        ["train_season", "as_of_date", "team", "player_name"]
        + [c for c, _ in feature_cols]
        + ["y_hr_final", "y_ops_final", "y_hr_ros", "y_ops_ros"]
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
        values.append([row_value(row, col, None) for col in insert_cols])

    executemany(conn, insert_sql, values)
    conn.commit()

    print(
        f"Training rows built for train_season={args.train_season}, "
        f"dates={len(sample_dates)}, team={args.team or 'ALL'}"
    )
    print(f"Rows inserted: {len(values)}")

    summary = execute(
        conn,
        """
        SELECT COUNT(*) AS cnt,
               AVG(y_hr_final) AS avg_hr,
               AVG(y_ops_final) AS avg_ops,
               AVG(y_hr_ros) AS avg_hr_ros,
               AVG(y_ops_ros) AS avg_ops_ros
        FROM hitter_training_rows
        WHERE train_season = ?
        """,
        [args.train_season],
    ).fetchone()
    if summary:
        print(
            f"Target summary: rows={int(row_value(summary, 'cnt', 0) or 0)} avg_hr={float(row_value(summary, 'avg_hr', 0) or 0):.3f} "
            f"avg_ops={float(row_value(summary, 'avg_ops', 0) or 0):.4f} avg_hr_ros={float(row_value(summary, 'avg_hr_ros', 0) or 0):.3f} "
            f"avg_ops_ros={float(row_value(summary, 'avg_ops_ros', 0) or 0):.4f}"
        )

    conn.close()


if __name__ == "__main__":
    main()
