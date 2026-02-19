import argparse
import sqlite3
from typing import Dict, List


def safe_col(name: str) -> str:
    if not name:
        return name
    if name[0].isdigit() or "-" in name or " " in name:
        return f'"{name}"'
    return name


def table_columns(conn: sqlite3.Connection, table: str) -> Dict[str, str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {row[1]: (row[2] or "") for row in rows}


def col_expr(columns: Dict[str, str], name: str) -> str:
    if name in columns:
        return safe_col(name)
    return "0"


def tb_adj_expr(columns: Dict[str, str]) -> str:
    h = col_expr(columns, "H")
    b2 = col_expr(columns, "2B")
    b3 = col_expr(columns, "3B")
    hr = col_expr(columns, "HR")
    tb = col_expr(columns, "TB")
    singles = (
        f"CASE WHEN ({h} - {b2} - {b3} - {hr}) < 0 THEN 0 "
        f"ELSE ({h} - {b2} - {b3} - {hr}) END"
    )
    derived = f"({singles} + 2*{b2} + 3*{b3} + 4*{hr})"
    if "TB" in columns:
        return f"CASE WHEN {tb} IS NOT NULL AND {tb} > 0 THEN {tb} ELSE {derived} END"
    return derived


def ensure_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS hitter_season_totals (
            season INTEGER NOT NULL,
            team TEXT NOT NULL,
            player_name TEXT NOT NULL,
            games INTEGER NOT NULL DEFAULT 0,
            PA INTEGER NOT NULL DEFAULT 0,
            AB INTEGER NOT NULL DEFAULT 0,
            H INTEGER NOT NULL DEFAULT 0,
            "2B" INTEGER NOT NULL DEFAULT 0,
            "3B" INTEGER NOT NULL DEFAULT 0,
            HR INTEGER NOT NULL DEFAULT 0,
            TB_adj INTEGER NOT NULL DEFAULT 0,
            RBI INTEGER NOT NULL DEFAULT 0,
            BB INTEGER NOT NULL DEFAULT 0,
            SO INTEGER NOT NULL DEFAULT 0,
            HBP INTEGER NOT NULL DEFAULT 0,
            SH INTEGER NOT NULL DEFAULT 0,
            SF INTEGER NOT NULL DEFAULT 0,
            SB INTEGER NOT NULL DEFAULT 0,
            CS INTEGER NOT NULL DEFAULT 0,
            GDP INTEGER NOT NULL DEFAULT 0,
            AVG REAL NOT NULL DEFAULT 0,
            OBP REAL NOT NULL DEFAULT 0,
            SLG REAL NOT NULL DEFAULT 0,
            OPS REAL NOT NULL DEFAULT 0,
            UNIQUE (season, team, player_name)
        )
        """
    )
    existing = {
        row[1] for row in conn.execute("PRAGMA table_info(hitter_season_totals)").fetchall()
    }
    if "SH" not in existing:
        conn.execute('ALTER TABLE hitter_season_totals ADD COLUMN SH INTEGER NOT NULL DEFAULT 0')
    conn.commit()


def build_upsert_sql(columns: List[str]) -> str:
    updates = []
    for col in columns:
        if col in {"season", "team", "player_name"}:
            continue
        updates.append(f"{safe_col(col)}=excluded.{safe_col(col)}")
    update_sql = ", ".join(updates)
    return update_sql


def main() -> None:
    parser = argparse.ArgumentParser(description="Build hitter season totals.")
    parser.add_argument("--db", default="db.sqlite3")
    parser.add_argument("--season", required=True, type=int)
    parser.add_argument("--team")
    parser.add_argument("--upsert", action="store_true")
    parser.add_argument("--preview", type=int, default=0)
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row

    if not conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='hitter_game_logs'"
    ).fetchone():
        raise SystemExit("Missing table: hitter_game_logs")

    ensure_table(conn)
    cols = table_columns(conn, "hitter_game_logs")

    required_cols = [
        "PA",
        "AB",
        "H",
        "2B",
        "3B",
        "HR",
        "TB",
        "RBI",
        "BB",
        "SO",
        "HBP",
        "SH",
        "SF",
        "SB",
        "CS",
        "GDP",
    ]
    missing = [c for c in required_cols if c not in cols]
    if missing:
        print(f"Missing columns treated as 0: {', '.join(missing)}")

    select_cols = {
        "PA": col_expr(cols, "PA"),
        "AB": col_expr(cols, "AB"),
        "H": col_expr(cols, "H"),
        "2B": col_expr(cols, "2B"),
        "3B": col_expr(cols, "3B"),
        "HR": col_expr(cols, "HR"),
        "RBI": col_expr(cols, "RBI"),
        "BB": col_expr(cols, "BB"),
        "SO": col_expr(cols, "SO"),
        "HBP": col_expr(cols, "HBP"),
        "SH": col_expr(cols, "SH"),
        "SF": col_expr(cols, "SF"),
        "SB": col_expr(cols, "SB"),
        "CS": col_expr(cols, "CS"),
        "GDP": col_expr(cols, "GDP"),
    }
    tb_adj = tb_adj_expr(cols)

    filters = ["substr(game_date, 1, 4) = ?"]
    params: List = [str(args.season)]
    if args.team:
        filters.append("team = ?")
        params.append(args.team)
    where_sql = " AND ".join(filters)

    sum_cols = {
        key: f"SUM({expr})" for key, expr in select_cols.items()
    }
    tb_adj_sum = f"SUM({tb_adj})"
    ab_sum = sum_cols["AB"]
    h_sum = sum_cols["H"]
    bb_sum = sum_cols["BB"]
    hbp_sum = sum_cols["HBP"]
    sf_sum = sum_cols["SF"]
    obp_den = f"({ab_sum} + {bb_sum} + {hbp_sum} + {sf_sum})"
    avg_expr = f"CASE WHEN {ab_sum} > 0 THEN 1.0*{h_sum}/{ab_sum} ELSE 0 END"
    obp_expr = (
        f"CASE WHEN {obp_den} > 0 THEN 1.0*({h_sum}+{bb_sum}+{hbp_sum})/"
        f"{obp_den} ELSE 0 END"
    )
    slg_expr = f"CASE WHEN {ab_sum} > 0 THEN 1.0*{tb_adj_sum}/{ab_sum} ELSE 0 END"
    ops_expr = f"({obp_expr} + {slg_expr})"

    insert_cols = [
        "season",
        "team",
        "player_name",
        "games",
        "PA",
        "AB",
        "H",
        "2B",
        "3B",
        "HR",
        "TB_adj",
        "RBI",
        "BB",
        "SO",
        "HBP",
        "SH",
        "SF",
        "SB",
        "CS",
        "GDP",
        "AVG",
        "OBP",
        "SLG",
        "OPS",
    ]
    insert_cols_sql = ", ".join(safe_col(c) for c in insert_cols)
    select_sql = f"""
        SELECT
            ? AS season,
            team,
            player_name,
            COUNT(DISTINCT game_id) AS games,
            {sum_cols["PA"]} AS PA,
            {sum_cols["AB"]} AS AB,
            {sum_cols["H"]} AS H,
            {sum_cols["2B"]} AS "2B",
            {sum_cols["3B"]} AS "3B",
            {sum_cols["HR"]} AS HR,
            {tb_adj_sum} AS TB_adj,
            {sum_cols["RBI"]} AS RBI,
            {sum_cols["BB"]} AS BB,
            {sum_cols["SO"]} AS SO,
            {sum_cols["HBP"]} AS HBP,
            {sum_cols["SH"]} AS SH,
            {sum_cols["SF"]} AS SF,
            {sum_cols["SB"]} AS SB,
            {sum_cols["CS"]} AS CS,
            {sum_cols["GDP"]} AS GDP,
            {avg_expr} AS AVG,
            {obp_expr} AS OBP,
            {slg_expr} AS SLG,
            {ops_expr} AS OPS
        FROM hitter_game_logs
        WHERE {where_sql}
        GROUP BY team, player_name
    """

    if args.upsert:
        update_sql = build_upsert_sql(insert_cols)
        sql = f"""
            INSERT INTO hitter_season_totals ({insert_cols_sql})
            {select_sql}
            ON CONFLICT(season, team, player_name) DO UPDATE SET
                {update_sql}
        """
    else:
        sql = f"""
            INSERT OR IGNORE INTO hitter_season_totals ({insert_cols_sql})
            {select_sql}
        """

    cursor = conn.cursor()
    cursor.execute(sql, [args.season] + params)
    conn.commit()

    print(
        f"Built hitter_season_totals for season={args.season}, team={args.team or 'ALL'}"
    )
    print(f"Rows affected: {cursor.rowcount}")

    if args.preview and args.preview > 0:
        preview_rows = conn.execute(
            """
            SELECT team, player_name, OPS, HR, AB, H
            FROM hitter_season_totals
            WHERE season = ?
            ORDER BY OPS DESC
            LIMIT ?
            """,
            (args.season, args.preview),
        ).fetchall()
        print("Preview top OPS")
        for row in preview_rows:
            print(
                f"{row['team']}\t{row['player_name']}\tOPS={row['OPS']:.4f}\tHR={row['HR']}"
            )

    conn.close()


if __name__ == "__main__":
    main()
