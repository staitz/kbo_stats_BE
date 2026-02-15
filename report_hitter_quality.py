import argparse
import csv
import sqlite3
from typing import Any, Dict, List, Sequence, Tuple


TABLE_NAME = "hitter_game_logs"
DEFAULT_DB = "db.sqlite3"

DAILY_STAT_COLUMNS = [
    "PA",
    "AB",
    "H",
    "2B",
    "3B",
    "HR",
    "BB",
    "SO",
    "HBP",
    "SF",
    "TB",
    "RBI",
    "SB",
    "CS",
    "GDP",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="KBO hitter data quality report")
    parser.add_argument("--db", default=DEFAULT_DB, help="SQLite DB path (default: db.sqlite3)")
    parser.add_argument("--season", help="YYYY")
    parser.add_argument("--start", help="YYYYMMDD")
    parser.add_argument("--end", help="YYYYMMDD")
    parser.add_argument("--team", help="team exact match")
    parser.add_argument("--csv", help="optional CSV output path for daily aggregates")
    args = parser.parse_args()

    if args.season and (args.start or args.end):
        parser.error("--season cannot be used with --start/--end")

    if args.start and not args.end:
        args.end = args.start
    if args.end and not args.start:
        args.start = args.end

    if args.season:
        if not (len(args.season) == 4 and args.season.isdigit()):
            parser.error("--season must be YYYY")

    for label in ("start", "end"):
        value = getattr(args, label)
        if value and not (len(value) == 8 and value.isdigit()):
            parser.error(f"--{label} must be YYYYMMDD")

    if args.start and args.end and args.end < args.start:
        parser.error("--end must be >= --start")

    return args


def _qcol(col: str) -> str:
    return f'"{col}"'


def _col_expr(col: str, existing_cols: Sequence[str]) -> str:
    if col in existing_cols:
        return f"COALESCE({_qcol(col)}, 0)"
    return "0"


def compute_tb_adjusted(row: Dict[str, Any]) -> int:
    h = int(row.get("H", 0) or 0)
    d2 = int(row.get("2B", 0) or 0)
    d3 = int(row.get("3B", 0) or 0)
    hr = int(row.get("HR", 0) or 0)
    tb = int(row.get("TB", 0) or 0)
    if tb > 0:
        return tb
    singles = h - d2 - d3 - hr
    if singles < 0:
        singles = 0
    return singles + 2 * d2 + 3 * d3 + 4 * hr


def _tb_adjusted_expr(existing_cols: Sequence[str]) -> str:
    h = _col_expr("H", existing_cols)
    d2 = _col_expr("2B", existing_cols)
    d3 = _col_expr("3B", existing_cols)
    hr = _col_expr("HR", existing_cols)
    tb = _col_expr("TB", existing_cols)
    singles = f"(CASE WHEN ({h} - {d2} - {d3} - {hr}) > 0 THEN ({h} - {d2} - {d3} - {hr}) ELSE 0 END)"
    derived_tb = f"({singles} + 2*({d2}) + 3*({d3}) + 4*({hr}))"
    return f"(CASE WHEN {tb} > 0 THEN {tb} ELSE {derived_tb} END)"


def get_date_filter_clause(args: argparse.Namespace) -> Tuple[str, List[Any], str]:
    clauses: List[str] = []
    params: List[Any] = []
    label_parts: List[str] = []

    if args.season:
        clauses.append("game_date LIKE ?")
        params.append(f"{args.season}%")
        label_parts.append(f"season={args.season}")
    elif args.start and args.end:
        clauses.append("game_date >= ?")
        clauses.append("game_date <= ?")
        params.extend([args.start, args.end])
        label_parts.append(f"start={args.start}")
        label_parts.append(f"end={args.end}")
    else:
        label_parts.append("date=all")

    if args.team:
        clauses.append("team = ?")
        params.append(args.team)
        label_parts.append(f"team={args.team}")

    where_clause = " AND ".join(clauses) if clauses else "1=1"
    label = ", ".join(label_parts)
    return where_clause, params, label


def _fetch_existing_columns(conn: sqlite3.Connection, table_name: str) -> List[str]:
    rows = conn.execute(f"PRAGMA table_info({_qcol(table_name)})").fetchall()
    return [str(r[1]) for r in rows]


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table_name,),
    ).fetchone()
    return bool(row)


def fetch_daily_aggregates(
    conn: sqlite3.Connection,
    where_clause: str,
    params: Sequence[Any],
    existing_cols: Sequence[str],
) -> List[Dict[str, Any]]:
    stat_selects = [
        f"COALESCE(SUM({_col_expr(col, existing_cols)}), 0) AS {_qcol(col)}"
        for col in DAILY_STAT_COLUMNS
    ]
    query = f"""
    SELECT
        game_date,
        COUNT(DISTINCT game_id) AS games,
        COUNT(DISTINCT player_name) AS players,
        {", ".join(stat_selects)}
    FROM {TABLE_NAME}
    WHERE {where_clause}
    GROUP BY game_date
    ORDER BY game_date ASC
    """
    cur = conn.execute(query, list(params))
    cols = [d[0] for d in cur.description]
    out: List[Dict[str, Any]] = []
    for row in cur.fetchall():
        item = {cols[i]: row[i] for i in range(len(cols))}
        for c in DAILY_STAT_COLUMNS + ["games", "players"]:
            item[c] = int(item.get(c, 0) or 0)
        out.append(item)
    return out


def _fetch_period_summary(
    conn: sqlite3.Connection,
    where_clause: str,
    params: Sequence[Any],
    existing_cols: Sequence[str],
) -> Dict[str, Any]:
    tb_adj_expr = _tb_adjusted_expr(existing_cols)
    query = f"""
    SELECT
        COUNT(DISTINCT game_id) AS games,
        COUNT(DISTINCT player_name) AS players,
        COALESCE(SUM({_col_expr("PA", existing_cols)}), 0) AS PA,
        COALESCE(SUM({_col_expr("AB", existing_cols)}), 0) AS AB,
        COALESCE(SUM({_col_expr("H", existing_cols)}), 0) AS H,
        COALESCE(SUM({_col_expr("HR", existing_cols)}), 0) AS HR,
        COALESCE(SUM({_col_expr("BB", existing_cols)}), 0) AS BB,
        COALESCE(SUM({_col_expr("SO", existing_cols)}), 0) AS SO,
        COALESCE(SUM({_col_expr("HBP", existing_cols)}), 0) AS HBP,
        COALESCE(SUM({_col_expr("SF", existing_cols)}), 0) AS SF,
        COALESCE(SUM({tb_adj_expr}), 0) AS TB_adj
    FROM {TABLE_NAME}
    WHERE {where_clause}
    """
    row = conn.execute(query, list(params)).fetchone()
    if not row:
        return {
            "games": 0,
            "players": 0,
            "PA": 0,
            "AB": 0,
            "H": 0,
            "HR": 0,
            "BB": 0,
            "SO": 0,
            "HBP": 0,
            "SF": 0,
            "TB_adj": 0,
            "OBP_total": 0.0,
            "SLG_total": 0.0,
            "OPS_total": 0.0,
        }

    summary = {
        "games": int(row[0] or 0),
        "players": int(row[1] or 0),
        "PA": int(row[2] or 0),
        "AB": int(row[3] or 0),
        "H": int(row[4] or 0),
        "HR": int(row[5] or 0),
        "BB": int(row[6] or 0),
        "SO": int(row[7] or 0),
        "HBP": int(row[8] or 0),
        "SF": int(row[9] or 0),
        "TB_adj": int(row[10] or 0),
    }
    obp_den = summary["AB"] + summary["BB"] + summary["HBP"] + summary["SF"]
    summary["OBP_total"] = ((summary["H"] + summary["BB"] + summary["HBP"]) / obp_den) if obp_den > 0 else 0.0
    summary["SLG_total"] = (summary["TB_adj"] / summary["AB"]) if summary["AB"] > 0 else 0.0
    summary["OPS_total"] = summary["OBP_total"] + summary["SLG_total"]
    return summary


def _rule_configs(existing_cols: Sequence[str]) -> Dict[str, Dict[str, Any]]:
    pa = _col_expr("PA", existing_cols)
    ab = _col_expr("AB", existing_cols)
    h = _col_expr("H", existing_cols)
    d2 = _col_expr("2B", existing_cols)
    d3 = _col_expr("3B", existing_cols)
    hr = _col_expr("HR", existing_cols)
    bb = _col_expr("BB", existing_cols)
    so = _col_expr("SO", existing_cols)
    hbp = _col_expr("HBP", existing_cols)
    sf = _col_expr("SF", existing_cols)
    tb = _col_expr("TB", existing_cols)
    tb_adj = _tb_adjusted_expr(existing_cols)
    obp_den = f"({ab} + {bb} + {hbp} + {sf})"

    return {
        "PA_LT_AB": {
            "title": "PA < AB",
            "required": ["PA", "AB"],
            "condition": f"({pa}) < ({ab})",
        },
        "AB_LT_H": {
            "title": "AB < H",
            "required": ["AB", "H"],
            "condition": f"({ab}) < ({h})",
        },
        "XBH_GT_H": {
            "title": "(2B + 3B + HR) > H",
            "required": ["2B", "3B", "HR", "H"],
            "condition": f"(({d2}) + ({d3}) + ({hr})) > ({h})",
        },
        "TB_LT_H": {
            "title": "TB_adj < H",
            "required": ["TB", "H", "2B", "3B", "HR"],
            "condition": f"({tb_adj}) < ({h})",
        },
        "OBP_DEN_ZERO_PA_POS": {
            "title": "OBP denominator == 0 and PA > 0",
            "required": ["AB", "BB", "HBP", "SF", "PA"],
            "condition": f"({obp_den}) = 0 AND ({pa}) > 0",
        },
        "AB_ZERO_INCONSISTENT": {
            "title": "AB == 0 and (H>0 or TB_adj>0 or HR>0)",
            "required": ["AB", "H", "TB", "2B", "3B", "HR"],
            "condition": f"({ab}) = 0 AND (({h}) > 0 OR ({tb_adj}) > 0 OR ({hr}) > 0)",
        },
    }


def fetch_anomaly_rows(
    conn: sqlite3.Connection,
    rule_name: str,
    where_clause: str,
    params: Sequence[Any],
    existing_cols: Sequence[str],
    limit: int = 10,
) -> Dict[str, Any]:
    rules = _rule_configs(existing_cols)
    if rule_name not in rules:
        raise ValueError(f"unknown rule_name: {rule_name}")

    rule = rules[rule_name]
    missing_required = [c for c in rule["required"] if c not in existing_cols]
    if missing_required:
        return {
            "rule_name": rule_name,
            "title": rule["title"],
            "skipped": True,
            "skip_reason": f"missing columns: {', '.join(missing_required)}",
            "total": 0,
            "daily_counts": [],
            "samples": [],
        }

    condition = rule["condition"]
    where_with_rule = f"({where_clause}) AND ({condition})"

    total_query = f"SELECT COUNT(*) FROM {TABLE_NAME} WHERE {where_with_rule}"
    total = int(conn.execute(total_query, list(params)).fetchone()[0] or 0)

    daily_query = f"""
    SELECT game_date, COUNT(*) AS cnt
    FROM {TABLE_NAME}
    WHERE {where_with_rule}
    GROUP BY game_date
    ORDER BY game_date ASC
    """
    daily_rows = conn.execute(daily_query, list(params)).fetchall()
    daily_counts = [{"game_date": str(r[0]), "count": int(r[1] or 0)} for r in daily_rows]

    tb_adj = _tb_adjusted_expr(existing_cols)
    sample_query = f"""
    SELECT
        game_date,
        game_id,
        team,
        player_name,
        {_col_expr("PA", existing_cols)} AS PA,
        {_col_expr("AB", existing_cols)} AS AB,
        {_col_expr("H", existing_cols)} AS H,
        {_col_expr("2B", existing_cols)} AS {_qcol("2B")},
        {_col_expr("3B", existing_cols)} AS {_qcol("3B")},
        {_col_expr("HR", existing_cols)} AS HR,
        {_col_expr("TB", existing_cols)} AS TB,
        {tb_adj} AS TB_adj,
        {_col_expr("BB", existing_cols)} AS BB,
        {_col_expr("SO", existing_cols)} AS SO,
        {_col_expr("HBP", existing_cols)} AS HBP,
        {_col_expr("SF", existing_cols)} AS SF,
        {_col_expr("RBI", existing_cols)} AS RBI
    FROM {TABLE_NAME}
    WHERE {where_with_rule}
    ORDER BY game_date ASC, game_id ASC, team ASC, player_name ASC
    LIMIT ?
    """
    cur = conn.execute(sample_query, list(params) + [limit])
    cols = [d[0] for d in cur.description]
    samples: List[Dict[str, Any]] = []
    for row in cur.fetchall():
        item = {cols[i]: row[i] for i in range(len(cols))}
        for k in ("PA", "AB", "H", "2B", "3B", "HR", "TB", "TB_adj", "BB", "SO", "HBP", "SF", "RBI"):
            item[k] = int(item.get(k, 0) or 0)
        samples.append(item)

    return {
        "rule_name": rule_name,
        "title": rule["title"],
        "skipped": False,
        "skip_reason": "",
        "total": total,
        "daily_counts": daily_counts,
        "samples": samples,
    }


def print_section(title: str) -> None:
    line = "=" * len(title)
    print(line)
    print(title)
    print(line)


def print_table(rows: List[Dict[str, Any]], columns: List[str]) -> None:
    if not rows:
        print("(no rows)")
        return

    str_rows = []
    for row in rows:
        str_rows.append({col: str(row.get(col, "")) for col in columns})

    widths: Dict[str, int] = {}
    for col in columns:
        widths[col] = max(len(col), max(len(r[col]) for r in str_rows))

    header = " | ".join(col.ljust(widths[col]) for col in columns)
    sep = "-+-".join("-" * widths[col] for col in columns)
    print(header)
    print(sep)
    for row in str_rows:
        print(" | ".join(row[col].ljust(widths[col]) for col in columns))


def write_csv(path: str, rows: List[Dict[str, Any]], columns: List[str]) -> None:
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        for r in rows:
            writer.writerow({c: r.get(c, 0) for c in columns})


def main() -> None:
    args = parse_args()
    conn = sqlite3.connect(args.db)
    try:
        if not _table_exists(conn, TABLE_NAME):
            print(f"[error] table not found: {TABLE_NAME}")
            return

        existing_cols = _fetch_existing_columns(conn, TABLE_NAME)
        where_clause, params, filter_label = get_date_filter_clause(args)
        missing_for_daily = [c for c in DAILY_STAT_COLUMNS if c not in existing_cols]

        daily_rows = fetch_daily_aggregates(conn, where_clause, params, existing_cols)
        summary = _fetch_period_summary(conn, where_clause, params, existing_cols)

        print_section("Summary")
        print(f"filter: {filter_label}")
        print(f"db: {args.db}")
        if missing_for_daily:
            print(f"[warn] missing columns treated as 0 in daily aggregates: {', '.join(missing_for_daily)}")
        print(
            f"games={summary['games']} players={summary['players']} "
            f"PA={summary['PA']} AB={summary['AB']} H={summary['H']} "
            f"HR={summary['HR']} BB={summary['BB']} SO={summary['SO']}"
        )
        print(
            f"OBP_total={summary['OBP_total']:.3f} "
            f"SLG_total={summary['SLG_total']:.3f} "
            f"OPS_total={summary['OPS_total']:.3f}"
        )

        print_section("Daily Aggregates")
        daily_columns = ["game_date", "games", "players"] + DAILY_STAT_COLUMNS
        print_table(daily_rows, daily_columns)
        if args.csv:
            write_csv(args.csv, daily_rows, daily_columns)
            print(f"[done] csv written: {args.csv}")

        print_section("Anomalies")
        rule_order = [
            "PA_LT_AB",
            "AB_LT_H",
            "XBH_GT_H",
            "TB_LT_H",
            "OBP_DEN_ZERO_PA_POS",
            "AB_ZERO_INCONSISTENT",
        ]
        for rule_name in rule_order:
            result = fetch_anomaly_rows(conn, rule_name, where_clause, params, existing_cols, limit=10)
            print(f"[rule] {result['title']}")
            if result["skipped"]:
                print(f"  skipped: {result['skip_reason']}")
                continue
            print(f"  total_violations: {result['total']}")
            print("  daily_counts:")
            print_table(result["daily_counts"], ["game_date", "count"])
            print("  sample_rows(top 10):")
            print_table(
                result["samples"],
                [
                    "game_date",
                    "game_id",
                    "team",
                    "player_name",
                    "PA",
                    "AB",
                    "H",
                    "2B",
                    "3B",
                    "HR",
                    "TB",
                    "TB_adj",
                    "BB",
                    "SO",
                    "HBP",
                    "SF",
                    "RBI",
                ],
            )
            print("")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
