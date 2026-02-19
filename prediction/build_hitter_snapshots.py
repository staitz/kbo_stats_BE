"""
Build hitter daily snapshot features from hitter_game_logs.

Examples:
  python -m prediction.build_hitter_snapshots --season 2025 --start 20250601 --end 20250630 --upsert
  python -m prediction.build_hitter_snapshots --season 2025 --as-of 20250610 --upsert --preview 5
"""

import argparse
import datetime as dt
import sqlite3
from collections import defaultdict, deque
from typing import Any, Deque, Dict, List, Sequence, Tuple


SOURCE_TABLE = "hitter_game_logs"
SNAPSHOT_TABLE = "hitter_daily_snapshots"
DEFAULT_DB = "db.sqlite3"


COUNT_COLS = [
    "PA",
    "AB",
    "H",
    "2B",
    "3B",
    "HR",
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

SNAPSHOT_COUNT_COLS = [
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
]

ROLLING_BASE_COLS = ["PA", "AB", "H", "HR", "BB", "SO", "TB_adj", "HBP", "SF"]


def _qcol(col: str) -> str:
    return f'"{col}"'


def _parse_yyyymmdd(value: str) -> dt.date:
    return dt.datetime.strptime(value, "%Y%m%d").date()


def _fmt_yyyymmdd(value: dt.date) -> str:
    return value.strftime("%Y%m%d")


def _iter_dates(start: str, end: str) -> List[str]:
    s = _parse_yyyymmdd(start)
    e = _parse_yyyymmdd(end)
    if e < s:
        raise ValueError("end must be >= start")
    out: List[str] = []
    cur = s
    while cur <= e:
        out.append(_fmt_yyyymmdd(cur))
        cur += dt.timedelta(days=1)
    return out


def _safe_int(value: Any) -> int:
    if value is None:
        return 0
    try:
        return int(value)
    except Exception:
        return 0


def _calc_rates(h: int, bb: int, hbp: int, sf: int, tb_adj: int, ab: int) -> Tuple[float, float, float, float]:
    avg = (h / ab) if ab > 0 else 0.0
    obp_den = ab + bb + hbp + sf
    obp = ((h + bb + hbp) / obp_den) if obp_den > 0 else 0.0
    slg = (tb_adj / ab) if ab > 0 else 0.0
    ops = obp + slg
    return avg, obp, slg, ops


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build hitter daily snapshots")
    parser.add_argument("--db", default=DEFAULT_DB, help="SQLite DB path (default: db.sqlite3)")
    parser.add_argument("--season", required=True, help="YYYY")
    parser.add_argument("--start", help="YYYYMMDD")
    parser.add_argument("--end", help="YYYYMMDD")
    parser.add_argument("--as-of", dest="as_of", help="YYYYMMDD")
    parser.add_argument("--team", help="team exact match")
    parser.add_argument("--upsert", action="store_true", help="upsert snapshot rows")
    parser.add_argument("--preview", type=int, default=0, help="preview top N OPS rows for single as_of")
    args = parser.parse_args()

    if not (len(args.season) == 4 and args.season.isdigit()):
        parser.error("--season must be YYYY")

    if args.as_of and (args.start or args.end):
        parser.error("--as-of cannot be used with --start/--end")

    for label in ("start", "end", "as_of"):
        value = getattr(args, label)
        if value and not (len(value) == 8 and value.isdigit()):
            parser.error(f"--{label.replace('_', '-')} must be YYYYMMDD")

    if args.start and not args.end:
        args.end = args.start
    if args.end and not args.start:
        args.start = args.end
    if args.start and args.end and args.end < args.start:
        parser.error("--end must be >= --start")

    return args


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table_name,),
    ).fetchone()
    return bool(row)


def _fetch_columns(conn: sqlite3.Connection, table_name: str) -> List[str]:
    rows = conn.execute(f"PRAGMA table_info({_qcol(table_name)})").fetchall()
    return [str(r[1]) for r in rows]


def _src_col_expr(col: str, existing_cols: Sequence[str]) -> str:
    if col in existing_cols:
        return f"COALESCE({_qcol(col)}, 0)"
    return "0"


def _tb_adj_row_expr(existing_cols: Sequence[str]) -> str:
    h = _src_col_expr("H", existing_cols)
    d2 = _src_col_expr("2B", existing_cols)
    d3 = _src_col_expr("3B", existing_cols)
    hr = _src_col_expr("HR", existing_cols)
    tb = _src_col_expr("TB", existing_cols)
    singles = f"(CASE WHEN ({h} - {d2} - {d3} - {hr}) > 0 THEN ({h} - {d2} - {d3} - {hr}) ELSE 0 END)"
    derived_tb = f"({singles} + 2*({d2}) + 3*({d3}) + 4*({hr}))"
    return f"(CASE WHEN {tb} > 0 THEN {tb} ELSE {derived_tb} END)"


def ensure_snapshot_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {SNAPSHOT_TABLE} (
            season INTEGER NOT NULL,
            as_of_date TEXT NOT NULL,
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
            PA_7 INTEGER NOT NULL DEFAULT 0,
            AB_7 INTEGER NOT NULL DEFAULT 0,
            H_7 INTEGER NOT NULL DEFAULT 0,
            HR_7 INTEGER NOT NULL DEFAULT 0,
            BB_7 INTEGER NOT NULL DEFAULT 0,
            SO_7 INTEGER NOT NULL DEFAULT 0,
            TB_adj_7 INTEGER NOT NULL DEFAULT 0,
            OPS_7 REAL NOT NULL DEFAULT 0,
            PA_14 INTEGER NOT NULL DEFAULT 0,
            AB_14 INTEGER NOT NULL DEFAULT 0,
            H_14 INTEGER NOT NULL DEFAULT 0,
            HR_14 INTEGER NOT NULL DEFAULT 0,
            BB_14 INTEGER NOT NULL DEFAULT 0,
            SO_14 INTEGER NOT NULL DEFAULT 0,
            TB_adj_14 INTEGER NOT NULL DEFAULT 0,
            OPS_14 REAL NOT NULL DEFAULT 0
        )
        """
    )

    required = {
        "season": "INTEGER NOT NULL DEFAULT 0",
        "as_of_date": "TEXT NOT NULL DEFAULT ''",
        "team": "TEXT NOT NULL DEFAULT ''",
        "player_name": "TEXT NOT NULL DEFAULT ''",
        "games": "INTEGER NOT NULL DEFAULT 0",
        "PA": "INTEGER NOT NULL DEFAULT 0",
        "AB": "INTEGER NOT NULL DEFAULT 0",
        "H": "INTEGER NOT NULL DEFAULT 0",
        "2B": "INTEGER NOT NULL DEFAULT 0",
        "3B": "INTEGER NOT NULL DEFAULT 0",
        "HR": "INTEGER NOT NULL DEFAULT 0",
        "TB_adj": "INTEGER NOT NULL DEFAULT 0",
        "RBI": "INTEGER NOT NULL DEFAULT 0",
        "BB": "INTEGER NOT NULL DEFAULT 0",
        "SO": "INTEGER NOT NULL DEFAULT 0",
        "HBP": "INTEGER NOT NULL DEFAULT 0",
        "SH": "INTEGER NOT NULL DEFAULT 0",
        "SF": "INTEGER NOT NULL DEFAULT 0",
        "SB": "INTEGER NOT NULL DEFAULT 0",
        "CS": "INTEGER NOT NULL DEFAULT 0",
        "GDP": "INTEGER NOT NULL DEFAULT 0",
        "AVG": "REAL NOT NULL DEFAULT 0",
        "OBP": "REAL NOT NULL DEFAULT 0",
        "SLG": "REAL NOT NULL DEFAULT 0",
        "OPS": "REAL NOT NULL DEFAULT 0",
        "PA_7": "INTEGER NOT NULL DEFAULT 0",
        "AB_7": "INTEGER NOT NULL DEFAULT 0",
        "H_7": "INTEGER NOT NULL DEFAULT 0",
        "HR_7": "INTEGER NOT NULL DEFAULT 0",
        "BB_7": "INTEGER NOT NULL DEFAULT 0",
        "SO_7": "INTEGER NOT NULL DEFAULT 0",
        "TB_adj_7": "INTEGER NOT NULL DEFAULT 0",
        "OPS_7": "REAL NOT NULL DEFAULT 0",
        "PA_14": "INTEGER NOT NULL DEFAULT 0",
        "AB_14": "INTEGER NOT NULL DEFAULT 0",
        "H_14": "INTEGER NOT NULL DEFAULT 0",
        "HR_14": "INTEGER NOT NULL DEFAULT 0",
        "BB_14": "INTEGER NOT NULL DEFAULT 0",
        "SO_14": "INTEGER NOT NULL DEFAULT 0",
        "TB_adj_14": "INTEGER NOT NULL DEFAULT 0",
        "OPS_14": "REAL NOT NULL DEFAULT 0",
    }
    existing = set(_fetch_columns(conn, SNAPSHOT_TABLE))
    for col, col_def in required.items():
        if col in existing:
            continue
        safe = _qcol(col) if col[0].isdigit() else col
        conn.execute(f"ALTER TABLE {SNAPSHOT_TABLE} ADD COLUMN {safe} {col_def}")

    conn.execute(
        f"""
        CREATE UNIQUE INDEX IF NOT EXISTS ux_{SNAPSHOT_TABLE}_key
        ON {SNAPSHOT_TABLE}(season, as_of_date, team, player_name)
        """
    )
    conn.commit()


def resolve_as_of_dates(conn: sqlite3.Connection, args: argparse.Namespace) -> List[str]:
    if args.as_of:
        return [args.as_of]

    if args.start and args.end:
        return _iter_dates(args.start, args.end)

    season_prefix = f"{args.season}%"
    params: List[Any] = [season_prefix]
    where = "game_date LIKE ?"
    if args.team:
        where += " AND team = ?"
        params.append(args.team)
    query = f"""
    SELECT DISTINCT game_date
    FROM {SOURCE_TABLE}
    WHERE {where}
    ORDER BY game_date ASC
    """
    return [str(r[0]) for r in conn.execute(query, params).fetchall()]


def fetch_daily_player_aggregates(
    conn: sqlite3.Connection,
    season: str,
    end_date: str,
    team: str,
    existing_cols: Sequence[str],
) -> Dict[str, Dict[Tuple[str, str], Dict[str, int]]]:
    params: List[Any] = [f"{season}0101", end_date]
    where = "game_date >= ? AND game_date <= ?"
    if team:
        where += " AND team = ?"
        params.append(team)

    sum_exprs = [f"COALESCE(SUM({_src_col_expr(col, existing_cols)}), 0) AS {_qcol(col)}" for col in COUNT_COLS]
    tb_adj_expr = _tb_adj_row_expr(existing_cols)
    query = f"""
    SELECT
        game_date,
        team,
        player_name,
        COUNT(DISTINCT game_id) AS games,
        {", ".join(sum_exprs)},
        COALESCE(SUM({tb_adj_expr}), 0) AS TB_adj
    FROM {SOURCE_TABLE}
    WHERE {where}
    GROUP BY game_date, team, player_name
    ORDER BY game_date ASC, team ASC, player_name ASC
    """

    cur = conn.execute(query, params)
    cols = [d[0] for d in cur.description]
    out: Dict[str, Dict[Tuple[str, str], Dict[str, int]]] = defaultdict(dict)
    for row in cur.fetchall():
        data = {cols[i]: row[i] for i in range(len(cols))}
        game_date = str(data["game_date"])
        key = (str(data["team"]), str(data["player_name"]))
        stat = {"games": _safe_int(data.get("games", 0))}
        for col in COUNT_COLS:
            stat[col] = _safe_int(data.get(col, 0))
        stat["TB_adj"] = _safe_int(data.get("TB_adj", 0))
        out[game_date][key] = stat
    return out


def _empty_count_dict() -> Dict[str, int]:
    return {c: 0 for c in SNAPSHOT_COUNT_COLS}


def _empty_roll_dict() -> Dict[str, int]:
    return {c: 0 for c in ROLLING_BASE_COLS}


def _state_add_roll(target: Dict[str, int], day_stat: Dict[str, int]) -> None:
    for col in ROLLING_BASE_COLS:
        target[col] += int(day_stat.get(col, 0))


def _state_sub_roll(target: Dict[str, int], day_stat: Dict[str, int]) -> None:
    for col in ROLLING_BASE_COLS:
        target[col] -= int(day_stat.get(col, 0))


def _build_snapshot_rows_for_date(
    season: int,
    as_of: str,
    states: Dict[Tuple[str, str], Dict[str, Any]],
) -> List[Tuple[Any, ...]]:
    out: List[Tuple[Any, ...]] = []
    for (team, player_name), state in states.items():
        cum = state["cum"]
        if cum["games"] <= 0:
            continue

        avg, obp, slg, ops = _calc_rates(
            h=cum["H"],
            bb=cum["BB"],
            hbp=cum["HBP"],
            sf=cum["SF"],
            tb_adj=cum["TB_adj"],
            ab=cum["AB"],
        )

        r7 = state["roll7_sum"]
        _, obp7, slg7, ops7 = _calc_rates(
            h=r7["H"],
            bb=r7["BB"],
            hbp=r7["HBP"],
            sf=r7["SF"],
            tb_adj=r7["TB_adj"],
            ab=r7["AB"],
        )

        r14 = state["roll14_sum"]
        _, obp14, slg14, ops14 = _calc_rates(
            h=r14["H"],
            bb=r14["BB"],
            hbp=r14["HBP"],
            sf=r14["SF"],
            tb_adj=r14["TB_adj"],
            ab=r14["AB"],
        )

        out.append(
            (
                season,
                as_of,
                team,
                player_name,
                cum["games"],
                cum["PA"],
                cum["AB"],
                cum["H"],
                cum["2B"],
                cum["3B"],
                cum["HR"],
                cum["TB_adj"],
                cum["RBI"],
                cum["BB"],
                cum["SO"],
                cum["HBP"],
                cum["SF"],
                cum["SB"],
                cum["CS"],
                cum["GDP"],
                round(avg, 6),
                round(obp, 6),
                round(slg, 6),
                round(ops, 6),
                r7["PA"],
                r7["AB"],
                r7["H"],
                r7["HR"],
                r7["BB"],
                r7["SO"],
                r7["TB_adj"],
                round(ops7, 6),
                r14["PA"],
                r14["AB"],
                r14["H"],
                r14["HR"],
                r14["BB"],
                r14["SO"],
                r14["TB_adj"],
                round(ops14, 6),
            )
        )
    return out


def _upsert_snapshot_rows(conn: sqlite3.Connection, rows: List[Tuple[Any, ...]], upsert: bool) -> int:
    if not rows:
        return 0

    cols = [
        "season",
        "as_of_date",
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
        "SF",
        "SB",
        "CS",
        "GDP",
        "AVG",
        "OBP",
        "SLG",
        "OPS",
        "PA_7",
        "AB_7",
        "H_7",
        "HR_7",
        "BB_7",
        "SO_7",
        "TB_adj_7",
        "OPS_7",
        "PA_14",
        "AB_14",
        "H_14",
        "HR_14",
        "BB_14",
        "SO_14",
        "TB_adj_14",
        "OPS_14",
    ]
    col_sql = ", ".join(_qcol(c) if c[0].isdigit() else c for c in cols)
    placeholders = ", ".join(["?"] * len(cols))

    before = conn.total_changes
    cur = conn.cursor()
    if not upsert:
        cur.executemany(
            f"""
            INSERT OR IGNORE INTO {SNAPSHOT_TABLE} ({col_sql})
            VALUES ({placeholders})
            """,
            rows,
        )
        conn.commit()
        return conn.total_changes - before

    update_cols = [c for c in cols if c not in ("season", "as_of_date", "team", "player_name")]
    set_parts = []
    for c in update_cols:
        lhs = _qcol(c) if c[0].isdigit() else c
        rhs = f"excluded.{_qcol(c)}" if c[0].isdigit() else f"excluded.{c}"
        set_parts.append(f"{lhs}={rhs}")
    set_sql = ", ".join(set_parts)
    cur.executemany(
        f"""
        INSERT INTO {SNAPSHOT_TABLE} ({col_sql})
        VALUES ({placeholders})
        ON CONFLICT(season, as_of_date, team, player_name) DO UPDATE SET
            {set_sql}
        """,
        rows,
    )
    conn.commit()
    return conn.total_changes - before


def build_snapshots(conn: sqlite3.Connection, args: argparse.Namespace) -> Tuple[int, int]:
    if not _table_exists(conn, SOURCE_TABLE):
        raise RuntimeError(f"source table not found: {SOURCE_TABLE}")

    src_cols = _fetch_columns(conn, SOURCE_TABLE)
    missing = [c for c in COUNT_COLS + ["TB"] if c not in src_cols]
    if missing:
        print(f"[warn] missing source columns treated as 0: {', '.join(missing)}")

    as_of_dates = resolve_as_of_dates(conn, args)
    if not as_of_dates:
        print("[warn] no as_of dates to process")
        return 0, 0

    season = int(args.season)
    max_as_of = max(as_of_dates)
    daily_map = fetch_daily_player_aggregates(
        conn=conn,
        season=args.season,
        end_date=max_as_of,
        team=args.team,
        existing_cols=src_cols,
    )

    states: Dict[Tuple[str, str], Dict[str, Any]] = {}
    total_written = 0
    processed_days = 0

    as_of_dates_sorted = sorted(as_of_dates)
    for as_of in as_of_dates_sorted:
        day_updates = daily_map.get(as_of, {})
        for key, day_stat in day_updates.items():
            if key not in states:
                states[key] = {
                    "cum": _empty_count_dict(),
                    "roll7_q": deque(),   # type: Deque[Tuple[str, Dict[str, int]]]
                    "roll14_q": deque(),  # type: Deque[Tuple[str, Dict[str, int]]]
                    "roll7_sum": _empty_roll_dict(),
                    "roll14_sum": _empty_roll_dict(),
                }

            state = states[key]
            cum = state["cum"]
            for c in SNAPSHOT_COUNT_COLS:
                cum[c] += int(day_stat.get(c, 0))

            state["roll7_q"].append((as_of, day_stat))
            _state_add_roll(state["roll7_sum"], day_stat)
            state["roll14_q"].append((as_of, day_stat))
            _state_add_roll(state["roll14_sum"], day_stat)

        d_as_of = _parse_yyyymmdd(as_of)
        min7 = _fmt_yyyymmdd(d_as_of - dt.timedelta(days=6))
        min14 = _fmt_yyyymmdd(d_as_of - dt.timedelta(days=13))

        for state in states.values():
            q7: Deque[Tuple[str, Dict[str, int]]] = state["roll7_q"]
            while q7 and q7[0][0] < min7:
                _, old = q7.popleft()
                _state_sub_roll(state["roll7_sum"], old)

            q14: Deque[Tuple[str, Dict[str, int]]] = state["roll14_q"]
            while q14 and q14[0][0] < min14:
                _, old = q14.popleft()
                _state_sub_roll(state["roll14_sum"], old)

        rows = _build_snapshot_rows_for_date(season=season, as_of=as_of, states=states)
        written = _upsert_snapshot_rows(conn, rows, upsert=args.upsert)
        total_written += written
        processed_days += 1
        print(f"[ok] as_of={as_of} rows={len(rows)} written={written}")

    print(f"[done] processed_days={processed_days} total_written={total_written}")
    return processed_days, total_written


def preview_top_ops(conn: sqlite3.Connection, season: int, as_of: str, limit: int, team: str) -> None:
    if limit <= 0:
        return
    params: List[Any] = [season, as_of]
    where = "season = ? AND as_of_date = ?"
    if team:
        where += " AND team = ?"
        params.append(team)
    query = f"""
    SELECT team, player_name, games, PA, AB, H, HR, OPS, OPS_7, OPS_14
    FROM {SNAPSHOT_TABLE}
    WHERE {where}
    ORDER BY OPS DESC, PA DESC, AB DESC, H DESC, player_name ASC
    LIMIT ?
    """
    params.append(limit)
    rows = conn.execute(query, params).fetchall()
    print(f"[preview] season={season} as_of={as_of} top={limit}")
    if not rows:
        print("(no rows)")
        return
    print("team | player_name | games | PA | AB | H | HR | OPS | OPS_7 | OPS_14")
    for r in rows:
        print(
            f"{r[0]} | {r[1]} | {int(r[2] or 0)} | {int(r[3] or 0)} | {int(r[4] or 0)} | "
            f"{int(r[5] or 0)} | {int(r[6] or 0)} | {float(r[7] or 0):.3f} | "
            f"{float(r[8] or 0):.3f} | {float(r[9] or 0):.3f}"
        )


def main() -> None:
    args = parse_args()
    conn = sqlite3.connect(args.db)
    try:
        ensure_snapshot_table(conn)
        build_snapshots(conn, args)

        if args.preview > 0:
            if args.as_of:
                preview_date = args.as_of
            elif args.end:
                preview_date = args.end
            elif args.start:
                preview_date = args.start
            else:
                dates = resolve_as_of_dates(conn, args)
                preview_date = dates[-1] if dates else ""
            if preview_date:
                preview_top_ops(conn, int(args.season), preview_date, args.preview, args.team)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
