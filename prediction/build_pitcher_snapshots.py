"""
Build pitcher daily snapshot features from pitcher_game_logs.

Mirrors the hitter_daily_snapshots pattern: for each as_of_date, produces
one row per (team, player_name) with season-to-date cumulative stats and
7-day / 14-day rolling windows.

Examples:
  python -m prediction.build_pitcher_snapshots --season 2026 --upsert
  python -m prediction.build_pitcher_snapshots --season 2026 --start 20260328 --end 20260412 --upsert
  python -m prediction.build_pitcher_snapshots --season 2026 --as-of 20260410 --upsert --preview 5
"""

import argparse
import datetime as dt
from collections import defaultdict, deque
from typing import Any, Deque, Dict, List, Sequence, Tuple

from db_support import connect_for_path, execute, executemany, is_postgres, row_value, table_columns, table_exists

SOURCE_TABLE = "pitcher_game_logs"
SNAPSHOT_TABLE = "pitcher_daily_snapshots"
DEFAULT_DB = "db.sqlite3"

# Columns summed from game logs
COUNT_COLS = [
    "OUTS",
    "W",
    "L",
    "SV",
    "HLD",
    "H",
    "ER",
    "BB",
    "SO",
    "HR",
    "HBP",
    "BK",
    "WP",
]

# Columns stored in the snapshot (cumulative)
SNAPSHOT_COUNT_COLS = [
    "games",
    "OUTS",
    "W",
    "L",
    "SV",
    "HLD",
    "H",
    "ER",
    "BB",
    "SO",
    "HR",
    "HBP",
    "BK",
    "WP",
]

# Columns tracked for 7d/14d rolling windows
ROLLING_BASE_COLS = ["OUTS", "ER", "BB", "SO", "HR", "HBP"]


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


def _calc_rates(
    outs: int, er: int, h: int, bb: int, so: int, hr: int, hbp: int,
) -> Tuple[float, float, float, float, float]:
    """Return (IP, ERA, WHIP, K9, BB9, KBB)."""
    ip = outs / 3.0 if outs > 0 else 0.0
    era = (er * 9.0 / ip) if ip > 0 else 0.0
    whip = ((bb + h) / ip) if ip > 0 else 0.0
    k9 = (so * 9.0 / ip) if ip > 0 else 0.0
    bb9 = (bb * 9.0 / ip) if ip > 0 else 0.0
    kbb = (so / bb) if bb > 0 else 0.0
    return ip, era, whip, k9, bb9, kbb


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build pitcher daily snapshots")
    parser.add_argument("--db", default=DEFAULT_DB, help="SQLite DB path (default: db.sqlite3)")
    parser.add_argument("--season", required=True, help="YYYY")
    parser.add_argument("--start", help="YYYYMMDD")
    parser.add_argument("--end", help="YYYYMMDD")
    parser.add_argument("--as-of", dest="as_of", help="YYYYMMDD")
    parser.add_argument("--team", help="team exact match")
    parser.add_argument("--upsert", action="store_true", help="upsert snapshot rows")
    parser.add_argument("--preview", type=int, default=0, help="preview top N ERA rows for single as_of")
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


def _table_exists(conn, table_name: str) -> bool:
    return table_exists(conn, table_name)


def _fetch_columns(conn, table_name: str) -> List[str]:
    return table_columns(conn, table_name)


def _lookup_col_name(col: str, existing_cols: Sequence[str]) -> str | None:
    wanted = col.lower()
    for existing in existing_cols:
        if str(existing).lower() == wanted:
            return str(existing)
    return None


def _src_col_expr(col: str, existing_cols: Sequence[str]) -> str:
    matched = _lookup_col_name(col, existing_cols)
    if matched:
        return f"COALESCE({_qcol(matched)}, 0)"
    return "0"


def _drop_legacy_war_column(conn) -> None:
    existing = {str(col).lower() for col in _fetch_columns(conn, SNAPSHOT_TABLE)}
    if "war" not in existing:
        return

    if is_postgres(conn):
        conn.execute(f"ALTER TABLE {SNAPSHOT_TABLE} DROP COLUMN IF EXISTS WAR")
        conn.commit()
        return

    conn.execute(f"ALTER TABLE {SNAPSHOT_TABLE} RENAME TO {SNAPSHOT_TABLE}__legacy")
    conn.execute(
        f"""
        CREATE TABLE {SNAPSHOT_TABLE} (
            season INTEGER NOT NULL,
            as_of_date TEXT NOT NULL,
            team TEXT NOT NULL,
            player_name TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT '',
            games INTEGER NOT NULL DEFAULT 0,
            OUTS INTEGER NOT NULL DEFAULT 0,
            IP REAL NOT NULL DEFAULT 0,
            W INTEGER NOT NULL DEFAULT 0,
            L INTEGER NOT NULL DEFAULT 0,
            SV INTEGER NOT NULL DEFAULT 0,
            HLD INTEGER NOT NULL DEFAULT 0,
            H INTEGER NOT NULL DEFAULT 0,
            ER INTEGER NOT NULL DEFAULT 0,
            BB INTEGER NOT NULL DEFAULT 0,
            SO INTEGER NOT NULL DEFAULT 0,
            HR INTEGER NOT NULL DEFAULT 0,
            HBP INTEGER NOT NULL DEFAULT 0,
            BK INTEGER NOT NULL DEFAULT 0,
            WP INTEGER NOT NULL DEFAULT 0,
            ERA REAL NOT NULL DEFAULT 0,
            WHIP REAL NOT NULL DEFAULT 0,
            K9 REAL NOT NULL DEFAULT 0,
            BB9 REAL NOT NULL DEFAULT 0,
            KBB REAL NOT NULL DEFAULT 0,
            OUTS_7 INTEGER NOT NULL DEFAULT 0,
            ER_7 INTEGER NOT NULL DEFAULT 0,
            BB_7 INTEGER NOT NULL DEFAULT 0,
            SO_7 INTEGER NOT NULL DEFAULT 0,
            HR_7 INTEGER NOT NULL DEFAULT 0,
            HBP_7 INTEGER NOT NULL DEFAULT 0,
            ERA_7 REAL NOT NULL DEFAULT 0,
            OUTS_14 INTEGER NOT NULL DEFAULT 0,
            ER_14 INTEGER NOT NULL DEFAULT 0,
            BB_14 INTEGER NOT NULL DEFAULT 0,
            SO_14 INTEGER NOT NULL DEFAULT 0,
            HR_14 INTEGER NOT NULL DEFAULT 0,
            HBP_14 INTEGER NOT NULL DEFAULT 0,
            ERA_14 REAL NOT NULL DEFAULT 0
        )
        """
    )
    conn.execute(
        f"""
        INSERT INTO {SNAPSHOT_TABLE} (
            season, as_of_date, team, player_name, role, games, OUTS, IP, W, L, SV, HLD,
            H, ER, BB, SO, HR, HBP, BK, WP, ERA, WHIP, K9, BB9, KBB,
            OUTS_7, ER_7, BB_7, SO_7, HR_7, HBP_7, ERA_7,
            OUTS_14, ER_14, BB_14, SO_14, HR_14, HBP_14, ERA_14
        )
        SELECT
            season, as_of_date, team, player_name, role, games, OUTS, IP, W, L, SV, HLD,
            H, ER, BB, SO, HR, HBP, BK, WP, ERA, WHIP, K9, BB9, KBB,
            OUTS_7, ER_7, BB_7, SO_7, HR_7, HBP_7, ERA_7,
            OUTS_14, ER_14, BB_14, SO_14, HR_14, HBP_14, ERA_14
        FROM {SNAPSHOT_TABLE}__legacy
        """
    )
    conn.execute(f"DROP TABLE {SNAPSHOT_TABLE}__legacy")
    conn.commit()


def ensure_snapshot_table(conn) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {SNAPSHOT_TABLE} (
            season INTEGER NOT NULL,
            as_of_date TEXT NOT NULL,
            team TEXT NOT NULL,
            player_name TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT '',
            games INTEGER NOT NULL DEFAULT 0,
            OUTS INTEGER NOT NULL DEFAULT 0,
            IP REAL NOT NULL DEFAULT 0,
            W INTEGER NOT NULL DEFAULT 0,
            L INTEGER NOT NULL DEFAULT 0,
            SV INTEGER NOT NULL DEFAULT 0,
            HLD INTEGER NOT NULL DEFAULT 0,
            H INTEGER NOT NULL DEFAULT 0,
            ER INTEGER NOT NULL DEFAULT 0,
            BB INTEGER NOT NULL DEFAULT 0,
            SO INTEGER NOT NULL DEFAULT 0,
            HR INTEGER NOT NULL DEFAULT 0,
            HBP INTEGER NOT NULL DEFAULT 0,
            BK INTEGER NOT NULL DEFAULT 0,
            WP INTEGER NOT NULL DEFAULT 0,
            ERA REAL NOT NULL DEFAULT 0,
            WHIP REAL NOT NULL DEFAULT 0,
            K9 REAL NOT NULL DEFAULT 0,
            BB9 REAL NOT NULL DEFAULT 0,
            KBB REAL NOT NULL DEFAULT 0,
            OUTS_7 INTEGER NOT NULL DEFAULT 0,
            ER_7 INTEGER NOT NULL DEFAULT 0,
            BB_7 INTEGER NOT NULL DEFAULT 0,
            SO_7 INTEGER NOT NULL DEFAULT 0,
            HR_7 INTEGER NOT NULL DEFAULT 0,
            HBP_7 INTEGER NOT NULL DEFAULT 0,
            ERA_7 REAL NOT NULL DEFAULT 0,
            OUTS_14 INTEGER NOT NULL DEFAULT 0,
            ER_14 INTEGER NOT NULL DEFAULT 0,
            BB_14 INTEGER NOT NULL DEFAULT 0,
            SO_14 INTEGER NOT NULL DEFAULT 0,
            HR_14 INTEGER NOT NULL DEFAULT 0,
            HBP_14 INTEGER NOT NULL DEFAULT 0,
            ERA_14 REAL NOT NULL DEFAULT 0
        )
        """
    )
    _drop_legacy_war_column(conn)

    required = {
        "season": "INTEGER NOT NULL DEFAULT 0",
        "as_of_date": "TEXT NOT NULL DEFAULT ''",
        "team": "TEXT NOT NULL DEFAULT ''",
        "player_name": "TEXT NOT NULL DEFAULT ''",
        "role": "TEXT NOT NULL DEFAULT ''",
        "games": "INTEGER NOT NULL DEFAULT 0",
        "OUTS": "INTEGER NOT NULL DEFAULT 0",
        "IP": "REAL NOT NULL DEFAULT 0",
        "W": "INTEGER NOT NULL DEFAULT 0",
        "L": "INTEGER NOT NULL DEFAULT 0",
        "SV": "INTEGER NOT NULL DEFAULT 0",
        "HLD": "INTEGER NOT NULL DEFAULT 0",
        "H": "INTEGER NOT NULL DEFAULT 0",
        "ER": "INTEGER NOT NULL DEFAULT 0",
        "BB": "INTEGER NOT NULL DEFAULT 0",
        "SO": "INTEGER NOT NULL DEFAULT 0",
        "HR": "INTEGER NOT NULL DEFAULT 0",
        "HBP": "INTEGER NOT NULL DEFAULT 0",
        "BK": "INTEGER NOT NULL DEFAULT 0",
        "WP": "INTEGER NOT NULL DEFAULT 0",
        "ERA": "REAL NOT NULL DEFAULT 0",
        "WHIP": "REAL NOT NULL DEFAULT 0",
        "K9": "REAL NOT NULL DEFAULT 0",
        "BB9": "REAL NOT NULL DEFAULT 0",
        "KBB": "REAL NOT NULL DEFAULT 0",
        "OUTS_7": "INTEGER NOT NULL DEFAULT 0",
        "ER_7": "INTEGER NOT NULL DEFAULT 0",
        "BB_7": "INTEGER NOT NULL DEFAULT 0",
        "SO_7": "INTEGER NOT NULL DEFAULT 0",
        "HR_7": "INTEGER NOT NULL DEFAULT 0",
        "HBP_7": "INTEGER NOT NULL DEFAULT 0",
        "ERA_7": "REAL NOT NULL DEFAULT 0",
        "OUTS_14": "INTEGER NOT NULL DEFAULT 0",
        "ER_14": "INTEGER NOT NULL DEFAULT 0",
        "BB_14": "INTEGER NOT NULL DEFAULT 0",
        "SO_14": "INTEGER NOT NULL DEFAULT 0",
        "HR_14": "INTEGER NOT NULL DEFAULT 0",
        "HBP_14": "INTEGER NOT NULL DEFAULT 0",
        "ERA_14": "REAL NOT NULL DEFAULT 0",
    }
    existing = {col.lower() for col in _fetch_columns(conn, SNAPSHOT_TABLE)}
    for col, col_def in required.items():
        if col.lower() in existing:
            continue
        conn.execute(f"ALTER TABLE {SNAPSHOT_TABLE} ADD COLUMN {col} {col_def}")

    conn.execute(
        f"""
        CREATE UNIQUE INDEX IF NOT EXISTS ux_{SNAPSHOT_TABLE}_key
        ON {SNAPSHOT_TABLE}(season, as_of_date, team, player_name)
        """
    )
    conn.commit()


def resolve_as_of_dates(conn, args: argparse.Namespace) -> List[str]:
    if args.as_of:
        return [args.as_of]

    if args.start and args.end:
        return _iter_dates(args.start, args.end)

    # Default: all distinct game_dates in this season
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
    rows = execute(conn, query, params).fetchall()
    return [str(r["game_date"] if isinstance(r, dict) else r[0]) for r in rows]


def fetch_daily_player_aggregates(
    conn,
    season: str,
    end_date: str,
    team: str,
    existing_cols: Sequence[str],
) -> Dict[str, Dict[Tuple[str, str], Dict[str, Any]]]:
    params: List[Any] = [f"{season}0101", end_date]
    where = "game_date >= ? AND game_date <= ?"
    if team:
        where += " AND team = ?"
        params.append(team)

    sum_exprs = [
        f"COALESCE(SUM({_src_col_expr(col, existing_cols)}), 0) AS {_qcol(col)}"
        for col in COUNT_COLS
    ]

    # Determine role: use the most common non-empty role for the player on that date
    role_expr = "COALESCE(MAX(NULLIF(role, '')), '')"
    role_col = _lookup_col_name("role", existing_cols)
    if not role_col:
        role_expr = "''"

    query = f"""
    SELECT
        game_date,
        team,
        player_name,
        {role_expr} AS role,
        COUNT(DISTINCT game_id) AS games,
        {", ".join(sum_exprs)}
    FROM {SOURCE_TABLE}
    WHERE {where}
    GROUP BY game_date, team, player_name
    ORDER BY game_date ASC, team ASC, player_name ASC
    """

    cur = execute(conn, query, params)
    cols = [d[0] for d in cur.description]
    out: Dict[str, Dict[Tuple[str, str], Dict[str, Any]]] = defaultdict(dict)
    for row in cur.fetchall():
        if isinstance(row, dict):
            data = {col: row.get(col) for col in cols}
        else:
            data = {cols[i]: row[i] for i in range(len(cols))}
        game_date = str(data["game_date"])
        key = (str(data["team"]), str(data["player_name"]))
        stat: Dict[str, Any] = {
            "games": _safe_int(data.get("games", 0)),
            "role": str(data.get("role") or ""),
        }
        for col in COUNT_COLS:
            stat[col] = _safe_int(data.get(col, 0))
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

        role = state.get("role", "")
        ip, era, whip, k9, bb9, kbb = _calc_rates(
            outs=cum["OUTS"],
            er=cum["ER"],
            h=cum["H"],
            bb=cum["BB"],
            so=cum["SO"],
            hr=cum["HR"],
            hbp=cum["HBP"],
        )
        r7 = state["roll7_sum"]
        ip7 = r7["OUTS"] / 3.0 if r7["OUTS"] > 0 else 0.0
        era7 = (r7["ER"] * 9.0 / ip7) if ip7 > 0 else 0.0

        r14 = state["roll14_sum"]
        ip14 = r14["OUTS"] / 3.0 if r14["OUTS"] > 0 else 0.0
        era14 = (r14["ER"] * 9.0 / ip14) if ip14 > 0 else 0.0

        out.append(
            (
                season,
                as_of,
                team,
                player_name,
                role,
                cum["games"],
                cum["OUTS"],
                round(ip, 3),
                cum["W"],
                cum["L"],
                cum["SV"],
                cum["HLD"],
                cum["H"],
                cum["ER"],
                cum["BB"],
                cum["SO"],
                cum["HR"],
                cum["HBP"],
                cum["BK"],
                cum["WP"],
                round(era, 4),
                round(whip, 4),
                round(k9, 4),
                round(bb9, 4),
                round(kbb, 4),
                # 7-day rolling
                r7["OUTS"],
                r7["ER"],
                r7["BB"],
                r7["SO"],
                r7["HR"],
                r7["HBP"],
                round(era7, 4),
                # 14-day rolling
                r14["OUTS"],
                r14["ER"],
                r14["BB"],
                r14["SO"],
                r14["HR"],
                r14["HBP"],
                round(era14, 4),
            )
        )
    return out


SNAPSHOT_COLS = [
    "season",
    "as_of_date",
    "team",
    "player_name",
    "role",
    "games",
    "OUTS",
    "IP",
    "W",
    "L",
    "SV",
    "HLD",
    "H",
    "ER",
    "BB",
    "SO",
    "HR",
    "HBP",
    "BK",
    "WP",
    "ERA",
    "WHIP",
    "K9",
    "BB9",
    "KBB",
    "OUTS_7",
    "ER_7",
    "BB_7",
    "SO_7",
    "HR_7",
    "HBP_7",
    "ERA_7",
    "OUTS_14",
    "ER_14",
    "BB_14",
    "SO_14",
    "HR_14",
    "HBP_14",
    "ERA_14",
]


def _upsert_snapshot_rows(conn, rows: List[Tuple[Any, ...]], upsert: bool) -> int:
    if not rows:
        return 0

    col_sql = ", ".join(c for c in SNAPSHOT_COLS)
    placeholders = ", ".join(["?"] * len(SNAPSHOT_COLS))

    before = getattr(conn, "total_changes", 0)
    if not upsert:
        sql = f"""
            INSERT INTO {SNAPSHOT_TABLE} ({col_sql})
            VALUES ({placeholders})
            """
        if is_postgres(conn):
            sql += " ON CONFLICT (season, as_of_date, team, player_name) DO NOTHING"
        else:
            sql = sql.replace("INSERT INTO", "INSERT OR IGNORE INTO", 1)
        executemany(conn, sql, rows)
        conn.commit()
        return (getattr(conn, "total_changes", 0) - before) if not is_postgres(conn) else len(rows)

    update_cols = [c for c in SNAPSHOT_COLS if c not in ("season", "as_of_date", "team", "player_name")]
    set_sql = ", ".join(f"{c}=excluded.{c}" for c in update_cols)
    executemany(
        conn,
        f"""
        INSERT INTO {SNAPSHOT_TABLE} ({col_sql})
        VALUES ({placeholders})
        ON CONFLICT(season, as_of_date, team, player_name) DO UPDATE SET
            {set_sql}
        """,
        rows,
    )
    conn.commit()
    return (getattr(conn, "total_changes", 0) - before) if not is_postgres(conn) else len(rows)


def build_snapshots(conn, args: argparse.Namespace) -> Tuple[int, int]:
    if not _table_exists(conn, SOURCE_TABLE):
        raise RuntimeError(f"source table not found: {SOURCE_TABLE}")

    src_cols = _fetch_columns(conn, SOURCE_TABLE)
    src_lower = {col.lower() for col in src_cols}
    missing = [c for c in COUNT_COLS if c.lower() not in src_lower]
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
                    "role": "",
                    "roll7_q": deque(),   # type: Deque[Tuple[str, Dict[str, int]]]
                    "roll14_q": deque(),  # type: Deque[Tuple[str, Dict[str, int]]]
                    "roll7_sum": _empty_roll_dict(),
                    "roll14_sum": _empty_roll_dict(),
                }

            state = states[key]
            cum = state["cum"]
            for c in SNAPSHOT_COUNT_COLS:
                cum[c] += int(day_stat.get(c, 0))

            # Keep the most recent non-empty role
            if day_stat.get("role"):
                state["role"] = day_stat["role"]

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

        rows = _build_snapshot_rows_for_date(
            season=season,
            as_of=as_of,
            states=states,
        )
        written = _upsert_snapshot_rows(conn, rows, upsert=args.upsert)
        total_written += written
        processed_days += 1
        print(f"[ok] as_of={as_of} rows={len(rows)} written={written}")

    print(f"[done] processed_days={processed_days} total_written={total_written}")
    return processed_days, total_written


def preview_top_era(conn, season: int, as_of: str, limit: int, team: str) -> None:
    if limit <= 0:
        return
    params: List[Any] = [season, as_of]
    where = "season = ? AND as_of_date = ?"
    if team:
        where += " AND team = ?"
        params.append(team)
    query = f"""
    SELECT team, player_name, role, games, OUTS, IP, ERA, WHIP, K9, ERA_7, ERA_14
    FROM {SNAPSHOT_TABLE}
    WHERE {where} AND OUTS >= 9
    ORDER BY ERA ASC, OUTS DESC, player_name ASC
    LIMIT ?
    """
    params.append(limit)
    rows = execute(conn, query, params).fetchall()
    print(f"[preview] season={season} as_of={as_of} top={limit}")
    if not rows:
        print("(no rows)")
        return
    print("team | player_name | role | games | IP | ERA | WHIP | K9 | ERA_7 | ERA_14")
    for r in rows:
        team_name = row_value(r, "team", r[0] if not isinstance(r, dict) else "")
        player_name = row_value(r, "player_name", r[1] if not isinstance(r, dict) else "")
        role = row_value(r, "role", r[2] if not isinstance(r, dict) else "")
        games = int(row_value(r, "games", 0) or 0)
        ip = float(row_value(r, "IP", 0) or 0)
        era = float(row_value(r, "ERA", 0) or 0)
        whip = float(row_value(r, "WHIP", 0) or 0)
        k9 = float(row_value(r, "K9", 0) or 0)
        era7 = float(row_value(r, "ERA_7", 0) or 0)
        era14 = float(row_value(r, "ERA_14", 0) or 0)
        print(
            f"{team_name} | {player_name} | {role} | {games} | {ip:.1f} | "
            f"{era:.3f} | {whip:.3f} | {k9:.2f} | {era7:.3f} | {era14:.3f}"
        )


def main() -> None:
    args = parse_args()
    conn = connect_for_path(args.db)
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
                preview_top_era(conn, int(args.season), preview_date, args.preview, args.team)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
