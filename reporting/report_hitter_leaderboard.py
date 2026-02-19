import argparse
import sqlite3
from typing import Any, Dict, List, Optional, Tuple

from collector.kbo_db import DB_PATH
from collector.kbo_hitter_parser import calc_ops, _calc_tb  # reuse helper for OPS/TB


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="KBO hitter season leaderboard (OPS/HR/AVG)")
    parser.add_argument("--season", default="2025", help="season year, e.g. 2025")
    parser.add_argument("--start", help="YYYYMMDD")
    parser.add_argument("--end", help="YYYYMMDD")
    parser.add_argument("--team", help="team name filter (exact match)")
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--min_pa", type=int, default=30)
    parser.add_argument("--min_ab", type=int, default=30)
    return parser.parse_args()


def _build_where(season: str, start: Optional[str], end: Optional[str], team: Optional[str]) -> Tuple[str, List[Any]]:
    clauses = []
    params: List[Any] = []

    if start or end:
        if start:
            clauses.append("game_date >= ?")
            params.append(start)
        if end:
            clauses.append("game_date <= ?")
            params.append(end)
    else:
        clauses.append("game_date LIKE ?")
        params.append(f"{season}%")

    if team:
        clauses.append("team = ?")
        params.append(team)

    where = " AND ".join(clauses) if clauses else "1=1"
    return where, params


def _fetch_agg_rows(conn: sqlite3.Connection, where: str, params: List[Any]) -> List[Dict[str, Any]]:
    # 시즌/기간 기준으로 선수별 누적 합계
    existing = {
        row[1] for row in conn.execute("PRAGMA table_info(hitter_game_logs)").fetchall()
    }
    # 컬럼이 없으면 0으로 대체해서 쿼리 에러를 방지
    def col_or_zero(col: str) -> str:
        return f"SUM({col}) AS {col}" if col in existing else f"0 AS {col}"

    query = f"""
    SELECT
        player_name,
        team,
        {col_or_zero("PA")},
        {col_or_zero("AB")},
        {col_or_zero("H")},
        {col_or_zero('"2B"')},
        {col_or_zero('"3B"')},
        {col_or_zero("HR")},
        {col_or_zero("BB")},
        {col_or_zero("HBP")},
        {col_or_zero("SF")},
        {col_or_zero("SO")},
        {col_or_zero("R")},
        {col_or_zero("RBI")},
        {col_or_zero("TB")}
    FROM hitter_game_logs
    WHERE {where}
    GROUP BY player_name, team
    """
    cur = conn.execute(query, params)
    cols = [c[0] for c in cur.description]
    out = []
    for row in cur.fetchall():
        out.append({cols[i]: row[i] for i in range(len(cols))})
    return out


def _coerce_int(value: Any) -> int:
    if value is None:
        return 0
    try:
        return int(value)
    except Exception:
        return 0


def _compute_metrics(row: Dict[str, Any]) -> Dict[str, Any]:
    # 누적 스탯을 기반으로 PA/OBP/SLG/OPS 계산
    stats = {
        "PA": _coerce_int(row.get("PA")),
        "AB": _coerce_int(row.get("AB")),
        "H": _coerce_int(row.get("H")),
        "2B": _coerce_int(row.get("2B")),
        "3B": _coerce_int(row.get("3B")),
        "HR": _coerce_int(row.get("HR")),
        "BB": _coerce_int(row.get("BB")),
        "HBP": _coerce_int(row.get("HBP")),
        "SF": _coerce_int(row.get("SF")),
        "SO": _coerce_int(row.get("SO")),
        "TB": _coerce_int(row.get("TB")),
        "R": _coerce_int(row.get("R")),
        "RBI": _coerce_int(row.get("RBI")),
    }

    if stats["PA"] == 0:
        stats["PA"] = stats["AB"] + stats["BB"] + stats["HBP"] + stats["SF"]
    if stats["TB"] == 0:
        stats["TB"] = _calc_tb(stats)

    ops = calc_ops(stats)
    ab = stats["AB"]
    avg = round(stats["H"] / ab, 3) if ab > 0 else 0.0

    stats["OPS"] = ops
    stats["AVG"] = avg
    return stats


def _print_ops_leaderboard(rows: List[Dict[str, Any]], limit: int, min_pa: int) -> None:
    print("[leaderboard] OPS")
    filtered = [r for r in rows if r["PA"] >= min_pa]
    filtered.sort(key=lambda x: x["OPS"], reverse=True)
    for r in filtered[:limit]:
        print(
            f"{r['player_name']} ({r['team']}) "
            f"PA={r['PA']} AB={r['AB']} H={r['H']} 2B={r['2B']} 3B={r['3B']} "
            f"HR={r['HR']} BB={r['BB']} SO={r['SO']} "
            f"OBP={_format_obp(r)} SLG={_format_slg(r)} OPS={r['OPS']:.3f}"
        )


def _print_hr_leaderboard(rows: List[Dict[str, Any]], limit: int) -> None:
    print("[leaderboard] HR")
    rows.sort(key=lambda x: x["HR"], reverse=True)
    for r in rows[:limit]:
        print(
            f"{r['player_name']} ({r['team']}) HR={r['HR']} "
            f"PA={r['PA']} AB={r['AB']} H={r['H']}"
        )


def _print_avg_leaderboard(rows: List[Dict[str, Any]], limit: int, min_ab: int) -> None:
    print("[leaderboard] AVG")
    filtered = [r for r in rows if r["AB"] >= min_ab]
    filtered.sort(key=lambda x: x["AVG"], reverse=True)
    for r in filtered[:limit]:
        print(
            f"{r['player_name']} ({r['team']}) AVG={r['AVG']:.3f} "
            f"AB={r['AB']} H={r['H']} HR={r['HR']}"
        )


def _format_obp(stats: Dict[str, Any]) -> str:
    ab = stats["AB"]
    h = stats["H"]
    bb = stats["BB"]
    hbp = stats["HBP"]
    sf = stats["SF"]
    den = ab + bb + hbp + sf
    val = (h + bb + hbp) / den if den > 0 else 0.0
    return f"{val:.3f}"


def _format_slg(stats: Dict[str, Any]) -> str:
    ab = stats["AB"]
    tb = stats["TB"]
    val = tb / ab if ab > 0 else 0.0
    return f"{val:.3f}"


def main() -> None:
    args = _parse_args()
    where, params = _build_where(args.season, args.start, args.end, args.team)

    conn = sqlite3.connect(DB_PATH)
    try:
        agg = _fetch_agg_rows(conn, where, params)
    finally:
        conn.close()

    if not agg:
        print("[warn] no rows matched")
        return

    computed: List[Dict[str, Any]] = []
    for row in agg:
        stats = _compute_metrics(row)
        stats["player_name"] = row["player_name"]
        stats["team"] = row["team"]
        computed.append(stats)

    _print_ops_leaderboard(computed, args.limit, args.min_pa)
    _print_hr_leaderboard(computed, args.limit)
    _print_avg_leaderboard(computed, args.limit, args.min_ab)


if __name__ == "__main__":
    main()
