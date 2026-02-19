import argparse
import sqlite3
from typing import Optional

from collector.kbo_db import DB_PATH


def _print_top_dates(conn: sqlite3.Connection) -> None:
    print("[report] top 10 dates by row count")
    rows = conn.execute(
        """
        SELECT game_date, COUNT(*) AS cnt
        FROM hitter_game_logs
        GROUP BY game_date
        ORDER BY cnt DESC
        LIMIT 10
        """
    ).fetchall()
    for game_date, cnt in rows:
        print(f"  {game_date}: {cnt}")


def _print_team_counts(conn: sqlite3.Connection) -> None:
    print("[report] team row counts")
    rows = conn.execute(
        """
        SELECT team, COUNT(*) AS cnt
        FROM hitter_game_logs
        GROUP BY team
        ORDER BY cnt DESC
        """
    ).fetchall()
    for team, cnt in rows:
        print(f"  {team}: {cnt}")


def _print_date_summary(conn: sqlite3.Connection, game_date: str) -> None:
    print(f"[report] date summary: {game_date}")
    game_cnt = conn.execute(
        """
        SELECT COUNT(DISTINCT game_id)
        FROM hitter_game_logs
        WHERE game_date = ?
        """,
        (game_date,),
    ).fetchone()[0]
    row_cnt = conn.execute(
        """
        SELECT COUNT(*)
        FROM hitter_game_logs
        WHERE game_date = ?
        """,
        (game_date,),
    ).fetchone()[0]
    print(f"  games: {game_cnt}")
    print(f"  rows: {row_cnt}")


def _print_anomalies(conn: sqlite3.Connection) -> None:
    print("[report] anomaly checks")
    name_empty = conn.execute(
        """
        SELECT COUNT(*)
        FROM hitter_game_logs
        WHERE player_name IS NULL OR TRIM(player_name) = ''
        """
    ).fetchone()[0]
    all_zero = conn.execute(
        """
        SELECT COUNT(*)
        FROM hitter_game_logs
        WHERE AB = 0 AND H = 0 AND HR = 0 AND BB = 0 AND SO = 0
        """
    ).fetchone()[0]
    extra_zero = conn.execute(
        """
        SELECT COUNT(*)
        FROM hitter_game_logs
        WHERE "2B" = 0 AND "3B" = 0 AND HBP = 0 AND SF = 0
        """
    ).fetchone()[0]
    total = conn.execute("SELECT COUNT(*) FROM hitter_game_logs").fetchone()[0]
    print(f"  empty player_name rows: {name_empty}")
    print(f"  all-zero stat rows: {all_zero}")
    if total > 0:
        ratio = round(extra_zero / total * 100, 2)
        print(f"  2B/3B/HBP/SF all-zero rows: {extra_zero} ({ratio}%)")
    else:
        print("  2B/3B/HBP/SF all-zero rows: 0 (0%)")


def _resolve_report_date(conn: sqlite3.Connection, start: Optional[str], date: Optional[str]) -> Optional[str]:
    if date:
        return date
    if start:
        return start
    row = conn.execute(
        """
        SELECT game_date
        FROM hitter_game_logs
        ORDER BY game_date DESC
        LIMIT 1
        """
    ).fetchone()
    return row[0] if row else None


def main() -> None:
    parser = argparse.ArgumentParser(description="KBO hitter DB verification report")
    parser.add_argument("--start", help="YYYYMMDD")
    parser.add_argument("--date", help="YYYYMMDD")
    args = parser.parse_args()

    conn = sqlite3.connect(DB_PATH)
    try:
        _print_top_dates(conn)
        _print_team_counts(conn)

        report_date = _resolve_report_date(conn, args.start, args.date)
        if report_date:
            _print_date_summary(conn, report_date)
        else:
            print("[report] date summary: no data")

        _print_anomalies(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
