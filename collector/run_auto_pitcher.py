import argparse
import datetime as dt
from zoneinfo import ZoneInfo

from collector.kbo_db import DB_PATH, init_db, migrate_pitcher_columns
from collector.run_range_pitcher import _iter_dates
from collector.run_daily_pitcher import collect_for_dates
from db_support import connect, fetchone, row_value


KST = ZoneInfo("Asia/Seoul")


def _current_season_start() -> str:
    """현재 연도 기준 KBO 정규시즌 시작일 (3월 28일) 반환."""
    year = dt.datetime.now(KST).year
    return f"{year}0328"


DEFAULT_SEASON_START = _current_season_start()


def _today_yyyymmdd_kst() -> str:
    return dt.datetime.now(KST).strftime("%Y%m%d")


def _add_days(yyyymmdd: str, days: int) -> str:
    d = dt.datetime.strptime(yyyymmdd, "%Y%m%d").date() + dt.timedelta(days=days)
    return d.strftime("%Y%m%d")


def _read_latest_game_date(conn) -> str:
    row = fetchone(
        conn,
        """
        SELECT game_date
        FROM pitcher_game_logs
        ORDER BY game_date DESC
        LIMIT 1
        """
    )
    return str(row_value(row, "game_date", "") or "")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Auto KBO pitcher collector (latest DB date to today, KST)"
    )
    parser.add_argument("--start", help="YYYYMMDD (optional; overrides DB-based start)")
    parser.add_argument("--end", help="YYYYMMDD (optional; default=today in KST)")
    parser.add_argument(
        "--season-start",
        default=DEFAULT_SEASON_START,
        help="YYYYMMDD (default: 20260328; do not collect before this date)",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=1,
        help="when start is auto-derived from DB latest date, include N days back (default: 1)",
    )
    parser.add_argument("--upsert", action="store_true", help="update existing rows on conflict")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    today = _today_yyyymmdd_kst()
    end = args.end or today
    season_start = args.season_start

    conn = connect(DB_PATH)
    try:
        init_db(conn)
        migrate_pitcher_columns(conn)
        latest = _read_latest_game_date(conn)
    finally:
        conn.close()

    if args.start:
        start = args.start
        source = "arg --start"
    elif latest:
        start = _add_days(latest, -max(0, args.lookback_days))
        source = f"db_latest({latest})-lookback({max(0, args.lookback_days)})"
    else:
        start = today
        source = "today (db empty)"

    if end < start:
        raise ValueError(f"end date must be >= start date (start={start}, end={end})")

    if season_start:
        if end < season_start:
            print(
                f"[auto-skip] season_start={season_start} end={end} "
                "-> season not started yet"
            )
            return
        if start < season_start:
            start = season_start
            source = f"{source}+clamped(season_start={season_start})"

    dates = _iter_dates(start, end)
    print(
        f"[auto] start={start} end={end} dates={len(dates)} "
        f"source={source} upsert={bool(args.upsert)}"
    )
    collect_for_dates(dates=dates, upsert=args.upsert)


if __name__ == "__main__":
    main()
