import argparse
import datetime as dt
from typing import List
from zoneinfo import ZoneInfo

from collector.kbo_naver_crawler import find_season_start_date
from collector.run_daily_pitcher import collect_for_dates


def _iter_dates(start_yyyymmdd: str, end_yyyymmdd: str) -> List[str]:
    start = dt.datetime.strptime(start_yyyymmdd, "%Y%m%d").date()
    end = dt.datetime.strptime(end_yyyymmdd, "%Y%m%d").date()
    if end < start:
        raise ValueError("end date must be >= start date")
    out = []
    cur = start
    while cur <= end:
        out.append(cur.strftime("%Y%m%d"))
        cur += dt.timedelta(days=1)
    return out


def _today_yyyymmdd_kst() -> str:
    return dt.datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y%m%d")


def main() -> None:
    parser = argparse.ArgumentParser(description="Range KBO pitcher log collector")
    parser.add_argument("--start", help="YYYYMMDD")
    parser.add_argument("--end", help="YYYYMMDD (default: today in KST)")
    parser.add_argument("--season", type=int, help="Season year for auto-start")
    parser.add_argument(
        "--auto-start",
        action="store_true",
        help="Auto-detect season start date from KBO schedule",
    )
    parser.add_argument("--upsert", action="store_true", help="update existing rows on conflict")
    args = parser.parse_args()

    end = args.end or _today_yyyymmdd_kst()
    start = args.start
    if args.auto_start:
        season = args.season or int(_today_yyyymmdd_kst()[:4])
        auto_start = find_season_start_date(str(season))
        if not auto_start:
            print(f"[auto] season={season} start not found; skipping")
            return
        start = auto_start
        print(f"[auto] season={season} start={start}")

    if not start:
        raise ValueError("start date is required (or use --auto-start)")

    if end < start:
        print(f"[run] start={start} end={end} => no dates to collect (pre-season)")
        return

    dates = _iter_dates(start, end)
    print(
        f"[run] start={start} end={end} dates={len(dates)} "
        f"upsert={bool(args.upsert)}"
    )
    collect_for_dates(dates=dates, upsert=args.upsert)


if __name__ == "__main__":
    main()
