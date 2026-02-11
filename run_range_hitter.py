import argparse
import datetime as dt
from typing import List
from zoneinfo import ZoneInfo

from run_daily_hitter import collect_for_dates


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
    parser = argparse.ArgumentParser(description="Range KBO hitter log collector")
    parser.add_argument("--start", required=True, help="YYYYMMDD")
    parser.add_argument("--end", help="YYYYMMDD (default: today in KST)")
    parser.add_argument("--upsert", action="store_true", help="update existing rows on conflict")
    args = parser.parse_args()

    end = args.end or _today_yyyymmdd_kst()
    dates = _iter_dates(args.start, end)
    print(
        f"[run] start={args.start} end={end} dates={len(dates)} "
        f"upsert={bool(args.upsert)}"
    )
    collect_for_dates(dates=dates, upsert=args.upsert)


if __name__ == "__main__":
    main()
