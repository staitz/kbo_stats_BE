import argparse
import datetime as dt
from typing import List, Tuple

from kbo_api import _make_driver, fetch_day_schedule
from kbo_db import DB_PATH, init_db, insert_rows, migrate_columns
from run_daily_hitter import _fetch_rows_for_game


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


def main() -> None:
    parser = argparse.ArgumentParser(description="Range KBO hitter log collector")
    parser.add_argument("--start", required=True, help="YYYYMMDD")
    parser.add_argument("--end", required=True, help="YYYYMMDD")
    parser.add_argument("--upsert", action="store_true", help="update existing rows on conflict")
    args = parser.parse_args()

    dates = _iter_dates(args.start, args.end)

    # 드라이버는 범위 전체에서 재사용해 속도와 안정성을 확보
    driver = _make_driver(headless=True)
    conn = None
    try:
        import sqlite3

        conn = sqlite3.connect(DB_PATH)
        init_db(conn)
        migrate_columns(conn)

        total_games = 0
        total_rows = 0
        total_inserted = 0
        total_ignored = 0
        skipped_days = 0

        for game_date in dates:
            games = fetch_day_schedule(game_date, debug=False)
            game_ids: List[Tuple[str, str, str]] = [
                (g.get("game_id"), g.get("away_team"), g.get("home_team"))
                for g in games
                if g.get("game_id")
            ]

            if not game_ids:
                skipped_days += 1
                print(f"[skip] date={game_date} no games")
                continue

            date_rows = 0
            date_inserted = 0
            date_games = len(game_ids)

            for game_id, away_team, home_team in game_ids:
                rows = _fetch_rows_for_game(
                    driver=driver,
                    game_date=game_date,
                    game_id=game_id,
                    away_team=away_team or "",
                    home_team=home_team or "",
                )
                inserted = insert_rows(conn, rows, upsert=args.upsert)
                date_rows += len(rows)
                date_inserted += inserted
                print(
                    f"[ok] date={game_date} game_id={game_id} rows={len(rows)} inserted={inserted}"
                )

            date_ignored = max(0, date_rows - date_inserted)
            total_games += date_games
            total_rows += date_rows
            total_inserted += date_inserted
            total_ignored += date_ignored
            print(
                f"[day] date={game_date} games={date_games} rows={date_rows} "
                f"inserted={date_inserted} ignored={date_ignored}"
            )

        print(
            f"[done] dates={len(dates)} games={total_games} rows={total_rows} "
            f"inserted={total_inserted} ignored={total_ignored} skipped_days={skipped_days}"
        )
    finally:
        try:
            driver.quit()
        except Exception:
            pass
        if conn:
            conn.close()


if __name__ == "__main__":
    main()
