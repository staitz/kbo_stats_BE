import argparse
import datetime as dt
import time
from typing import Any, Dict, List, Tuple
from zoneinfo import ZoneInfo

from collector.kbo_api import _make_driver
from collector.kbo_naver_crawler import fetch_day_schedule, parse_naver_boxscore
from collector.kbo_db import DB_PATH, init_db, insert_rows, migrate_columns
from db_support import connect
from selenium.common.exceptions import TimeoutException, WebDriverException


KST = ZoneInfo("Asia/Seoul")


def _today_yyyymmdd_kst() -> str:
    return dt.datetime.now(KST).strftime("%Y%m%d")


def _fetch_rows_for_game(
    driver,
    game_date: str,
    game_id: str,
    away_team: str,
    home_team: str,
) -> List[Dict[str, Any]]:
    # driver argument is kept for compatibility but not used for Naver (unless fallback needed)
    return parse_naver_boxscore(
        game_id=game_id,
        game_date=game_date,
        away_team=away_team,
        home_team=home_team,
        debug=False
    )

def _record_failed_date(game_date: str, reason: str) -> None:
    with open("failed_dates.txt", "a", encoding="utf-8") as f:
        f.write(f"{game_date}\t{reason}\n")


def collect_for_dates(
    dates: List[str],
    upsert: bool = False,
) -> Dict[str, int]:
    if not dates:
        return {
            "dates": 0,
            "games": 0,
            "rows": 0,
            "inserted": 0,
            "ignored": 0,
            "skipped_days": 0,
        }

    conn = connect(DB_PATH)
    init_db(conn)
    migrate_columns(conn)

    driver = _make_driver(headless=True)
    try:
        total_games = 0
        total_rows = 0
        total_inserted = 0
        total_ignored = 0
        skipped_days = 0
        failed_days = 0
        success_days = 0

        for game_date in dates:
            retries = [2, 5, 10]
            attempt = 0
            while True:
                try:
                    games = fetch_day_schedule(game_date, debug=False)
                    game_ids: List[Tuple[str, str, str]] = [
                        (g.get("game_id"), g.get("away_team"), g.get("home_team"))
                        for g in games
                        if g.get("game_id")
                    ]

                    if not game_ids:
                        skipped_days += 1
                        print(f"[skip] date={game_date} no games")
                        break

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
                        inserted = insert_rows(conn, rows, upsert=upsert)
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
                    success_days += 1
                    break
                except (TimeoutException, WebDriverException) as exc:
                    if attempt >= len(retries):
                        failed_days += 1
                        _record_failed_date(game_date, f"{type(exc).__name__}: {exc}")
                        print(f"[fail] date={game_date} error={type(exc).__name__}")
                        break
                    wait_s = retries[attempt]
                    attempt += 1
                    print(
                        f"[retry] date={game_date} attempt={attempt} wait={wait_s}s "
                        f"error={type(exc).__name__}"
                    )
                    try:
                        driver.quit()
                    except Exception:
                        pass
                    driver = _make_driver(headless=True)
                    time.sleep(wait_s)

        summary = {
            "dates": len(dates),
            "games": total_games,
            "rows": total_rows,
            "inserted": total_inserted,
            "ignored": total_ignored,
            "skipped_days": skipped_days,
            "failed_days": failed_days,
            "success_days": success_days,
        }
        print(
            f"[done] dates={summary['dates']} games={summary['games']} rows={summary['rows']} "
            f"inserted={summary['inserted']} ignored={summary['ignored']} "
            f"skipped_days={summary['skipped_days']} success_days={summary['success_days']} "
            f"failed_days={summary['failed_days']}"
        )
        return summary
    finally:
        driver.quit()
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Daily KBO hitter log collector")
    parser.add_argument("date", nargs="?", help="YYYYMMDD (default: today in KST)")
    parser.add_argument("--upsert", action="store_true", help="update existing rows on conflict")
    args = parser.parse_args()

    game_date = args.date or _today_yyyymmdd_kst()
    print(f"[run] date={game_date} upsert={bool(args.upsert)}")
    collect_for_dates(dates=[game_date], upsert=args.upsert)


if __name__ == "__main__":
    main()
