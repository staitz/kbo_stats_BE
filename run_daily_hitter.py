import argparse
import sqlite3
import time
from typing import Any, Dict, Iterable, List, Tuple

from kbo_api import _make_driver, _wait_ready, fetch_day_schedule
from kbo_db import DB_PATH, init_db, insert_rows, migrate_columns
from kbo_hitter_parser import parse_hitter_rows_from_dom_tables


def _extract_dom_tables(driver) -> List[Dict[str, Any]]:
    # DOM 테이블을 직접 수집해 파서에 전달
    return driver.execute_script(
        """
        function findTeamLabel(el) {
          let cur = el;
          for (let i = 0; i < 6 && cur; i++) {
            const prev = cur.previousElementSibling;
            if (prev && prev.innerText) {
              const lines = prev.innerText.split('\\n').map(s => s.trim()).filter(Boolean);
              for (const line of lines) {
                if (line.includes('타자 기록')) {
                  return line;
                }
              }
            }
            cur = cur.parentElement;
          }
          return '';
        }

        const tables = Array.from(document.querySelectorAll('table'));
        return tables.map((tbl, idx) => {
          const headers = [];
          const rows = [];
          const headerCells = tbl.querySelectorAll('thead tr th');
          if (headerCells.length) {
            headerCells.forEach(h => headers.push((h.innerText || '').trim()));
          } else {
            const firstRow = tbl.querySelector('tr');
            if (firstRow) {
              firstRow.querySelectorAll('th,td').forEach(c => headers.push((c.innerText || '').trim()));
            }
          }
          const bodyRows = tbl.querySelectorAll('tbody tr');
          const targetRows = bodyRows.length ? bodyRows : tbl.querySelectorAll('tr');
          targetRows.forEach(tr => {
            const cells = Array.from(tr.querySelectorAll('th,td')).map(c => (c.innerText || '').trim());
            if (cells.length) rows.push(cells);
          });
          return {
            index: idx,
            team: findTeamLabel(tbl),
            headers: headers,
            rows: rows,
            text: (tbl.innerText || '').trim()
          };
        });
        """
    )


def _fetch_rows_for_game(
    driver,
    game_date: str,
    game_id: str,
    away_team: str,
    home_team: str,
    sections: Iterable[str] = ("REVIEW", "BOX", "RECORD"),
) -> List[Dict[str, Any]]:
    for section in sections:
        url = (
            "https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx"
            f"?gameDate={game_date}&gameId={game_id}&section={section}"
        )
        driver.get(url)
        _wait_ready(driver, 15)
        time.sleep(2)

        dom_tables = _extract_dom_tables(driver)
        rows = parse_hitter_rows_from_dom_tables(
            tables=dom_tables or [],
            game_date=game_date,
            game_id=game_id,
            away_team=away_team,
            home_team=home_team,
            debug=False,
        )
        if rows:
            return rows
    return []


def main() -> None:
    parser = argparse.ArgumentParser(description="Daily KBO hitter log collector")
    parser.add_argument("date", help="YYYYMMDD")
    parser.add_argument("--upsert", action="store_true", help="update existing rows on conflict")
    args = parser.parse_args()

    game_date = args.date
    games = fetch_day_schedule(game_date, debug=False)
    game_ids = [
        (g.get("game_id"), g.get("away_team"), g.get("home_team"))
        for g in games
        if g.get("game_id")
    ]

    if not game_ids:
        print("[warn] no games found for date:", game_date)
        return

    conn = sqlite3.connect(DB_PATH)
    init_db(conn)
    migrate_columns(conn)

    driver = _make_driver(headless=True)
    try:
        inserted_total = 0
        for game_id, away_team, home_team in game_ids:
            rows = _fetch_rows_for_game(
                driver=driver,
                game_date=game_date,
                game_id=game_id,
                away_team=away_team or "",
                home_team=home_team or "",
            )
            inserted = insert_rows(conn, rows, upsert=args.upsert)
            inserted_total += inserted
            print(
                f"[ok] game_id={game_id} rows={len(rows)} inserted={inserted}"
            )
        print(f"[done] inserted_total={inserted_total}")
    finally:
        driver.quit()
        conn.close()


if __name__ == "__main__":
    main()
