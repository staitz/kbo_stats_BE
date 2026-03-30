import argparse
import datetime as dt
import json
from typing import Any, Dict, List, Tuple
from zoneinfo import ZoneInfo

import requests
from collector.kbo_api import fetch_day_schedule as fetch_kbo_day_schedule
from collector.kbo_naver_crawler import (
    fetch_day_schedule as fetch_naver_day_schedule,
    parse_naver_boxscore,
)
from collector.kbo_db import DB_PATH, init_db, insert_rows, migrate_columns
from db_support import connect


KST = ZoneInfo("Asia/Seoul")
KBO_BOX_SCORE_URL = "https://www.koreabaseball.com/ws/Schedule.asmx/GetBoxScoreScroll"
KBO_HEADERS = {
    "user-agent": "Mozilla/5.0",
    "x-requested-with": "XMLHttpRequest",
    "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
    "accept": "application/json, text/plain, */*",
}


def _today_yyyymmdd_kst() -> str:
    return dt.datetime.now(KST).strftime("%Y%m%d")


def _fetch_rows_for_game(
    game_date: str,
    game_id: str,
    away_team: str,
    home_team: str,
) -> List[Dict[str, Any]]:
    rows = parse_naver_boxscore(
        game_id=game_id,
        game_date=game_date,
        away_team=away_team,
        home_team=home_team,
        debug=False,
    )
    if rows:
        return rows
    return _fetch_rows_from_kbo_boxscore(
        game_date=game_date,
        game_id=game_id,
        away_team=away_team,
        home_team=home_team,
    )


def _cell_text(cell: Dict[str, Any]) -> str:
    return str(cell.get("Text") or "").replace("&nbsp;", "").replace("<br />", "/").strip()


def _safe_int(value: str) -> int:
    clean = str(value or "").strip().replace(",", "")
    if not clean or clean == "-":
        return 0
    return int(float(clean))


def _clean_event_tokens(text: str) -> List[str]:
    clean = str(text or "").replace("&nbsp;", " ").replace("<br />", "/")
    return [token.strip() for token in clean.split("/") if token.strip()]


def _count_tokens(tokens: List[str], keywords: Tuple[str, ...]) -> int:
    return sum(1 for token in tokens if any(keyword in token for keyword in keywords))


def _fetch_schedule_for_date(game_date: str) -> List[dict]:
    games = fetch_naver_day_schedule(game_date, debug=False)
    if games:
        return games
    return fetch_kbo_day_schedule(game_date, debug=False)


def _fetch_rows_from_kbo_boxscore(
    game_date: str,
    game_id: str,
    away_team: str,
    home_team: str,
) -> List[Dict[str, Any]]:
    payload = {
        "leId": "1",
        "srId": "0",
        "seasonId": str(game_date)[:4],
        "gameId": str(game_id),
    }
    referer = (
        "https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx"
        f"?gameDate={game_date}&gameId={game_id}&section=REVIEW"
    )
    response = requests.post(
        KBO_BOX_SCORE_URL,
        data=payload,
        headers={**KBO_HEADERS, "referer": referer},
        timeout=20,
    )
    response.raise_for_status()
    data = response.json()
    arr = data.get("arrHitter") or []
    if not isinstance(arr, list) or not arr:
        return []

    teams = [away_team or "", home_team or ""]
    out_rows: List[Dict[str, Any]] = []
    for idx, block in enumerate(arr[:2]):
        if not isinstance(block, dict):
            continue
        table1_raw = block.get("table1")
        table2_raw = block.get("table2")
        table3_raw = block.get("table3")
        if not table1_raw or not table2_raw or not table3_raw:
            continue

        table1 = json.loads(table1_raw)
        table2 = json.loads(table2_raw)
        table3 = json.loads(table3_raw)
        meta_rows = table1.get("rows") or []
        event_rows = table2.get("rows") or []
        stat_rows = table3.get("rows") or []
        team_name = teams[idx] if idx < len(teams) else ""

        for meta_row, event_row, stat_row in zip(meta_rows, event_rows, stat_rows):
            meta_cells = [_cell_text(cell) for cell in (meta_row.get("row") or [])]
            stat_cells = [_cell_text(cell) for cell in (stat_row.get("row") or [])]
            event_cells = [_cell_text(cell) for cell in (event_row.get("row") or [])]
            if len(meta_cells) < 3 or len(stat_cells) < 4:
                continue

            player_name = meta_cells[2].strip()
            if not player_name:
                continue

            event_tokens = []
            for cell_text in event_cells:
                event_tokens.extend(_clean_event_tokens(cell_text))

            ab = _safe_int(stat_cells[0])
            h = _safe_int(stat_cells[1])
            r = _safe_int(stat_cells[2])
            rbi = _safe_int(stat_cells[3])
            doubles = _count_tokens(event_tokens, ("2루타", "우2", "좌2", "중2"))
            triples = _count_tokens(event_tokens, ("3루타", "우3", "좌3", "중3"))
            hr = _count_tokens(event_tokens, ("홈런", "우홈", "좌홈", "중홈", "좌중홈", "우중홈", "그홈"))
            bb = _count_tokens(event_tokens, ("4구", "볼넷"))
            so = _count_tokens(event_tokens, ("삼진",))
            sh = _count_tokens(event_tokens, ("희번", "희생번트", "희타"))
            sf = _count_tokens(event_tokens, ("희비", "희플", "희생플라이"))
            hbp = _count_tokens(event_tokens, ("사구",))
            gdp = _count_tokens(event_tokens, ("병살",))
            singles = max(0, h - doubles - triples - hr)
            tb = singles + doubles * 2 + triples * 3 + hr * 4
            pa = ab + bb + hbp + sh + sf

            out_rows.append(
                {
                    "game_date": game_date,
                    "game_id": game_id,
                    "team": team_name,
                    "player_name": player_name,
                    "AB": ab,
                    "H": h,
                    "HR": hr,
                    "BB": bb,
                    "SO": so,
                    "SH": sh,
                    "2B": doubles,
                    "3B": triples,
                    "HBP": hbp,
                    "SF": sf,
                    "R": r,
                    "RBI": rbi,
                    "TB": tb,
                    "PA": pa,
                    "SB": 0,
                    "CS": 0,
                    "GDP": gdp,
                }
            )
    return out_rows


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

    try:
        total_games = 0
        total_rows = 0
        total_inserted = 0
        total_ignored = 0
        skipped_days = 0
        failed_days = 0
        success_days = 0

        for game_date in dates:
            try:
                games = _fetch_schedule_for_date(game_date)
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
            except Exception as exc:
                failed_days += 1
                _record_failed_date(game_date, f"{type(exc).__name__}: {exc}")
                print(f"[fail] date={game_date} error={type(exc).__name__}: {exc}")

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
