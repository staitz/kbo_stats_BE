import argparse
import datetime as dt
import json
import time
from typing import Any, Dict, List, Tuple
from zoneinfo import ZoneInfo

import requests
from collector.kbo_api import fetch_day_schedule as fetch_kbo_day_schedule
from collector.kbo_naver_crawler import (
    fetch_day_schedule as fetch_naver_day_schedule,
    parse_naver_pitcher_boxscore,
)
from collector.kbo_db import (
    DB_PATH,
    init_db,
    insert_pitcher_rows,
    migrate_pitcher_columns,
)
from db_support import connect, execute
from selenium.common.exceptions import TimeoutException, WebDriverException


KST = ZoneInfo("Asia/Seoul")
KBO_BOX_SCORE_URL = "https://www.koreabaseball.com/ws/Schedule.asmx/GetBoxScoreScroll"
KBO_HEADERS = {
    "user-agent": "Mozilla/5.0",
    "x-requested-with": "XMLHttpRequest",
    "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
    "accept": "application/json, text/plain, */*",
}
TEAM_CODE_MAP = {
    "HT": "KIA",
    "LG": "LG",
    "KT": "KT",
    "NC": "NC",
    "OB": "두산",
    "LT": "롯데",
    "SS": "삼성",
    "SK": "SSG",
    "WO": "키움",
    "HH": "한화",
}


def _today_yyyymmdd_kst() -> str:
    return dt.datetime.now(KST).strftime("%Y%m%d")


def _fetch_rows_for_game(
    game_date: str,
    game_id: str,
    away_team: str,
    home_team: str,
) -> List[Dict[str, Any]]:
    rows = parse_naver_pitcher_boxscore(
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


def _parse_ip_to_outs(ip_text: str) -> int:
    clean = str(ip_text or "").strip()
    if not clean or clean == "&nbsp;":
        return 0
    if " " in clean and "/" in clean:
        whole, frac = clean.split(" ", 1)
        whole_outs = int(whole) * 3
        if frac == "1/3":
            return whole_outs + 1
        if frac == "2/3":
            return whole_outs + 2
        return whole_outs
    if "/" in clean:
        if clean == "1/3":
            return 1
        if clean == "2/3":
            return 2
    return int(float(clean)) * 3


def _cell_text(cell: Dict[str, Any]) -> str:
    return str(cell.get("Text") or "").replace("&nbsp;", "").strip()


def _normalize_pitcher_role(role_text: str) -> str:
    clean = str(role_text or "").strip()
    if clean == "선발":
        return "SP"
    if clean:
        return "RP"
    return ""


def _safe_int(text: str) -> int:
    clean = str(text or "").strip().replace(",", "")
    if not clean or clean == "-":
        return 0
    return int(float(clean))


def _safe_float(text: str) -> float:
    clean = str(text or "").strip().replace(",", "")
    if not clean or clean == "-":
        return 0.0
    return float(clean)


def _infer_teams_from_game_id(game_id: str) -> Tuple[str, str]:
    gid = str(game_id or "").strip()
    if len(gid) < 12:
        return "", ""
    away_code = gid[8:10]
    home_code = gid[10:12]
    return TEAM_CODE_MAP.get(away_code, ""), TEAM_CODE_MAP.get(home_code, "")


def _load_games_from_hitter_logs(conn, game_date: str) -> List[Tuple[str, str, str]]:
    rows = execute(
        conn,
        """
        SELECT DISTINCT game_id
        FROM hitter_game_logs
        WHERE game_date = ?
        ORDER BY game_id
        """,
        [game_date],
    ).fetchall()
    out: List[Tuple[str, str, str]] = []
    for row in rows:
        game_id = str(row[0] or "").strip()
        if not game_id:
            continue
        away_team, home_team = _infer_teams_from_game_id(game_id)
        out.append((game_id, away_team, home_team))
    return out


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
    arr = data.get("arrPitcher") or []
    if not isinstance(arr, list) or not arr:
        return []

    teams = [away_team or "", home_team or ""]
    out_rows: List[Dict[str, Any]] = []
    for idx, block in enumerate(arr[:2]):
        table_raw = block.get("table") if isinstance(block, dict) else None
        if not table_raw:
            continue
        table = json.loads(table_raw)
        header_rows = table.get("headers") or []
        if not header_rows:
            continue
        headers = [_cell_text(cell) for cell in (header_rows[0].get("row") or [])]
        rows = table.get("rows") or []
        col_idx = {name: i for i, name in enumerate(headers)}
        team_name = teams[idx] if idx < len(teams) else ""
        for row in rows:
            cells = [_cell_text(cell) for cell in (row.get("row") or [])]
            if not cells:
                continue
            player_name = cells[col_idx.get("선수명", 0)].strip()
            if not player_name:
                continue
            ip_text = cells[col_idx.get("이닝", -1)] if "이닝" in col_idx else "0"
            outs = _parse_ip_to_outs(ip_text)
            bb_total = int(cells[col_idx.get("4사구", -1)] or 0) if "4사구" in col_idx else 0
            result_text = cells[col_idx.get("결과", -1)] if "결과" in col_idx else ""
            role_text = cells[col_idx.get("등판", -1)] if "등판" in col_idx else ""
            out_rows.append(
                {
                    "game_date": game_date,
                    "game_id": game_id,
                    "team": team_name,
                    "player_name": player_name,
                    "role": _normalize_pitcher_role(role_text),
                    "W": int(result_text == "승"),
                    "L": int(result_text == "패"),
                    "SV": int(result_text == "세"),
                    "HLD": int(result_text == "홀"),
                    "IP": round(outs / 3.0, 4),
                    "OUTS": outs,
                    "BF": _safe_int(cells[col_idx.get("타자", -1)]) if "타자" in col_idx else 0,
                    "NP": _safe_int(cells[col_idx.get("투구수", -1)]) if "투구수" in col_idx else 0,
                    "H": _safe_int(cells[col_idx.get("피안타", -1)]) if "피안타" in col_idx else 0,
                    "R": _safe_int(cells[col_idx.get("실점", -1)]) if "실점" in col_idx else 0,
                    "ER": _safe_int(cells[col_idx.get("자책", -1)]) if "자책" in col_idx else 0,
                    "BB": bb_total,
                    "SO": _safe_int(cells[col_idx.get("삼진", -1)]) if "삼진" in col_idx else 0,
                    "HR": _safe_int(cells[col_idx.get("홈런", -1)]) if "홈런" in col_idx else 0,
                    "HBP": 0,
                    "BK": 0,
                    "WP": 0,
                    "ERA": _safe_float(cells[col_idx.get("평균자책점", -1)]) if "평균자책점" in col_idx else 0.0,
                }
            )
    return out_rows


def _record_failed_date(game_date: str, reason: str) -> None:
    with open("failed_pitcher_dates.txt", "a", encoding="utf-8") as f:
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
    migrate_pitcher_columns(conn)

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
                    games = _fetch_schedule_for_date(game_date)
                    game_ids: List[Tuple[str, str, str]] = [
                        (g.get("game_id"), g.get("away_team"), g.get("home_team"))
                        for g in games
                        if g.get("game_id")
                    ]
                    if not game_ids:
                        game_ids = _load_games_from_hitter_logs(conn, game_date)

                    if not game_ids:
                        skipped_days += 1
                        print(f"[skip] date={game_date} no games")
                        break

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
                        inserted = insert_pitcher_rows(conn, rows, upsert=upsert)
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
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Daily KBO pitcher log collector")
    parser.add_argument("date", nargs="?", help="YYYYMMDD (default: today in KST)")
    parser.add_argument("--upsert", action="store_true", help="update existing rows on conflict")
    args = parser.parse_args()

    game_date = args.date or _today_yyyymmdd_kst()
    print(f"[run] date={game_date} upsert={bool(args.upsert)}")
    collect_for_dates(dates=[game_date], upsert=args.upsert)


if __name__ == "__main__":
    main()
