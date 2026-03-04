import re
from typing import Any, Dict, List, Optional

import requests

BASE_URL = "https://api-gw.sports.naver.com"

DEFAULT_HEADERS = {
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "referer": "https://sports.news.naver.com/",
    "origin": "https://sports.news.naver.com",
    "accept": "application/json",
}


def fetch_json(url: str, timeout: int = 15) -> Optional[Dict[str, Any]]:
    try:
        response = requests.get(url, headers=DEFAULT_HEADERS, timeout=timeout)
        if response.ok:
            return response.json()
        print(f"[naver_crawler] non-200 response: {url} | status={response.status_code}")
    except Exception as e:
        print(f"[naver_crawler] exception during fetch: {url} | error: {e}")
    return None


def _normalize_game(game: Dict[str, Any], date_hint: str = "") -> Dict[str, Any]:
    date_str = str(game.get("gameDate") or date_hint or "")
    date_norm = date_str.replace("-", "")
    game_id = game.get("gameId")
    away = game.get("awayTeamName")
    home = game.get("homeTeamName")
    stadium = game.get("stadium")
    status = game.get("statusInfo")
    time_norm = game.get("gameTime")
    fallback_id = f"{date_norm}_{away}_{home}_{time_norm or 'NA'}"
    return {
        "date": date_norm,
        "time": time_norm,
        "away_team": away,
        "home_team": home,
        "stadium": stadium,
        "status": status,
        "status_confidence": 1.0,
        "status_reason": "naver_api",
        "game_id": game_id,
        "fallback_id": fallback_id,
    }


def fetch_month_schedule(
    season_id: str, game_month: str, debug: bool = False, *args, **kwargs
) -> List[dict]:
    if len(game_month) == 1:
        game_month = f"0{game_month}"

    url = (
        f"{BASE_URL}/schedule/games"
        f"?upperCategoryId=kbaseball&categoryId=kbo"
        f"&year={season_id}&month={game_month}"
    )
    data = fetch_json(url)
    if not data or "result" not in data:
        return []

    out: List[dict] = []
    games_or_days = (data.get("result") or {}).get("games") or []

    for item in games_or_days:
        if not isinstance(item, dict):
            continue

        # Some responses are grouped by date:
        #   {"gameDate":"YYYY-MM-DD", "games":[...]}
        if isinstance(item.get("games"), list):
            date_hint = str(item.get("gameDate") or "")
            for game in item.get("games", []):
                if isinstance(game, dict):
                    out.append(_normalize_game(game, date_hint=date_hint))
            continue

        # Some responses are flat game arrays:
        out.append(_normalize_game(item))

    return out


def fetch_day_schedule(date_yyyymmdd: str, debug: bool = False, *args, **kwargs) -> List[dict]:
    if len(date_yyyymmdd) < 8:
        return []
    year = date_yyyymmdd[:4]
    month = date_yyyymmdd[4:6]

    month_data = fetch_month_schedule(year, month, debug=debug)
    return [game for game in month_data if game.get("date") == date_yyyymmdd]


def parse_naver_boxscore(
    game_id: str, game_date: str, away_team: str, home_team: str, debug: bool = False
) -> List[Dict[str, Any]]:
    url = f"{BASE_URL}/schedule/games/kbo/{game_id}/record"
    data = fetch_json(url)
    if not data or "result" not in data:
        return []

    record_data = (data.get("result") or {}).get("recordData") or {}
    batters_boxscore = record_data.get("battersBoxscore") or {}
    if not isinstance(batters_boxscore, dict):
        return []

    out_rows: List[Dict[str, Any]] = []
    for team_key, players in batters_boxscore.items():
        if not isinstance(players, list):
            continue
        for p in players:
            if not isinstance(p, dict):
                continue
            name = p.get("playerName")
            if not name:
                continue

            row = {
                "game_date": game_date,
                "game_id": game_id,
                "team": team_key,
                "player_name": name,
                "AB": int(p.get("ab") or 0),
                "H": int(p.get("hit") or 0),
                "HR": int(p.get("hr") or 0),
                "BB": int(p.get("bb") or 0),
                "SO": int(p.get("so") or 0),
                "R": int(p.get("run") or p.get("r") or 0),
                "RBI": int(p.get("rbi") or 0),
                "SB": int(p.get("sb") or 0),
                "CS": int(p.get("cs") or 0),
                "SH": int(p.get("sh") or 0),
                "SF": int(p.get("sf") or 0),
                "HBP": int(p.get("hp") or 0),
                "GDP": int(p.get("gdp") or 0),
                "2B": 0,
                "3B": 0,
                "PA": 0,
                "TB": 0,
            }
            row["PA"] = row["AB"] + row["BB"] + row["HBP"] + row["SH"] + row["SF"]
            row["TB"] = row["H"] + (3 * row["HR"])
            out_rows.append(row)

    return out_rows


def find_season_start_date(season_id: str, debug: bool = False) -> Optional[str]:
    meta_url = f"{BASE_URL}/schedule/season?upperCategoryId=kbaseball&categoryId=kbo"
    meta = fetch_json(meta_url)
    if meta and isinstance(meta.get("result"), dict):
        start_date = str(meta["result"].get("startDate") or "")
        if re.match(r"^\d{4}-\d{2}-\d{2}$", start_date):
            return start_date.replace("-", "")

    # Fallback strategy if season metadata is missing.
    for month in ["03", "04"]:
        games = fetch_month_schedule(season_id, month)
        if games:
            return min(g["date"] for g in games if g.get("date"))
    return None
