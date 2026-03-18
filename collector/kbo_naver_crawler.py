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


def _safe_int(value: Any) -> int:
    try:
        text = str(value).strip().replace(",", "")
        if not text:
            return 0
        return int(float(text))
    except Exception:
        return 0


def _safe_float(value: Any) -> float:
    try:
        text = str(value).strip().replace(",", "")
        if not text:
            return 0.0
        return float(text)
    except Exception:
        return 0.0


def _pick_value(source: Dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in source and source.get(key) not in (None, ""):
            return source.get(key)
    return None


def _ip_to_outs(value: Any) -> int:
    text = str(value or "").strip()
    if not text:
        return 0
    if "." not in text:
        return _safe_int(text) * 3
    left, right = text.split(".", 1)
    whole = _safe_int(left)
    frac = _safe_int(right[:1])
    frac = min(max(frac, 0), 2)
    return whole * 3 + frac


def _normalize_pitcher_role(player: Dict[str, Any]) -> str:
    raw = str(
        _pick_value(
            player,
            "role",
            "positionName",
            "position",
            "pitcherRole",
            "pitcherType",
            "type",
        )
        or ""
    ).strip()
    if not raw:
        if _safe_int(_pick_value(player, "hold", "hld")) > 0:
            return "RP"
        if _safe_int(_pick_value(player, "save", "sv")) > 0:
            return "CL"
        return ""

    normalized = raw.upper()
    if any(token in normalized for token in ("START", "SP", "선발")):
        return "SP"
    if any(token in normalized for token in ("CLOSE", "CL", "마무리")):
        return "CL"
    if any(token in normalized for token in ("RELIEF", "RP", "중계", "구원")):
        return "RP"
    return raw


def fetch_game_record(
    game_id: str,
    debug: bool = False,
) -> Dict[str, Any]:
    url = f"{BASE_URL}/schedule/games/kbo/{game_id}/record"
    data = fetch_json(url)
    if not data or "result" not in data:
        return {}

    record_data = (data.get("result") or {}).get("recordData") or {}
    return record_data if isinstance(record_data, dict) else {}


def parse_naver_boxscore(
    game_id: str, game_date: str, away_team: str, home_team: str, debug: bool = False
) -> List[Dict[str, Any]]:
    record_data = fetch_game_record(game_id=game_id, debug=debug)
    if not record_data:
        return []

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


def parse_naver_pitcher_boxscore(
    game_id: str, game_date: str, away_team: str, home_team: str, debug: bool = False
) -> List[Dict[str, Any]]:
    record_data = fetch_game_record(game_id=game_id, debug=debug)
    if not record_data:
        return []

    pitchers_boxscore = record_data.get("pitchersBoxscore") or {}
    if not isinstance(pitchers_boxscore, dict):
        return []

    out_rows: List[Dict[str, Any]] = []
    for team_key, players in pitchers_boxscore.items():
        if not isinstance(players, list):
            continue
        for p in players:
            if not isinstance(p, dict):
                continue
            name = str(_pick_value(p, "playerName", "name", "nameDisplay") or "").strip()
            if not name:
                continue

            ip_value = _pick_value(p, "inn", "ip", "inning", "innings")
            outs = _ip_to_outs(ip_value)
            row = {
                "game_date": game_date,
                "game_id": game_id,
                "team": team_key,
                "player_name": name,
                "role": _normalize_pitcher_role(p),
                "W": _safe_int(_pick_value(p, "win", "w")),
                "L": _safe_int(_pick_value(p, "lose", "loss", "l")),
                "SV": _safe_int(_pick_value(p, "save", "sv")),
                "HLD": _safe_int(_pick_value(p, "hold", "hld")),
                "IP": round(outs / 3.0, 4),
                "OUTS": outs,
                "BF": _safe_int(_pick_value(p, "bf", "battersFaced")),
                "NP": _safe_int(_pick_value(p, "np", "pc", "pitches")),
                "H": _safe_int(_pick_value(p, "hit", "h")),
                "R": _safe_int(_pick_value(p, "run", "r")),
                "ER": _safe_int(_pick_value(p, "er")),
                "BB": _safe_int(_pick_value(p, "bb")),
                "SO": _safe_int(_pick_value(p, "so", "kk", "k")),
                "HR": _safe_int(_pick_value(p, "hr")),
                "HBP": _safe_int(_pick_value(p, "hp", "hbp")),
                "BK": _safe_int(_pick_value(p, "bk")),
                "WP": _safe_int(_pick_value(p, "wp")),
                "ERA": _safe_float(_pick_value(p, "era")),
            }
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
