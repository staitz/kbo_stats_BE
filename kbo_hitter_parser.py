from __future__ import annotations

"""
KBO GameCenter hitter parser (minimum viable stats).

Rules summary (extend as needed):
- Ignore empty/blank event codes.
- BB: "4구"
- SO: "삼진"
- 2B: contains "우2"/"좌2"/"중2" or "2루타"
- 3B: contains "우3"/"좌3"/"중3" or "3루타"
- HR: contains "홈런"
- H: includes HR/2B/3B plus single hits ("안타" or "1안")
- AB: exclude BB/HBP/SF, include others (heuristic)
- SF: contains "희비" or "희플"
- HBP: contains "사구"
"""

from typing import Any, Dict, List, Optional, Tuple


EVENT_RULES = {
    "ignore_tokens": {"", " "},
    "bb": ["4구"],
    "so": ["삼진"],
    "hbp": ["사구"],
    "sf": ["희비", "희플"],
    "double": ["우2", "좌2", "중2", "2루타"],
    "triple": ["우3", "좌3", "중3", "3루타"],
    "hr": ["홈런"],
    "single_patterns": ["안타", "1안"],
}


def _clean_events(events: List[str]) -> List[str]:
    cleaned = []
    for e in events:
        if e in EVENT_RULES["ignore_tokens"]:
            continue
        if isinstance(e, str):
            ee = e.strip()
            if ee in EVENT_RULES["ignore_tokens"]:
                continue
            cleaned.append(ee)
    return cleaned


def debug_hitter_shape(data: Dict[str, Any]) -> None:
    print("[debug] data.keys() =", list(data.keys()))

    arr = data.get("arrHitter") or []
    print("[debug] arrHitter count =", len(arr))

    for i, team_block in enumerate(arr):
        lineup = team_block.get("lineup") or []
        at_bats = team_block.get("atBats") or []
        first_lineup = lineup[0] if lineup else None
        first_player = None
        if isinstance(first_lineup, dict):
            first_player = first_lineup.get("이름")

        # heuristic: try to locate a team field
        team_field = None
        for key in ("team", "teamName", "clubName", "shortName", "awayTeam", "homeTeam"):
            if key in team_block:
                team_field = (key, team_block.get(key))
                break

        print(
            f"[debug] arrHitter[{i}] len(lineup)={len(lineup)} len(atBats)={len(at_bats)} "
            f"first_player={first_player} first_lineup={first_lineup} team_field={team_field}"
        )


def _infer_team_order(
    data: Dict[str, Any],
    away_team: str,
    home_team: str,
) -> Tuple[List[Optional[str]], bool, Optional[str]]:
    """
    Returns (team_names, order_assumed, hint_source)
    team_names length == len(arrHitter), with None for unknown.
    """
    arr = data.get("arrHitter") or []
    team_names: List[Optional[str]] = [None] * len(arr)

    # A) look for explicit away/home team names anywhere in data
    hints = {}
    for key in ("awayTeam", "homeTeam", "away_team", "home_team", "awayName", "homeName"):
        if key in data:
            hints[key] = data.get(key)

    # also check tableEtc if present
    table_etc = data.get("tableEtc")
    if isinstance(table_etc, dict):
        for key in ("awayTeam", "homeTeam", "awayName", "homeName"):
            if key in table_etc:
                hints[f"tableEtc.{key}"] = table_etc.get(key)

    if hints:
        away = hints.get("awayTeam") or hints.get("away_team") or hints.get("awayName")
        home = hints.get("homeTeam") or hints.get("home_team") or hints.get("homeName")
        away = away or hints.get("tableEtc.awayTeam") or hints.get("tableEtc.awayName")
        home = home or hints.get("tableEtc.homeTeam") or hints.get("tableEtc.homeName")

        if away and home and len(arr) >= 2:
            team_names[0] = away
            team_names[1] = home
            return team_names, False, "data/tableEtc away/home fields"

    # B) fallback to function args, but try to detect order via scoreboard hints
    # common patterns: lineScore/scoreBoard may list away first then home
    scoreboard_keys = ["lineScore", "linescore", "scoreBoard", "scoreboard", "score"]
    for key in scoreboard_keys:
        if key in data:
            sb = data.get(key)
            if isinstance(sb, dict):
                # try common fields
                away = sb.get("awayTeam") or sb.get("away") or sb.get("awayName")
                home = sb.get("homeTeam") or sb.get("home") or sb.get("homeName")
                if away and home and len(arr) >= 2:
                    team_names[0] = away
                    team_names[1] = home
                    return team_names, False, f"data.{key} away/home fields"

    # C) last resort: assume arrHitter[0]=away, arrHitter[1]=home
    if len(arr) >= 1:
        team_names[0] = away_team
    if len(arr) >= 2:
        team_names[1] = home_team

    return team_names, True, "assumed order: arrHitter[0]=away, arrHitter[1]=home"


def parse_events_to_stats(events: List[str]) -> Dict[str, int]:
    cleaned = _clean_events(events)

    stats = {
        "PA": len(cleaned),
        "AB": 0,
        "H": 0,
        "2B": 0,
        "3B": 0,
        "HR": 0,
        "BB": 0,
        "HBP": 0,
        "SO": 0,
        "SF": 0,
    }

    for e in cleaned:
        # BB
        if any(tok in e for tok in EVENT_RULES["bb"]):
            stats["BB"] += 1

        # SO
        if any(tok in e for tok in EVENT_RULES["so"]):
            stats["SO"] += 1

        # HBP
        if any(tok in e for tok in EVENT_RULES["hbp"]):
            stats["HBP"] += 1

        # SF
        if any(tok in e for tok in EVENT_RULES["sf"]):
            stats["SF"] += 1

        # Extra base hits
        if any(tok in e for tok in EVENT_RULES["double"]):
            stats["2B"] += 1
        if any(tok in e for tok in EVENT_RULES["triple"]):
            stats["3B"] += 1
        if any(tok in e for tok in EVENT_RULES["hr"]):
            stats["HR"] += 1

        # Hit detection
        is_hit = False
        if any(tok in e for tok in EVENT_RULES["hr"]):
            is_hit = True
        elif any(tok in e for tok in EVENT_RULES["double"]):
            is_hit = True
        elif any(tok in e for tok in EVENT_RULES["triple"]):
            is_hit = True
        elif any(tok in e for tok in EVENT_RULES["single_patterns"]):
            is_hit = True

        if is_hit:
            stats["H"] += 1

        # AB heuristic: exclude BB/HBP/SF
        if (
            not any(tok in e for tok in EVENT_RULES["bb"])
            and not any(tok in e for tok in EVENT_RULES["hbp"])
            and not any(tok in e for tok in EVENT_RULES["sf"])
        ):
            stats["AB"] += 1

    return stats


def parse_hitter_rows(
    data: Dict[str, Any],
    game_date: str,
    game_id: str,
    away_team: str,
    home_team: str,
) -> List[Dict[str, Any]]:
    arr = data.get("arrHitter") or []

    team_names, order_assumed, hint_source = _infer_team_order(
        data=data,
        away_team=away_team,
        home_team=home_team,
    )

    if order_assumed:
        print(f"[debug] team order assumed: {hint_source}")
    else:
        print(f"[debug] team order from hint: {hint_source}")

    rows: List[Dict[str, Any]] = []

    for team_idx, team_block in enumerate(arr):
        lineup = team_block.get("lineup") or []
        at_bats = team_block.get("atBats") or []

        # safety: length mismatch
        if len(at_bats) != len(lineup):
            print(
                f"[debug] length mismatch at team_idx={team_idx}: "
                f"len(lineup)={len(lineup)} len(atBats)={len(at_bats)}"
            )

        for i, player in enumerate(lineup):
            events = at_bats[i] if i < len(at_bats) else []
            events_clean = _clean_events(events)
            stats = parse_events_to_stats(events)

            row = {
                "game_date": game_date,
                "game_id": game_id,
                "team": team_names[team_idx] if team_idx < len(team_names) else None,
                "batting_order": int(player.get("타순")) if str(player.get("타순", "")).isdigit() else player.get("타순"),
                "position": player.get("포지션"),
                "player_name": player.get("이름"),
                "events": events_clean,
                **stats,
            }
            rows.append(row)

    return rows


if __name__ == "__main__":
    # minimal sample
    sample_data = {
        "tableEtc": {"foo": "bar"},
        "arrHitter": [
            {
                "lineup": [
                    {"타순": "1", "포지션": "중", "이름": "김지찬"},
                    {"타순": "2", "포지션": "2", "이름": "홍길동"},
                ],
                "atBats": [
                    ["유직", " ", "투병", " ", "1땅", " ", "4구", "좌비", " "],
                    ["투땅", " ", " ", "3직", " ", "삼진", "2비", " ", "2땅"],
                ],
            }
        ],
    }

    debug_hitter_shape(sample_data)
    rows = parse_hitter_rows(
        data=sample_data,
        game_date="20250610",
        game_id="20250610SSHT0",
        away_team="삼성",
        home_team="KT",
    )

    print("[debug] parsed rows sample:")
    for r in rows[:2]:
        print(r)


