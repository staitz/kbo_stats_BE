from __future__ import annotations

"""
KBO GameCenter hitter parser (minimum viable stats).

Rules summary (extend as needed):
- Ignore empty/blank event codes.
- BB: contains "4구"
- SO: contains "삼진"
- HBP: contains "사구"
- SF: contains "희비" or "희플"
- 2B: contains "우2"/"좌2"/"중2" or "2루타"
- 3B: contains "우3"/"좌3"/"중3" or "3루타"
- HR: contains "홈런"
- H: includes HR/2B/3B plus single hits ("안타", "1안", etc.)
- AB: AB = PA - BB - HBP - SF (minimum version)
"""

import json
import re
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
    # keep this simple and extend as needed
    # NOTE: "좌비/우비/중비/2비" are fly-out markers, not hits.
    "single_tokens": ["안타", "1안", "좌전안타", "우전안타", "중전안타", "내야안타", "번트안타", "1루타"],
}

SINGLE_REGEXES = [
    re.compile(r"1안"),
    re.compile(r"안타"),
    re.compile(r"1루타"),
]


def _clean_events(events: List[str]) -> List[str]:
    cleaned: List[str] = []
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
    if isinstance(arr, list) and arr:
        first_block = arr[0]
        if isinstance(first_block, dict):
            print("[debug] arrHitter[0] keys =", list(first_block.keys()))
        else:
            print("[debug] arrHitter[0] type =", type(first_block))

    for name in ("table1", "table2", "table3"):
        _debug_table_summary(name, data.get(name))

    for i, team_block in enumerate(arr):
        lineup = team_block.get("lineup") or []
        at_bats = team_block.get("atBats") or []
        first_lineup = lineup[0] if lineup else None
        first_player = None
        if isinstance(first_lineup, dict):
            first_player = first_lineup.get("이름")

        # heuristic: try to locate a team field
        team_field = None
        for key in (
            "team",
            "teamName",
            "clubName",
            "shortName",
            "awayTeam",
            "homeTeam",
        ):
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

    # A) look for explicit team names inside each team block
    team_keys = ("team", "teamName", "clubName", "shortName")
    per_block_found = True
    for i, team_block in enumerate(arr):
        found = None
        for key in team_keys:
            if key in team_block and team_block.get(key):
                found = team_block.get(key)
                break
        team_names[i] = found
        if not found:
            per_block_found = False

    if per_block_found and len(arr) > 0:
        return team_names, False, "arrHitter[*] team fields"

    # B) look for explicit away/home team names anywhere in data
    hints = {}
    for key in (
        "awayTeam",
        "homeTeam",
        "away_team",
        "home_team",
        "awayName",
        "homeName",
    ):
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

    # C) fallback to function args, but try to detect order via scoreboard hints
    scoreboard_keys = ["lineScore", "linescore", "scoreBoard", "scoreboard", "score"]
    for key in scoreboard_keys:
        if key in data:
            sb = data.get(key)
            if isinstance(sb, dict):
                away = sb.get("awayTeam") or sb.get("away") or sb.get("awayName")
                home = sb.get("homeTeam") or sb.get("home") or sb.get("homeName")
                if away and home and len(arr) >= 2:
                    team_names[0] = away
                    team_names[1] = home
                    return team_names, False, f"data.{key} away/home fields"

    # D) last resort: assume arrHitter[0]=away, arrHitter[1]=home
    if len(arr) >= 1:
        team_names[0] = away_team
    if len(arr) >= 2:
        team_names[1] = home_team

    return team_names, True, "assumed order: arrHitter[0]=away, arrHitter[1]=home"


def _has_any(event: str, tokens: List[str]) -> bool:
    return any(tok in event for tok in tokens)


def _is_single_hit(event: str) -> bool:
    if _has_any(event, EVENT_RULES["single_tokens"]):
        return True
    return any(rx.search(event) for rx in SINGLE_REGEXES)


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
        if _has_any(e, EVENT_RULES["bb"]):
            stats["BB"] += 1

        # SO
        if _has_any(e, EVENT_RULES["so"]):
            stats["SO"] += 1

        # HBP
        if _has_any(e, EVENT_RULES["hbp"]):
            stats["HBP"] += 1

        # SF
        if _has_any(e, EVENT_RULES["sf"]):
            stats["SF"] += 1

        # Extra base hits
        if _has_any(e, EVENT_RULES["double"]):
            stats["2B"] += 1
        if _has_any(e, EVENT_RULES["triple"]):
            stats["3B"] += 1
        if _has_any(e, EVENT_RULES["hr"]):
            stats["HR"] += 1

        # Hit detection
        is_hit = False
        if _has_any(e, EVENT_RULES["hr"]):
            is_hit = True
        elif _has_any(e, EVENT_RULES["double"]):
            is_hit = True
        elif _has_any(e, EVENT_RULES["triple"]):
            is_hit = True
        elif _is_single_hit(e):
            is_hit = True

        if is_hit:
            stats["H"] += 1

    # AB heuristic: exclude BB/HBP/SF
    stats["AB"] = max(0, stats["PA"] - stats["BB"] - stats["HBP"] - stats["SF"])

    return stats


def parse_hitter_rows(
    data: Dict[str, Any],
    game_date: str,
    game_id: str,
    away_team: str,
    home_team: str,
) -> List[Dict[str, Any]]:
    data_bs = data
    data_sb: Optional[Dict[str, Any]] = None
    if isinstance(data, dict) and ("boxscore" in data or "scoreboard" in data):
        if isinstance(data.get("boxscore"), dict):
            data_bs = data.get("boxscore")
        if isinstance(data.get("scoreboard"), dict):
            data_sb = data.get("scoreboard")

    arr = data_bs.get("arrHitter") or []

    team_names, order_assumed, hint_source = _infer_team_order(
        data=data_bs,
        away_team=away_team,
        home_team=home_team,
    )

    if order_assumed:
        print(f"[debug] team order assumed: {hint_source}")
        team_status = "order_assumed"
    else:
        print(f"[debug] team order from hint: {hint_source}")
        team_status = f"hint:{hint_source}"

    rows: List[Dict[str, Any]] = []

    has_lineup_path = _has_lineup_path(arr)
    has_table_path = _has_table_path(arr)

    if not has_lineup_path:
        if has_table_path:
            for team_idx, team_block in enumerate(arr):
                if not isinstance(team_block, dict):
                    continue
                table_rows = _parse_hitter_rows_from_table_bundle(
                    table1=team_block.get("table1"),
                    table2=team_block.get("table2"),
                    table3=team_block.get("table3"),
                    game_date=game_date,
                    game_id=game_id,
                    default_team=team_names[team_idx] if team_idx < len(team_names) else None,
                    team_status="table_fallback:boxscore",
                )
                rows.extend(table_rows)
            if rows:
                return rows

        if data_sb:
            table_rows = _parse_hitter_rows_from_table_bundle(
                table1=data_sb.get("table1"),
                table2=data_sb.get("table2"),
                table3=data_sb.get("table3"),
                game_date=game_date,
                game_id=game_id,
                default_team=None,
                team_status="table_fallback:scoreboard",
                allow_any_table=True,
            )
            if table_rows:
                return table_rows

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

            batting_order_raw = player.get("타순")
            batting_order = (
                int(batting_order_raw)
                if str(batting_order_raw).isdigit()
                else batting_order_raw
            )

            row = {
                "game_date": game_date,
                "game_id": game_id,
                "team": team_names[team_idx] if team_idx < len(team_names) else None,
                "batting_order": batting_order,
                "position": player.get("포지션"),
                "player_name": player.get("이름"),
                "PA": stats["PA"],
                "AB": stats["AB"],
                "H": stats["H"],
                "2B": stats["2B"],
                "3B": stats["3B"],
                "HR": stats["HR"],
                "BB": stats["BB"],
                "HBP": stats["HBP"],
                "SO": stats["SO"],
                "SF": stats["SF"],
                "events": events_clean,
                "team_status": team_status,
            }
            rows.append(row)

    return rows


def _has_lineup_path(arr: Any) -> bool:
    if not isinstance(arr, list):
        return False
    for team_block in arr:
        if not isinstance(team_block, dict):
            continue
        if team_block.get("lineup") or team_block.get("atBats"):
            return True
    return False


def _has_table_path(arr: Any) -> bool:
    if not isinstance(arr, list):
        return False
    for team_block in arr:
        if not isinstance(team_block, dict):
            continue
        if team_block.get("table1") or team_block.get("table2") or team_block.get("table3"):
            return True
    return False


def _safe_json_loads(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return None
    return value


def _table_len(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, (list, dict, str)):
        try:
            return len(value)
        except Exception:
            return None
    return None


def _header_text(header: Any) -> str:
    if isinstance(header, dict):
        for key in ("text", "label", "name", "title", "header", "Text"):
            val = header.get(key)
            if isinstance(val, str) and val.strip():
                return val.strip()
        return str(header)
    if isinstance(header, str):
        return header.strip()
    return str(header)


def _extract_headers(table: Any) -> List[str]:
    if not isinstance(table, dict):
        return []
    headers = table.get("headers") or table.get("header") or table.get("ths") or []
    if not isinstance(headers, list):
        return []
    extracted: List[str] = []
    for h in headers:
        if isinstance(h, dict) and isinstance(h.get("row"), list):
            extracted.extend([_header_text(cell) for cell in h.get("row")])
            continue
        extracted.append(_header_text(h))
    return [h for h in extracted if h]


def _extract_rows(table: Any) -> List[Any]:
    if not isinstance(table, dict):
        return []
    rows = table.get("rows") or table.get("row") or table.get("trs") or []
    return rows if isinstance(rows, list) else []


def _debug_table_summary(name: str, raw_value: Any) -> None:
    val_type = type(raw_value)
    val_len = _table_len(raw_value)
    print(f"[debug] {name} type={val_type} len={val_len}")
    parsed = _safe_json_loads(raw_value)
    if not isinstance(parsed, dict):
        return
    headers = _extract_headers(parsed)
    if headers:
        print(f"[debug] {name} headers sample={headers[:1]}")
    rows = _extract_rows(parsed)
    if rows:
        print(f"[debug] {name} rows[0] sample={rows[0]}")


def _normalize_key(value: str) -> str:
    return re.sub(r"\s+", "", value or "").lower()


def _normalize_player_name(name: Any) -> str:
    if not isinstance(name, str):
        return ""
    return re.sub(r"\s+", "", name.strip())


def _row_to_dict(row: Any, headers: List[str]) -> Dict[str, Any]:
    if isinstance(row, dict):
        if isinstance(row.get("row"), list):
            cells = [
                cell.get("Text") if isinstance(cell, dict) else cell
                for cell in row.get("row")
            ]
            return _row_to_dict(cells, headers)
        return row
    if isinstance(row, list):
        row_dict: Dict[str, Any] = {}
        for idx, cell in enumerate(row):
            key = headers[idx] if idx < len(headers) else f"col{idx}"
            row_dict[key] = cell
        return row_dict
    return {}


def _get_value_by_keys(row: Dict[str, Any], keys: List[str]) -> Optional[Any]:
    if not row:
        return None
    normalized = {_normalize_key(k): v for k, v in row.items() if isinstance(k, str)}
    for key in keys:
        norm = _normalize_key(key)
        if norm in normalized:
            return normalized[norm]
        for row_key, value in normalized.items():
            if norm and (norm in row_key or row_key in norm):
                return value
    return None


def _to_int(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        cleaned = value.strip().replace(",", "")
        if cleaned.isdigit():
            return int(cleaned)
        try:
            return int(float(cleaned))
        except Exception:
            return 0
    return 0


def _extract_events_from_row(row: Dict[str, Any]) -> List[str]:
    event_keys = ["events", "event", "이벤트", "타석결과", "타격결과", "타석내용", "결과"]
    raw = _get_value_by_keys(row, event_keys)
    if isinstance(raw, list):
        return _clean_events([str(x) for x in raw])
    if isinstance(raw, str):
        parts = re.split(r"[|,/]+", raw)
        return _clean_events([p.strip() for p in parts if p.strip()])
    return []


def _is_hitter_table(headers: List[str]) -> bool:
    if not headers:
        return False
    header_text = " ".join(headers)
    hitters = ["타수", "안타", "홈런", "삼진", "볼넷", "2루타", "3루타", "사구", "희비", "타석"]
    hits = sum(1 for h in hitters if h in header_text)
    return hits >= 2


def _hitter_header_score(headers: List[str]) -> int:
    if not headers:
        return 0
    header_text = " ".join(headers)
    hitters = ["타수", "안타", "홈런", "삼진", "볼넷", "2루타", "3루타", "사구", "희비", "타석"]
    return sum(1 for h in hitters if h in header_text)


def _choose_hitter_table(table2: Any, table3: Any) -> Tuple[Optional[Any], Optional[str]]:
    if table2 is None and table3 is None:
        return None, None
    headers2 = _extract_headers(table2)
    headers3 = _extract_headers(table3)
    score2 = _hitter_header_score(headers2)
    score3 = _hitter_header_score(headers3)
    if score2 == 0 and score3 == 0 and not _is_hitter_table(headers2) and not _is_hitter_table(headers3):
        return None, None
    if _is_hitter_table(headers2) and not _is_hitter_table(headers3):
        return table2, "table2"
    if _is_hitter_table(headers3) and not _is_hitter_table(headers2):
        return table3, "table3"
    if _is_hitter_table(headers2) and _is_hitter_table(headers3):
        if len(_extract_rows(table2)) >= len(_extract_rows(table3)):
            return table2, "table2"
        return table3, "table3"
    if score2 >= score3:
        return table2, "table2"
    return table3, "table3"


def _parse_lineup_table(table: Any) -> List[Dict[str, Any]]:
    headers = _extract_headers(table)
    rows = _extract_rows(table)
    lineup: List[Dict[str, Any]] = []
    for idx, row in enumerate(rows):
        row_dict = _row_to_dict(row, headers)
        batting_order = _get_value_by_keys(row_dict, ["타순", "순번", "타순/포지션", "battingorder", "order"])
        position = _get_value_by_keys(row_dict, ["포지션", "수비", "position"])
        name = _get_value_by_keys(row_dict, ["선수", "선수명", "이름", "타자", "player", "playername"])
        team = _get_value_by_keys(row_dict, ["팀", "구단", "team", "club"])
        if name is None:
            continue
        lineup.append(
            {
                "batting_order": batting_order if batting_order is not None else idx + 1,
                "position": position,
                "player_name": name,
                "team": team,
            }
        )
    return lineup


def _parse_stats_table(table: Any) -> List[Dict[str, Any]]:
    headers = _extract_headers(table)
    rows = _extract_rows(table)
    stats_rows: List[Dict[str, Any]] = []
    stat_keys = {
        "PA": ["타석", "pa"],
        "AB": ["타수", "ab"],
        "H": ["안타", "h"],
        "2B": ["2루타", "2b", "2루"],
        "3B": ["3루타", "3b", "3루"],
        "HR": ["홈런", "hr"],
        "BB": ["볼넷", "bb", "4구"],
        "HBP": ["사구", "hbp"],
        "SO": ["삼진", "so"],
        "SF": ["희비", "희플", "sf"],
    }
    for row in rows:
        row_dict = _row_to_dict(row, headers)
        name = _get_value_by_keys(row_dict, ["선수", "선수명", "이름", "타자", "player", "playername"])
        team = _get_value_by_keys(row_dict, ["팀", "구단", "team", "club"])
        events = _extract_events_from_row(row_dict)
        stat_values = {k: _to_int(_get_value_by_keys(row_dict, v)) for k, v in stat_keys.items()}
        stats_rows.append(
            {
                "player_name": name,
                "team": team,
                "events": events,
                "stats": stat_values,
            }
        )
    return stats_rows


def _parse_hitter_rows_from_table_bundle(
    table1: Any,
    table2: Any,
    table3: Any,
    game_date: str,
    game_id: str,
    default_team: Optional[str],
    team_status: str,
    allow_any_table: bool = False,
) -> List[Dict[str, Any]]:
    table1 = _safe_json_loads(table1)
    table2 = _safe_json_loads(table2)
    table3 = _safe_json_loads(table3)

    lineup_rows = _parse_lineup_table(table1) if table1 else []
    hitter_table, hitter_table_name = _choose_hitter_table(table2, table3)
    if hitter_table is None and allow_any_table:
        if table2 is not None:
            hitter_table = table2
            hitter_table_name = "table2_unverified"
        elif table3 is not None:
            hitter_table = table3
            hitter_table_name = "table3_unverified"
    stats_rows = _parse_stats_table(hitter_table) if hitter_table else []
    if not lineup_rows and not stats_rows:
        return []

    stats_by_name: Dict[str, Dict[str, Any]] = {}
    for row in stats_rows:
        name = row.get("player_name")
        norm = _normalize_player_name(name)
        if not norm:
            continue
        stats_by_name[norm] = row

    rows: List[Dict[str, Any]] = []
    source_rows = lineup_rows or [
        {
            "batting_order": None,
            "position": None,
            "player_name": r.get("player_name"),
            "team": r.get("team"),
        }
        for r in stats_rows
    ]

    status = team_status or "table_fallback"
    if hitter_table_name:
        status = f"{status}:{hitter_table_name}"

    for item in source_rows:
        player_name = item.get("player_name")
        norm = _normalize_player_name(player_name)
        stats_entry = stats_by_name.get(norm, {})
        stats = stats_entry.get("stats") or {
            "PA": 0,
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
        events = stats_entry.get("events") or []
        if events and not any(stats.values()):
            derived = parse_events_to_stats(events)
            stats = derived

        team = item.get("team") or stats_entry.get("team") or default_team

        batting_order_raw = item.get("batting_order")
        batting_order = (
            int(batting_order_raw)
            if str(batting_order_raw).isdigit()
            else batting_order_raw
        )

        rows.append(
            {
                "game_date": game_date,
                "game_id": game_id,
                "team": team,
                "batting_order": batting_order,
                "position": item.get("position"),
                "player_name": player_name,
                "PA": stats.get("PA", 0),
                "AB": stats.get("AB", 0),
                "H": stats.get("H", 0),
                "2B": stats.get("2B", 0),
                "3B": stats.get("3B", 0),
                "HR": stats.get("HR", 0),
                "BB": stats.get("BB", 0),
                "HBP": stats.get("HBP", 0),
                "SO": stats.get("SO", 0),
                "SF": stats.get("SF", 0),
                "events": events,
                "team_status": status,
            }
        )

    return rows


if __name__ == "__main__":
    # placeholder sample (replace with real data dict at runtime)
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
