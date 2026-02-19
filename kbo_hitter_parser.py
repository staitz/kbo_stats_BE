from __future__ import annotations

"""
KBO GameCenter hitter parser (minimum viable stats).

Rules summary (extend as needed):
- Ignore empty/blank event codes.
- BB: contains walk tokens ("4구", "볼넷", "고의4구", etc.)
- SO: contains "삼진"
- HBP: contains "사구"
- SH: contains "희번" or "희생번트"
- SF: contains "희비" or "희플"
- 2B: contains "우2"/"좌2"/"중2" or "2루타"
- 3B: contains "우3"/"좌3"/"중3" or "3루타"
- HR: contains "홈런"
- H: includes HR/2B/3B plus single hits ("안타", "1안", etc.)
- AB: AB = PA - BB - HBP - SF - SH (minimum version)
"""

import json
import re
from typing import Any, Dict, List, Optional, Tuple

try:
    import pandas as pd
except Exception:  # pragma: no cover - optional dependency
    pd = None


EVENT_RULES = {
    "ignore_tokens": {"", " ", "-", "0"},
    "bb": ["4구", "볼넷", "고4", "고의4구", "자동고의4구", "자동 고의4구", "고의 볼넷"],
    "so": ["삼진"],
    "hbp": ["사구"],
    "sf": ["희비", "희플"],
    "sh": ["희번", "희생번트", "희타"],
    "double": ["우2", "좌2", "중2", "2루타"],
    "triple": ["우3", "좌3", "중3", "3루타"],
    "hr": ["홈런", "홈"],
    # keep this simple and extend as needed
    # NOTE: "좌비/우비/중비/2비" are fly-out markers, not hits.
    "single_tokens": ["안타", "1안", "좌전안타", "우전안타", "중전안타", "내야안타", "번트안타", "1루타"],
}

SINGLE_REGEXES = [
    re.compile(r"1안"),
    re.compile(r"안타"),
    re.compile(r"1루타"),
]

HITTER_COLUMN_ALIASES = {
    "player_name": [
        "선수", "선수명", "이름", "타자", "타격", "player", "batter", "playername"
    ],
    "team": [
        "팀", "구단", "team", "club"
    ],
    "AB": ["타수", "ab"],
    "H": ["안타", "h"],
    "2B": ["2루타", "2b", "2루"],
    "3B": ["3루타", "3b", "3루"],
    "HR": ["홈런", "hr"],
    "BB": ["볼넷", "bb", "4구", "고의4구", "자동고의4구", "자동 고의4구", "고의 볼넷"],
    "IBB": ["고4", "고의4구", "자동고의4구", "자동 고의4구", "ibb"],
    "HBP": ["사구", "hbp"],
    "SH": ["희번", "희생번트", "sh"],
    "SF": ["희비", "희플", "희생플라이", "sf"],
    "R": ["득점", "r", "run"],
    "RBI": ["타점", "rbi"],
    "TB": ["루타", "tb", "totalbases", "totalbase"],
    "PA": ["타석", "pa"],
    "SB": ["도루", "sb"],
    "CS": ["도루사", "cs"],
    "GDP": ["병살", "병살타", "gdp"],
    "SO": ["삼진", "so"],
}

# 헤더에서 최소 2개 이상 잡히면 타자 테이블로 인식
HITTER_TABLE_TOKENS = ["타수", "ab", "안타", "h", "홈런", "hr", "삼진", "so", "볼넷", "bb", "타점", "득점"]
PITCHER_TABLE_TOKENS = ["등판", "이닝", "투구수", "자책", "평균자책점", "승", "패", "세", "홀드", "세이브"]


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


def _normalize_header_text(value: Any) -> str:
    # 공백/기호 제거 + 소문자화로 헤더 비교를 안정화
    if value is None:
        return ""
    if not isinstance(value, str):
        value = str(value)
    text = re.sub(r"<[^>]+>", "", value)
    text = text.strip().lower()
    text = re.sub(r"[()\[\]{}./]", " ", text)
    text = re.sub(r"\s+", "", text)
    return text


def _flatten_columns(columns: Any) -> List[str]:
    # MultiIndex 컬럼을 "상위 하위" 형태로 펼침
    if pd is not None and isinstance(columns, pd.MultiIndex):
        out: List[str] = []
        for col in columns:
            parts = [str(p) for p in col if p and str(p) != "nan"]
            out.append(" ".join(parts).strip())
        return out
    return [str(c).strip() for c in list(columns)]


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
    if re.search(r"(?:[좌우중]|[1-9])안", event):
        return True
    return any(rx.search(event) for rx in SINGLE_REGEXES)


def _is_double_hit(event: str) -> bool:
    if _has_any(event, EVENT_RULES["double"]):
        return True
    return bool(re.search(r"(?:[좌우중][좌우중]?2|2루타)", event))


def _is_triple_hit(event: str) -> bool:
    if _has_any(event, EVENT_RULES["triple"]):
        return True
    return bool(re.search(r"(?:[좌우중][좌우중]?3|3루타)", event))


def _is_home_run_hit(event: str) -> bool:
    if _has_any(event, EVENT_RULES["hr"]):
        return True
    return False


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
        "SH": 0,
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

        # SH
        if _has_any(e, EVENT_RULES["sh"]):
            stats["SH"] += 1

        # SF
        if _has_any(e, EVENT_RULES["sf"]):
            stats["SF"] += 1

        # Extra base hits
        if _is_double_hit(e):
            stats["2B"] += 1
        if _is_triple_hit(e):
            stats["3B"] += 1
        if _is_home_run_hit(e):
            stats["HR"] += 1

        # Hit detection
        is_hit = False
        if _is_home_run_hit(e):
            is_hit = True
        elif _is_double_hit(e):
            is_hit = True
        elif _is_triple_hit(e):
            is_hit = True
        elif _is_single_hit(e):
            is_hit = True

        if is_hit:
            stats["H"] += 1

    # AB heuristic: exclude BB/HBP/SF
    stats["AB"] = max(
        0, stats["PA"] - stats["BB"] - stats["HBP"] - stats["SF"] - stats["SH"]
    )

    return stats


def _map_hitter_columns(headers: List[str]) -> Dict[str, Optional[str]]:
    # 다양한 헤더 표기를 통일해 필요한 컬럼을 찾는다
    normalized = {_normalize_header_text(h): h for h in headers}
    mapped: Dict[str, Optional[str]] = {k: None for k in HITTER_COLUMN_ALIASES.keys()}
    for key, aliases in HITTER_COLUMN_ALIASES.items():
        for alias in aliases:
            norm = _normalize_header_text(alias)
            if norm in normalized:
                mapped[key] = normalized[norm]
                break
            for header_norm, header_raw in normalized.items():
                if norm and (norm in header_norm or header_norm in norm):
                    mapped[key] = header_raw
                    break
            if mapped[key]:
                break
    return mapped


def _is_hitter_table_headers(headers: List[str]) -> bool:
    # 타자 테이블 판별: 핵심 스탯 컬럼 2개 이상
    if not headers:
        return False
    header_norm = " ".join(_normalize_header_text(h) for h in headers)
    hits = sum(1 for tok in HITTER_TABLE_TOKENS if tok in header_norm)
    pitcher_hits = sum(1 for tok in PITCHER_TABLE_TOKENS if tok in header_norm)
    return hits >= 2 and pitcher_hits == 0


def _is_name_table_headers(headers: List[str]) -> bool:
    if not headers:
        return False
    header_norm = " ".join(_normalize_header_text(h) for h in headers)
    return "선수명" in header_norm or "선수" in header_norm or "player" in header_norm


def _is_event_table_headers(headers: List[str]) -> bool:
    # 이닝별 이벤트 테이블 판별: 1,2,3... 같은 숫자 헤더 비율이 높음
    if not headers:
        return False
    inning_like = 0
    total = 0
    for h in headers:
        text = str(h).strip()
        if not text:
            continue
        total += 1
        if re.fullmatch(r"\d{1,2}", text):
            inning_like += 1
    if total == 0:
        return False
    return total >= 7 and inning_like >= max(7, int(total * 0.7))


def _normalize_team_label(value: str) -> str:
    if not value:
        return ""
    text = value.strip()
    text = re.sub(r"\s+", " ", text)
    text = text.replace("타자 기록", "").strip()
    return text


def _normalize_team_name(team: str, away_team: str, home_team: str) -> str:
    if not team:
        return team
    if away_team and away_team in team:
        return away_team
    if home_team and home_team in team:
        return home_team
    return team


def _extract_player_name_from_row(row: List[Any]) -> str:
    if not row:
        return ""
    # 보통 마지막 컬럼이 선수명
    candidate = str(row[-1]).strip()
    if candidate:
        return candidate
    # 한글 이름이 포함된 셀을 탐색
    for cell in row:
        cell_text = str(cell).strip()
        if re.search(r"[가-힣]", cell_text):
            return cell_text
    return ""


def _to_int_html(value: Any) -> int:
    # '-', 공백 등은 0 처리
    if value is None:
        return 0
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value).strip()
    if text in {"", "-", "—", "–"}:
        return 0
    text = re.sub(r"[,\s]", "", text)
    if text.isdigit():
        return int(text)
    try:
        return int(float(text))
    except Exception:
        return 0


def _is_summary_player(name: str) -> bool:
    # 합계/팀합계 행 제거
    if not name:
        return True
    clean = _normalize_header_text(name)
    return clean in {"합계", "팀합계", "team", "total"}


def _extract_team_from_table(df) -> Optional[str]:
    # pandas가 캡션을 제공하면 팀명 힌트로 사용
    if df is None:
        return None
    try:
        caption = getattr(df, "attrs", {}).get("caption")
    except Exception:
        caption = None
    if isinstance(caption, str) and caption.strip():
        return caption.strip()
    return None


def parse_hitter_rows_from_html(
    html: str,
    game_date: str,
    game_id: str,
    away_team: str,
    home_team: str,
    debug: bool = False,
    text_fallback: Optional[str] = None,
) -> List[Dict[str, Any]]:
    def _read_html_tables(raw_html: str) -> List[Any]:
        if pd is None:
            return []
        try:
            return pd.read_html(raw_html)
        except Exception:
            return []

    if pd is None:
        if debug:
            print("[debug] pandas not available; cannot parse HTML tables")
        return parse_hitter_rows_from_text(
            text=text_fallback or "",
            game_date=game_date,
            game_id=game_id,
            away_team=away_team,
            home_team=home_team,
            debug=debug,
        )

    tables = _read_html_tables(html)
    if not tables:
        # HTML 전체 파싱 실패 시 table 조각 단위로 재시도
        fragments = re.findall(r"<table[\s\S]*?</table>", html, re.IGNORECASE)
        if debug:
            print(f"[debug] pandas.read_html failed; fragments={len(fragments)}")
        for frag in fragments:
            tables.extend(_read_html_tables(frag))

    if not tables:
        if debug:
            print("[debug] pandas.read_html failed")
        return parse_hitter_rows_from_text(
            text=text_fallback or "",
            game_date=game_date,
            game_id=game_id,
            away_team=away_team,
            home_team=home_team,
            debug=debug,
        )

    candidates: List[Tuple[int, Any, List[str]]] = []
    all_headers: List[Tuple[int, List[str]]] = []
    for idx, df in enumerate(tables):
        headers = _flatten_columns(df.columns)
        all_headers.append((idx, headers))
        if debug:
            print(f"[debug] table#{idx} headers={headers}")
        if _is_hitter_table_headers(headers):
            candidates.append((idx, df, headers))

    if not candidates:
        if debug:
            print("[debug] no hitter table candidates found")
            for idx, headers in all_headers:
                print(f"[debug] candidate_headers table#{idx}={headers}")
        return []

    rows: List[Dict[str, Any]] = []

    team_defaults: List[Optional[str]] = []
    if len(candidates) == 2:
        team_defaults = [away_team, home_team]
    else:
        team_defaults = [None for _ in candidates]

    for i, (idx, df, headers) in enumerate(candidates):
        mapped = _map_hitter_columns(headers)
        team_default = team_defaults[i] if i < len(team_defaults) else None
        team_hint = _extract_team_from_table(df)

        for _, row in df.iterrows():
            row_dict = row.to_dict()
            name_col = mapped.get("player_name")
            player_name = row_dict.get(name_col) if name_col else None
            player_name = str(player_name).strip() if player_name is not None else ""
            if _is_summary_player(player_name):
                continue

            team_col = mapped.get("team")
            team_val = row_dict.get(team_col) if team_col else None
            team = str(team_val).strip() if team_val is not None else None
            if not team:
                team = team_hint or team_default

            bb_total = _to_int_html(row_dict.get(mapped.get("BB"))) + _to_int_html(
                row_dict.get(mapped.get("IBB"))
            )
            row_out = {
                "game_date": game_date,
                "game_id": game_id,
                "team": team,
                "player_name": player_name,
                "AB": _to_int_html(row_dict.get(mapped.get("AB"))),
                "H": _to_int_html(row_dict.get(mapped.get("H"))),
                "HR": _to_int_html(row_dict.get(mapped.get("HR"))),
                "BB": bb_total,
                "SO": _to_int_html(row_dict.get(mapped.get("SO"))),
            }
            rows.append(row_out)

    return rows


def parse_hitter_rows_from_text(
    text: str,
    game_date: str,
    game_id: str,
    away_team: str,
    home_team: str,
    debug: bool = False,
) -> List[Dict[str, Any]]:
    """
    테이블 태그가 없을 때를 대비한 텍스트 기반 파서.
    - "OOO 타자 기록" 블록을 찾고
    - 헤더에 AB/H/HR/BB/SO 중 2개 이상 포함된 라인 이후의 row를 파싱
    - 선수명이 없는 행은 제외
    """
    if not text:
        if debug:
            print("[debug] text_fallback empty")
        return []

    norm = text.replace("\xa0", " ")
    norm = re.sub(r"\r\n|\r", "\n", norm)
    lines = [re.sub(r"\s+", " ", ln).strip() for ln in norm.split("\n")]
    lines = [ln for ln in lines if ln]

    team_blocks: List[Tuple[str, List[str]]] = []
    current_team = None
    current_lines: List[str] = []

    team_pat = re.compile(r"(.+?)\s+타자\s*기록")
    stop_tokens = ["투수 기록", "개인정보", "Copyright"]

    for ln in lines:
        if any(tok in ln for tok in stop_tokens):
            if current_team and current_lines:
                team_blocks.append((current_team, current_lines))
            current_team = None
            current_lines = []
            continue

        m = team_pat.search(ln)
        if m:
            if current_team and current_lines:
                team_blocks.append((current_team, current_lines))
            current_team = m.group(1).strip()
            current_lines = []
            continue

        if current_team:
            current_lines.append(ln)

    if current_team and current_lines:
        team_blocks.append((current_team, current_lines))

    if debug:
        print(f"[debug] text team_blocks={len(team_blocks)}")

    rows: List[Dict[str, Any]] = []
    for team_name, block in team_blocks:
        header_idx = -1
        header_cols: List[str] = []
        for i, ln in enumerate(block):
            header_cols = ln.split(" ")
            if _is_hitter_table_headers(header_cols):
                header_idx = i
                break

        if header_idx == -1:
            if debug:
                print(f"[debug] no hitter header in team block={team_name}")
            continue

        mapped = _map_hitter_columns(header_cols)

        for ln in block[header_idx + 1:]:
            if ln.upper() == "TOTAL":
                break
            if "타자 기록" in ln or "투수 기록" in ln:
                break
            parts = ln.split(" ")
            if len(parts) < 2:
                continue

            row_dict = {header_cols[i]: parts[i] if i < len(parts) else "" for i in range(len(header_cols))}
            name_col = mapped.get("player_name")
            player_name = row_dict.get(name_col) if name_col else None
            player_name = str(player_name).strip() if player_name is not None else ""
            if _is_summary_player(player_name):
                continue

            row_out = {
                "game_date": game_date,
                "game_id": game_id,
                "team": team_name or away_team or home_team,
                "player_name": player_name,
                "AB": _to_int_html(row_dict.get(mapped.get("AB"))),
                "H": _to_int_html(row_dict.get(mapped.get("H"))),
                "HR": _to_int_html(row_dict.get(mapped.get("HR"))),
                "BB": _to_int_html(row_dict.get(mapped.get("BB"))),
                "SO": _to_int_html(row_dict.get(mapped.get("SO"))),
            }
            rows.append(row_out)

    return rows


def parse_hitter_rows_from_dom_tables(
    tables: List[Dict[str, Any]],
    game_date: str,
    game_id: str,
    away_team: str,
    home_team: str,
    debug: bool = False,
) -> List[Dict[str, Any]]:
    """
    Selenium JS로 추출한 DOM 테이블 배열을 직접 파싱한다.
    tables item 예시:
    {
      "index": 3,
      "team": "삼성 라이온즈",
      "headers": ["타수", "안타", ...],
      "rows": [["김지찬", "4", "1", ...], ...],
      "text": "..."
    }
    """
    if not tables:
        if debug:
            print("[debug] dom tables empty")
        return []

    # 1) 테이블 메타 정리
    metas: List[Dict[str, Any]] = []
    for t in tables:
        headers = t.get("headers") or []
        rows = t.get("rows") or []
        if debug:
            print(
                f"[debug] dom table#{t.get('index')} headers_len={len(headers)} rows_len={len(rows)} "
                f"headers={headers[:10]}"
            )
        metas.append(
            {
                "index": t.get("index"),
                "team": _normalize_team_label(t.get("team") or ""),
                "headers": headers,
                "rows": rows,
            }
        )

    # 2) 이름 테이블 + 스탯 테이블 매칭
    name_tables = [m for m in metas if _is_name_table_headers(m["headers"])]
    name_tables = sorted(name_tables, key=lambda x: int(x["index"]))
    stat_tables = [m for m in metas if _is_hitter_table_headers(m["headers"])]
    stat_tables = sorted(stat_tables, key=lambda x: int(x["index"]))
    event_tables = [m for m in metas if _is_event_table_headers(m["headers"])]
    event_tables = sorted(event_tables, key=lambda x: int(x["index"]))

    # 이름 테이블 기준으로 팀 순서 보정
    if len(name_tables) == 2:
        for i, nt in enumerate(name_tables):
            inferred = away_team if i == 0 else home_team
            if not nt.get("team") or nt.get("team") not in {away_team, home_team}:
                nt["team"] = inferred

    rows_out: List[Dict[str, Any]] = []
    for stat_idx, stat in enumerate(stat_tables):
        # 가까운 이름 테이블 찾기 (행 수가 유사하고 인접한 테이블)
        candidates = []
        for name in name_tables:
            stat_idx_val = int(stat["index"])
            name_idx_val = int(name["index"])
            if name_idx_val >= stat_idx_val:
                continue
            idx_gap = stat_idx_val - name_idx_val
            row_gap = abs(len(stat["rows"]) - len(name["rows"]))
            if idx_gap <= 3 and row_gap <= 1:
                candidates.append((row_gap, idx_gap, name))
        if not candidates:
            continue
        candidates.sort(key=lambda x: (x[0], x[1]))
        name_tbl = candidates[0][2]

        team = name_tbl.get("team") or stat.get("team") or ""
        team = _normalize_team_name(team, away_team, home_team)
        if not team:
            team = away_team if len(rows_out) == 0 else home_team

        headers = stat["headers"]
        mapped = _map_hitter_columns(headers)
        event_tbl = None
        name_idx = int(name_tbl["index"])
        stat_idx_val = int(stat["index"])
        event_candidates = []
        for evt in event_tables:
            evt_idx = int(evt["index"])
            if evt_idx <= name_idx:
                continue
            idx_gap = abs(stat_idx_val - evt_idx)
            row_gap = abs(len(evt["rows"]) - len(name_tbl["rows"]))
            if row_gap <= 1 and idx_gap <= 3:
                event_candidates.append((idx_gap, row_gap, evt))
        if event_candidates:
            event_candidates.sort(key=lambda x: (x[0], x[1]))
            event_tbl = event_candidates[0][2]

        for r_idx, row in enumerate(stat["rows"]):
            if not isinstance(row, list) or not row:
                continue
            if row[0] and str(row[0]).strip().upper() == "TOTAL":
                break

            name_row = name_tbl["rows"][r_idx] if r_idx < len(name_tbl["rows"]) else []
            player_name = _extract_player_name_from_row(name_row)
            if _is_summary_player(player_name):
                continue

            row_dict = {headers[i]: row[i] if i < len(row) else "" for i in range(len(headers))}
            stats_raw = {
                "AB": _to_int_html(row_dict.get(mapped.get("AB"))),
                "H": _to_int_html(row_dict.get(mapped.get("H"))),
                "2B": _to_int_html(row_dict.get(mapped.get("2B"))),
                "3B": _to_int_html(row_dict.get(mapped.get("3B"))),
                "HR": _to_int_html(row_dict.get(mapped.get("HR"))),
                "BB": _to_int_html(row_dict.get(mapped.get("BB"))),
                "IBB": _to_int_html(row_dict.get(mapped.get("IBB"))),
                "HBP": _to_int_html(row_dict.get(mapped.get("HBP"))),
                "SH": _to_int_html(row_dict.get(mapped.get("SH"))),
                "SF": _to_int_html(row_dict.get(mapped.get("SF"))),
                "R": _to_int_html(row_dict.get(mapped.get("R"))),
                "RBI": _to_int_html(row_dict.get(mapped.get("RBI"))),
                "TB": _to_int_html(row_dict.get(mapped.get("TB"))),
                "PA": _to_int_html(row_dict.get(mapped.get("PA"))),
                "SB": _to_int_html(row_dict.get(mapped.get("SB"))),
                "CS": _to_int_html(row_dict.get(mapped.get("CS"))),
                "GDP": _to_int_html(row_dict.get(mapped.get("GDP"))),
                "SO": _to_int_html(row_dict.get(mapped.get("SO"))),
            }
            event_stats = {
                "PA": 0,
                "AB": 0,
                "H": 0,
                "2B": 0,
                "3B": 0,
                "HR": 0,
                "BB": 0,
                "HBP": 0,
                "SH": 0,
                "SF": 0,
                "SO": 0,
                "TB": 0,
            }
            if event_tbl and r_idx < len(event_tbl["rows"]):
                evt_row = event_tbl["rows"][r_idx]
                if isinstance(evt_row, list):
                    events = [str(v).strip() for v in evt_row if str(v).strip()]
                    event_stats = parse_events_to_stats(events)

            # 상세 스탯은 이벤트 기반으로 보강 (기본 스탯 테이블은 AB/H/RBI/R 위주인 경우가 많음)
            for key in ("2B", "3B", "HR", "BB", "HBP", "SH", "SF", "SO"):
                if stats_raw.get(key, 0) == 0 and event_stats.get(key, 0) > 0:
                    stats_raw[key] = int(event_stats[key])
            # KBO boxscore may split intentional walks into a separate "고4/IBB" column.
            # Persist BB as total walks by folding IBB into BB.
            stats_raw["BB"] = int(stats_raw.get("BB", 0)) + int(stats_raw.get("IBB", 0))
            has_pa_column = mapped.get("PA") is not None
            if not has_pa_column:
                stats_raw["PA"] = 0
            elif stats_raw["PA"] == 0 and event_stats.get("PA", 0) > 0:
                stats_raw["PA"] = int(event_stats["PA"])
            # H는 기본 스탯 테이블 우선, 없으면 이벤트 기반
            if stats_raw["H"] == 0 and event_stats.get("H", 0) > 0:
                stats_raw["H"] = int(event_stats["H"])
            if stats_raw["TB"] == 0:
                stats_raw["TB"] = _calc_tb(stats_raw)
            # REVIEW 섹션은 PA 컬럼이 없는 경우가 많아서 이벤트 PA를 그대로 쓰면
            # 행 오프셋/교체 표기로 인해 AB+BB+HBP+SF+SH와 어긋날 수 있다.
            if stats_raw["PA"] == 0:
                stats_raw["PA"] = (
                    stats_raw["AB"]
                    + stats_raw["BB"]
                    + stats_raw["HBP"]
                    + stats_raw["SF"]
                    + stats_raw.get("SH", 0)
                )
            rows_out.append(
                {
                    "game_date": game_date,
                    "game_id": game_id,
                    "team": team,
                    "player_name": player_name,
                    "AB": stats_raw["AB"],
                    "H": stats_raw["H"],
                    "2B": stats_raw["2B"],
                    "3B": stats_raw["3B"],
                    "HR": stats_raw["HR"],
                    "BB": stats_raw["BB"],
                    "HBP": stats_raw["HBP"],
                    "SH": stats_raw["SH"],
                    "SF": stats_raw["SF"],
                    "R": stats_raw["R"],
                    "RBI": stats_raw["RBI"],
                    "TB": stats_raw["TB"],
                    "PA": stats_raw["PA"],
                    "SB": stats_raw["SB"],
                    "CS": stats_raw["CS"],
                    "GDP": stats_raw["GDP"],
                    "SO": stats_raw["SO"],
                }
            )

    return rows_out


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


def _calc_tb(stats: Dict[str, int]) -> int:
    if stats.get("TB", 0) > 0:
        return stats["TB"]
    h = stats.get("H", 0)
    d2 = stats.get("2B", 0)
    d3 = stats.get("3B", 0)
    hr = stats.get("HR", 0)
    single = max(0, h - d2 - d3 - hr)
    return single + 2 * d2 + 3 * d3 + 4 * hr


def calc_ops(stats: Dict[str, int]) -> float:
    # OBP = (H + BB + HBP) / (AB + BB + HBP + SF)
    # SLG = TB / AB
    ab = stats.get("AB", 0)
    h = stats.get("H", 0)
    bb = stats.get("BB", 0)
    hbp = stats.get("HBP", 0)
    sf = stats.get("SF", 0)
    tb = _calc_tb(stats)

    obp_den = ab + bb + hbp + sf
    obp = (h + bb + hbp) / obp_den if obp_den > 0 else 0.0
    slg = tb / ab if ab > 0 else 0.0
    return round(obp + slg, 4)


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
        "BB": ["볼넷", "bb", "4구", "고의4구", "자동고의4구", "자동 고의4구", "고의 볼넷"],
        "IBB": ["고4", "고의4구", "자동고의4구", "자동 고의4구", "ibb"],
        "HBP": ["사구", "hbp"],
        "SO": ["삼진", "so"],
        "SH": ["희번", "희생번트", "sh"],
        "SF": ["희비", "희플", "sf"],
    }
    for row in rows:
        row_dict = _row_to_dict(row, headers)
        name = _get_value_by_keys(row_dict, ["선수", "선수명", "이름", "타자", "player", "playername"])
        team = _get_value_by_keys(row_dict, ["팀", "구단", "team", "club"])
        events = _extract_events_from_row(row_dict)
        stat_values = {k: _to_int(_get_value_by_keys(row_dict, v)) for k, v in stat_keys.items()}
        stat_values["BB"] = int(stat_values.get("BB", 0)) + int(stat_values.get("IBB", 0))
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
            "SH": 0,
            "SF": 0,
        }
        events = stats_entry.get("events") or []
        if events and not any(stats.values()):
            derived = parse_events_to_stats(events)
            stats = derived

        if stats.get("PA", 0) == 0:
            stats["PA"] = (
                stats.get("AB", 0)
                + stats.get("BB", 0)
                + stats.get("HBP", 0)
                + stats.get("SF", 0)
                + stats.get("SH", 0)
            )

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
                "SH": stats.get("SH", 0),
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
