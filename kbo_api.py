import json
import os
import re
import time
from datetime import datetime
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode

import requests
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

from kbo_hitter_parser import debug_hitter_shape, parse_hitter_rows

SCHEDULE_PAGE_URL = "https://www.koreabaseball.com/Schedule/Schedule.aspx"
GET_SCHEDULE_LIST_URL = "https://www.koreabaseball.com/ws/Schedule.asmx/GetScheduleList"
SESSION_CACHE_PATH = ".kbo_session.json"


@dataclass
class CapturedSession:
    url: str
    headers: Dict[str, str]
    post_data: str
    cookies: Dict[str, str]


def _make_driver(headless=True):
    chrome_options = Options()
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--lang=ko-KR")
    chrome_options.add_argument("--remote-allow-origins=*")
    if headless:
        chrome_options.add_argument("--headless=new")

    chrome_options.set_capability("goog:loggingPrefs", {"performance": "ALL"})

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=chrome_options
    )
    driver.execute_cdp_cmd("Network.enable", {})
    driver.execute_cdp_cmd("Network.setCacheDisabled", {"cacheDisabled": True})
    return driver


def _wait_ready(driver, wait_sec=10):
    wait = WebDriverWait(driver, wait_sec)
    wait.until(lambda d: d.execute_script("return document.readyState") == "complete")


def _drain_performance_logs(driver):
    try:
        driver.get_log("performance")
    except Exception:
        pass


def _len_or_none(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, (list, dict, str)):
        try:
            return len(value)
        except Exception:
            return None
    return None


def _safe_json_loads(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return None
    return value


def _debug_table_dump(name: str, raw_value: Any) -> None:
    print(f"[debug] {name} type={type(raw_value)} len={_len_or_none(raw_value)}")
    parsed = _safe_json_loads(raw_value)
    if not isinstance(parsed, dict):
        return
    headers = parsed.get("headers") or parsed.get("header") or []
    if isinstance(headers, list):
        print(f"[debug] {name} headers full={headers}")
    rows = parsed.get("rows") or parsed.get("row") or []
    if isinstance(rows, list):
        print(f"[debug] {name} rows len={len(rows)}")
        if rows:
            row0 = rows[0]
            row0_type = type(row0)
            row0_shape = "dict.row" if isinstance(row0, dict) and "row" in row0 else "list"
            try:
                row0_dump = json.dumps(row0, ensure_ascii=False)
            except Exception:
                row0_dump = str(row0)
            print(f"[debug] {name} rows[0] type={row0_type} shape={row0_shape}")
            print(f"[debug] {name} rows[0] dump={row0_dump[:500]}")


def _fetch_form_endpoint(driver, url: str, payload: Dict[str, str]) -> Tuple[Optional[int], str]:
    body = urlencode(payload)
    script = """
    const url = arguments[0];
    const body = arguments[1];
    const done = arguments[arguments.length - 1];

    fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Accept": "application/json, text/plain, */*",
        "X-Requested-With": "XMLHttpRequest"
      },
      credentials: "include",
      body: body
    })
    .then(r => r.text().then(text => done({ ok: true, status: r.status, text })))
    .catch(err => done({ ok: false, error: String(err) }));
    """
    result = driver.execute_async_script(script, url, body)
    if not result or not result.get("ok"):
        raise RuntimeError(f"fetch failed: {result.get('error') if result else 'no result'}")
    return result.get("status"), result.get("text") or ""


def fetch_gamecenter_hitter_data(
    driver,
    game_date: str,
    game_id: str,
    away_team: str,
    home_team: str,
    payload_extra: Optional[Dict[str, str]] = None,
    endpoints: Optional[List[str]] = None,
    debug: bool = True,
    run_parser: bool = True
) -> dict:
    """
    GameCenter ???JSON??Selenium fetch濡?諛쏆븘 Python dict濡?諛섑솚.
    data ?앹꽦 吏곹썑 debug_hitter_shape / parse_hitter_rows ?몄텧 ?ы븿.
    """
    gc_url = (
        "https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx"
        f"?gameDate={game_date}&gameId={game_id}&section=REVIEW"
    )

    driver.get(gc_url)
    _wait_ready(driver, 15)

    if f"gameDate={game_date}" not in (driver.current_url or ""):
        try:
            driver.execute_script("window.location.href = arguments[0];", gc_url)
            _wait_ready(driver, 15)
        except Exception:
            pass

    if debug:
        print(f"[debug] gc_url={gc_url}")
        print(f"[debug] current_url={driver.current_url}")
        try:
            print(f"[debug] cookies={len(driver.get_cookies())}")
        except Exception:
            print("[debug] cookies=0")
        if f"gameDate={game_date}" not in (driver.current_url or ""):
            print("[debug] warning: query params missing after load")

    payload = {
        "leId": "1",
        "srId": "0",
        "seasonId": str(game_date)[:4],
        "gameId": str(game_id),
    }
    if payload_extra:
        payload.update(payload_extra)

    if not endpoints:
        endpoints = [
            "https://www.koreabaseball.com/ws/Schedule.asmx/GetScoreBoardScroll",
            "https://www.koreabaseball.com/ws/Schedule.asmx/GetBoxScoreScroll",
        ]

    sb_url = endpoints[0]
    bs_url = endpoints[1] if len(endpoints) > 1 else endpoints[0]

    status_sb, raw_sb = _fetch_form_endpoint(driver, sb_url, payload)
    if debug:
        print(f"[debug] endpoint=GetScoreBoardScroll status={status_sb}")
    data_sb = _parse_json_response(raw_sb)
    if data_sb is None:
        raise RuntimeError("failed to parse GetScoreBoardScroll response")

    if debug and isinstance(data_sb, dict):
        for name in ("table1", "table2", "table3"):
            _debug_table_dump(f"scoreboard.{name}", data_sb.get(name))

    sb_ids = {}
    if isinstance(data_sb, dict):
        le_id = data_sb.get("LE_ID")
        sr_id = data_sb.get("SR_ID")
        season_id = data_sb.get("SEASON_ID")
        g_id = data_sb.get("G_ID")
        if le_id and sr_id and season_id and g_id:
            sb_ids = {
                "leId": str(le_id),
                "srId": str(sr_id),
                "seasonId": str(season_id),
                "gameId": str(g_id),
            }

    bs_payload = sb_ids or payload
    status_bs, raw_bs = _fetch_form_endpoint(driver, bs_url, bs_payload)
    if debug:
        print(f"[debug] endpoint=GetBoxScoreScroll status={status_bs}")
    data_bs = _parse_json_response(raw_bs)
    if data_bs is None:
        raise RuntimeError("failed to parse GetBoxScoreScroll response")

    if debug and isinstance(data_bs, dict):
        debug_hitter_shape(data_bs)

    combined = {
        "scoreboard": data_sb,
        "boxscore": data_bs,
    }

    if run_parser:
        rows = parse_hitter_rows(
            data=combined,
            game_date=game_date,
            game_id=game_id,
            away_team=away_team,
            home_team=home_team
        )
        print(rows[:2])

    return combined

def _js_fetch(driver, url, payload_dict):
    body = urlencode(payload_dict)
    script = """
    const url = arguments[0];
    const body = arguments[1];
    const done = arguments[arguments.length - 1];

    fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest"
      },
      credentials: "include",
      body: body
    })
    .then(r => r.text())
    .then(text => done({ ok: true, text }))
    .catch(err => done({ ok: false, error: String(err) }));
    """
    result = driver.execute_async_script(script, url, body)
    if not result.get("ok"):
        raise RuntimeError(result.get("error"))
    return result.get("text", "")


def _capture_request_and_response(
    driver,
    target_url,
    timeout=20,
    expect_post_substr=None
):
    request_info = {}
    response_info = {}

    start = time.time()
    while time.time() - start < timeout:
        logs = driver.get_log("performance")
        for entry in logs:
            msg = json.loads(entry["message"])["message"]
            method = msg.get("method")
            params = msg.get("params", {})

            if method == "Network.requestWillBeSent":
                req = params.get("request", {})
                if req.get("url") == target_url:
                    post_data = req.get("postData", "")
                    if expect_post_substr and expect_post_substr not in post_data:
                        continue
                    request_info = {
                        "url": req.get("url"),
                        "headers": req.get("headers", {}),
                        "postData": post_data,
                        "requestId": params.get("requestId")
                    }

            if method == "Network.responseReceived":
                resp = params.get("response", {})
                if resp.get("url") == target_url:
                    response_info = {
                        "status": resp.get("status"),
                        "headers": resp.get("headers", {}),
                        "requestId": params.get("requestId")
                    }

            if method == "Network.loadingFinished":
                req_id = params.get("requestId")
                if response_info.get("requestId") == req_id:
                    try:
                        body = driver.execute_cdp_cmd(
                            "Network.getResponseBody",
                            {"requestId": req_id}
                        )
                        response_info["body"] = body.get("body", "")
                        return request_info, response_info
                    except Exception:
                        pass

        time.sleep(0.2)

    return request_info, response_info


def save_session_cache(captured: CapturedSession, path=SESSION_CACHE_PATH):
    data = {
        "url": captured.url,
        "headers": captured.headers,
        "post_data": captured.post_data,
        "cookies": captured.cookies
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def load_session_cache(path=SESSION_CACHE_PATH) -> Optional[CapturedSession]:
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return CapturedSession(
            url=data.get("url", GET_SCHEDULE_LIST_URL),
            headers=data.get("headers", {}),
            post_data=data.get("post_data", ""),
            cookies=data.get("cookies", {})
        )
    except Exception:
        return None


def capture_session_via_selenium(
    season_id: str,
    game_month: str,
    le_id: str = "1",
    sr_id_list: str = "0,9,6",
    team_id: str = "",
    headless: bool = True,
    debug: bool = False
) -> CapturedSession:
    driver = _make_driver(headless=headless)
    try:
        driver.get(SCHEDULE_PAGE_URL)
        _wait_ready(driver, 10)

        payload = {
            "leId": str(le_id),
            "srIdList": str(sr_id_list),
            "seasonId": str(season_id),
            "gameMonth": str(game_month).zfill(2),
            "teamId": str(team_id)
        }

        _drain_performance_logs(driver)
        _ = _js_fetch(driver, GET_SCHEDULE_LIST_URL, payload)
        expect = f"gameMonth={str(game_month).zfill(2)}"
        req_info, resp_info = _capture_request_and_response(
            driver,
            GET_SCHEDULE_LIST_URL,
            expect_post_substr=expect
        )

        if debug:
            print(f"[debug] browser status: {resp_info.get('status')}")
            if req_info:
                print("[debug] captured postData:", req_info.get("postData", ""))

        cookies = {c["name"]: c["value"] for c in driver.get_cookies()}
        headers = dict(req_info.get("headers", {}))
        headers.pop("Content-Length", None)
        headers.pop("Host", None)
        headers.setdefault("Referer", SCHEDULE_PAGE_URL)
        headers.setdefault("Origin", "https://www.koreabaseball.com")
        headers.setdefault("X-Requested-With", "XMLHttpRequest")
        headers.setdefault("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
        headers.setdefault("Accept", "application/json, text/javascript, */*; q=0.01")

        captured = CapturedSession(
            url=req_info.get("url", GET_SCHEDULE_LIST_URL),
            headers=headers,
            post_data=req_info.get("postData", ""),
            cookies=cookies
        )
        save_session_cache(captured)
        return captured
    finally:
        driver.quit()


def _build_requests_session(captured: CapturedSession) -> requests.Session:
    s = requests.Session()
    for k, v in captured.cookies.items():
        s.cookies.set(k, v)
    return s


def _post_with_session(
    session: requests.Session,
    captured: CapturedSession,
    payload: Dict[str, str],
    debug: bool = False
) -> Tuple[int, str]:
    data = urlencode(payload)
    resp = session.post(captured.url, data=data, headers=captured.headers)
    if debug:
        print(f"[debug] requests status: {resp.status_code}")
    return resp.status_code, resp.text


def _parse_json_response(text: str):
    try:
        data = json.loads(text)
        if isinstance(data, dict) and "d" in data:
            d = data["d"]
            if isinstance(d, str):
                try:
                    return json.loads(d)
                except Exception:
                    return d
            return d
        return data
    except Exception:
        return None


def _strip_tags(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"<[^>]+>", "", text).strip()


def _extract_teams(text: str) -> Tuple[str, str]:
    if not text:
        return "", ""
    parts = re.findall(r"<span>(.*?)</span>", text)
    if len(parts) >= 2:
        return _strip_tags(parts[0]), _strip_tags(parts[-1])
    clean = _strip_tags(text)
    if "vs" in clean:
        left, right = clean.split("vs", 1)
        return left.strip(), right.strip()
    return clean, ""


def _extract_time(text: str) -> str:
    if not text:
        return ""
    clean = _strip_tags(text)
    m = re.search(r"\b([01]?\d|2[0-3]):[0-5]\d\b", clean)
    return m.group(0) if m else ""


def _extract_bracket_stadium(text: str) -> str:
    if not text:
        return ""
    m = re.search(r"\[(.+?)\]", text)
    return m.group(1).strip() if m else ""


def _extract_paren_status(text: str) -> str:
    if not text:
        return ""
    m = re.search(r"\(([^)]+)\)", text)
    if not m:
        return ""
    val = m.group(1).strip()
    if val in {"-", "N/A"}:
        return ""
    return val


def _guess_status(text: str) -> str:
    if not text:
        return ""
    clean = _strip_tags(text)
    if any(
        k in clean
        for k in [
            "\ucde8\uc18c",  # 취소
            "\uc5f0\uae30",  # 연기
            "\uc911\ub2e8",  # 중단
            "\uc11c\uc2a4\ud39c\ub514\ub4dc",  # 서스펜디드
        ]
    ):
        return clean
    return ""


def _guess_stadium(text: str) -> str:
    if not text:
        return ""
    clean = _strip_tags(text)
    if 0 < len(clean) <= 8 and not re.search(r"\d", clean):
        return clean
    return ""


def _extract_scores(play_text: str) -> List[int]:
    if not play_text:
        return []
    scores = re.findall(r">\s*(\d+)\s*<", play_text)
    return [int(x) for x in scores]


def _infer_status(play_text: str, time_text: str = "", relay_text: str = "") -> Tuple[str, float, str]:
    text = _strip_tags(play_text or "")
    scores = _extract_scores(play_text or "")
    if len(scores) >= 2:
        return "finished", 0.6, "score_present"

    time_clean = _strip_tags(time_text or "")
    if re.search(r"\b([01]?\d|2[0-3]):[0-5]\d\b", time_clean):
        return "scheduled", 0.4, "time_only"

    if text or relay_text:
        return "unknown", 0.3, "no_signal"
    return "unknown", 0.1, "empty"


def _maybe_enrich_status(
    session: requests.Session,
    game_id: Optional[str],
    date_yyyymmdd: str,
    current_status: str,
    confidence: float,
    today_only: bool = True,
    debug: bool = False
) -> Tuple[str, float, str]:
    return current_status, confidence, "skip_enrich"


def _class_buckets(rows: List[dict]) -> Dict[str, List[str]]:
    buckets: Dict[str, List[str]] = {}
    for row_wrapper in rows:
        row = row_wrapper.get("row") if isinstance(row_wrapper, dict) else None
        if not isinstance(row, list):
            continue
        for cell in row:
            if not isinstance(cell, dict):
                continue
            cls = cell.get("Class") or ""
            text = cell.get("Text") or ""
            buckets.setdefault(cls, [])
            if len(buckets[cls]) < 3 and text:
                buckets[cls].append(text)
    return buckets


def normalize_rows_to_games(
    rows: List[dict],
    season_id: Optional[str] = None,
    filter_date: Optional[str] = None,
    debug: bool = False,
    enrich_status: bool = False,
    enrich_today_only: bool = True,
    session: Optional[requests.Session] = None
) -> List[dict]:
    if not isinstance(rows, list):
        return []

    if debug:
        buckets = _class_buckets(rows)
        print("[debug] cell.Class types:", sorted(buckets.keys()))
        for cls, samples in buckets.items():
            print(f"[debug] class={cls} sample={samples[:2]}")

    out = []
    current_date = ""
    for row_wrapper in rows:
        row = row_wrapper.get("row") if isinstance(row_wrapper, dict) else None
        if not isinstance(row, list):
            continue

        day_text = ""
        time_text = ""
        play_text = ""
        stadium_text = ""
        status_text = ""
        game_id = ""
        relay_text = ""

        for cell in row:
            if not isinstance(cell, dict):
                continue
            cls = cell.get("Class") or ""
            text = cell.get("Text") or ""

            if cls == "day":
                day_text = _strip_tags(text)
            elif cls == "time":
                time_text = _strip_tags(text)
            elif cls == "play":
                play_text = text
            elif cls in ("place", "stadium", "ground", "park"):
                stadium_text = _strip_tags(text)
            elif cls in ("state", "status", "result"):
                status_text = _strip_tags(text)
            elif cls == "relay":
                relay_text = text
                m = re.search(r"gameId=([0-9A-Za-z_-]+)", text)
                if m:
                    game_id = m.group(1)

            if cls != "day":
                if not stadium_text:
                    stadium_text = _extract_bracket_stadium(text) or _guess_stadium(text)
                if not status_text:
                    status_text = _extract_paren_status(text) or _guess_status(text)

        if day_text:
            current_date = day_text

        date_norm = re.sub(r"[^0-9]", "", current_date)
        if season_id and len(date_norm) == 4:
            date_norm = f"{season_id}{date_norm}"

        if filter_date and date_norm and date_norm != filter_date:
            continue

        away, home = _extract_teams(play_text)
        time_norm = _extract_time(time_text)

        status, conf, reason = _infer_status(play_text, time_text, relay_text)
        if status_text:
            status = status_text
            conf = 0.9
            reason = "cell_status"

        if enrich_status and session:
            status, conf, reason = _maybe_enrich_status(
                session,
                game_id,
                date_norm,
                status,
                conf,
                today_only=enrich_today_only,
                debug=debug
            )

        fallback_id = ""
        if not game_id:
            fallback_id = f"{date_norm}_{away}_{home}_{time_norm or 'NA'}"

        out.append({
            "date": date_norm,
            "time": time_norm,
            "away_team": away,
            "home_team": home,
            "stadium": stadium_text or None,
            "status": status,
            "status_confidence": round(conf, 2),
            "status_reason": reason,
            "game_id": game_id or None,
            "fallback_id": fallback_id or None
        })

    return out


def _fetch_with_fallback(
    payload: Dict[str, str],
    season_id: str,
    filter_date: Optional[str] = None,
    debug: bool = False,
    enrich_status: bool = False,
    enrich_today_only: bool = True
) -> List[dict]:
    captured = load_session_cache()
    if not captured:
        if debug:
            print("[debug] no cache, capturing via selenium")
        captured = capture_session_via_selenium(
            season_id=season_id,
            game_month=payload.get("gameMonth", "01"),
            headless=not debug,
            debug=debug
        )

    session = _build_requests_session(captured)
    status, text = _post_with_session(session, captured, payload, debug=debug)
    if status == 401:
        if debug:
            print("[debug] 401 detected, recapturing via selenium")
        captured = capture_session_via_selenium(
            season_id=season_id,
            game_month=payload.get("gameMonth", "01"),
            headless=not debug,
            debug=debug
        )
        session = _build_requests_session(captured)
        status, text = _post_with_session(session, captured, payload, debug=debug)

    if debug:
        print("[debug] raw body (first 1000):", text[:1000])

    data = _parse_json_response(text)
    if isinstance(data, dict) and "rows" in data:
        return normalize_rows_to_games(
            data.get("rows", []),
            season_id=season_id,
            filter_date=filter_date,
            debug=debug,
            enrich_status=enrich_status,
            enrich_today_only=enrich_today_only,
            session=session
        )

    return []


def fetch_month_schedule(
    season_id: str,
    game_month: str,
    debug: bool = False,
    enrich_status: bool = False,
    enrich_today_only: bool = True
) -> List[dict]:
    payload = {
        "leId": "1",
        "srIdList": "0,9,6",
        "seasonId": str(season_id),
        "gameMonth": str(game_month).zfill(2),
        "teamId": ""
    }
    return _fetch_with_fallback(
        payload,
        season_id=str(season_id),
        debug=debug,
        enrich_status=enrich_status,
        enrich_today_only=enrich_today_only
    )


def fetch_day_schedule(
    date_yyyymmdd: str,
    debug: bool = False,
    enrich_status: bool = False,
    enrich_today_only: bool = True
) -> List[dict]:
    payload = {
        "leId": "1",
        "srIdList": "0,9,6",
        "seasonId": str(date_yyyymmdd[:4]),
        "gameMonth": str(date_yyyymmdd[4:6]),
        "teamId": "",
        "gameDate": str(date_yyyymmdd)
    }
    return _fetch_with_fallback(
        payload,
        season_id=str(date_yyyymmdd[:4]),
        filter_date=str(date_yyyymmdd),
        debug=debug,
        enrich_status=enrich_status,
        enrich_today_only=enrich_today_only
    )


def find_season_start_date(season_id: str, debug: bool = False) -> Optional[str]:
    candidates: List[str] = []
    skip_terms = {"시범", "연습", "퓨처스"}
    for month in range(1, 13):
        games = fetch_month_schedule(
            season_id=str(season_id),
            game_month=str(month).zfill(2),
            debug=debug
        )
        for g in games:
            date_val = g.get("date")
            status = (g.get("status") or "").strip()
            if status and any(term in status for term in skip_terms):
                continue
            if not g.get("game_id"):
                continue
            if isinstance(date_val, str) and len(date_val) == 8 and date_val.startswith(str(season_id)):
                candidates.append(date_val)
    if not candidates:
        return None
    return min(candidates)

