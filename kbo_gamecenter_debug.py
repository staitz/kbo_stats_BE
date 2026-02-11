import json
import re
import time
from typing import Any, Dict, List, Optional, Tuple

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options


def _wait_ready(driver, wait_sec=15):
    wait = WebDriverWait(driver, wait_sec)
    wait.until(lambda d: d.execute_script("return document.readyState") == "complete")


def get_stealth_chrome_options(headless: bool = True) -> Options:
    options = Options()
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--lang=ko-KR")
    options.add_argument("--remote-allow-origins=*")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    if headless:
        options.add_argument("--headless=new")
    options.set_capability("goog:loggingPrefs", {"performance": "ALL"})
    return options


def remove_webdriver_property(driver) -> None:
    try:
        driver.execute_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
        )
    except Exception:
        pass


def verify_session_cookies(driver) -> Dict[str, Any]:
    cookies = driver.get_cookies()
    cookie_map = {c.get("name"): c.get("value") for c in cookies}
    session_id = cookie_map.get("ASP.NET_SessionId")
    has_session = session_id is not None
    if has_session:
        short_id = session_id[:10]
    else:
        short_id = None
    print(f"[Cookies] has_aspnet_session={has_session} session_id_head={short_id}")
    print(f"[Cookies] names={list(cookie_map.keys())}")
    return {
        "has_aspnet_session": has_session,
        "session_id": session_id,
        "all_cookies": cookie_map
    }


def _parse_perf_logs(driver) -> List[dict]:
    logs = []
    try:
        logs = driver.get_log("performance")
    except Exception:
        return []
    events = []
    for entry in logs:
        try:
            msg = json.loads(entry["message"])["message"]
            events.append(msg)
        except Exception:
            continue
    return events


def capture_network_with_cdp(driver, game_id: str, game_date: str) -> List[Dict[str, Any]]:
    driver.execute_cdp_cmd("Network.enable", {})
    driver.execute_cdp_cmd("Network.setCacheDisabled", {"cacheDisabled": True})

    url = (
        "https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx"
        f"?gameDate={game_date}&gameId={game_id}&section=REVIEW"
    )
    driver.get(url)
    _wait_ready(driver, 15)
    time.sleep(5)

    events = _parse_perf_logs(driver)
    req_map: Dict[str, Dict[str, Any]] = {}
    resp_map: Dict[str, Dict[str, Any]] = {}

    for msg in events:
        method = msg.get("method")
        params = msg.get("params", {})
        if method == "Network.requestWillBeSent":
            req = params.get("request", {})
            req_id = params.get("requestId")
            req_map[req_id] = {
                "url": req.get("url"),
                "method": req.get("method"),
                "headers": req.get("headers", {}),
                "postData": req.get("postData")
            }
        elif method == "Network.responseReceived":
            resp = params.get("response", {})
            req_id = params.get("requestId")
            resp_map[req_id] = {
                "status": resp.get("status"),
                "headers": resp.get("headers", {}),
                "url": resp.get("url")
            }

    results = []
    for req_id, req in req_map.items():
        url = req.get("url") or ""
        if "/ws/" not in url:
            continue
        item = {
            "url": url,
            "method": req.get("method"),
            "headers": req.get("headers"),
            "postData": req.get("postData"),
            "status": None,
            "response_body": None
        }
        resp = resp_map.get(req_id)
        if resp:
            item["status"] = resp.get("status")
            try:
                body = driver.execute_cdp_cmd(
                    "Network.getResponseBody",
                    {"requestId": req_id}
                )
                item["response_body"] = body.get("body", "")[:500]
            except Exception:
                item["response_body"] = None
        results.append(item)

    print(f"[CDP] captured_ws_requests={len(results)}")
    return results


def inject_xhr_interceptor(driver) -> None:
    script = r"""
    (function() {
      if (window.__xhrInterceptInstalled) return;
      window.__xhrInterceptInstalled = true;
      window.capturedRequests = [];

      function pushRecord(rec) {
        try { window.capturedRequests.push(rec); } catch (e) {}
      }

      const origOpen = XMLHttpRequest.prototype.open;
      const origSend = XMLHttpRequest.prototype.send;
      XMLHttpRequest.prototype.open = function(method, url) {
        this.___method = method;
        this.___url = url;
        return origOpen.apply(this, arguments);
      };
      XMLHttpRequest.prototype.send = function(body) {
        const xhr = this;
        const start = Date.now();
        function record() {
          const text = xhr.responseText || "";
          pushRecord({
            type: "xhr",
            method: xhr.___method,
            url: xhr.___url,
            body: body || null,
            status: xhr.status,
            response: text.slice(0, 500),
            duration_ms: Date.now() - start
          });
        }
        xhr.addEventListener("loadend", record);
        return origSend.apply(this, arguments);
      };

      const origFetch = window.fetch;
      window.fetch = function() {
        const args = arguments;
        const req = args[0];
        const init = args[1] || {};
        const method = (init.method || "GET").toUpperCase();
        const url = (typeof req === "string") ? req : (req && req.url);
        const body = init.body || null;
        const start = Date.now();
        return origFetch.apply(this, args).then(function(res) {
          const cloned = res.clone();
          return cloned.text().then(function(text) {
            pushRecord({
              type: "fetch",
              method: method,
              url: url,
              body: body,
              status: res.status,
              response: text.slice(0, 500),
              duration_ms: Date.now() - start
            });
            return res;
          });
        });
      };
    })();
    """
    driver.execute_cdp_cmd(
        "Page.addScriptToEvaluateOnNewDocument",
        {"source": script}
    )


def _try_click_by_text(driver, labels: List[str]) -> None:
    for label in labels:
        try:
            elems = driver.find_elements(By.PARTIAL_LINK_TEXT, label)
            if elems:
                elems[0].click()
                print(f"[Clicked] {label}")
                time.sleep(2)
        except Exception:
            continue


def get_captured_requests(driver, game_id: str, game_date: str) -> List[Dict[str, Any]]:
    inject_xhr_interceptor(driver)
    url = (
        "https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx"
        f"?gameDate={game_date}&gameId={game_id}&section=REVIEW"
    )
    driver.get(url)
    _wait_ready(driver, 15)
    time.sleep(3)

    _try_click_by_text(driver, ["타자", "REVIEW", "라인업", "투수", "기록"])

    try:
        logs = driver.execute_script("return window.capturedRequests || []")
    except Exception:
        logs = []

    ws_logs = [x for x in logs if isinstance(x, dict) and "/ws/" in (x.get("url") or "")]
    print(f"[Intercepted] captured_ws_requests={len(ws_logs)}")
    return ws_logs


def comprehensive_network_debug(driver, game_id: str, game_date: str) -> Dict[str, Any]:
    driver.execute_cdp_cmd("Network.enable", {})
    driver.execute_cdp_cmd("Network.setCacheDisabled", {"cacheDisabled": True})
    inject_xhr_interceptor(driver)

    url = (
        "https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx"
        f"?gameDate={game_date}&gameId={game_id}&section=REVIEW"
    )
    driver.get(url)
    _wait_ready(driver, 15)
    time.sleep(3)

    print(f"[debug] current_url={driver.current_url}")
    cookies_info = verify_session_cookies(driver)

    _try_click_by_text(driver, ["타자", "REVIEW", "라인업", "투수", "기록"])

    xhr_logs = []
    try:
        xhr_logs = driver.execute_script("return window.capturedRequests || []")
    except Exception:
        xhr_logs = []

    cdp_logs = _parse_perf_logs(driver)
    ws_cdp = []
    for msg in cdp_logs:
        if msg.get("method") == "Network.requestWillBeSent":
            req = msg.get("params", {}).get("request", {})
            url = req.get("url") or ""
            if "/ws/" in url:
                ws_cdp.append({
                    "url": url,
                    "method": req.get("method"),
                    "postData": req.get("postData")
                })

    game_asmx_calls = [x for x in ws_cdp if "/ws/Game.asmx/" in (x.get("url") or "")]

    print(f"[CDP] ws_requests={len(ws_cdp)}")
    for i, item in enumerate(ws_cdp[:20]):
        print(f"[CDP] #{i} {item.get('method')} {item.get('url')}")
        if item.get("postData"):
            print(f"[CDP] #{i} postData={item.get('postData')}")
    print(f"[Intercepted] ws_requests={len([x for x in xhr_logs if '/ws/' in (x.get('url') or '')])}")
    print(f"[CDP] game_asmx_calls={len(game_asmx_calls)}")

    return {
        "cookies": cookies_info,
        "xhr_logs": xhr_logs,
        "cdp_logs": ws_cdp,
        "game_asmx_calls": game_asmx_calls,
        "current_url": driver.current_url
    }


def _parse_json_response(text_or_dict):
    try:
        data = text_or_dict
        if isinstance(text_or_dict, str):
            data = json.loads(text_or_dict)
        if isinstance(data, dict) and "d" in data:
            d = data["d"]
            if isinstance(d, str):
                data = json.loads(d)
            else:
                data = d
        if isinstance(data, dict):
            return data
        if isinstance(data, list):
            return {"_list": data}
        return None
    except Exception:
        raw = text_or_dict if isinstance(text_or_dict, str) else str(text_or_dict)
        print("[debug] parse failed raw head:", raw[:500])
        raise


def fetch_hitter_data_v2(driver, game_id: str, game_date: str, endpoint: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    url = (
        "https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx"
        f"?gameDate={game_date}&gameId={game_id}&section=REVIEW"
    )
    driver.get(url)
    _wait_ready(driver, 15)
    time.sleep(2)

    referer = driver.current_url
    payload_json = json.dumps(payload, ensure_ascii=False)

    fetch_script = f"""
    const callback = arguments[arguments.length - 1];
    fetch("{endpoint}", {{
        method: "POST",
        headers: {{
            "Content-Type": "application/json; charset=utf-8",
            "Accept": "application/json, text/plain, */*",
            "X-Requested-With": "XMLHttpRequest",
            "Origin": "https://www.koreabaseball.com",
            "Referer": "{referer}"
        }},
        credentials: "include",
        body: JSON.stringify({payload_json})
    }})
    .then(res => res.text().then(text => callback({{ ok: true, status: res.status, text }})))
    .catch(err => callback({{ ok: false, error: String(err) }}));
    """
    result = driver.execute_async_script(fetch_script)
    if not result or not result.get("ok"):
        raise RuntimeError(f"fetch failed: {result.get('error') if result else 'no result'}")

    status = result.get("status")
    text = result.get("text") or ""
    print(f"[API] status={status}")
    if status == 401:
        print(f"[API] current_url={driver.current_url}")
        print(f"[API] cookies={len(driver.get_cookies())}")
        print(f"[API] raw_text_head={text[:500]}")
        raise RuntimeError("unauthorized 401")

    data = _parse_json_response(text)
    if isinstance(data, dict) and {"Message", "StackTrace", "ExceptionType"} <= set(data.keys()):
        raise RuntimeError(f"server error payload (status={status}): {data}")
    return data


def test_api_parameter_combinations(driver, game_id: str, game_date: str) -> List[Dict[str, Any]]:
    endpoints = [
        "/ws/Game.asmx/GetHitterRecord",
        "/ws/Game.asmx/GetHitterRecordAll",
        "/ws/Game.asmx/GetBatterBoxScore",
        "/ws/Schedule.asmx/GetGamePlayerRecord",
        "/ws/Schedule.asmx/GetScoreBoardScroll",
    ]
    payloads = [
        {"gameDate": game_date, "gameId": game_id},
        {"gameDate": game_date, "gameId": game_id, "inning": "1"},
        {"gameDate": game_date, "gameId": game_id, "section": "REVIEW"},
        {"gameId": game_id},
    ]

    successes = []
    for endpoint in endpoints:
        for payload in payloads:
            print(f"[API] testing endpoint={endpoint} payload={payload}")
            try:
                data = fetch_hitter_data_v2(driver, game_id, game_date, endpoint, payload)
                arr = data.get("arrHitter") if isinstance(data, dict) else None
                arr_len = len(arr) if isinstance(arr, list) else 0
                successes.append({
                    "endpoint": endpoint,
                    "payload": payload,
                    "arrHitter_len": arr_len,
                    "response_keys": list(data.keys()) if isinstance(data, dict) else None
                })
                print(f"[OK] arrHitter_len={arr_len}")
            except Exception as exc:
                print(f"[FAIL] {endpoint} {payload} -> {exc}")
    return successes


def simulate_gamecenter_interactions(driver, game_id: str, game_date: str) -> Dict[str, Any]:
    url = (
        "https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx"
        f"?gameDate={game_date}&gameId={game_id}&section=REVIEW"
    )
    driver.get(url)
    _wait_ready(driver, 15)
    time.sleep(2)

    results = {}
    labels = ["타자", "투수", "REVIEW", "라인업", "1회", "2회", "3회"]
    for label in labels:
        _try_click_by_text(driver, [label])
        logs = driver.execute_script("return window.capturedRequests || []")
        ws_logs = [x for x in logs if "/ws/" in (x.get("url") or "")]
        results[label] = ws_logs[-5:]
        print(f"[Clicked] {label} ws_logs={len(ws_logs)}")
    return results


def trace_js_functions(driver, game_id: str, game_date: str) -> Dict[str, Any]:
    url = (
        "https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx"
        f"?gameDate={game_date}&gameId={game_id}&section=REVIEW"
    )
    driver.get(url)
    _wait_ready(driver, 15)
    time.sleep(2)

    source = driver.page_source or ""
    patterns = ["GetHitterRecord", "arrHitter", "/ws/Game.asmx", "loadHitterData"]
    found = {p: (p in source) for p in patterns}
    print(f"[debug] js_patterns_found={found}")

    func_checks = {}
    for name in ["loadHitterData", "getGameData", "fnGetHitterRecord"]:
        try:
            exists = driver.execute_script(f"return typeof window.{name} !== 'undefined';")
            func_checks[name] = bool(exists)
        except Exception:
            func_checks[name] = False

    results = {"pattern_found": found, "functions": func_checks}
    return results


def collect_gamecenter_hitter_data(driver, game_id: str, game_date: str) -> Dict[str, Any]:
    verify_session_cookies(driver)
    debug_info = comprehensive_network_debug(driver, game_id, game_date)

    for item in debug_info.get("game_asmx_calls", []):
        endpoint = item.get("url")
        if not endpoint:
            continue
        payload = {"gameDate": game_date, "gameId": game_id}
        try:
            data = fetch_hitter_data_v2(driver, game_id, game_date, endpoint, payload)
            arr = data.get("arrHitter") if isinstance(data, dict) else None
            if isinstance(arr, list) and len(arr) >= 2:
                return {
                    "success": True,
                    "raw_response": data,
                    "method_used": f"endpoint:{endpoint}"
                }
        except Exception:
            continue

    return {
        "success": False,
        "raw_response": None,
        "method_used": "none"
    }


if __name__ == "__main__":
    game_date = "20250610"
    game_id = "20250610SSHT0"

    options = get_stealth_chrome_options(headless=True)
    driver = webdriver.Chrome(options=options)
    try:
        remove_webdriver_property(driver)
        comprehensive_network_debug(driver, game_id, game_date)
    finally:
        driver.quit()
