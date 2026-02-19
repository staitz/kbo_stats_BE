import json
import re
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs
from urllib.parse import urlencode

from selenium.webdriver.common.by import By

from collector.kbo_api import _make_driver, _wait_ready
from collector.kbo_hitter_parser import parse_hitter_rows_from_dom_tables, parse_hitter_rows_from_html


def _drain_perf_logs(driver) -> None:
    try:
        driver.get_log("performance")
    except Exception:
        pass


def _safe_text(value: Any) -> str:
    if value is None:
        return ""
    if not isinstance(value, str):
        value = str(value)
    return value.encode("cp949", "backslashreplace").decode("cp949")


def _parse_perf_logs(driver) -> List[dict]:
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


def _collect_ws_requests(events: List[dict]) -> List[Dict[str, Any]]:
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
                "postData": req.get("postData"),
            }
        elif method == "Network.responseReceived":
            resp = params.get("response", {})
            req_id = params.get("requestId")
            resp_map[req_id] = {
                "status": resp.get("status"),
                "url": resp.get("url"),
            }

    results: List[Dict[str, Any]] = []
    for req_id, req in req_map.items():
        url = req.get("url") or ""
        if "/ws/" not in url or ".asmx" not in url:
            continue
        resp = resp_map.get(req_id, {})
        results.append(
            {
                "url": url,
                "method": req.get("method"),
                "postData": req.get("postData"),
                "status": resp.get("status"),
            }
        )
    return results


def _try_click_by_text(driver, labels: List[str]) -> bool:
    for label in labels:
        try:
            elems = driver.find_elements(By.PARTIAL_LINK_TEXT, label)
            if not elems:
                elems = driver.find_elements(By.XPATH, f"//*[contains(text(), '{label}')]")
            for elem in elems:
                if not elem:
                    continue
                driver.execute_script("arguments[0].click();", elem)
                print(f"[click] label={label}")
                return True
        except Exception:
            continue
    return False


def _debug_list_iframes(driver) -> None:
    try:
        frames = driver.find_elements(By.TAG_NAME, "iframe")
    except Exception:
        return
    print(f"[debug] iframes_count={len(frames)}")
    for idx, f in enumerate(frames[:5]):
        try:
            src = _safe_text(f.get_attribute("src") or "")
            fid = _safe_text(f.get_attribute("id") or "")
            name = _safe_text(f.get_attribute("name") or "")
            print(f"[debug] iframe#{idx} id='{fid}' name='{name}' src='{src}'")
        except Exception:
            continue


def _debug_list_links(driver, limit: int = 30) -> None:
    try:
        anchors = driver.find_elements(By.TAG_NAME, "a")
    except Exception:
        return
    samples = []
    for a in anchors:
        try:
            text = (a.text or "").strip()
            href = a.get_attribute("href") or ""
            if text or href:
                samples.append((_safe_text(text), _safe_text(href)))
        except Exception:
            continue
    print(f"[debug] anchors_count={len(samples)}")
    for text, href in samples[:limit]:
        print(f"[debug] anchor text='{text}' href='{href}'")


def _scan_click_candidates(driver, keywords: List[str], limit: int = 20) -> List[Dict[str, Any]]:
    script = """
    const keywords = arguments[0];
    const limit = arguments[1];
    const nodes = Array.from(document.querySelectorAll('a, button, li, span, div'));
    const results = [];
    for (const el of nodes) {
      const text = (el.innerText || '').trim();
      if (!text) continue;
      for (const kw of keywords) {
        if (text.includes(kw)) {
          results.push({
            text,
            tag: el.tagName,
            id: el.id || '',
            className: el.className || '',
            onclick: el.getAttribute('onclick') || ''
          });
          break;
        }
      }
      if (results.length >= limit) break;
    }
    return results;
    """
    try:
        return driver.execute_script(script, keywords, limit) or []
    except Exception:
        return []


def _js_click_by_text(driver, keywords: List[str]) -> bool:
    script = """
    const keywords = arguments[0];
    const nodes = Array.from(document.querySelectorAll('a, button, li, span, div'));
    for (const kw of keywords) {
      for (const el of nodes) {
        const text = (el.innerText || '').trim();
        if (text && text.includes(kw)) {
          el.click();
          return true;
        }
      }
    }
    return false;
    """
    try:
        return bool(driver.execute_script(script, keywords))
    except Exception:
        return False


def _replay_fetch(driver, url: str, post_data: Optional[str]) -> Tuple[Optional[int], str]:
    body = post_data or ""
    is_json = body.strip().startswith("{")
    if is_json:
        script = """
        const url = arguments[0];
        const body = arguments[1];
        const done = arguments[arguments.length - 1];
        fetch(url, {
          method: "POST",
          headers: {
            "Content-Type": "application/json; charset=UTF-8",
            "Accept": "application/json, text/plain, */*",
            "X-Requested-With": "XMLHttpRequest"
          },
          credentials: "include",
          body: body
        }).then(r => r.text().then(text => done({ ok: true, status: r.status, text })))
          .catch(err => done({ ok: false, error: String(err) }));
        """
    else:
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
        }).then(r => r.text().then(text => done({ ok: true, status: r.status, text })))
          .catch(err => done({ ok: false, error: String(err) }));
        """
    result = driver.execute_async_script(script, url, body)
    if not result or not result.get("ok"):
        return None, ""
    return result.get("status"), result.get("text") or ""


def _has_player_name_payload(text: str) -> bool:
    if not text:
        return False
    if any(key in text for key in ["선수", "선수명", "타자", "이름", "player", "batter"]):
        return True
    return False


def _summarize_payload_keys(post_data: Optional[str]) -> List[str]:
    if not post_data:
        return []
    body = post_data.strip()
    if body.startswith("{") and body.endswith("}"):
        try:
            data = json.loads(body)
            if isinstance(data, dict):
                return list(data.keys())
        except Exception:
            return []
    try:
        parsed = parse_qs(body, keep_blank_values=True)
        return list(parsed.keys())
    except Exception:
        return []


def _build_default_payload(game_date: str, game_id: str) -> Dict[str, str]:
    return {
        "leId": "1",
        "srId": "0",
        "seasonId": str(game_date)[:4],
        "gameId": str(game_id),
    }


def _parse_json_response(text: str) -> Optional[Any]:
    try:
        data = json.loads(text)
    except Exception:
        return None
    if isinstance(data, dict) and "d" in data:
        d = data.get("d")
        if isinstance(d, str):
            try:
                return json.loads(d)
            except Exception:
                return d
        return d
    return data


def _find_player_snippets(value: Any, path: str = "") -> List[str]:
    snippets: List[str] = []
    if isinstance(value, dict):
        for k, v in value.items():
            new_path = f"{path}.{k}" if path else str(k)
            snippets.extend(_find_player_snippets(v, new_path))
    elif isinstance(value, list):
        for idx, item in enumerate(value[:50]):
            new_path = f"{path}[{idx}]"
            snippets.extend(_find_player_snippets(item, new_path))
    elif isinstance(value, str):
        if any(tok in value for tok in ["선수명", "선수", "타자"]):
            snippet = value.replace("\n", " ")[:200]
            snippets.append(f"{path}: {snippet}")
    return snippets


def main():
    game_date = "20250610"
    game_id = "20250610SSHT0"
    away_team = "삼성"
    home_team = "KIA"

    driver = _make_driver(headless=False)
    selected_section = None
    try:
        try:
            driver.execute_cdp_cmd("Network.enable", {})
            driver.execute_cdp_cmd("Network.setCacheDisabled", {"cacheDisabled": True})
        except Exception:
            pass

        sections = ["REVIEW", "BOX", "RECORD"]
        ws_requests: List[Dict[str, Any]] = []
        rows: List[Dict[str, Any]] = []

        for idx, section in enumerate(sections):
            _drain_perf_logs(driver)
            gc_url = (
                "https://www.koreabaseball.com/Schedule/GameCenter/Main.aspx"
                f"?gameDate={game_date}&gameId={game_id}&section={section}"
            )
            driver.get(gc_url)
            _wait_ready(driver, 15)
            time.sleep(2)
            page_src = driver.page_source or ""
            table_count = driver.execute_script(
                "return document.querySelectorAll('table').length"
            )
            if table_count == 0:
                print("[debug] table_count=0")
            else:
                print(f"[debug] table_count={table_count}")

            # 테이블/유사테이블 DOM을 최대한 수집
            dom_html = driver.execute_script(
                """
                const keywords = ['타자 기록','타자기록','타격 기록','타격기록','타자','HITTER','BATTER'];
                const nodes = Array.from(document.querySelectorAll('table, [role=table], .table, .tbl, [class*=table]'));
                const picked = [];
                for (const el of nodes) {
                  const text = (el.innerText || '');
                  if (keywords.some(k => text.includes(k))) {
                    picked.push(el.outerHTML);
                  }
                }
                return picked.join("\\n");
                """
            )
            if dom_html:
                print("[debug] dom_html captured")
            else:
                print("[debug] dom_html empty")

            dom_tables = driver.execute_script(
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
            if dom_tables:
                print(f"[debug] dom_tables={len(dom_tables)}")
                for t in dom_tables[:3]:
                    print(
                        f"[debug] dom_tables sample idx={t.get('index')} "
                        f"headers_len={len(t.get('headers') or [])} "
                        f"rows_len={len(t.get('rows') or [])}"
                    )
            else:
                print("[debug] dom_tables empty")

            rows = parse_hitter_rows_from_dom_tables(
                tables=dom_tables or [],
                game_date=game_date,
                game_id=game_id,
                away_team=away_team,
                home_team=home_team,
                debug=True,
            )
            if not rows:
                # 폴백: HTML 파싱/텍스트 파싱
                dom_text = driver.execute_script(
                    """
                    const keywords = ['타자 기록','타자기록','타격 기록','타격기록'];
                    const nodes = Array.from(document.querySelectorAll('body *'));
                    for (const el of nodes) {
                      const text = (el.innerText || '').trim();
                      if (!text) continue;
                      if (keywords.some(k => text.includes(k))) {
                        return text;
                      }
                    }
                    return '';
                    """
                )
                rows = parse_hitter_rows_from_html(
                    html=dom_html or page_src,
                    game_date=game_date,
                    game_id=game_id,
                    away_team=away_team,
                    home_team=home_team,
                    debug=True,
                    text_fallback=dom_text,
                )
            print(f"[debug] section={section} rows={len(rows)}")
            print(rows[:2])
            if rows:
                selected_section = section
                break
            try:
                endpoints = sorted(set(re.findall(r"/ws/[^'\\\"]+", page_src)))
                if endpoints:
                    print(f"[debug] section={section} html_endpoints={endpoints[:10]}")
            except Exception:
                pass
            events_load = _parse_perf_logs(driver)
            ws_load = _collect_ws_requests(events_load)
            print(f"[debug] section={section} load_ws_requests={len(ws_load)}")
            for req in ws_load[:10]:
                print(
                    f"[debug] section={section} ws url="
                    f"{req.get('url')} method={req.get('method')} status={req.get('status')}"
                )
                print(f"[debug] section={section} ws postData={req.get('postData')}")

            if idx == 0:
                _debug_list_iframes(driver)
                _debug_list_links(driver, limit=40)
                candidates = _scan_click_candidates(
                    driver,
                    ["타자", "타격", "기록", "박스", "박스스코어", "BOX", "HITTER", "BATTER"],
                    limit=20,
                )
                if candidates:
                    print(f"[debug] click_candidates={len(candidates)}")
                    for c in candidates:
                        text = _safe_text(c.get("text"))
                        tag = _safe_text(c.get("tag"))
                        cid = _safe_text(c.get("id"))
                        cls = _safe_text(c.get("className"))
                        onclick = _safe_text(c.get("onclick"))
                        print(
                            f"[debug] cand tag={tag} id='{cid}' class='{cls}' "
                            f"text='{text}' onclick='{onclick}'"
                        )

            events_before = _parse_perf_logs(driver)
            seen_request_ids = {
                e.get("params", {}).get("requestId")
                for e in events_before
                if e.get("method") in ("Network.requestWillBeSent", "Network.responseReceived")
            }

            clicked = _try_click_by_text(
                driver,
                ["타자", "타격", "기록", "박스", "박스스코어", "BOX", "HITTER", "BATTER"],
            )
            if not clicked:
                clicked = _js_click_by_text(
                    driver,
                    ["타자", "타격", "기록", "박스", "박스스코어", "BOX", "HITTER", "BATTER"],
                )
                if clicked:
                    print("[click] via js_click_by_text")
            if not clicked:
                try:
                    frames = driver.find_elements(By.TAG_NAME, "iframe")
                except Exception:
                    frames = []
                for fidx, frame in enumerate(frames[:3]):
                    try:
                        driver.switch_to.frame(frame)
                        clicked = _try_click_by_text(
                            driver,
                            ["타자", "타격", "기록", "박스", "박스스코어", "BOX", "HITTER", "BATTER"],
                        )
                        driver.switch_to.default_content()
                        if clicked:
                            print(f"[click] via iframe index={fidx}")
                            break
                    except Exception:
                        try:
                            driver.switch_to.default_content()
                        except Exception:
                            pass
            time.sleep(3 if clicked else 1)
            events = _parse_perf_logs(driver)
            events = [
                e for e in events
                if e.get("params", {}).get("requestId") not in seen_request_ids
            ]
            ws_requests = _collect_ws_requests(events)

        if not selected_section and ws_requests:
            print(f"[debug] new_ws_requests={len(ws_requests)}")
            for req in ws_requests:
                print(
                    "[debug] ws url="
                    f"{req.get('url')} method={req.get('method')} status={req.get('status')}"
                )
                print(f"[debug] ws postData={req.get('postData')}")
                keys = _summarize_payload_keys(req.get("postData"))
                if keys:
                    print(f"[debug] ws payload keys={keys}")

            priority = [
                r for r in ws_requests
                if re.search(r"(hitter|batter|box|record|score|hitterrecord)", r.get("url", ""), re.I)
            ]
            picks = priority[:2] if priority else ws_requests[:2]
            for req in picks:
                status, body = _replay_fetch(driver, req.get("url"), req.get("postData"))
                print(f"[debug] replay status={status} url={req.get('url')}")
                if _has_player_name_payload(body):
                    print("[debug] replay body looks like player table")
                else:
                    print("[debug] replay body does not look like player table")
        elif not selected_section:
            print("[debug] no new /ws/*.asmx requests detected after click")

        if not selected_section:
            # Try replaying known endpoints from HTML (HTML 파싱 실패 시에만)
            default_payload = _build_default_payload(game_date, game_id)
            for endpoint in [
                "https://www.koreabaseball.com/ws/Schedule.asmx/GetScoreBoardScroll",
                "https://www.koreabaseball.com/ws/Schedule.asmx/GetBoxScoreScroll",
            ]:
                post_data = urlencode(default_payload)
                keys = _summarize_payload_keys(post_data)
                print(f"[debug] replay endpoint={endpoint} payload keys={keys}")
                status, body = _replay_fetch(driver, endpoint, post_data)
                print(f"[debug] replay endpoint status={status}")
                if _has_player_name_payload(body):
                    print("[debug] replay endpoint has player-like fields")
                    parsed = _parse_json_response(body)
                    snippets = _find_player_snippets(parsed) if parsed is not None else []
                    if snippets:
                        print(f"[debug] replay player snippets={snippets[:3]}")
                else:
                    print("[debug] replay endpoint has no player-like fields")

    finally:
        driver.quit()

    if selected_section:
        print(f"[ok] selected section={selected_section}")
    else:
        print("[fail] no section produced hitter rows")


if __name__ == "__main__":
    main()

