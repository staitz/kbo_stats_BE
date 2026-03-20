import argparse
import datetime as dt
import re
from io import StringIO
from typing import Any

import pandas as pd
import requests

from db_support import connect_for_path, execute
from collector.kbreport_db import init_kbreport_tables


BASE_URL = "http://www.kbreport.sbs/player/detail/{player_id}"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36"
)

TEAM_CODE_TO_NAME = {
    "1": "DOOSAN",
    "2": "SAMSUNG",
    "3": "KIA",
    "4": "KIWOOM",
    "5": "LG",
    "6": "SSG",
    "7": "NC",
    "8": "HANWHA",
    "9": "LOTTE",
    "15": "KT",
    "16": "SSG",
}

TEAM_ALIAS_TO_NAME = {
    "DOOSAN": "DOOSAN",
    "SAMSUNG": "SAMSUNG",
    "KIA": "KIA",
    "HERO": "KIWOOM",
    "HEROES": "KIWOOM",
    "KIWOOM": "KIWOOM",
    "LG": "LG",
    "SK": "SSG",
    "SSG": "SSG",
    "NC": "NC",
    "HANWHA": "HANWHA",
    "LOTTE": "LOTTE",
    "KT": "KT",
    "두산": "DOOSAN",
    "삼성": "SAMSUNG",
    "키움": "KIWOOM",
    "한화": "HANWHA",
    "롯데": "LOTTE",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch KBReport hitter splits (homeaway/pitchside/opposite/month)")
    parser.add_argument("--db", default="kbo_stats.db")
    parser.add_argument("--season", type=int, required=True)
    parser.add_argument("--player-id", required=True, help="KBReport player detail id, e.g. 2231")
    parser.add_argument("--player-name", help="optional override")
    return parser.parse_args()


def _fetch_html(player_id: str, params: dict[str, str] | None = None) -> str:
    url = BASE_URL.format(player_id=player_id)
    resp = requests.get(url, params=params or {}, timeout=20, headers={"User-Agent": USER_AGENT})
    resp.raise_for_status()
    resp.encoding = resp.apparent_encoding or resp.encoding
    return resp.text


def _parse_player_name(html: str, fallback: str) -> str:
    m = re.search(r"<title>\s*선수기록\s*:\s*(.*?)\s*:\s*KBReport\s*</title>", html, re.I | re.S)
    if m:
        return m.group(1).strip()
    return fallback


def _find_stat_table(html: str) -> pd.DataFrame:
    tables = pd.read_html(StringIO(html))
    for df in tables:
        cols = [str(c).strip() for c in df.columns]
        joined = "|".join(cols)
        if "시즌" in joined and "타석" in joined and "타수" in joined and "안타" in joined:
            return df
    raise RuntimeError("No hitter stat table found on KBReport page")


def _row_for_season(df: pd.DataFrame, season: int) -> pd.Series | None:
    c_season = None
    for c in df.columns:
        if "시즌" in str(c):
            c_season = c
            break
    if c_season is None:
        return None
    hit = df[df[c_season].astype(str).str.contains(str(season), na=False)]
    if hit.empty:
        return None
    return hit.iloc[0]


def _pick_col(df: pd.DataFrame, keys: list[str]) -> str | None:
    norm = {re.sub(r"\s+", "", str(c).lower()): c for c in df.columns}
    for k in keys:
        nk = re.sub(r"\s+", "", k.lower())
        if nk in norm:
            return norm[nk]
        for kk, raw in norm.items():
            if nk and (nk in kk or kk in nk):
                return raw
    return None


def _to_int(v: Any) -> int | None:
    if v is None:
        return None
    s = str(v).strip().replace(",", "")
    if s in {"", "-", "nan", "None"}:
        return None
    try:
        return int(float(s))
    except Exception:
        return None


def _to_float(v: Any) -> float | None:
    if v is None:
        return None
    s = str(v).strip().replace(",", "")
    if s in {"", "-", "nan", "None"}:
        return None
    try:
        return float(s)
    except Exception:
        return None


def _row_to_stat(df: pd.DataFrame, row: pd.Series) -> dict[str, Any]:
    c_games = _pick_col(df, ["경기", "G"])
    c_pa = _pick_col(df, ["타석", "PA"])
    c_ab = _pick_col(df, ["타수", "AB"])
    c_h = _pick_col(df, ["안타", "H"])
    c_hr = _pick_col(df, ["홈런", "HR"])
    c_bb = _pick_col(df, ["볼넷", "BB"])
    c_so = _pick_col(df, ["삼진", "SO"])
    c_avg = _pick_col(df, ["타율", "AVG"])
    c_obp = _pick_col(df, ["출루율", "OBP"])
    c_slg = _pick_col(df, ["장타율", "SLG"])
    c_ops = _pick_col(df, ["OPS"])
    return {
        "games": _to_int(row.get(c_games)) if c_games else None,
        "PA": _to_int(row.get(c_pa)) if c_pa else None,
        "AB": _to_int(row.get(c_ab)) if c_ab else None,
        "H": _to_int(row.get(c_h)) if c_h else None,
        "HR": _to_int(row.get(c_hr)) if c_hr else None,
        "BB": _to_int(row.get(c_bb)) if c_bb else None,
        "SO": _to_int(row.get(c_so)) if c_so else None,
        "AVG": _to_float(row.get(c_avg)) if c_avg else None,
        "OBP": _to_float(row.get(c_obp)) if c_obp else None,
        "SLG": _to_float(row.get(c_slg)) if c_slg else None,
        "OPS": _to_float(row.get(c_ops)) if c_ops else None,
    }


def _extract_opposite_options(html: str) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    m = re.search(r"<select[^>]+id=\"split02_1_opposite\"[^>]*>([\s\S]*?)</select>", html, re.I)
    if not m:
        return out
    for value, label in re.findall(r"<option\s+value=\"([^\"]*)\"[^>]*>(.*?)</option>", m.group(1), re.I | re.S):
        clean = re.sub(r"<[^>]+>", "", label).strip()
        if value.strip().isdigit():
            out.append((value.strip(), clean))
    return out


def _normalize_team_name(code: str, label: str) -> str:
    if code in TEAM_CODE_TO_NAME:
        return TEAM_CODE_TO_NAME[code]
    raw = (label or "").strip()
    upper = raw.upper()
    if upper in TEAM_ALIAS_TO_NAME:
        return TEAM_ALIAS_TO_NAME[upper]
    # fallback: keep original label
    return raw


def _save_split(
    conn,
    season: int,
    player_id: str,
    player_name: str,
    split_group: str,
    split_key: str,
    split_label: str,
    stat: dict[str, Any],
    source_url: str,
    collected_at: str,
) -> None:
    execute(
        conn,
        """
        INSERT INTO kbreport_hitter_splits
        (season, kbreport_player_id, player_name, split_group, split_key, split_label,
         games, PA, AB, H, HR, BB, SO, AVG, OBP, SLG, OPS, source, source_url, collected_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'KBREPORT', ?, ?)
        ON CONFLICT(season, kbreport_player_id, split_group, split_key) DO UPDATE SET
          player_name=excluded.player_name,
          split_label=excluded.split_label,
          games=excluded.games,
          PA=excluded.PA,
          AB=excluded.AB,
          H=excluded.H,
          HR=excluded.HR,
          BB=excluded.BB,
          SO=excluded.SO,
          AVG=excluded.AVG,
          OBP=excluded.OBP,
          SLG=excluded.SLG,
          OPS=excluded.OPS,
          source=excluded.source,
          source_url=excluded.source_url,
          collected_at=excluded.collected_at
        """,
        [
            season,
            player_id,
            player_name,
            split_group,
            split_key,
            split_label,
            stat["games"],
            stat["PA"],
            stat["AB"],
            stat["H"],
            stat["HR"],
            stat["BB"],
            stat["SO"],
            stat["AVG"],
            stat["OBP"],
            stat["SLG"],
            stat["OPS"],
            source_url,
            collected_at,
        ],
    )


def main() -> None:
    args = parse_args()
    player_id = str(args.player_id).strip()
    base_url = BASE_URL.format(player_id=player_id)
    collected_at = dt.datetime.utcnow().isoformat() + "Z"

    base_html = _fetch_html(player_id)
    player_name = args.player_name.strip() if args.player_name else _parse_player_name(base_html, player_id)

    # split definitions
    split_specs: list[tuple[str, str, str]] = [
        ("homeaway", "home", "HOME"),
        ("homeaway", "away", "AWAY"),
        ("pitchside", "pitchL", "VS_LHP"),
        ("pitchside", "pitchR", "VS_RHP"),
    ]
    for month in range(3, 12):
        split_specs.append(("month", str(month), f"MONTH_{month:02d}"))
    for code, team in _extract_opposite_options(base_html):
        normalized = _normalize_team_name(code, team)
        split_specs.append(("opposite", code, f"VS_TEAM_{normalized}"))

    conn = connect_for_path(args.db)
    try:
        init_kbreport_tables(conn)
        written = 0
        for split01, split_value, split_key in split_specs:
            params = {
                "rows": "20",
                "order": "",
                "orderType": "",
                "teamId": "",
                "defense_no": "",
                "option": str(args.season),
                "split01": split01,
                "split02_1": split_value,
                "split02_2": split_value if split01 == "month" else "",
                "split02": split_value,
            }
            html = _fetch_html(player_id, params=params)
            df = _find_stat_table(html)
            row = _row_for_season(df, args.season)
            if row is None:
                continue
            stat = _row_to_stat(df, row)
            split_label = split_value
            if split01 == "opposite":
                split_label = _normalize_team_name(split_value, split_value)

            _save_split(
                conn=conn,
                season=args.season,
                player_id=player_id,
                player_name=player_name,
                split_group=split01,
                split_key=split_key,
                split_label=split_label,
                stat=stat,
                source_url=requests.Request("GET", base_url, params=params).prepare().url,
                collected_at=collected_at,
            )
            written += 1

        conn.commit()
        print(f"[ok] kbreport_hitter_splits upserted={written} player_id={player_id} player_name={player_name}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
