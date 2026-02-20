import argparse
import sqlite3
from urllib.parse import parse_qs, urlparse

from collector.fetch_kbreport_hitter_splits import TEAM_CODE_TO_NAME, _normalize_team_name


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Normalize KBReport opposite split team labels")
    parser.add_argument("--db", default="kbo_stats.db")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    conn = sqlite3.connect(args.db)
    try:
        cur = conn.cursor()
        rows = cur.execute(
            """
            SELECT season, kbreport_player_id, split_group, split_key, split_label, source_url
            FROM kbreport_hitter_splits
            WHERE split_group = 'opposite'
            """
        ).fetchall()
        updated = 0
        for season, player_id, split_group, split_key, split_label, source_url in rows:
            code = ""
            if source_url:
                try:
                    qs = parse_qs(urlparse(source_url).query)
                    code = (qs.get("split02_1") or [""])[0].strip()
                except Exception:
                    code = ""
            # recover code from source_url is not stored separately; use split_key text fallback only
            team_name = ""
            if split_key and str(split_key).startswith("VS_TEAM_"):
                team_name = str(split_key).replace("VS_TEAM_", "", 1).strip()
            if code in TEAM_CODE_TO_NAME:
                normalized = TEAM_CODE_TO_NAME[code]
            else:
                normalized = _normalize_team_name(code, team_name or str(split_label or ""))
            new_key = f"VS_TEAM_{normalized}"
            new_label = normalized
            if new_key == split_key and new_label == split_label:
                continue
            cur.execute(
                """
                UPDATE kbreport_hitter_splits
                SET split_key = ?, split_label = ?
                WHERE season = ? AND kbreport_player_id = ? AND split_group = ? AND split_key = ?
                """,
                (new_key, new_label, season, player_id, split_group, split_key),
            )
            updated += 1

        conn.commit()
        print(f"[ok] normalized opposite split rows updated={updated}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
