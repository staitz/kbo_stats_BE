import argparse
import datetime as dt

from collector.kbo_api import fetch_month_schedule
from db_support import connect_for_path, execute


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync team schedule from Naver Sports API")
    parser.add_argument("--db", default="kbo_stats.db")
    parser.add_argument("--season", type=int, required=True)
    parser.add_argument("--start-month", type=int, default=1)
    parser.add_argument("--end-month", type=int, default=12)
    return parser.parse_args()


def _init_table(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS team_schedule (
            season INTEGER NOT NULL,
            game_date TEXT NOT NULL,
            game_id TEXT,
            schedule_key TEXT NOT NULL,
            away_team TEXT NOT NULL,
            home_team TEXT NOT NULL,
            game_time TEXT,
            stadium TEXT,
            status TEXT,
            source TEXT NOT NULL DEFAULT 'NAVER_SPORTS',
            collected_at TEXT NOT NULL,
            PRIMARY KEY (season, schedule_key)
        )
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_team_schedule_season_key
        ON team_schedule (season, schedule_key)
        """
    )
    conn.commit()


def _build_schedule_key(row: dict) -> str:
    game_id = str(row.get("game_id") or "").strip()
    if game_id:
        return game_id
    fallback = str(row.get("fallback_id") or "").strip()
    if fallback:
        return fallback
    game_date = str(row.get("date") or "").strip()
    away = str(row.get("away_team") or "").strip()
    home = str(row.get("home_team") or "").strip()
    game_time = str(row.get("time") or "").strip()
    return f"{game_date}_{away}_{home}_{game_time}"


def main() -> None:
    args = parse_args()
    season = int(args.season)
    start_month = max(1, min(12, int(args.start_month)))
    end_month = max(1, min(12, int(args.end_month)))
    if end_month < start_month:
        raise ValueError("end-month must be >= start-month")

    conn = connect_for_path(args.db)
    try:
        _init_table(conn)
        now = dt.datetime.utcnow().isoformat() + "Z"
        upserted = 0
        for month in range(start_month, end_month + 1):
            rows = fetch_month_schedule(str(season), str(month).zfill(2), debug=False)
            for row in rows:
                game_date = str(row.get("date") or "").strip()
                away_team = str(row.get("away_team") or "").strip()
                home_team = str(row.get("home_team") or "").strip()
                if not game_date or not away_team or not home_team:
                    continue
                schedule_key = _build_schedule_key(row)
                execute(
                    conn,
                    """
                    INSERT INTO team_schedule
                    (season, game_date, game_id, schedule_key, away_team, home_team, game_time, stadium, status, source, collected_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'NAVER_SPORTS', ?)
                    ON CONFLICT(season, schedule_key) DO UPDATE SET
                      game_date=excluded.game_date,
                      game_id=excluded.game_id,
                      away_team=excluded.away_team,
                      home_team=excluded.home_team,
                      game_time=excluded.game_time,
                      stadium=excluded.stadium,
                      status=excluded.status,
                      source=excluded.source,
                      collected_at=excluded.collected_at
                    """,
                    [
                        season,
                        game_date,
                        str(row.get("game_id") or "").strip() or None,
                        schedule_key,
                        away_team,
                        home_team,
                        str(row.get("time") or "").strip() or None,
                        str(row.get("stadium") or "").strip() or None,
                        str(row.get("status") or "").strip() or None,
                        now,
                    ],
                )
                upserted += 1
        conn.commit()
        print(
            f"[ok] team_schedule upserted={upserted} "
            f"season={season} months={start_month:02d}-{end_month:02d}"
        )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
