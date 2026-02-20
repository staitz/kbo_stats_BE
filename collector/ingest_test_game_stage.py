import argparse
import datetime as dt
import sqlite3
from typing import Any

from collector.statiz_db import init_statiz_tables


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Test-only single game staging for game_results_stage + innings_scores_stage"
    )
    parser.add_argument("--db", default="kbo_stats.db")
    parser.add_argument("--date", default="20250612", help="YYYYMMDD")
    parser.add_argument("--game-id", help="optional exact game_id")
    parser.add_argument(
        "--source-url",
        default="TEST_LOCAL_DERIVED",
        help="store provenance in stage tables",
    )
    return parser.parse_args()


def _row_as_dict(columns: list[str], row: tuple[Any, ...]) -> dict[str, Any]:
    return {columns[i]: row[i] for i in range(len(columns))}


def main() -> None:
    args = parse_args()
    now = dt.datetime.utcnow().isoformat() + "Z"

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    try:
        init_statiz_tables(conn)

        if args.game_id:
            rows = conn.execute(
                """
                SELECT game_date, game_id, team, SUM(R) AS runs
                FROM hitter_game_logs
                WHERE game_date = ? AND game_id = ?
                GROUP BY game_date, game_id, team
                ORDER BY team ASC
                """,
                (args.date, args.game_id),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT game_date, game_id, team, SUM(R) AS runs
                FROM hitter_game_logs
                WHERE game_date = ?
                GROUP BY game_date, game_id, team
                ORDER BY game_id ASC, team ASC
                """,
                (args.date,),
            ).fetchall()

        if not rows:
            raise SystemExit(f"No hitter_game_logs rows for date={args.date} game_id={args.game_id}")

        first_game_id = rows[0]["game_id"]
        game_rows = [r for r in rows if r["game_id"] == first_game_id]
        if len(game_rows) < 2:
            raise SystemExit(f"Need at least 2 teams in game_id={first_game_id}; found={len(game_rows)}")

        away = _row_as_dict(list(game_rows[0].keys()), tuple(game_rows[0]))
        home = _row_as_dict(list(game_rows[1].keys()), tuple(game_rows[1]))

        conn.execute(
            """
            INSERT INTO game_results_stage
            (game_id, game_date, home_team, away_team, home_score, away_score, source, source_url, collected_at)
            VALUES (?, ?, ?, ?, ?, ?, 'TEST_LOCAL_DERIVED', ?, ?)
            ON CONFLICT(game_id) DO UPDATE SET
              game_date=excluded.game_date,
              home_team=excluded.home_team,
              away_team=excluded.away_team,
              home_score=excluded.home_score,
              away_score=excluded.away_score,
              source=excluded.source,
              source_url=excluded.source_url,
              collected_at=excluded.collected_at
            """,
            (
                first_game_id,
                away["game_date"],
                home["team"],
                away["team"],
                int(home["runs"] or 0),
                int(away["runs"] or 0),
                args.source_url,
                now,
            ),
        )

        # Test-only dummy inning row: we only validate stage write path before league start.
        conn.execute(
            """
            INSERT INTO innings_scores_stage
            (game_id, inning_no, away_runs, home_runs, source, source_url, collected_at)
            VALUES (?, 1, ?, ?, 'TEST_LOCAL_DERIVED', ?, ?)
            ON CONFLICT(game_id, inning_no) DO UPDATE SET
              away_runs=excluded.away_runs,
              home_runs=excluded.home_runs,
              source=excluded.source,
              source_url=excluded.source_url,
              collected_at=excluded.collected_at
            """,
            (
                first_game_id,
                int(away["runs"] or 0),
                int(home["runs"] or 0),
                args.source_url,
                now,
            ),
        )

        conn.commit()
        print(
            f"[ok] staged test game: date={away['game_date']} game_id={first_game_id} "
            f"away={away['team']}({int(away['runs'] or 0)}) home={home['team']}({int(home['runs'] or 0)})"
        )
        print("[ok] staged test inning row: inning_no=1 (test-only placeholder)")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
