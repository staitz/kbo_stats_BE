from typing import Any

from django.db import connection


def query_all(sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    with connection.cursor() as cursor:
        cursor.execute(sql, params)
        cols = [c[0] for c in cursor.description]
        rows = cursor.fetchall()
    return [dict(zip(cols, row)) for row in rows]


def query_one(sql: str, params: tuple[Any, ...] = ()) -> dict[str, Any] | None:
    rows = query_all(sql, params)
    return rows[0] if rows else None


def table_exists(table_name: str) -> bool:
    return table_name in connection.introspection.table_names()


def table_has_column(table_name: str, column_name: str) -> bool:
    if not table_exists(table_name):
        return False
    with connection.cursor() as cursor:
        description = connection.introspection.get_table_description(cursor, table_name)
    names = {col.name for col in description}
    return column_name in names


def default_season() -> int | None:
    if not table_exists("hitter_season_totals"):
        return None
    row = query_one("SELECT MAX(season) AS season FROM hitter_season_totals")
    if row and row.get("season"):
        return int(row["season"])
    return None


def season_game_window(season: int) -> dict[str, Any] | None:
    return query_one(
        """
        SELECT MIN(game_date) AS min_date, MAX(game_date) AS max_date
        FROM hitter_game_logs
        WHERE substr(game_date, 1, 4) = %s
        """,
        (str(season),),
    )


def max_team_games(season: int, team: str = "") -> int:
    if not table_exists("hitter_game_logs"):
        return 0
    where = ["substr(game_date, 1, 4) = %s"]
    params: list[Any] = [str(season)]
    if team:
        where.append("team = %s")
        params.append(team)
    
    where_sql = " AND ".join(where)
    row = query_one(
        f"""
        SELECT MAX(team_games) AS max_games
        FROM (
            SELECT team, COUNT(DISTINCT game_id) AS team_games
            FROM hitter_game_logs
            WHERE {where_sql}
            GROUP BY team
        )
        """,
        tuple(params),
    )
    return int((row or {}).get("max_games") or 0)


def leaderboard_candidate_count(season: int, min_pa: int, team: str = "") -> int:
    where = ["season = %s", "PA >= %s"]
    params: list[Any] = [season, min_pa]
    if team:
        where.append("team = %s")
        params.append(team)
    row = query_one(
        f"SELECT COUNT(*) AS total FROM hitter_season_totals WHERE {' AND '.join(where)}",
        tuple(params),
    )
    return int((row or {}).get("total") or 0)


def hitter_totals_fallback_season(requested_season: int) -> int | None:
    row = query_one("SELECT MAX(season) AS season FROM hitter_season_totals WHERE season < %s", (requested_season,))
    value = (row or {}).get("season")
    return int(value) if value else None


def latest_standings_as_of(season: int) -> str | None:
    row = query_one("SELECT MAX(as_of_date) AS as_of_date FROM team_standings WHERE season = %s", (season,))
    return (row or {}).get("as_of_date")


def standings_fallback_season(requested_season: int) -> int | None:
    row = query_one("SELECT MAX(season) AS season FROM team_standings WHERE season < %s", (requested_season,))
    value = (row or {}).get("season")
    return int(value) if value else None


def standings_rows(season: int, as_of_date: str) -> list[dict[str, Any]]:
    return query_all(
        """
        SELECT rank, team, games, wins, losses, draws, win_pct, gb, recent_10, streak, home_record, away_record
        FROM team_standings
        WHERE season = %s AND as_of_date = %s
        ORDER BY rank ASC, team ASC
        """,
        (season, as_of_date),
    )


def home_base_totals(season: int) -> dict[str, Any]:
    return query_one(
        """
        SELECT
            COUNT(*) AS players,
            COUNT(DISTINCT team) AS teams,
            COALESCE(SUM(HR), 0) AS total_hr,
            COALESCE(SUM(PA), 0) AS total_pa
        FROM hitter_season_totals
        WHERE season = %s
        """,
        (season,),
    ) or {"players": 0, "teams": 0, "total_hr": 0, "total_pa": 0}


def latest_game_date(season: int) -> str | None:
    row = query_one(
        "SELECT MAX(game_date) AS latest_game_date FROM hitter_game_logs WHERE substr(game_date, 1, 4) = %s",
        (str(season),),
    )
    return (row or {}).get("latest_game_date")


def latest_prediction_date(season: int) -> str | None:
    row = query_one("SELECT MAX(as_of_date) AS latest_prediction_date FROM hitter_predictions WHERE season = %s", (season,))
    return (row or {}).get("latest_prediction_date")


def top_ops_rows(season: int, min_pa: int, limit: int = 5) -> list[dict[str, Any]]:
    return query_all(
        """
        SELECT team, player_name, PA, H, HR, RBI, OPS
        FROM hitter_season_totals
        WHERE season = %s AND PA >= %s
        ORDER BY OPS DESC, PA DESC
        LIMIT %s
        """,
        (season, min_pa, limit),
    )


def top_avg_rows(season: int, min_pa: int, limit: int = 5) -> list[dict[str, Any]]:
    return query_all(
        """
        SELECT team, player_name, PA, H, HR, RBI, AVG, OPS
        FROM hitter_season_totals
        WHERE season = %s AND PA >= %s
        ORDER BY AVG DESC, PA DESC
        LIMIT %s
        """,
        (season, min_pa, limit),
    )


def top_hr_rows(season: int, min_pa: int, limit: int = 5) -> list[dict[str, Any]]:
    return query_all(
        """
        SELECT team, player_name, PA, H, HR, RBI, OPS
        FROM hitter_season_totals
        WHERE season = %s AND PA >= %s
        ORDER BY HR DESC, PA DESC
        LIMIT %s
        """,
        (season, min_pa, limit),
    )


def top_war_rows(season: int, min_pa: int, limit: int = 5) -> list[dict[str, Any]]:
    # Prefer real WAR column if it exists.
    if table_has_column("hitter_season_totals", "WAR"):
        return query_all(
            """
            SELECT team, player_name, PA, H, HR, RBI, OPS, WAR
            FROM hitter_season_totals
            WHERE season = %s AND PA >= %s
            ORDER BY WAR DESC, PA DESC
            LIMIT %s
            """,
            (season, min_pa, limit),
        )

    # Fallback: pseudo-WAR approximation from OPS and PA to keep UI populated.
    return query_all(
        """
        SELECT
            team,
            player_name,
            PA,
            H,
            HR,
            RBI,
            OPS,
            ROUND(((OPS - 0.700) * PA) / 70.0, 2) AS WAR
        FROM hitter_season_totals
        WHERE season = %s AND PA >= %s
        ORDER BY WAR DESC, PA DESC
        LIMIT %s
        """,
        (season, min_pa, limit),
    )


def top_era_rows(season: int, limit: int = 5) -> list[dict[str, Any]]:
    # No pitcher table in current MVP DB; derive team run-prevention ranking (RA/G) from game logs.
    if not table_exists("hitter_game_logs"):
        return []

    return query_all(
        """
        WITH team_scores AS (
            SELECT game_id, team, COALESCE(SUM(R), 0) AS runs
            FROM hitter_game_logs
            WHERE substr(game_date, 1, 4) = %s
            GROUP BY game_id, team
        ),
        paired AS (
            SELECT a.game_id, a.team AS team, b.runs AS runs_allowed
            FROM team_scores a
            JOIN team_scores b ON a.game_id = b.game_id AND a.team <> b.team
        )
        SELECT
            team,
            team AS player_name,
            ROUND(1.0 * SUM(runs_allowed) / COUNT(*), 3) AS ERA
        FROM paired
        GROUP BY team
        ORDER BY ERA ASC, team ASC
        LIMIT %s
        """,
        (str(season), limit),
    )


def standings_preview_rows(season: int, as_of_date: str, limit: int = 10) -> list[dict[str, Any]]:
    return query_all(
        """
        SELECT rank, team, wins, losses, draws, win_pct, gb
        FROM team_standings
        WHERE season = %s AND as_of_date = %s
        ORDER BY rank ASC
        LIMIT %s
        """,
        (season, as_of_date, limit),
    )


def leaderboard_total(season: int, min_pa: int, team: str = "") -> int:
    where = ["season = %s", "PA >= %s"]
    params: list[Any] = [season, min_pa]
    if team:
        where.append("team = %s")
        params.append(team)
    row = query_one(f"SELECT COUNT(*) AS total FROM hitter_season_totals WHERE {' AND '.join(where)}", tuple(params))
    return int((row or {}).get("total") or 0)


def leaderboard_rows(
    season: int,
    min_pa: int,
    order_metric: str,
    limit: int,
    offset: int,
    team: str = "",
) -> list[dict[str, Any]]:
    where = ["season = %s", "PA >= %s"]
    params: list[Any] = [season, min_pa]
    if team:
        where.append("team = %s")
        params.append(team)
    where_sql = " AND ".join(where)

    order_clause = f"{order_metric} DESC"
    if order_metric == "AVG":
        order_clause = "AVG DESC"

    return query_all(
        f"""
        SELECT team, player_name, games, PA, AB, H, HR, RBI, AVG, OBP, SLG, OPS
        FROM hitter_season_totals
        WHERE {where_sql}
        ORDER BY {order_clause}, PA DESC, player_name ASC
        LIMIT %s OFFSET %s
        """,
        tuple(params + [limit, offset]),
    )


def predictions_latest_date(season: int) -> str | None:
    rows = query_all(
        """
        SELECT MAX(as_of_date) AS latest_date
        FROM hitter_predictions
        WHERE season = %s
        """,
        (season,),
    )
    return rows[0]["latest_date"] if rows else None


def predictions_latest_rows(season: int, latest_date: str, limit: int = 100) -> list[dict[str, Any]]:
    return query_all(
        """
        SELECT team, player_name, predicted_hr_final, predicted_ops_final,
               confidence_level, confidence_score, pa_to_date, blend_weight, model_source
        FROM hitter_predictions
        WHERE season = %s AND as_of_date = %s
        ORDER BY predicted_ops_final DESC
        LIMIT %s
        """,
        (season, latest_date, limit),
    )


def player_search_rows(season: int, q: str, limit: int, team: str = "") -> list[dict[str, Any]]:
    where = ["season = %s", "player_name LIKE %s"]
    params: list[Any] = [season, f"%{q}%"]
    if team:
        where.append("team = %s")
        params.append(team)
    return query_all(
        f"""
        SELECT team, player_name, PA, AB, H, HR, OPS
        FROM hitter_season_totals
        WHERE {' AND '.join(where)}
        ORDER BY OPS DESC, PA DESC
        LIMIT %s
        """,
        tuple(params + [limit]),
    )


def player_season_rows(player_name: str) -> list[dict[str, Any]]:
    return query_all(
        """
        SELECT season, team, games, PA, AB, H, "2B", "3B", HR, RBI, BB, SO, HBP, SH, SF, SB, CS, GDP, AVG, OBP, SLG, OPS
        FROM hitter_season_totals
        WHERE player_name = %s
        ORDER BY season DESC, team ASC
        """,
        (player_name,),
    )


def player_latest_prediction(season: int, player_name: str) -> dict[str, Any] | None:
    return query_one(
        """
        SELECT season, as_of_date, team, predicted_hr_final, predicted_ops_final,
               confidence_level, confidence_score, model_source
        FROM hitter_predictions
        WHERE season = %s AND player_name = %s
        ORDER BY as_of_date DESC
        LIMIT 1
        """,
        (season, player_name),
    )


def player_trend_rows(season: int, player_name: str) -> list[dict[str, Any]]:
    return query_all(
        """
        SELECT as_of_date, team, PA, HR, OPS, OPS_7, OPS_14
        FROM hitter_daily_snapshots
        WHERE season = %s AND player_name = %s
        ORDER BY as_of_date ASC, team ASC
        """,
        (season, player_name),
    )


def player_monthly_rows(player_name: str, season: int, tb_expr: str) -> list[dict[str, Any]]:
    return query_all(
        f"""
        SELECT
            substr(game_date, 1, 6) AS month,
            team,
            COUNT(DISTINCT game_id) AS games,
            COALESCE(SUM(PA), 0) AS PA,
            COALESCE(SUM(AB), 0) AS AB,
            COALESCE(SUM(H), 0) AS H,
            COALESCE(SUM(HR), 0) AS HR,
            COALESCE(SUM(BB), 0) AS BB,
            COALESCE(SUM(SO), 0) AS SO,
            COALESCE(SUM(HBP), 0) AS HBP,
            COALESCE(SUM(SF), 0) AS SF,
            COALESCE(SUM({tb_expr}), 0) AS TB_adj
        FROM hitter_game_logs
        WHERE player_name = %s
          AND substr(game_date, 1, 4) = %s
        GROUP BY substr(game_date, 1, 6), team
        ORDER BY month ASC, team ASC
        """,
        (player_name, str(season)),
    )


def player_vs_team_rows(player_name: str, season: int, tb_expr: str) -> list[dict[str, Any]]:
    return query_all(
        f"""
        SELECT
            team AS player_team,
            opp_team,
            COUNT(DISTINCT game_id) AS games,
            COALESCE(SUM(PA), 0) AS PA,
            COALESCE(SUM(AB), 0) AS AB,
            COALESCE(SUM(H), 0) AS H,
            COALESCE(SUM(HR), 0) AS HR,
            COALESCE(SUM(BB), 0) AS BB,
            COALESCE(SUM(SO), 0) AS SO,
            COALESCE(SUM({tb_expr}), 0) AS TB_adj
        FROM (
            SELECT
                g1.game_id,
                g1.team,
                g1.player_name,
                g1.PA,
                g1.AB,
                g1.H,
                g1.HR,
                g1.BB,
                g1.SO,
                g1."2B",
                g1."3B",
                g1.TB,
                (
                    SELECT MIN(g2.team)
                    FROM hitter_game_logs g2
                    WHERE g2.game_id = g1.game_id AND g2.team <> g1.team
                ) AS opp_team
            FROM hitter_game_logs g1
            WHERE g1.player_name = %s
              AND substr(g1.game_date, 1, 4) = %s
        ) x
        WHERE opp_team IS NOT NULL
        GROUP BY team, opp_team
        ORDER BY PA DESC, opp_team ASC
        """,
        (player_name, str(season)),
    )


def player_recent_games_rows(player_name: str, season: int, recent_n: int) -> list[dict[str, Any]]:
    return query_all(
        """
        WITH player_games AS (
            SELECT DISTINCT game_id, game_date
            FROM hitter_game_logs
            WHERE player_name = %s
              AND substr(game_date, 1, 4) = %s
            ORDER BY game_date DESC, game_id DESC
            LIMIT %s
        )
        SELECT
            g.game_date,
            g.game_id,
            g.team,
            COALESCE(SUM(g.PA), 0) AS PA,
            COALESCE(SUM(g.AB), 0) AS AB,
            COALESCE(SUM(g.H), 0) AS H,
            COALESCE(SUM(g.HR), 0) AS HR,
            COALESCE(SUM(g.BB), 0) AS BB,
            COALESCE(SUM(g.SO), 0) AS SO,
            COALESCE(SUM(g.TB), 0) AS TB
        FROM hitter_game_logs g
        JOIN player_games p ON p.game_id = g.game_id AND p.game_date = g.game_date
        WHERE g.player_name = %s
        GROUP BY g.game_date, g.game_id, g.team
        ORDER BY g.game_date DESC, g.game_id DESC
        """,
        (player_name, str(season), recent_n, player_name),
    )


def player_current_aggregate(season: int, player_name: str, ops_expr: str) -> dict[str, Any] | None:
    return query_one(
        f"""
        SELECT
            COUNT(DISTINCT team) AS teams,
            COALESCE(SUM(games), 0) AS games,
            COALESCE(SUM(PA), 0) AS PA,
            COALESCE(SUM(AB), 0) AS AB,
            COALESCE(SUM(H), 0) AS H,
            COALESCE(SUM("2B"), 0) AS "2B",
            COALESCE(SUM("3B"), 0) AS "3B",
            COALESCE(SUM(HR), 0) AS HR,
            COALESCE(SUM(RBI), 0) AS RBI,
            COALESCE(SUM(BB), 0) AS BB,
            COALESCE(SUM(SO), 0) AS SO,
            COALESCE(SUM(HBP), 0) AS HBP,
            COALESCE(SUM(SF), 0) AS SF,
            COALESCE(SUM(SH), 0) AS SH,
            COALESCE(SUM(SB), 0) AS SB,
            COALESCE(SUM(CS), 0) AS CS,
            COALESCE(SUM(GDP), 0) AS GDP,
            {ops_expr} AS OPS
        FROM hitter_season_totals
        WHERE season = %s AND player_name = %s
        """,
        (season, player_name),
    )


def player_kbreport_split_rows(season: int, player_name: str) -> list[dict[str, Any]]:
    return query_all(
        """
        SELECT split_group, split_key, split_label, PA, AB, H, HR, BB, SO, AVG, OBP, SLG, OPS
        FROM kbreport_hitter_splits
        WHERE season = %s AND player_name = %s
        ORDER BY split_group ASC, split_key ASC
        """,
        (season, player_name),
    )


def team_summary(season: int, team: str) -> dict[str, Any] | None:
    return query_one(
        """
        SELECT
            COUNT(*) AS players,
            COALESCE(SUM(games), 0) AS games,
            COALESCE(SUM(PA), 0) AS PA,
            COALESCE(SUM(AB), 0) AS AB,
            COALESCE(SUM(H), 0) AS H,
            COALESCE(SUM(HR), 0) AS HR,
            COALESCE(SUM(RBI), 0) AS RBI,
            COALESCE(SUM(BB), 0) AS BB,
            COALESCE(SUM(SO), 0) AS SO,
            COALESCE(AVG(OPS), 0) AS avg_player_ops
        FROM hitter_season_totals
        WHERE season = %s AND team = %s
        """,
        (season, team),
    )


def team_leaders_ops(season: int, team: str, min_pa: int, limit: int = 10) -> list[dict[str, Any]]:
    return query_all(
        """
        SELECT player_name, PA, H, HR, RBI, OPS
        FROM hitter_season_totals
        WHERE season = %s AND team = %s AND PA >= %s
        ORDER BY OPS DESC, PA DESC
        LIMIT %s
        """,
        (season, team, min_pa, limit),
    )


def team_leaders_hr(season: int, team: str, min_pa: int, limit: int = 10) -> list[dict[str, Any]]:
    return query_all(
        """
        SELECT player_name, PA, H, HR, RBI, OPS
        FROM hitter_season_totals
        WHERE season = %s AND team = %s AND PA >= %s
        ORDER BY HR DESC, PA DESC
        LIMIT %s
        """,
        (season, team, min_pa, limit),
    )


def team_monthly_rows(team: str, season: int, tb_expr: str) -> list[dict[str, Any]]:
    return query_all(
        f"""
        SELECT
            substr(game_date, 1, 6) AS month,
            COUNT(DISTINCT game_id) AS games,
            COALESCE(SUM(PA), 0) AS PA,
            COALESCE(SUM(AB), 0) AS AB,
            COALESCE(SUM(H), 0) AS H,
            COALESCE(SUM(HR), 0) AS HR,
            COALESCE(SUM(RBI), 0) AS RBI,
            COALESCE(SUM(BB), 0) AS BB,
            COALESCE(SUM(SO), 0) AS SO,
            COALESCE(SUM({tb_expr}), 0) AS TB_adj
        FROM hitter_game_logs
        WHERE team = %s
          AND substr(game_date, 1, 4) = %s
        GROUP BY substr(game_date, 1, 6)
        ORDER BY month ASC
        """,
        (team, str(season)),
    )


def team_recent_games(team: str, season: int, limit: int = 20) -> list[dict[str, Any]]:
    return query_all(
        """
        SELECT game_date, game_id, COUNT(*) AS batter_rows
        FROM hitter_game_logs
        WHERE team = %s
          AND substr(game_date, 1, 4) = %s
        GROUP BY game_date, game_id
        ORDER BY game_date DESC, game_id DESC
        LIMIT %s
        """,
        (team, str(season), limit),
    )


def team_latest_prediction_date(season: int, team: str) -> str | None:
    row = query_one(
        "SELECT MAX(as_of_date) AS latest_date FROM hitter_predictions WHERE season = %s AND team = %s",
        (season, team),
    )
    return (row or {}).get("latest_date")


def team_latest_predictions(season: int, team: str, latest_date: str, limit: int = 10) -> list[dict[str, Any]]:
    return query_all(
        """
        SELECT player_name, predicted_hr_final, predicted_ops_final, confidence_level, blend_weight, model_source
        FROM hitter_predictions
        WHERE season = %s AND team = %s AND as_of_date = %s
        ORDER BY predicted_ops_final DESC
        LIMIT %s
        """,
        (season, team, latest_date, limit),
    )


def player_compare_rows(season: int, names: list[str]) -> list[dict[str, Any]]:
    placeholders = ", ".join(["%s"] * len(names))
    return query_all(
        f"""
        SELECT season, team, player_name, games, PA, AB, H, HR, RBI, BB, SO, AVG, OBP, SLG, OPS
        FROM hitter_season_totals
        WHERE season = %s
          AND player_name IN ({placeholders})
        ORDER BY player_name ASC, OPS DESC, PA DESC
        """,
        tuple([season] + names),
    )


def games_by_date_rows(game_date: str, season: int, limit: int) -> list[dict[str, Any]]:
    params: list[Any] = []
    where = []
    if game_date:
        where.append("game_date = %s")
        params.append(game_date)
    else:
        where.append("substr(game_date, 1, 4) = %s")
        params.append(str(season))
    where_sql = " AND ".join(where)
    return query_all(
        f"""
        WITH team_scores AS (
            SELECT
                game_date,
                game_id,
                team,
                COALESCE(SUM(R), 0) AS runs
            FROM hitter_game_logs
            WHERE {where_sql}
            GROUP BY game_date, game_id, team
        ),
        ranked AS (
            SELECT
                game_date,
                game_id,
                team,
                runs,
                ROW_NUMBER() OVER (PARTITION BY game_id ORDER BY team ASC) AS rn
            FROM team_scores
        )
        SELECT
            a.game_date,
            a.game_id,
            a.team AS away_team,
            a.runs AS away_score,
            b.team AS home_team,
            b.runs AS home_score
        FROM ranked a
        JOIN ranked b ON a.game_id = b.game_id
        WHERE a.rn = 1 AND b.rn = 2
        ORDER BY a.game_date DESC, a.game_id DESC
        LIMIT %s
        """,
        tuple(params + [limit]),
    )


def game_boxscore_rows(game_id: str) -> list[dict[str, Any]]:
    return query_all(
        """
        SELECT
            game_date, game_id, team, player_name,
            PA, AB, H, "2B", "3B", HR, BB, SO, HBP, SH, SF, R, RBI, SB, CS, GDP
        FROM hitter_game_logs
        WHERE game_id = %s
        ORDER BY team ASC, player_name ASC
        """,
        (game_id,),
    )


def game_boxscore_team_summaries(game_id: str) -> list[dict[str, Any]]:
    return query_all(
        """
        SELECT
            team,
            COALESCE(SUM(R), 0) AS score,
            COALESCE(SUM(PA), 0) AS PA,
            COALESCE(SUM(AB), 0) AS AB,
            COALESCE(SUM(H), 0) AS H,
            COALESCE(SUM(HR), 0) AS HR,
            COALESCE(SUM(BB), 0) AS BB,
            COALESCE(SUM(SO), 0) AS SO,
            COALESCE(SUM(RBI), 0) AS RBI
        FROM hitter_game_logs
        WHERE game_id = %s
        GROUP BY team
        ORDER BY team ASC
        """,
        (game_id,),
    )
