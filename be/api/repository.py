from collections import defaultdict
from typing import Any

from django.db import connection


OPS_WAR_FALLBACK_BASELINE = 0.700
OPS_WAR_FALLBACK_DIVISOR = 70.0


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


def pitcher_leaderboard_candidate_count(season: int, min_outs: int, team: str = "") -> int:
    where = ["season = %s", "OUTS >= %s"]
    params: list[Any] = [season, min_outs]
    if team:
        where.append("team = %s")
        params.append(team)
    row = query_one(
        f"SELECT COUNT(*) AS total FROM pitcher_season_totals WHERE {' AND '.join(where)}",
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


def logs_latest_season_at_or_before(requested_season: int) -> int | None:
    if not table_exists("hitter_game_logs"):
        return None
    row = query_one(
        """
        SELECT MAX(CAST(substr(game_date, 1, 4) AS INTEGER)) AS season
        FROM hitter_game_logs
        WHERE CAST(substr(game_date, 1, 4) AS INTEGER) <= %s
        """,
        (requested_season,),
    )
    value = (row or {}).get("season")
    return int(value) if value else None


def logs_latest_game_date(season: int) -> str | None:
    if not table_exists("hitter_game_logs"):
        return None
    row = query_one(
        "SELECT MAX(game_date) AS as_of_date FROM hitter_game_logs WHERE substr(game_date, 1, 4) = %s",
        (str(season),),
    )
    return (row or {}).get("as_of_date")


def _team_game_rows(season: int) -> list[dict[str, Any]]:
    return query_all(
        """
        WITH team_scores AS (
            SELECT
                game_date,
                game_id,
                team,
                COALESCE(SUM(R), 0) AS runs
            FROM hitter_game_logs
            WHERE substr(game_date, 1, 4) = %s
            GROUP BY game_date, game_id, team
        )
        SELECT
            a.game_date,
            a.game_id,
            a.team AS team,
            b.team AS opp_team,
            a.runs AS team_runs,
            b.runs AS opp_runs,
            CASE
                WHEN a.runs > b.runs THEN 'W'
                WHEN a.runs < b.runs THEN 'L'
                ELSE 'D'
            END AS result
        FROM team_scores a
        JOIN team_scores b ON a.game_id = b.game_id AND a.team <> b.team
        ORDER BY a.game_date DESC, a.game_id DESC
        """,
        (str(season),),
    )


def computed_standings_rows(season: int) -> list[dict[str, Any]]:
    rows = _team_game_rows(season)
    by_team: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        team = str(row.get("team") or "").strip()
        if not team:
            continue
        by_team[team].append(row)

    standings: list[dict[str, Any]] = []
    for team, games in by_team.items():
        wins = sum(1 for g in games if g.get("result") == "W")
        losses = sum(1 for g in games if g.get("result") == "L")
        draws = sum(1 for g in games if g.get("result") == "D")
        games_cnt = len(games)
        pct = (wins / (wins + losses)) if (wins + losses) > 0 else 0.0

        latest10 = games[:10]
        r10_w = sum(1 for g in latest10 if g.get("result") == "W")
        r10_l = sum(1 for g in latest10 if g.get("result") == "L")
        r10_d = sum(1 for g in latest10 if g.get("result") == "D")
        recent_10 = f"{r10_w}승{r10_d}무{r10_l}패"

        streak_type = ""
        streak_count = 0
        for g in games:
            result = str(g.get("result") or "")
            if result not in {"W", "L"}:
                continue
            if not streak_type:
                streak_type = result
                streak_count = 1
                continue
            if result == streak_type:
                streak_count += 1
            else:
                break
        streak = f"{streak_count}{'연승' if streak_type == 'W' else '연패'}" if streak_type else "-"

        standings.append(
            {
                "team": team,
                "games": games_cnt,
                "wins": wins,
                "losses": losses,
                "draws": draws,
                "win_pct": round(pct, 3),
                "recent_10": recent_10,
                "streak": streak,
                "home_record": None,
                "away_record": None,
            }
        )

    standings.sort(key=lambda r: (-float(r["win_pct"]), -int(r["wins"]), int(r["losses"]), str(r["team"])))
    if not standings:
        return []

    leader_wins = int(standings[0]["wins"])
    leader_losses = int(standings[0]["losses"])

    for idx, row in enumerate(standings, start=1):
        gb = ((leader_wins - int(row["wins"])) + (int(row["losses"]) - leader_losses)) / 2.0
        row["rank"] = idx
        row["gb"] = round(gb, 1)

    return standings


def home_base_totals(season: int) -> dict[str, Any]:
    base = query_one(
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

    games_row = query_one(
        """
        SELECT COUNT(DISTINCT game_id) AS total_games
        FROM hitter_game_logs
        WHERE substr(game_date, 1, 4) = %s
        """,
        (str(season),),
    )
    base["total_games"] = (games_row or {}).get("total_games") or 0
    return base


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
    if table_has_column("hitter_season_totals", "batter_war"):
        return query_all(
            """
            SELECT team, player_name, PA, H, HR, RBI, OPS, batter_war AS WAR
            FROM hitter_season_totals
            WHERE season = %s AND PA >= %s
            ORDER BY batter_war DESC, PA DESC
            LIMIT %s
            """,
            (season, min_pa, limit),
        )

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

    # Fallback: keep the historical OPS/PA proxy if richer batter_war inputs
    # are unavailable in the current table shape.
    return query_all(
        f"""
        SELECT
            team,
            player_name,
            PA,
            H,
            HR,
            RBI,
            OPS,
            ROUND(((OPS - {OPS_WAR_FALLBACK_BASELINE}) * PA) / {OPS_WAR_FALLBACK_DIVISOR}, 2) AS WAR
        FROM hitter_season_totals
        WHERE season = %s AND PA >= %s
        ORDER BY WAR DESC, PA DESC
        LIMIT %s
        """,
        (season, min_pa, limit),
    )


def top_era_rows(season: int, limit: int = 5, min_outs: int = 15) -> list[dict[str, Any]]:
    if table_exists("pitcher_season_totals"):
        return query_all(
            """
            SELECT
                team,
                player_name,
                ROUND(ERA, 3) AS ERA,
                ROUND(WHIP, 3) AS WHIP,
                SO,
                W,
                SV,
                HLD,
                OUTS
            FROM pitcher_season_totals
            WHERE season = %s AND OUTS >= %s
            ORDER BY ERA ASC, OUTS DESC, SO DESC, player_name ASC
            LIMIT %s
            """,
            (season, min_outs, limit),
        )

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


def pitcher_leaderboard_total(season: int, min_outs: int, team: str = "") -> int:
    where = ["season = %s", "OUTS >= %s"]
    params: list[Any] = [season, min_outs]
    if team:
        where.append("team = %s")
        params.append(team)
    row = query_one(
        f"SELECT COUNT(*) AS total FROM pitcher_season_totals WHERE {' AND '.join(where)}",
        tuple(params),
    )
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


def pitcher_leaderboard_rows(
    season: int,
    min_outs: int,
    order_metric: str,
    limit: int,
    offset: int,
    team: str = "",
) -> list[dict[str, Any]]:
    where = ["season = %s", "OUTS >= %s"]
    params: list[Any] = [season, min_outs]
    if team:
        where.append("team = %s")
        params.append(team)
    where_sql = " AND ".join(where)

    order_clause = f"{order_metric} ASC"
    if order_metric in {"W", "SV", "HLD", "SO", "IP"}:
        order_clause = f"{order_metric} DESC"

    return query_all(
        f"""
        SELECT
            team,
            player_name,
            role,
            games,
            W,
            L,
            SV,
            HLD,
            ROUND(IP, 1) AS IP,
            OUTS,
            H,
            ER,
            BB,
            SO,
            ROUND(ERA, 3) AS ERA,
            ROUND(WHIP, 3) AS WHIP,
            ROUND(K9, 2) AS K9,
            ROUND(BB9, 2) AS BB9,
            ROUND(KBB, 2) AS KBB
        FROM pitcher_season_totals
        WHERE {where_sql}
        ORDER BY {order_clause}, OUTS DESC, SO DESC, player_name ASC
        LIMIT %s OFFSET %s
        """,
        tuple(params + [limit, offset]),
    )


def predictions_latest_date(season: int, mode: str = "prediction", model_version: str | None = None) -> str | None:
    """Return the most recent as_of_date for the given season, prediction mode, and model version.

    Args:
        season:        Season year.
        mode:          ``'prediction'`` (default) or ``'projection'``.
        model_version: If given, restrict to this model_version (e.g. ``'hitter_mvp_v2'``).
                       ``None`` (default) matches any version.
    """
    # Build optional model_version filter
    version_clause = " AND model_version = %s" if model_version else ""
    version_params = (model_version,) if model_version else ()
    try:
        rows = query_all(
            f"""
            SELECT MAX(as_of_date) AS latest_date
            FROM hitter_predictions
            WHERE season = %s AND prediction_mode = %s{version_clause}
            """,
            (season, mode) + version_params,
        )
    except Exception:  # noqa: BLE001  — column may not exist in older DBs
        rows = query_all(
            """
            SELECT MAX(as_of_date) AS latest_date
            FROM hitter_predictions
            WHERE season = %s
            """,
            (season,),
        )
    return rows[0]["latest_date"] if rows else None


def predictions_latest_rows(
    season: int,
    latest_date: str,
    limit: int = 100,
    mode: str = "prediction",
    model_version: str | None = None,
) -> list[dict[str, Any]]:
    """Fetch hitter prediction rows for the given season, as_of_date, mode, and model version.

    Falls back to unfiltered query if the prediction_mode/model_version column is missing.
    """
    version_clause = " AND model_version = %s" if model_version else ""
    version_params = (model_version,) if model_version else ()
    try:
        return query_all(
            f"""
            SELECT team, player_name, predicted_hr_final, predicted_ops_final, predicted_war_final,
                   confidence_level, confidence_score, pa_to_date, blend_weight, model_source,
                   prediction_mode, model_version, model_season
            FROM hitter_predictions
            WHERE season = %s AND as_of_date = %s AND prediction_mode = %s{version_clause}
            ORDER BY predicted_ops_final DESC
            LIMIT %s
            """,
            (season, latest_date, mode) + version_params + (limit,),
        )
    except Exception:  # noqa: BLE001
        return query_all(
            """
            SELECT team, player_name, predicted_hr_final, predicted_ops_final, predicted_war_final,
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
    where_sql = " AND ".join(where)

    # Hitter-only when pitcher table is absent
    if not table_exists("pitcher_season_totals"):
        return query_all(
            f"""
            SELECT
                'hitter' AS player_type,
                team, player_name, PA, AB, H, HR, OPS,
                CASE WHEN AB > 0 THEN ROUND(1.0 * H / AB, 3) ELSE 0 END AS AVG
            FROM hitter_season_totals
            WHERE {where_sql}
            ORDER BY
                CASE WHEN PA >= 100 THEN 1 ELSE 0 END DESC,
                CASE WHEN AB > 0 THEN 1.0 * H / AB ELSE 0 END DESC,
                COALESCE(OPS, 0) DESC, PA DESC
            LIMIT %s
            """,
            tuple(params + [limit]),
        )

    return query_all(
        f"""
        SELECT player_type, team, player_name, PA, AB, H, HR, OPS, AVG
        FROM (
            SELECT
                'hitter' AS player_type,
                team, player_name, PA, AB, H, HR, OPS,
                CASE WHEN AB > 0 THEN ROUND(1.0 * H / AB, 3) ELSE NULL END AS AVG
            FROM hitter_season_totals
            WHERE {where_sql}
            UNION ALL
            SELECT
                'pitcher' AS player_type,
                team, player_name,
                NULL AS PA, NULL AS AB, NULL AS H, NULL AS HR, NULL AS OPS, NULL AS AVG
            FROM pitcher_season_totals
            WHERE {where_sql}
        )
        ORDER BY
            CASE WHEN player_type = 'hitter' AND PA >= 100 THEN 2
                 WHEN player_type = 'hitter' THEN 1
                 ELSE 0 END DESC,
            CASE WHEN player_type = 'hitter' AND AB > 0 THEN 1.0 * H / AB ELSE 0.0 END DESC,
            COALESCE(OPS, 0.0) DESC,
            COALESCE(PA, 0) DESC
        LIMIT %s
        """,
        tuple(params + params + [limit]),
    )


def player_distinct_names(season: int | None = None) -> list[str]:
    if season is None:
        rows = query_all(
            """
            SELECT DISTINCT player_name
            FROM hitter_season_totals
            WHERE TRIM(player_name) <> ''
            ORDER BY player_name ASC
            """
        )
    else:
        rows = query_all(
            """
            SELECT DISTINCT player_name
            FROM hitter_season_totals
            WHERE season = %s AND TRIM(player_name) <> ''
            ORDER BY player_name ASC
            """,
            (season,),
    )
    return [str(r.get("player_name") or "").strip() for r in rows if str(r.get("player_name") or "").strip()]


def pitcher_distinct_names(season: int | None = None) -> list[str]:
    if season is None:
        rows = query_all(
            """
            SELECT DISTINCT player_name
            FROM pitcher_season_totals
            WHERE TRIM(player_name) <> ''
            ORDER BY player_name ASC
            """
        )
    else:
        rows = query_all(
            """
            SELECT DISTINCT player_name
            FROM pitcher_season_totals
            WHERE season = %s AND TRIM(player_name) <> ''
            ORDER BY player_name ASC
            """,
            (season,),
        )
    return [str(r.get("player_name") or "").strip() for r in rows if str(r.get("player_name") or "").strip()]


def statiz_player_name_by_id(player_id: str) -> str | None:
    if not table_exists("statiz_players"):
        return None
    row = query_one(
        """
        SELECT player_name
        FROM statiz_players
        WHERE player_id = %s
        LIMIT 1
        """,
        (player_id,),
    )
    name = str((row or {}).get("player_name") or "").strip()
    return name or None


def statiz_player_id_by_name(player_name: str) -> str | None:
    if not table_exists("statiz_players"):
        return None
    row = query_one(
        """
        SELECT MIN(player_id) AS player_id
        FROM statiz_players
        WHERE player_name = %s
          AND player_id NOT LIKE 'mock_%%'
        """,
        (player_name,),
    )
    pid = str((row or {}).get("player_id") or "").strip()
    return pid or None


def statiz_player_ids_by_names(names: list[str]) -> dict[str, str]:
    cleaned = [str(n).strip() for n in names if str(n).strip()]
    if not cleaned or not table_exists("statiz_players"):
        return {}

    placeholders = ", ".join(["%s"] * len(cleaned))
    rows = query_all(
        f"""
        SELECT player_name, MIN(player_id) AS player_id
        FROM statiz_players
        WHERE player_name IN ({placeholders})
          AND player_id NOT LIKE 'mock_%%'
        GROUP BY player_name
        """,
        tuple(cleaned),
    )
    return {
        str(r.get("player_name") or "").strip(): str(r.get("player_id") or "").strip()
        for r in rows
        if str(r.get("player_name") or "").strip() and str(r.get("player_id") or "").strip()
    }


def player_season_rows(player_name: str, team: str | None = None) -> list[dict[str, Any]]:
    # Prefer the richer wOBA-based batter_war when present.
    # Keep the historical OPS fallback only for older tables that do not
    # yet expose batter_war/WAR.
    if table_has_column("hitter_season_totals", "batter_war"):
        war_expr = "batter_war AS WAR"
    elif table_has_column("hitter_season_totals", "WAR"):
        war_expr = "WAR"
    else:
        war_expr = (
            f"ROUND(((OPS - {OPS_WAR_FALLBACK_BASELINE}) * PA) / "
            f"{OPS_WAR_FALLBACK_DIVISOR}, 2) AS WAR"
        )

    where = ["TRIM(player_name) = TRIM(%s)"]
    params: list[Any] = [player_name]
    if team:
        where.append("TRIM(team) = TRIM(%s)")
        params.append(team)

    return query_all(
        f"""
        SELECT season, team, games, PA, AB, H, "2B", "3B", HR, RBI, BB, SO, HBP, SH, SF, SB, CS, GDP, AVG, OBP, SLG, OPS,
               {war_expr}
        FROM hitter_season_totals
        WHERE {' AND '.join(where)}
        ORDER BY season DESC, team ASC
        """,
        tuple(params),
    )


def pitcher_player_season_rows(player_name: str, team: str | None = None) -> list[dict[str, Any]]:
    where = ["TRIM(player_name) = TRIM(%s)"]
    params: list[Any] = [player_name]
    if team:
        where.append("TRIM(team) = TRIM(%s)")
        params.append(team)

    return query_all(
        f"""
        SELECT
            season,
            team,
            player_name,
            role,
            games,
            W,
            L,
            SV,
            HLD,
            ROUND(IP, 1) AS IP,
            OUTS,
            H,
            ER,
            BB,
            SO,
            ROUND(ERA, 3) AS ERA,
            ROUND(WHIP, 3) AS WHIP,
            ROUND(K9, 2) AS K9,
            ROUND(BB9, 2) AS BB9,
            ROUND(KBB, 2) AS KBB,
            ROUND(FIP, 3) AS FIP,
            ROUND(COALESCE(pitcher_war, WAR), 2) AS WAR
        FROM pitcher_season_totals
        WHERE {' AND '.join(where)}
        ORDER BY season DESC, team ASC
        """,
        tuple(params),
    )


def pitcher_player_monthly_rows(player_name: str, season: int, team: str | None = None) -> list[dict[str, Any]]:
    where = ["player_name = %s", "substr(game_date, 1, 4) = %s"]
    params: list[Any] = [player_name, str(season)]
    if team:
        where.append("team = %s")
        params.append(team)

    return query_all(
        f"""
        WITH monthly_raw AS (
            SELECT
                substr(game_date, 1, 6) AS month,
                team,
                COUNT(DISTINCT game_id) AS games,
                SUM(W)   AS W,
                SUM(L)   AS L,
                SUM(SV)  AS SV,
                SUM(HLD) AS HLD,
                SUM(OUTS) AS OUTS,
                SUM(H)   AS H,
                SUM(ER)  AS ER,
                SUM(BB)  AS BB,
                SUM(SO)  AS SO
            FROM pitcher_game_logs
            WHERE {' AND '.join(where)}
            GROUP BY substr(game_date, 1, 6), team
        ),
        monthly_cum AS (
            SELECT
                month, team, games, W, L, SV, HLD, OUTS, H, ER, BB, SO,
                ROUND(1.0 * OUTS / 3.0, 1) AS IP,
                SUM(OUTS) OVER (ORDER BY month ROWS UNBOUNDED PRECEDING) AS cum_outs,
                SUM(ER)   OVER (ORDER BY month ROWS UNBOUNDED PRECEDING) AS cum_er,
                SUM(H)    OVER (ORDER BY month ROWS UNBOUNDED PRECEDING) AS cum_h,
                SUM(BB)   OVER (ORDER BY month ROWS UNBOUNDED PRECEDING) AS cum_bb,
                SUM(SO)   OVER (ORDER BY month ROWS UNBOUNDED PRECEDING) AS cum_so
            FROM monthly_raw
        )
        SELECT
            month, team, games,
            W, L, SV, HLD,
            OUTS, IP, H, ER, BB, SO,
            CASE WHEN cum_outs > 0 THEN ROUND(27.0 * cum_er  / cum_outs, 3) ELSE 0 END AS ERA,
            CASE WHEN cum_outs > 0 THEN ROUND(3.0  * (cum_bb + cum_h) / cum_outs, 3) ELSE 0 END AS WHIP,
            CASE WHEN cum_outs > 0 THEN ROUND(27.0 * cum_so  / cum_outs, 2) ELSE 0 END AS K9,
            CASE WHEN cum_outs > 0 THEN ROUND(27.0 * cum_bb  / cum_outs, 2) ELSE 0 END AS BB9
        FROM monthly_cum
        ORDER BY month ASC, team ASC
        """,
        tuple(params),
    )


def pitcher_player_current_aggregate(season: int, player_name: str, team: str | None = None) -> dict[str, Any] | None:
    where = ["TRIM(player_name) = TRIM(%s)", "substr(game_date, 1, 4) = %s"]
    params: list[Any] = [player_name, str(season)]
    if team:
        where.append("TRIM(team) = TRIM(%s)")
        params.append(team)

    return query_one(
        f"""
        SELECT
            MAX(game_date) AS latest_game_date,
            MIN(team) AS team,
            COUNT(DISTINCT game_id) AS games,
            SUM(W) AS W,
            SUM(L) AS L,
            SUM(SV) AS SV,
            SUM(HLD) AS HLD,
            SUM(OUTS) AS OUTS,
            ROUND(1.0 * SUM(OUTS) / 3.0, 1) AS IP,
            SUM(H) AS H,
            SUM(ER) AS ER,
            SUM(BB) AS BB,
            SUM(SO) AS SO,
            CASE WHEN SUM(OUTS) > 0 THEN ROUND(27.0 * SUM(ER) / SUM(OUTS), 3) ELSE 0 END AS ERA,
            CASE WHEN SUM(OUTS) > 0 THEN ROUND(3.0 * (SUM(BB) + SUM(H)) / SUM(OUTS), 3) ELSE 0 END AS WHIP,
            CASE WHEN SUM(OUTS) > 0 THEN ROUND(27.0 * SUM(SO) / SUM(OUTS), 2) ELSE 0 END AS K9,
            CASE WHEN SUM(OUTS) > 0 THEN ROUND(27.0 * SUM(BB) / SUM(OUTS), 2) ELSE 0 END AS BB9,
            CASE WHEN SUM(BB) > 0 THEN ROUND(1.0 * SUM(SO) / SUM(BB), 2) ELSE NULL END AS KBB
        FROM pitcher_game_logs
        WHERE {' AND '.join(where)}
        """,
        tuple(params),
    )


def player_latest_prediction(season: int, player_name: str, team: str | None = None) -> dict[str, Any] | None:
    where = ["season = %s", "TRIM(player_name) = TRIM(%s)"]
    params: list[Any] = [season, player_name]
    if team:
        where.append("TRIM(team) = TRIM(%s)")
        params.append(team)
    where_sql = " AND ".join(where)

    try:
        return query_one(
            f"""
            SELECT season, as_of_date, team, player_name, predicted_hr_final, predicted_ops_final, predicted_war_final,
                   confidence_level, confidence_score, model_source, pa_to_date, prediction_mode
            FROM hitter_predictions
            WHERE {where_sql}
            ORDER BY
                CASE WHEN prediction_mode = 'prediction' THEN 0 ELSE 1 END ASC,
                as_of_date DESC
            LIMIT 1
            """,
            tuple(params),
        )
    except Exception:  # noqa: BLE001
        return query_one(
            f"""
            SELECT season, as_of_date, team, predicted_hr_final, predicted_ops_final, predicted_war_final,
                   confidence_level, confidence_score, model_source
            FROM hitter_predictions
            WHERE {where_sql}
            ORDER BY as_of_date DESC
            LIMIT 1
            """,
            tuple(params),
        )


def pitcher_player_latest_prediction(season: int, player_name: str, team: str | None = None) -> dict[str, Any] | None:
    where = ["season = %s", "TRIM(player_name) = TRIM(%s)"]
    params: list[Any] = [season, player_name]
    if team:
        where.append("TRIM(team) = TRIM(%s)")
        params.append(team)
    return query_one(
        f"""
        SELECT
            season,
            as_of_date,
            team,
            player_name,
            role,
            predicted_era_final,
            predicted_whip_final,
            predicted_war_final,
            ip_to_date,
            so_to_date,
            confidence_score,
            confidence_level,
            model_source
        FROM pitcher_predictions
        WHERE {' AND '.join(where)}
        ORDER BY as_of_date DESC
        LIMIT 1
        """,
        tuple(params),
    )


def prediction_rows_for_as_of(season: int, as_of_date: str) -> list[dict[str, Any]]:
    try:
        return query_all(
            """
            SELECT team, player_name, predicted_ops_final, predicted_war_final, pa_to_date
            FROM hitter_predictions
            WHERE season = %s AND as_of_date = %s
            """,
            (season, as_of_date),
        )
    except Exception:  # noqa: BLE001
        return query_all(
            """
            SELECT team, player_name, predicted_ops_final, 0.0 AS predicted_war_final, 0.0 AS pa_to_date
            FROM hitter_predictions
            WHERE season = %s AND as_of_date = %s
            """,
            (season, as_of_date),
        )


def player_trend_rows(season: int, player_name: str, team: str | None = None) -> list[dict[str, Any]]:
    where = ["season = %s", "player_name = %s"]
    params: list[Any] = [season, player_name]
    if team:
        where.append("team = %s")
        params.append(team)

    return query_all(
        f"""
        SELECT as_of_date, team, PA, HR, OPS, OPS_7, OPS_14
        FROM hitter_daily_snapshots
        WHERE {' AND '.join(where)}
        ORDER BY as_of_date ASC, team ASC
        """,
        tuple(params),
    )


def player_monthly_rows(player_name: str, season: int, tb_expr: str, team: str | None = None) -> list[dict[str, Any]]:
    where = ["player_name = %s", "substr(game_date, 1, 4) = %s"]
    params: list[Any] = [player_name, str(season)]
    if team:
        where.append("team = %s")
        params.append(team)

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
        WHERE {' AND '.join(where)}
        GROUP BY substr(game_date, 1, 6), team
        ORDER BY month ASC, team ASC
        """,
        tuple(params),
    )


def player_vs_team_rows(player_name: str, season: int, tb_expr: str, team: str | None = None) -> list[dict[str, Any]]:
    where = ["g1.player_name = %s", "substr(g1.game_date, 1, 4) = %s"]
    params: list[Any] = [player_name, str(season)]
    if team:
        where.append("g1.team = %s")
        params.append(team)

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
            WHERE {' AND '.join(where)}
        ) x
        WHERE opp_team IS NOT NULL
        GROUP BY team, opp_team
        ORDER BY PA DESC, opp_team ASC
        """,
        tuple(params),
    )


def player_recent_games_rows(player_name: str, season: int, recent_n: int, team: str | None = None) -> list[dict[str, Any]]:
    where_p = ["player_name = %s", "substr(game_date, 1, 4) = %s"]
    params_p: list[Any] = [player_name, str(season)]
    where_g = ["g.player_name = %s"]
    params_g: list[Any] = [player_name]
    
    if team:
        where_p.append("team = %s")
        params_p.append(team)
        where_g.append("g.team = %s")
        params_g.append(team)

    return query_all(
        f"""
        WITH player_games AS (
            SELECT DISTINCT game_id, game_date
            FROM hitter_game_logs
            WHERE {' AND '.join(where_p)}
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
        WHERE {' AND '.join(where_g)}
        GROUP BY g.game_date, g.game_id, g.team
        ORDER BY g.game_date DESC, g.game_id DESC
        """,
        tuple(params_p + [recent_n] + params_g),
    )


def player_current_aggregate(season: int, player_name: str, ops_expr: str, team: str | None = None) -> dict[str, Any] | None:
    where = ["season = %s", "TRIM(player_name) = TRIM(%s)"]
    params: list[Any] = [season, player_name]
    if team:
        where.append("TRIM(team) = TRIM(%s)")
        params.append(team)

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
        WHERE {' AND '.join(where)}
        """,
        tuple(params),
    )


def player_kbreport_split_rows(season: int, player_name: str, team: str | None = None) -> list[dict[str, Any]]:
    # kbreport splits don't always have team filtering easily available in the same table,
    # but we can try if the column exists in that particular table.
    # Currently assuming it might not have team or we can optionally query it.
    where = ["season = %s", "player_name = %s"]
    params: list[Any] = [season, player_name]
    return query_all(
        f"""
        SELECT split_group, split_key, split_label, PA, AB, H, HR, BB, SO, AVG, OBP, SLG, OPS
        FROM kbreport_hitter_splits
        WHERE {' AND '.join(where)}
        ORDER BY split_group ASC, split_key ASC
        """,
        tuple(params),
        )


def pitcher_prediction_rows_for_as_of(season: int, as_of_date: str) -> list[dict[str, Any]]:
    return query_all(
        """
        SELECT
            team,
            player_name,
            COALESCE(role, '') AS role,
            COALESCE(predicted_era_final, 0) AS predicted_era_final,
            COALESCE(predicted_whip_final, 0) AS predicted_whip_final,
            COALESCE(predicted_war_final, 0) AS predicted_war_final,
            COALESCE(ip_to_date, 0) AS ip_to_date,
            COALESCE(so_to_date, 0) AS so_to_date
        FROM pitcher_predictions
        WHERE season = %s AND as_of_date = %s
        """,
        (season, as_of_date),
    )


def pitcher_latest_prediction_date(season: int) -> str | None:
    row = query_one(
        "SELECT MAX(as_of_date) AS latest_prediction_date FROM pitcher_predictions WHERE season = %s",
        (season,),
    )
    return (row or {}).get("latest_prediction_date")


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
        SELECT 
            h.player_name, 
            MAX(sp.birth_date) AS birth_date,
            SUM(h.PA) AS PA, 
            SUM(h.H) AS H, 
            SUM(h.HR) AS HR, 
            SUM(h.RBI) AS RBI, 
            MAX(h.OPS) AS OPS
        FROM hitter_season_totals h
        LEFT JOIN statiz_players sp ON 
            (sp.player_name = h.player_name OR sp.player_name LIKE '%% ' || h.player_name)
        WHERE h.season = %s AND h.team = %s AND h.PA >= %s
        GROUP BY h.player_name
        ORDER BY MAX(h.OPS) DESC, SUM(h.PA) DESC
        LIMIT %s
        """,
        (season, team, min_pa, limit),
    )


def team_leaders_hr(season: int, team: str, min_pa: int, limit: int = 10) -> list[dict[str, Any]]:
    return query_all(
        """
        SELECT 
            h.player_name, 
            MAX(sp.birth_date) AS birth_date,
            SUM(h.PA) AS PA, 
            SUM(h.H) AS H, 
            SUM(h.HR) AS HR, 
            SUM(h.RBI) AS RBI, 
            MAX(h.OPS) AS OPS
        FROM hitter_season_totals h
        LEFT JOIN statiz_players sp ON 
            (sp.player_name = h.player_name OR sp.player_name LIKE '%% ' || h.player_name)
        WHERE h.season = %s AND h.team = %s AND h.PA >= %s
        GROUP BY h.player_name
        ORDER BY SUM(h.HR) DESC, SUM(h.PA) DESC
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
        WITH team_scores AS (
            SELECT
                game_date,
                game_id,
                team,
                COALESCE(SUM(R), 0) AS runs
            FROM hitter_game_logs
            WHERE substr(game_date, 1, 4) = %s
            GROUP BY game_date, game_id, team
        )
        SELECT
            a.game_date,
            a.game_id,
            b.team AS opp_team,
            a.runs AS team_score,
            b.runs AS opp_score,
            CASE
                WHEN a.runs > b.runs THEN 'W'
                WHEN a.runs < b.runs THEN 'L'
                ELSE 'D'
            END AS result
        FROM team_scores a
        JOIN team_scores b ON a.game_id = b.game_id AND a.team <> b.team
        WHERE a.team = %s
        ORDER BY a.game_date DESC, a.game_id DESC
        LIMIT %s
        """,
        (str(season), team, limit),
    )


def team_h2h_rows(team: str, season: int) -> list[dict[str, Any]]:
    return query_all(
        """
        WITH team_scores AS (
            SELECT
                game_date,
                game_id,
                team,
                COALESCE(SUM(R), 0) AS runs
            FROM hitter_game_logs
            WHERE substr(game_date, 1, 4) = %s
            GROUP BY game_date, game_id, team
        )
        SELECT
            b.team AS opp_team,
            SUM(CASE WHEN a.runs > b.runs THEN 1 ELSE 0 END) AS wins,
            SUM(CASE WHEN a.runs < b.runs THEN 1 ELSE 0 END) AS losses,
            SUM(CASE WHEN a.runs = b.runs THEN 1 ELSE 0 END) AS draws,
            SUM(a.runs) AS runs_for,
            SUM(b.runs) AS runs_against
        FROM team_scores a
        JOIN team_scores b ON a.game_id = b.game_id AND a.team <> b.team
        WHERE a.team = %s
        GROUP BY b.team
        ORDER BY b.team ASC
        """,
        (str(season), team),
    )


def team_schedule_rows(team: str, season: int, limit: int = 60) -> list[dict[str, Any]]:
    if not table_exists("team_schedule"):
        return []
    return query_all(
        """
        SELECT
            game_date,
            game_id,
            away_team,
            home_team,
            game_time,
            stadium,
            status
        FROM team_schedule
        WHERE season = %s
          AND (away_team = %s OR home_team = %s)
        ORDER BY game_date DESC, game_time DESC, game_id DESC
        LIMIT %s
        """,
        (season, team, team, limit),
    )


def team_result_by_game(team: str, season: int) -> dict[str, dict[str, Any]]:
    if not table_exists("hitter_game_logs"):
        return {}
    rows = query_all(
        """
        WITH team_scores AS (
            SELECT
                game_date,
                game_id,
                team,
                COALESCE(SUM(R), 0) AS runs
            FROM hitter_game_logs
            WHERE substr(game_date, 1, 4) = %s
            GROUP BY game_date, game_id, team
        )
        SELECT
            a.game_date,
            a.game_id,
            b.team AS opp_team,
            a.runs AS team_score,
            b.runs AS opp_score,
            CASE
                WHEN a.runs > b.runs THEN 'W'
                WHEN a.runs < b.runs THEN 'L'
                ELSE 'D'
            END AS result
        FROM team_scores a
        JOIN team_scores b ON a.game_id = b.game_id AND a.team <> b.team
        WHERE a.team = %s
        """,
        (str(season), team),
    )
    by_game: dict[str, dict[str, Any]] = {}
    for row in rows:
        game_id = str(row.get("game_id") or "").strip()
        if not game_id:
            continue
        by_game[game_id] = row
    return by_game


def team_latest_prediction_date(season: int, team: str) -> str | None:
    row = query_one(
        "SELECT MAX(as_of_date) AS latest_date FROM hitter_predictions WHERE season = %s AND team = %s",
        (season, team),
    )
    return (row or {}).get("latest_date")


def team_latest_predictions(season: int, team: str, latest_date: str, limit: int = 10) -> list[dict[str, Any]]:
    return query_all(
        """
        SELECT player_name, predicted_hr_final, predicted_ops_final, predicted_war_final,
               confidence_level, blend_weight, model_source
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


def player_profile_info(player_name: str) -> dict[str, Any] | None:
    return query_one(
        """
        SELECT birth_date, position, bats_throws, debut_year
        FROM statiz_players
        WHERE player_name = %s
           OR player_name LIKE %s
           OR player_id LIKE %s
        ORDER BY collected_at DESC
        LIMIT 1
        """,
        (player_name, f"% {player_name}", f"{player_name}|%"),
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
