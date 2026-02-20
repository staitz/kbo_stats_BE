from datetime import datetime
from typing import Any

from django.db import connection
from django.http import JsonResponse
from django.views.decorators.http import require_GET


def _query_all(sql: str, params: tuple[Any, ...] = ()) -> list[dict]:
    with connection.cursor() as cursor:
        cursor.execute(sql, params)
        cols = [c[0] for c in cursor.description]
        rows = cursor.fetchall()
    return [dict(zip(cols, row)) for row in rows]


def _query_one(sql: str, params: tuple[Any, ...] = ()) -> dict | None:
    rows = _query_all(sql, params)
    return rows[0] if rows else None


def _table_exists(table_name: str) -> bool:
    return table_name in connection.introspection.table_names()


def _parse_int(value: Any, default: int, min_value: int | None = None, max_value: int | None = None) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = default
    if min_value is not None:
        parsed = max(min_value, parsed)
    if max_value is not None:
        parsed = min(max_value, parsed)
    return parsed


def _default_season() -> int:
    row = _query_one("SELECT MAX(season) AS season FROM hitter_season_totals")
    if row and row.get("season"):
        return int(row["season"])
    return datetime.now().year


def _parse_yyyymmdd(value: str) -> datetime | None:
    if not value or len(value) != 8 or not value.isdigit():
        return None
    try:
        return datetime.strptime(value, "%Y%m%d")
    except ValueError:
        return None


def _season_progress_min_pa(season: int) -> int:
    window = _query_one(
        """
        SELECT MIN(game_date) AS min_date, MAX(game_date) AS max_date
        FROM hitter_game_logs
        WHERE substr(game_date, 1, 4) = %s
        """,
        (str(season),),
    )
    min_date = _parse_yyyymmdd((window or {}).get("min_date") or "")
    max_date = _parse_yyyymmdd((window or {}).get("max_date") or "")
    if not min_date or not max_date:
        return 30
    days = max(0, (max_date - min_date).days)
    if days < 14:
        return 30
    if days < 35:
        return 50
    if days < 49:
        return 70
    return 100


def _pick_effective_min_pa_for_leaderboard(
    season: int,
    team: str,
    base_min_pa: int,
    auto_relax: bool,
    min_count: int = 20,
) -> int:
    if not auto_relax:
        return base_min_pa

    candidates = [base_min_pa, 70, 50, 30]
    dedup: list[int] = []
    for c in candidates:
        if c not in dedup:
            dedup.append(c)

    for c in dedup:
        where = ["season = %s", "PA >= %s"]
        params: list[Any] = [season, c]
        if team:
            where.append("team = %s")
            params.append(team)
        total_row = _query_one(
            f"SELECT COUNT(*) AS total FROM hitter_season_totals WHERE {' AND '.join(where)}",
            tuple(params),
        )
        if int((total_row or {}).get("total") or 0) >= min_count:
            return c

    return dedup[-1]


def _safe_tb_expr(tb_col: str = "TB_adj") -> str:
    return """
    (CASE
        WHEN COALESCE(%(tb_col)s, 0) > 0 THEN COALESCE(%(tb_col)s, 0)
        ELSE
            (CASE WHEN (COALESCE(H, 0) - COALESCE("2B", 0) - COALESCE("3B", 0) - COALESCE(HR, 0)) > 0
                  THEN (COALESCE(H, 0) - COALESCE("2B", 0) - COALESCE("3B", 0) - COALESCE(HR, 0))
                  ELSE 0 END)
            + 2 * COALESCE("2B", 0)
            + 3 * COALESCE("3B", 0)
            + 4 * COALESCE(HR, 0)
    END)
    """ % {"tb_col": tb_col}


def _safe_ops_expr(sum_ab: str, sum_h: str, sum_bb: str, sum_hbp: str, sum_sf: str, sum_tb: str) -> str:
    return f"""
    (
      (CASE WHEN (({sum_ab}) + ({sum_bb}) + ({sum_hbp}) + ({sum_sf})) > 0
            THEN 1.0 * (({sum_h}) + ({sum_bb}) + ({sum_hbp}))
                 / (({sum_ab}) + ({sum_bb}) + ({sum_hbp}) + ({sum_sf}))
            ELSE 0 END)
      +
      (CASE WHEN ({sum_ab}) > 0 THEN 1.0 * ({sum_tb}) / ({sum_ab}) ELSE 0 END)
    )
    """


@require_GET
def health(_request):
    return JsonResponse({"status": "ok"})


@require_GET
def standings(request):
    requested_season = _parse_int(request.GET.get("season"), _default_season(), min_value=1982, max_value=2100)
    season = requested_season
    mode = "SEASON_MATCH"

    if not _table_exists("team_standings"):
        return JsonResponse(
            {
                "requested_season": requested_season,
                "effective_season": None,
                "as_of_date": None,
                "mode": "NO_DATA",
                "rows": [],
            }
        )

    latest_for_requested = _query_one(
        "SELECT MAX(as_of_date) AS as_of_date FROM team_standings WHERE season = %s",
        (requested_season,),
    )
    as_of_date = (latest_for_requested or {}).get("as_of_date")

    if not as_of_date:
        fallback = _query_one(
            "SELECT MAX(season) AS season FROM team_standings WHERE season < %s",
            (requested_season,),
        )
        fallback_season = (fallback or {}).get("season")
        if fallback_season:
            season = int(fallback_season)
            mode = "PRESEASON_FALLBACK"
            latest_for_fallback = _query_one(
                "SELECT MAX(as_of_date) AS as_of_date FROM team_standings WHERE season = %s",
                (season,),
            )
            as_of_date = (latest_for_fallback or {}).get("as_of_date")

    if not as_of_date:
        return JsonResponse(
            {
                "requested_season": requested_season,
                "effective_season": None,
                "as_of_date": None,
                "mode": "NO_DATA",
                "rows": [],
            }
        )

    rows = _query_all(
        """
        SELECT rank, team, games, wins, losses, draws, win_pct, gb, recent_10, streak, home_record, away_record
        FROM team_standings
        WHERE season = %s AND as_of_date = %s
        ORDER BY rank ASC, team ASC
        """,
        (season, as_of_date),
    )
    return JsonResponse(
        {
            "requested_season": requested_season,
            "effective_season": season,
            "as_of_date": as_of_date,
            "mode": mode,
            "rows": rows,
        }
    )


@require_GET
def home_summary(request):
    season = _parse_int(request.GET.get("season"), _default_season(), min_value=1982, max_value=2100)
    min_pa_raw = request.GET.get("min_pa")
    if min_pa_raw is None or str(min_pa_raw).strip() == "":
        min_pa = _season_progress_min_pa(season)
        min_pa_policy = "AUTO_BY_SEASON_PROGRESS"
    else:
        min_pa = _parse_int(min_pa_raw, 100, min_value=0, max_value=700)
        min_pa_policy = "MANUAL"

    base = _query_one(
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

    latest_game = _query_one(
        "SELECT MAX(game_date) AS latest_game_date FROM hitter_game_logs WHERE substr(game_date, 1, 4) = %s",
        (str(season),),
    )
    latest_pred = _query_one(
        "SELECT MAX(as_of_date) AS latest_prediction_date FROM hitter_predictions WHERE season = %s",
        (season,),
    )

    top_ops = _query_all(
        """
        SELECT team, player_name, PA, HR, OPS
        FROM hitter_season_totals
        WHERE season = %s AND PA >= %s
        ORDER BY OPS DESC, PA DESC
        LIMIT 5
        """,
        (season, min_pa),
    )
    top_hr = _query_all(
        """
        SELECT team, player_name, PA, HR, OPS
        FROM hitter_season_totals
        WHERE season = %s AND PA >= %s
        ORDER BY HR DESC, PA DESC
        LIMIT 5
        """,
        (season, min_pa),
    )
    standings_preview = []
    standings_as_of = None
    if _table_exists("team_standings"):
        standings_meta = _query_one(
            "SELECT MAX(as_of_date) AS as_of_date FROM team_standings WHERE season = %s",
            (season,),
        )
        standings_as_of = (standings_meta or {}).get("as_of_date")
        if standings_as_of:
            standings_preview = _query_all(
                """
            SELECT rank, team, wins, losses, draws, win_pct, gb
            FROM team_standings
            WHERE season = %s AND as_of_date = %s
            ORDER BY rank ASC
            LIMIT 10
            """,
            (season, standings_as_of),
        )

    return JsonResponse(
        {
            "season": season,
            "latest_game_date": (latest_game or {}).get("latest_game_date"),
            "latest_prediction_date": (latest_pred or {}).get("latest_prediction_date"),
            "totals": base,
            "min_pa": min_pa,
            "min_pa_policy": min_pa_policy,
            "leaderboards": {"ops_top5": top_ops, "hr_top5": top_hr},
            "standings_preview": {
                "as_of_date": standings_as_of,
                "rows": standings_preview,
            },
            "notes": [
                "pitcher and team standings endpoints are not included in hitter-only MVP",
            ],
        }
    )


@require_GET
def leaderboard(request):
    season = _parse_int(request.GET.get("season"), _default_season(), min_value=1982, max_value=2100)
    metric = str(request.GET.get("metric", "OPS")).upper().strip()
    min_pa_raw = request.GET.get("min_pa")
    team = str(request.GET.get("team", "")).strip()
    if min_pa_raw is None or str(min_pa_raw).strip() == "":
        requested_min_pa = _season_progress_min_pa(season)
        min_pa_policy = "AUTO_BY_SEASON_PROGRESS_WITH_RELAX"
        min_pa = _pick_effective_min_pa_for_leaderboard(
            season=season,
            team=team,
            base_min_pa=requested_min_pa,
            auto_relax=True,
        )
    else:
        requested_min_pa = _parse_int(min_pa_raw, 100, min_value=0, max_value=700)
        min_pa = requested_min_pa
        min_pa_policy = "MANUAL"
    limit = _parse_int(request.GET.get("limit"), 20, min_value=1, max_value=200)
    offset = _parse_int(request.GET.get("offset"), 0, min_value=0, max_value=100000)

    allowed_metrics = {
        "OPS": "OPS",
        "HR": "HR",
        "AVG": "AVG",
        "OBP": "OBP",
        "SLG": "SLG",
        "RBI": "RBI",
        "H": "H",
    }
    order_metric = allowed_metrics.get(metric, "OPS")

    where = ["season = %s", "PA >= %s"]
    params: list[Any] = [season, min_pa]
    if team:
        where.append("team = %s")
        params.append(team)
    where_sql = " AND ".join(where)

    total_row = _query_one(f"SELECT COUNT(*) AS total FROM hitter_season_totals WHERE {where_sql}", tuple(params))
    total = int((total_row or {}).get("total") or 0)

    rows = _query_all(
        f"""
        SELECT team, player_name, games, PA, AB, H, HR, RBI, AVG, OBP, SLG, OPS
        FROM hitter_season_totals
        WHERE {where_sql}
        ORDER BY {order_metric} DESC, PA DESC, player_name ASC
        LIMIT %s OFFSET %s
        """,
        tuple(params + [limit, offset]),
    )

    return JsonResponse(
        {
            "season": season,
            "metric": order_metric,
            "requested_min_pa": requested_min_pa,
            "effective_min_pa": min_pa,
            "min_pa_policy": min_pa_policy,
            "team": team or None,
            "total": total,
            "limit": limit,
            "offset": offset,
            "rows": rows,
        }
    )


@require_GET
def predictions_latest(request):
    season = _parse_int(request.GET.get("season"), _default_season(), min_value=1982, max_value=2100)
    latest_rows = _query_all(
        """
        SELECT MAX(as_of_date) AS latest_date
        FROM hitter_predictions
        WHERE season = %s
        """,
        (season,),
    )
    latest_date = latest_rows[0]["latest_date"] if latest_rows else None
    if not latest_date:
        return JsonResponse({"season": season, "latest_date": None, "rows": []})
    rows = _query_all(
        """
        SELECT team, player_name, predicted_hr_final, predicted_ops_final,
               confidence_level, confidence_score, pa_to_date, blend_weight, model_source
        FROM hitter_predictions
        WHERE season = %s AND as_of_date = %s
        ORDER BY predicted_ops_final DESC
        LIMIT 100
        """,
        (season, latest_date),
    )
    return JsonResponse({"season": season, "latest_date": latest_date, "rows": rows})


@require_GET
def player_search(request):
    q = str(request.GET.get("q", "")).strip()
    season = _parse_int(request.GET.get("season"), _default_season(), min_value=1982, max_value=2100)
    limit = _parse_int(request.GET.get("limit"), 30, min_value=1, max_value=100)
    team = str(request.GET.get("team", "")).strip()
    if not q:
        return JsonResponse({"season": season, "q": q, "rows": []})

    where = ["season = %s", "player_name LIKE %s"]
    params: list[Any] = [season, f"%{q}%"]
    if team:
        where.append("team = %s")
        params.append(team)

    rows = _query_all(
        f"""
        SELECT team, player_name, PA, AB, H, HR, OPS
        FROM hitter_season_totals
        WHERE {' AND '.join(where)}
        ORDER BY OPS DESC, PA DESC
        LIMIT %s
        """,
        tuple(params + [limit]),
    )
    return JsonResponse({"season": season, "q": q, "team": team or None, "rows": rows})


@require_GET
def player_detail(request, player_name: str):
    season = _parse_int(request.GET.get("season"), _default_season(), min_value=1982, max_value=2100)
    name = player_name.strip()
    recent_n = _parse_int(request.GET.get("recent_n"), 10, min_value=1, max_value=60)

    season_rows = _query_all(
        """
        SELECT season, team, games, PA, AB, H, "2B", "3B", HR, RBI, BB, SO, HBP, SH, SF, SB, CS, GDP, AVG, OBP, SLG, OPS
        FROM hitter_season_totals
        WHERE player_name = %s
        ORDER BY season DESC, team ASC
        """,
        (name,),
    )
    if not season_rows:
        return JsonResponse({"error": "player_not_found", "player_name": name}, status=404)

    current_rows = [r for r in season_rows if int(r.get("season") or 0) == season]
    latest_prediction = _query_one(
        """
        SELECT season, as_of_date, team, predicted_hr_final, predicted_ops_final,
               confidence_level, confidence_score, model_source
        FROM hitter_predictions
        WHERE season = %s AND player_name = %s
        ORDER BY as_of_date DESC
        LIMIT 1
        """,
        (season, name),
    )

    trend_rows = _query_all(
        """
        SELECT as_of_date, team, PA, HR, OPS, OPS_7, OPS_14
        FROM hitter_daily_snapshots
        WHERE season = %s AND player_name = %s
        ORDER BY as_of_date ASC, team ASC
        """,
        (season, name),
    )

    tb_expr = _safe_tb_expr("TB")
    monthly = _query_all(
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
        (name, str(season)),
    )

    for row in monthly:
        ab = int(row.get("AB") or 0)
        h = int(row.get("H") or 0)
        bb = int(row.get("BB") or 0)
        hbp = int(row.get("HBP") or 0)
        sf = int(row.get("SF") or 0)
        tb = int(row.get("TB_adj") or 0)
        obp_den = ab + bb + hbp + sf
        row["AVG"] = round(h / ab, 4) if ab > 0 else 0.0
        row["OBP"] = round((h + bb + hbp) / obp_den, 4) if obp_den > 0 else 0.0
        row["SLG"] = round(tb / ab, 4) if ab > 0 else 0.0
        row["OPS"] = round(row["OBP"] + row["SLG"], 4)

    # KBO-only split: vs_team
    vs_team = _query_all(
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
            COALESCE(SUM({_safe_tb_expr("TB")}), 0) AS TB_adj
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
        (name, str(season)),
    )
    for row in vs_team:
        ab = int(row.get("AB") or 0)
        h = int(row.get("H") or 0)
        bb = int(row.get("BB") or 0)
        tb = int(row.get("TB_adj") or 0)
        row["AVG"] = round(h / ab, 4) if ab > 0 else 0.0
        row["OBP"] = round((h + bb) / (ab + bb), 4) if (ab + bb) > 0 else 0.0
        row["SLG"] = round(tb / ab, 4) if ab > 0 else 0.0
        row["OPS"] = round(row["OBP"] + row["SLG"], 4)

    # KBO-only split: recent N game logs (latest)
    recent_games = _query_all(
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
        (name, str(season), recent_n, name),
    )
    for row in recent_games:
        ab = int(row.get("AB") or 0)
        h = int(row.get("H") or 0)
        bb = int(row.get("BB") or 0)
        tb = int(row.get("TB") or 0)
        row["AVG"] = round(h / ab, 4) if ab > 0 else 0.0
        row["OBP"] = round((h + bb) / (ab + bb), 4) if (ab + bb) > 0 else 0.0
        row["SLG"] = round(tb / ab, 4) if ab > 0 else 0.0
        row["OPS"] = round(row["OBP"] + row["SLG"], 4)

    current_agg = _query_one(
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
            { _safe_ops_expr('COALESCE(SUM(AB),0)','COALESCE(SUM(H),0)','COALESCE(SUM(BB),0)','COALESCE(SUM(HBP),0)','COALESCE(SUM(SF),0)','COALESCE(SUM(TB_adj),0)') } AS OPS
        FROM hitter_season_totals
        WHERE season = %s AND player_name = %s
        """,
        (season, name),
    )

    kbreport_splits: dict[str, list[dict[str, Any]]] = {
        "homeaway": [],
        "pitchside": [],
        "opposite": [],
        "month": [],
    }
    if _table_exists("kbreport_hitter_splits"):
        ext_rows = _query_all(
            """
            SELECT split_group, split_key, split_label, PA, AB, H, HR, BB, SO, AVG, OBP, SLG, OPS
            FROM kbreport_hitter_splits
            WHERE season = %s AND player_name = %s
            ORDER BY split_group ASC, split_key ASC
            """,
            (season, name),
        )
        for row in ext_rows:
            group = str(row.get("split_group") or "")
            if group in kbreport_splits:
                kbreport_splits[group].append(row)

    return JsonResponse(
        {
            "season": season,
            "recent_n": recent_n,
            "player_name": name,
            "profile": {
                "player_name": name,
                "teams_in_season": sorted({r["team"] for r in current_rows}) if current_rows else [],
            },
            "season_aggregate": current_agg,
            "season_rows": current_rows,
            "season_by_year": season_rows,
            "monthly_splits": monthly,
            "vs_team_splits": vs_team,
            "recent_game_logs": recent_games,
            "trend": trend_rows,
            "kbreport_splits": kbreport_splits,
            "latest_prediction": latest_prediction,
            "notes": [
                "handedness/home-away/opponent splits require additional source fields and are not in current DB",
            ],
        }
    )


@require_GET
def team_detail(request, team: str):
    season = _parse_int(request.GET.get("season"), _default_season(), min_value=1982, max_value=2100)
    name = team.strip()
    min_pa_raw = request.GET.get("min_pa")
    if min_pa_raw is None or str(min_pa_raw).strip() == "":
        requested_min_pa = _season_progress_min_pa(season)
        effective_min_pa = _pick_effective_min_pa_for_leaderboard(
            season=season,
            team=name,
            base_min_pa=requested_min_pa,
            auto_relax=True,
            min_count=5,
        )
        min_pa_policy = "AUTO_BY_SEASON_PROGRESS_WITH_RELAX"
    else:
        requested_min_pa = _parse_int(min_pa_raw, 100, min_value=0, max_value=700)
        effective_min_pa = requested_min_pa
        min_pa_policy = "MANUAL"

    team_summary = _query_one(
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
        (season, name),
    )
    if not team_summary or int(team_summary.get("players") or 0) == 0:
        return JsonResponse({"error": "team_not_found", "team": name, "season": season}, status=404)

    leaders_ops = _query_all(
        """
        SELECT player_name, PA, H, HR, RBI, OPS
        FROM hitter_season_totals
        WHERE season = %s AND team = %s AND PA >= %s
        ORDER BY OPS DESC, PA DESC
        LIMIT 10
        """,
        (season, name, effective_min_pa),
    )
    leaders_hr = _query_all(
        """
        SELECT player_name, PA, H, HR, RBI, OPS
        FROM hitter_season_totals
        WHERE season = %s AND team = %s AND PA >= %s
        ORDER BY HR DESC, PA DESC
        LIMIT 10
        """,
        (season, name, effective_min_pa),
    )

    monthly = _query_all(
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
            COALESCE(SUM({_safe_tb_expr("TB")}), 0) AS TB_adj
        FROM hitter_game_logs
        WHERE team = %s
          AND substr(game_date, 1, 4) = %s
        GROUP BY substr(game_date, 1, 6)
        ORDER BY month ASC
        """,
        (name, str(season)),
    )
    for row in monthly:
        ab = int(row.get("AB") or 0)
        h = int(row.get("H") or 0)
        bb = int(row.get("BB") or 0)
        so = int(row.get("SO") or 0)
        tb = int(row.get("TB_adj") or 0)
        row["AVG"] = round(h / ab, 4) if ab > 0 else 0.0
        row["SLG"] = round(tb / ab, 4) if ab > 0 else 0.0
        row["BB_K"] = round(bb / so, 4) if so > 0 else None

    recent_games = _query_all(
        """
        SELECT game_date, game_id, COUNT(*) AS batter_rows
        FROM hitter_game_logs
        WHERE team = %s
          AND substr(game_date, 1, 4) = %s
        GROUP BY game_date, game_id
        ORDER BY game_date DESC, game_id DESC
        LIMIT 20
        """,
        (name, str(season)),
    )

    pred_date = _query_one(
        "SELECT MAX(as_of_date) AS latest_date FROM hitter_predictions WHERE season = %s AND team = %s",
        (season, name),
    )
    latest_predictions: list[dict] = []
    latest_date = (pred_date or {}).get("latest_date")
    if latest_date:
        latest_predictions = _query_all(
            """
            SELECT player_name, predicted_hr_final, predicted_ops_final, confidence_level, blend_weight, model_source
            FROM hitter_predictions
            WHERE season = %s AND team = %s AND as_of_date = %s
            ORDER BY predicted_ops_final DESC
            LIMIT 10
            """,
            (season, name, latest_date),
        )

    return JsonResponse(
        {
            "season": season,
            "team": name,
            "requested_min_pa": requested_min_pa,
            "effective_min_pa": effective_min_pa,
            "min_pa_policy": min_pa_policy,
            "summary": team_summary,
            "leaders": {"ops_top10": leaders_ops, "hr_top10": leaders_hr},
            "monthly_trend": monthly,
            "recent_games": recent_games,
            "latest_prediction_date": latest_date,
            "latest_predictions": latest_predictions,
            "notes": [
                "team standings and win/loss require game result table (not present in current hitter-only DB)",
            ],
        }
    )


@require_GET
def player_compare(request):
    season = _parse_int(request.GET.get("season"), _default_season(), min_value=1982, max_value=2100)
    names_raw = str(request.GET.get("names", "")).strip()
    names = [n.strip() for n in names_raw.split(",") if n.strip()]
    if len(names) < 2:
        return JsonResponse({"error": "at_least_two_names_required", "season": season}, status=400)

    placeholders = ", ".join(["%s"] * len(names))
    rows = _query_all(
        f"""
        SELECT season, team, player_name, games, PA, AB, H, HR, RBI, BB, SO, AVG, OBP, SLG, OPS
        FROM hitter_season_totals
        WHERE season = %s
          AND player_name IN ({placeholders})
        ORDER BY player_name ASC, OPS DESC, PA DESC
        """,
        tuple([season] + names),
    )
    return JsonResponse({"season": season, "names": names, "rows": rows})


@require_GET
def games_by_date(request):
    game_date = str(request.GET.get("date", "")).strip()
    season = _parse_int(request.GET.get("season"), _default_season(), min_value=1982, max_value=2100)
    limit = _parse_int(request.GET.get("limit"), 20, min_value=1, max_value=200)

    params: list[Any] = []
    where = []
    if game_date:
        where.append("game_date = %s")
        params.append(game_date)
    else:
        where.append("substr(game_date, 1, 4) = %s")
        params.append(str(season))
    where_sql = " AND ".join(where)

    # hitter_game_logs only has hitter stats, so score is derived by SUM(R).
    rows = _query_all(
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
    return JsonResponse({"season": season, "date": game_date or None, "rows": rows})


@require_GET
def game_boxscore(request, game_id: str):
    rows = _query_all(
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
    if not rows:
        return JsonResponse({"error": "game_not_found", "game_id": game_id}, status=404)

    game_date = rows[0]["game_date"]
    team_summaries = _query_all(
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

    return JsonResponse(
        {
            "game_id": game_id,
            "game_date": game_date,
            "teams": team_summaries,
            "hitter_rows": rows,
            "notes": [
                "inning-by-inning scoreboard is unavailable in current KBO hitter-only table",
            ],
        }
    )
