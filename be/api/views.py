from datetime import datetime
from typing import Any

from django.db import DatabaseError
from django.http import JsonResponse
from django.views.decorators.http import require_GET

from . import repository as repo


def _error_json(error: str, detail: str, status: int, extra: dict[str, Any] | None = None) -> JsonResponse:
    payload: dict[str, Any] = {"error": error, "detail": detail}
    if extra:
        payload.update(extra)
    return JsonResponse(payload, status=status)


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
    season = repo.default_season()
    if season:
        return season
    return datetime.now().year


def _parse_yyyymmdd(value: str) -> datetime | None:
    if not value or len(value) != 8 or not value.isdigit():
        return None
    try:
        return datetime.strptime(value, "%Y%m%d")
    except ValueError:
        return None


def _season_progress_min_pa(season: int) -> int:
    if not repo.table_exists("hitter_game_logs"):
        return 30
    window = repo.season_game_window(season)
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
    if not repo.table_exists("hitter_season_totals"):
        return base_min_pa

    candidates = [base_min_pa, 70, 50, 30]
    dedup: list[int] = []
    for candidate in candidates:
        if candidate not in dedup:
            dedup.append(candidate)

    for candidate in dedup:
        if repo.leaderboard_candidate_count(season=season, min_pa=candidate, team=team) >= min_count:
            return candidate

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


def _missing_required_tables(tables: list[str]) -> list[str]:
    return [table for table in tables if not repo.table_exists(table)]


@require_GET
def health(_request):
    return JsonResponse({"status": "ok"})


@require_GET
def standings(request):
    requested_season = _parse_int(request.GET.get("season"), _default_season(), min_value=1982, max_value=2100)
    season = requested_season
    mode = "SEASON_MATCH"

    try:
        if not repo.table_exists("team_standings"):
            return JsonResponse(
                {
                    "requested_season": requested_season,
                    "effective_season": None,
                    "as_of_date": None,
                    "mode": "NO_DATA",
                    "rows": [],
                }
            )

        as_of_date = repo.latest_standings_as_of(requested_season)

        if not as_of_date:
            fallback_season = repo.standings_fallback_season(requested_season)
            if fallback_season:
                season = fallback_season
                mode = "PRESEASON_FALLBACK"
                as_of_date = repo.latest_standings_as_of(season)

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

        rows = repo.standings_rows(season, as_of_date)
        return JsonResponse(
            {
                "requested_season": requested_season,
                "effective_season": season,
                "as_of_date": as_of_date,
                "mode": mode,
                "rows": rows,
            }
        )
    except DatabaseError:
        return _error_json("database_error", "failed to load standings", 500)


@require_GET
def home_summary(request):
    season = _parse_int(request.GET.get("season"), _default_season(), min_value=1982, max_value=2100)
    min_pa_raw = request.GET.get("min_pa")

    missing = _missing_required_tables(["hitter_season_totals"])
    if missing:
        return _error_json("missing_table", f"required table missing: {', '.join(missing)}", 503)

    if min_pa_raw is None or str(min_pa_raw).strip() == "":
        min_pa = _season_progress_min_pa(season)
        min_pa_policy = "AUTO_BY_SEASON_PROGRESS"
    else:
        min_pa = _parse_int(min_pa_raw, 100, min_value=0, max_value=700)
        min_pa_policy = "MANUAL"

    try:
        base = repo.home_base_totals(season)
        latest_game = repo.latest_game_date(season) if repo.table_exists("hitter_game_logs") else None
        latest_pred = repo.latest_prediction_date(season) if repo.table_exists("hitter_predictions") else None
        top_ops = repo.top_ops_rows(season, min_pa, 5)
        top_hr = repo.top_hr_rows(season, min_pa, 5)

        standings_preview: list[dict[str, Any]] = []
        standings_as_of = None
        if repo.table_exists("team_standings"):
            standings_as_of = repo.latest_standings_as_of(season)
            if standings_as_of:
                standings_preview = repo.standings_preview_rows(season, standings_as_of, 10)

        return JsonResponse(
            {
                "season": season,
                "latest_game_date": latest_game,
                "latest_prediction_date": latest_pred,
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
    except DatabaseError:
        return _error_json("database_error", "failed to load home summary", 500)


@require_GET
def leaderboard(request):
    season = _parse_int(request.GET.get("season"), _default_season(), min_value=1982, max_value=2100)
    metric = str(request.GET.get("metric", "OPS")).upper().strip()
    min_pa_raw = request.GET.get("min_pa")
    team = str(request.GET.get("team", "")).strip()
    limit = _parse_int(request.GET.get("limit"), 20, min_value=1, max_value=200)
    offset = _parse_int(request.GET.get("offset"), 0, min_value=0, max_value=100000)

    missing = _missing_required_tables(["hitter_season_totals"])
    if missing:
        return _error_json("missing_table", f"required table missing: {', '.join(missing)}", 503)

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

    try:
        total = repo.leaderboard_total(season=season, min_pa=min_pa, team=team)
        rows = repo.leaderboard_rows(
            season=season,
            min_pa=min_pa,
            order_metric=order_metric,
            limit=limit,
            offset=offset,
            team=team,
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
    except DatabaseError:
        return _error_json("database_error", "failed to load leaderboard", 500)


@require_GET
def predictions_latest(request):
    season = _parse_int(request.GET.get("season"), _default_season(), min_value=1982, max_value=2100)

    if not repo.table_exists("hitter_predictions"):
        return JsonResponse({"season": season, "latest_date": None, "rows": []})

    try:
        latest_date = repo.predictions_latest_date(season)
        if not latest_date:
            return JsonResponse({"season": season, "latest_date": None, "rows": []})
        rows = repo.predictions_latest_rows(season=season, latest_date=latest_date, limit=100)
        return JsonResponse({"season": season, "latest_date": latest_date, "rows": rows})
    except DatabaseError:
        return _error_json("database_error", "failed to load predictions", 500)


@require_GET
def player_search(request):
    q = str(request.GET.get("q", "")).strip()
    season = _parse_int(request.GET.get("season"), _default_season(), min_value=1982, max_value=2100)
    limit = _parse_int(request.GET.get("limit"), 30, min_value=1, max_value=100)
    team = str(request.GET.get("team", "")).strip()

    missing = _missing_required_tables(["hitter_season_totals"])
    if missing:
        return _error_json("missing_table", f"required table missing: {', '.join(missing)}", 503)

    if not q:
        return JsonResponse({"season": season, "q": q, "rows": []})

    try:
        rows = repo.player_search_rows(season=season, q=q, limit=limit, team=team)
        return JsonResponse({"season": season, "q": q, "team": team or None, "rows": rows})
    except DatabaseError:
        return _error_json("database_error", "failed to search players", 500)


@require_GET
def player_detail(request, player_name: str):
    season = _parse_int(request.GET.get("season"), _default_season(), min_value=1982, max_value=2100)
    name = player_name.strip()
    recent_n = _parse_int(request.GET.get("recent_n"), 10, min_value=1, max_value=60)

    missing = _missing_required_tables(["hitter_season_totals"])
    if missing:
        return _error_json("missing_table", f"required table missing: {', '.join(missing)}", 503)

    try:
        season_rows = repo.player_season_rows(name)
        if not season_rows:
            return _error_json("player_not_found", f"player not found: {name}", 404, {"player_name": name})

        current_rows = [row for row in season_rows if int(row.get("season") or 0) == season]

        latest_prediction = None
        if repo.table_exists("hitter_predictions"):
            latest_prediction = repo.player_latest_prediction(season=season, player_name=name)

        trend_rows: list[dict[str, Any]] = []
        if repo.table_exists("hitter_daily_snapshots"):
            trend_rows = repo.player_trend_rows(season=season, player_name=name)

        monthly: list[dict[str, Any]] = []
        vs_team: list[dict[str, Any]] = []
        recent_games: list[dict[str, Any]] = []
        if repo.table_exists("hitter_game_logs"):
            tb_expr = _safe_tb_expr("TB")
            monthly = repo.player_monthly_rows(player_name=name, season=season, tb_expr=tb_expr)
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

            vs_team = repo.player_vs_team_rows(player_name=name, season=season, tb_expr=_safe_tb_expr("TB"))
            for row in vs_team:
                ab = int(row.get("AB") or 0)
                h = int(row.get("H") or 0)
                bb = int(row.get("BB") or 0)
                tb = int(row.get("TB_adj") or 0)
                row["AVG"] = round(h / ab, 4) if ab > 0 else 0.0
                row["OBP"] = round((h + bb) / (ab + bb), 4) if (ab + bb) > 0 else 0.0
                row["SLG"] = round(tb / ab, 4) if ab > 0 else 0.0
                row["OPS"] = round(row["OBP"] + row["SLG"], 4)

            recent_games = repo.player_recent_games_rows(player_name=name, season=season, recent_n=recent_n)
            for row in recent_games:
                ab = int(row.get("AB") or 0)
                h = int(row.get("H") or 0)
                bb = int(row.get("BB") or 0)
                tb = int(row.get("TB") or 0)
                row["AVG"] = round(h / ab, 4) if ab > 0 else 0.0
                row["OBP"] = round((h + bb) / (ab + bb), 4) if (ab + bb) > 0 else 0.0
                row["SLG"] = round(tb / ab, 4) if ab > 0 else 0.0
                row["OPS"] = round(row["OBP"] + row["SLG"], 4)

        current_agg = repo.player_current_aggregate(
            season=season,
            player_name=name,
            ops_expr=_safe_ops_expr(
                "COALESCE(SUM(AB),0)",
                "COALESCE(SUM(H),0)",
                "COALESCE(SUM(BB),0)",
                "COALESCE(SUM(HBP),0)",
                "COALESCE(SUM(SF),0)",
                "COALESCE(SUM(TB_adj),0)",
            ),
        )

        kbreport_splits: dict[str, list[dict[str, Any]]] = {
            "homeaway": [],
            "pitchside": [],
            "opposite": [],
            "month": [],
        }
        if repo.table_exists("kbreport_hitter_splits"):
            ext_rows = repo.player_kbreport_split_rows(season=season, player_name=name)
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
    except DatabaseError:
        return _error_json("database_error", "failed to load player detail", 500)


@require_GET
def team_detail(request, team: str):
    season = _parse_int(request.GET.get("season"), _default_season(), min_value=1982, max_value=2100)
    name = team.strip()
    min_pa_raw = request.GET.get("min_pa")

    missing = _missing_required_tables(["hitter_season_totals"])
    if missing:
        return _error_json("missing_table", f"required table missing: {', '.join(missing)}", 503)

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

    try:
        team_summary = repo.team_summary(season=season, team=name)
        if not team_summary or int(team_summary.get("players") or 0) == 0:
            return _error_json("team_not_found", f"team not found: {name}", 404, {"team": name, "season": season})

        leaders_ops = repo.team_leaders_ops(season=season, team=name, min_pa=effective_min_pa, limit=10)
        leaders_hr = repo.team_leaders_hr(season=season, team=name, min_pa=effective_min_pa, limit=10)

        monthly: list[dict[str, Any]] = []
        recent_games: list[dict[str, Any]] = []
        if repo.table_exists("hitter_game_logs"):
            monthly = repo.team_monthly_rows(team=name, season=season, tb_expr=_safe_tb_expr("TB"))
            for row in monthly:
                ab = int(row.get("AB") or 0)
                h = int(row.get("H") or 0)
                bb = int(row.get("BB") or 0)
                so = int(row.get("SO") or 0)
                tb = int(row.get("TB_adj") or 0)
                row["AVG"] = round(h / ab, 4) if ab > 0 else 0.0
                row["SLG"] = round(tb / ab, 4) if ab > 0 else 0.0
                row["BB_K"] = round(bb / so, 4) if so > 0 else None

            recent_games = repo.team_recent_games(team=name, season=season, limit=20)

        latest_date = None
        latest_predictions: list[dict[str, Any]] = []
        if repo.table_exists("hitter_predictions"):
            latest_date = repo.team_latest_prediction_date(season=season, team=name)
            if latest_date:
                latest_predictions = repo.team_latest_predictions(
                    season=season,
                    team=name,
                    latest_date=latest_date,
                    limit=10,
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
    except DatabaseError:
        return _error_json("database_error", "failed to load team detail", 500)


@require_GET
def player_compare(request):
    season = _parse_int(request.GET.get("season"), _default_season(), min_value=1982, max_value=2100)
    names_raw = str(request.GET.get("names", "")).strip()
    names = [n.strip() for n in names_raw.split(",") if n.strip()]

    missing = _missing_required_tables(["hitter_season_totals"])
    if missing:
        return _error_json("missing_table", f"required table missing: {', '.join(missing)}", 503)

    if len(names) < 2:
        return _error_json(
            "at_least_two_names_required",
            "query parameter 'names' must include at least two comma-separated player names",
            400,
            {"season": season},
        )

    try:
        rows = repo.player_compare_rows(season=season, names=names)
        return JsonResponse({"season": season, "names": names, "rows": rows})
    except DatabaseError:
        return _error_json("database_error", "failed to compare players", 500)


@require_GET
def games_by_date(request):
    game_date = str(request.GET.get("date", "")).strip()
    season = _parse_int(request.GET.get("season"), _default_season(), min_value=1982, max_value=2100)
    limit = _parse_int(request.GET.get("limit"), 20, min_value=1, max_value=200)

    if not repo.table_exists("hitter_game_logs"):
        return JsonResponse({"season": season, "date": game_date or None, "rows": []})

    try:
        rows = repo.games_by_date_rows(game_date=game_date, season=season, limit=limit)
        return JsonResponse({"season": season, "date": game_date or None, "rows": rows})
    except DatabaseError:
        return _error_json("database_error", "failed to load games", 500)


@require_GET
def game_boxscore(request, game_id: str):
    if not repo.table_exists("hitter_game_logs"):
        return _error_json("missing_table", "required table missing: hitter_game_logs", 503, {"game_id": game_id})

    try:
        rows = repo.game_boxscore_rows(game_id)
        if not rows:
            return _error_json("game_not_found", f"game not found: {game_id}", 404, {"game_id": game_id})

        game_date = rows[0]["game_date"]
        team_summaries = repo.game_boxscore_team_summaries(game_id)
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
    except DatabaseError:
        return _error_json("database_error", "failed to load game boxscore", 500)
