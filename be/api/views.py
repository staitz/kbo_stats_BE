import hashlib
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


def _clamp(value: float, minimum: float, maximum: float) -> float:
    return max(minimum, min(maximum, value))


def _estimate_hitter_projection(
    latest_prediction: dict[str, Any] | None,
    current_agg: dict[str, Any] | None,
    season: int,
) -> dict[str, Any] | None:
    if not latest_prediction or not current_agg:
        return latest_prediction

    enriched = dict(latest_prediction)
    team = str(enriched.get("team") or "").strip()
    team_games = repo.max_team_games(season, team) if team else 0
    season_games = 144
    pace_factor = float(season_games) / float(team_games) if team_games > 0 else 1.0
    pace_factor = _clamp(pace_factor, 1.0, 2.5)

    hits_to_date = float(current_agg.get("H") or 0)
    rbi_to_date = float(current_agg.get("RBI") or 0)
    enriched["predicted_hits_final"] = round(hits_to_date * pace_factor)
    enriched["predicted_rbi_final"] = round(rbi_to_date * pace_factor)

    as_of_date = str(enriched.get("as_of_date") or "").strip()
    if not as_of_date:
        return enriched

    comparison_rows = repo.prediction_rows_for_as_of(season=season, as_of_date=as_of_date)
    qualified = [row for row in comparison_rows if float(row.get("pa_to_date") or 0) >= 80]
    if not qualified:
        qualified = comparison_rows
    if not qualified:
        return enriched

    qualified.sort(
        key=lambda row: (
            -float(row.get("predicted_war_final") or 0),
            -float(row.get("predicted_ops_final") or 0),
            str(row.get("player_name") or ""),
        )
    )

    total = len(qualified)
    player_name = str(enriched.get("player_name") or "").strip()
    player_row = next((row for row in qualified if str(row.get("player_name") or "").strip() == player_name), None)
    if not player_row:
        return enriched

    rank = next(
        (index for index, row in enumerate(qualified, start=1) if str(row.get("player_name") or "").strip() == player_name),
        total,
    )
    percentile = 1.0 - ((rank - 1) / max(total, 1))
    leader_war = float(qualified[0].get("predicted_war_final") or 0)
    player_war = float(player_row.get("predicted_war_final") or 0)
    war_ratio = player_war / leader_war if leader_war > 0 else 0.0

    mvp_prob = ((percentile ** 2.6) * 0.42) + (max(0.0, war_ratio - 0.7) * 0.30)
    gg_prob = (percentile * 0.72) + (max(0.0, war_ratio - 0.55) * 0.25)

    enriched["mvp_probability"] = round(_clamp(mvp_prob, 0.01, 0.65), 4)
    enriched["golden_glove_probability"] = round(_clamp(gg_prob, 0.05, 0.92), 4)
    return enriched


def _season_progress_min_pa(season: int, team: str = "") -> int:
    max_games = repo.max_team_games(season, team)
    return int(max_games * 3.1)


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


def _virtual_player_id(player_name: str) -> str:
    normalized = str(player_name or "").strip()
    digest = hashlib.sha1(normalized.encode("utf-8")).hexdigest()[:12]
    return f"p_{digest}"


def _resolve_player_name_from_id(player_id: str, season: int) -> str | None:
    pid = str(player_id or "").strip()
    if not pid:
        return None

    statiz_name = repo.statiz_player_name_by_id(pid)
    if statiz_name:
        return statiz_name

    for name in repo.player_distinct_names(season):
        if _virtual_player_id(name) == pid:
            return name

    for name in repo.player_distinct_names(None):
        if _virtual_player_id(name) == pid:
            return name

    return None


def _preferred_player_id(player_name: str) -> str:
    name = str(player_name or "").strip()
    if not name:
        return ""
    statiz_id = repo.statiz_player_id_by_name(name)
    if statiz_id:
        return statiz_id
    return _virtual_player_id(name)


def _preferred_player_ids(names: list[str]) -> dict[str, str]:
    mapping = {str(n).strip(): _virtual_player_id(str(n).strip()) for n in names if str(n).strip()}
    statiz_map = repo.statiz_player_ids_by_names([n for n in mapping.keys()])
    for name, pid in statiz_map.items():
        mapping[name] = pid
    return mapping


@require_GET
def health(_request):
    return JsonResponse({"status": "ok"})


@require_GET
def standings(request):
    requested_season = _parse_int(request.GET.get("season"), _default_season(), min_value=1982, max_value=2100)
    season = requested_season

    try:
        as_of_date = repo.logs_latest_game_date(season)
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

        rows = repo.computed_standings_rows(season)
        return JsonResponse(
            {
                "requested_season": requested_season,
                "effective_season": season,
                "as_of_date": as_of_date,
                "mode": "SEASON_MATCH",
                "rows": rows,
            }
        )
    except DatabaseError:
        return _error_json("database_error", "failed to load standings", 500)


@require_GET
def home_summary(request):
    requested_season = _parse_int(request.GET.get("season"), _default_season(), min_value=1982, max_value=2100)
    season = requested_season
    mode = "SEASON_MATCH"
    min_pa_raw = request.GET.get("min_pa")

    missing = _missing_required_tables(["hitter_season_totals"])
    if missing:
        return _error_json("missing_table", f"required table missing: {', '.join(missing)}", 503)

    try:
        if repo.leaderboard_candidate_count(season=season, min_pa=0) == 0:
            return JsonResponse(
                {
                    "season": requested_season,
                    "requested_season": requested_season,
                    "effective_season": None,
                    "mode": "NO_DATA",
                    "latest_game_date": None,
                    "latest_prediction_date": None,
                    "totals": {"players": 0, "teams": 0, "total_hr": 0, "total_pa": 0},
                    "min_pa": 0,
                    "effective_min_pa": 0,
                    "min_pa_policy": "AUTO_BY_SEASON_PROGRESS",
                    "leaderboards": {
                        "avg_top5": [],
                        "hr_top5": [],
                        "ops_top5": [],
                        "era_top5": [],
                        "war_top5": [],
                    },
                    "standings_preview": {"as_of_date": None, "rows": []},
                    "notes": ["아직 데이터가 없습니다."],
                }
            )

        # --- min_pa: auto or manual, always based on the resolved season ---
        if min_pa_raw is None or str(min_pa_raw).strip() == "":
            min_pa = _season_progress_min_pa(season)
            min_pa_policy = "AUTO_BY_SEASON_PROGRESS"
        else:
            min_pa = _parse_int(min_pa_raw, 100, min_value=0, max_value=700)
            min_pa_policy = "MANUAL"

        effective_min_pa = _pick_effective_min_pa_for_leaderboard(
            season=season,
            team="",
            base_min_pa=min_pa,
            auto_relax=False,
            min_count=5,
        )

        base = repo.home_base_totals(season)
        latest_game = repo.latest_game_date(season) if repo.table_exists("hitter_game_logs") else None
        latest_pred = repo.latest_prediction_date(season) if repo.table_exists("hitter_predictions") else None
        top_avg = repo.top_avg_rows(season, effective_min_pa, 5)
        top_ops = repo.top_ops_rows(season, effective_min_pa, 5)
        top_hr = repo.top_hr_rows(season, effective_min_pa, 5)
        # Pitcher dataset is not available yet in this MVP.
        # Keep ERA card empty on the frontend until pitcher stats are integrated.
        top_era: list[dict[str, Any]] = []
        top_war = repo.top_war_rows(season, effective_min_pa, 5)

        standings_as_of = repo.logs_latest_game_date(season)
        standings_preview = repo.computed_standings_rows(season)[:10]

        return JsonResponse(
            {
                "season": season,
                "requested_season": requested_season,
                "effective_season": season,
                "mode": mode,
                "latest_game_date": latest_game,
                "latest_prediction_date": latest_pred,
                "totals": base,
                "min_pa": min_pa,
                "effective_min_pa": effective_min_pa,
                "min_pa_policy": min_pa_policy,
                "leaderboards": {
                    "avg_top5": top_avg,
                    "hr_top5": top_hr,
                    "ops_top5": top_ops,
                    "era_top5": top_era,
                    "war_top5": top_war,
                },
                "standings_preview": {
                    "as_of_date": standings_as_of,
                    "rows": standings_preview,
                },
                "notes": [
                    "standings are derived from Naver-based hitter_game_logs by game result aggregation",
                ],
            }
        )
    except DatabaseError:
        return _error_json("database_error", "failed to load home summary", 500)


@require_GET
def leaderboard(request):
    requested_season = _parse_int(request.GET.get("season"), _default_season(), min_value=1982, max_value=2100)
    season = requested_season
    mode = "SEASON_MATCH"
    metric = str(request.GET.get("metric", "OPS")).upper().strip()
    min_pa_raw = request.GET.get("min_pa")
    team = str(request.GET.get("team", "")).strip()
    limit = _parse_int(request.GET.get("limit"), 20, min_value=1, max_value=200)
    offset = _parse_int(request.GET.get("offset"), 0, min_value=0, max_value=100000)

    missing = _missing_required_tables(["hitter_season_totals"])
    if missing:
        return _error_json("missing_table", f"required table missing: {', '.join(missing)}", 503)

    if repo.leaderboard_candidate_count(season=season, min_pa=0, team=team) == 0:
        return JsonResponse(
            {
                "season": requested_season,
                "requested_season": requested_season,
                "effective_season": None,
                "mode": "NO_DATA",
                "metric": metric,
                "requested_min_pa": 0,
                "effective_min_pa": 0,
                "min_pa_policy": "AUTO_BY_SEASON_PROGRESS",
                "team": team or None,
                "total": 0,
                "limit": limit,
                "offset": offset,
                "rows": [],
            }
        )

    if min_pa_raw is None or str(min_pa_raw).strip() == "":
        requested_min_pa = _season_progress_min_pa(season, team)
        min_pa_policy = "AUTO_BY_SEASON_PROGRESS"
        min_pa = _pick_effective_min_pa_for_leaderboard(
            season=season,
            team=team,
            base_min_pa=requested_min_pa,
            auto_relax=False,  # 규정타석 ON 상태 — 팀 필터 여부와 무관하게 완화하지 않음
            min_count=20,
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
        player_id_map = _preferred_player_ids([str(r.get("player_name") or "") for r in rows])
        for row in rows:
            name = str(row.get("player_name") or "").strip()
            row["player_id"] = player_id_map.get(name, _virtual_player_id(name))

        return JsonResponse(
            {
                "season": season,
                "requested_season": requested_season,
                "effective_season": season,
                "mode": mode,
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
    # mode: 'prediction' (default, daily in-season) or 'projection' (pre-season)
    mode = str(request.GET.get("mode", "prediction")).strip().lower()
    if mode not in {"prediction", "projection"}:
        mode = "prediction"
    # model_version: optional filter e.g. 'hitter_mvp_v2'. None = no filter (latest across all versions)
    model_version: str | None = request.GET.get("model_version") or None

    if not repo.table_exists("hitter_predictions"):
        return JsonResponse({"season": season, "mode": mode, "model_version": model_version, "latest_date": None, "rows": []})

    try:
        latest_date = repo.predictions_latest_date(season, mode=mode, model_version=model_version)
        if not latest_date:
            return JsonResponse({"season": season, "mode": mode, "model_version": model_version, "latest_date": None, "rows": []})
        rows = repo.predictions_latest_rows(season=season, latest_date=latest_date, limit=100, mode=mode, model_version=model_version)
        return JsonResponse({"season": season, "mode": mode, "model_version": model_version, "latest_date": latest_date, "rows": rows})
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
        for row in rows:
            name = str(row.get("player_name") or "").strip()
            team_str = str(row.get("team") or "").strip()
            row["player_id"] = f"{name}_{team_str}" if team_str else name
        return JsonResponse({"season": season, "q": q, "team": team or None, "rows": rows})
    except DatabaseError:
        return _error_json("database_error", "failed to search players", 500)


@require_GET
def player_detail(request, player_id: str):
    season = _parse_int(request.GET.get("season"), _default_season(), min_value=1982, max_value=2100)
    pid = player_id.strip()
    
    target_team = None
    if "_" in pid and not pid.startswith("p_"):
        name_part, team_part = pid.rsplit("_", 1)
        name = _resolve_player_name_from_id(pid, season) or name_part
        target_team = team_part if team_part else None
    else:
        name = _resolve_player_name_from_id(pid, season) or pid
        
    recent_n = _parse_int(request.GET.get("recent_n"), 10, min_value=1, max_value=60)

    missing = _missing_required_tables(["hitter_season_totals"])
    if missing:
        return _error_json("missing_table", f"required table missing: {', '.join(missing)}", 503)

    try:
        season_rows = repo.player_season_rows(name, team=target_team)
        if not season_rows:
            return _error_json(
                "player_not_found",
                f"player not found: {name}",
                404,
                {"player_id": pid, "player_name": name},
            )

        current_rows = [row for row in season_rows if int(row.get("season") or 0) == season]

        latest_prediction = None
        if repo.table_exists("hitter_predictions"):
            latest_prediction = repo.player_latest_prediction(season=season, player_name=name, team=target_team)

        trend_rows: list[dict[str, Any]] = []
        if repo.table_exists("hitter_daily_snapshots"):
            trend_rows = repo.player_trend_rows(season=season, player_name=name, team=target_team)

        monthly: list[dict[str, Any]] = []
        vs_team: list[dict[str, Any]] = []
        recent_games: list[dict[str, Any]] = []
        if repo.table_exists("hitter_game_logs"):
            tb_expr = _safe_tb_expr("TB")
            monthly = repo.player_monthly_rows(player_name=name, season=season, tb_expr=tb_expr, team=target_team)
            for row in monthly:
                ab = int(row.get("AB") or 0)
                h = int(row.get("H") or 0)
                bb = int(row.get("BB") or 0)
                hbp = int(row.get("HBP") or 0)
                sf = int(row.get("SF") or 0)
                tb = int(row.get("TB_adj") or 0)
                obp_den = ab + bb + hbp + sf
                avg = float(h / ab) if ab > 0 else 0.0
                obp = float(h + bb + hbp) / obp_den if obp_den > 0 else 0.0
                slg = float(tb / ab) if ab > 0 else 0.0
                row["AVG"] = round(avg, 4)  # pyre-ignore
                row["OBP"] = round(obp, 4)  # pyre-ignore
                row["SLG"] = round(slg, 4)  # pyre-ignore
                row["OPS"] = round(obp + slg, 4)  # pyre-ignore

            vs_team = repo.player_vs_team_rows(player_name=name, season=season, tb_expr=_safe_tb_expr("TB"), team=target_team)
            for row in vs_team:
                ab = int(row.get("AB") or 0)
                h = int(row.get("H") or 0)
                bb = int(row.get("BB") or 0)
                tb = int(row.get("TB_adj") or 0)
                avg = float(h / ab) if ab > 0 else 0.0
                obp = float(h + bb) / (ab + bb) if (ab + bb) > 0 else 0.0
                slg = float(tb / ab) if ab > 0 else 0.0
                row["AVG"] = round(avg, 4)  # pyre-ignore
                row["OBP"] = round(obp, 4)  # pyre-ignore
                row["SLG"] = round(slg, 4)  # pyre-ignore
                row["OPS"] = round(obp + slg, 4)  # pyre-ignore

            recent_games = repo.player_recent_games_rows(player_name=name, season=season, recent_n=recent_n, team=target_team)
            for row in recent_games:
                ab = int(row.get("AB") or 0)
                h = int(row.get("H") or 0)
                bb = int(row.get("BB") or 0)
                tb = int(row.get("TB") or 0)
                avg = float(h / ab) if ab > 0 else 0.0
                obp = float(h + bb) / (ab + bb) if (ab + bb) > 0 else 0.0
                slg = float(tb / ab) if ab > 0 else 0.0
                row["AVG"] = round(avg, 4)  # pyre-ignore
                row["OBP"] = round(obp, 4)  # pyre-ignore
                row["SLG"] = round(slg, 4)  # pyre-ignore
                row["OPS"] = round(obp + slg, 4)  # pyre-ignore

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
            team=target_team,
        )
        latest_prediction = _estimate_hitter_projection(
            latest_prediction=latest_prediction,
            current_agg=current_agg,
            season=season,
        )

        kbreport_splits: dict[str, list[dict[str, Any]]] = {
            "homeaway": [],
            "pitchside": [],
            "opposite": [],
            "month": [],
        }
        if repo.table_exists("kbreport_hitter_splits"):
            ext_rows = repo.player_kbreport_split_rows(season=season, player_name=name, team=target_team)
            for row in ext_rows:
                group = str(row.get("split_group") or "")
                if group in kbreport_splits:
                    kbreport_splits[group].append(row)

        return JsonResponse(
            {
                "season": season,
                "recent_n": recent_n,
                "player_id": _preferred_player_id(name),
                "player_name": name,
                "profile": {
                    "player_id": _preferred_player_id(name),
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
    requested_season = _parse_int(request.GET.get("season"), _default_season(), min_value=1982, max_value=2100)
    season = requested_season
    name = team.strip()
    min_pa_raw = request.GET.get("min_pa")

    missing = _missing_required_tables(["hitter_season_totals"])
    if missing:
        return _error_json("missing_table", f"required table missing: {', '.join(missing)}", 503)

    if min_pa_raw is None or str(min_pa_raw).strip() == "":
        requested_min_pa = _season_progress_min_pa(season, name)
        effective_min_pa = _pick_effective_min_pa_for_leaderboard(
            season=season,
            team=name,
            base_min_pa=requested_min_pa,
            auto_relax=False,
            min_count=5,
        )
        min_pa_policy = "AUTO_BY_SEASON_PROGRESS"
    else:
        requested_min_pa = _parse_int(min_pa_raw, 100, min_value=0, max_value=700)
        effective_min_pa = requested_min_pa
        min_pa_policy = "MANUAL"

    try:
        if repo.leaderboard_candidate_count(season=season, min_pa=0, team=name) == 0:
            return JsonResponse(
                {
                    "season": requested_season,
                    "team": name,
                    "mode": "NO_DATA",
                    "detail": "아직 데이터가 없습니다.",
                    "summary": {},
                    "leaders": {"ops_top10": [], "hr_top10": []},
                    "monthly_trend": [],
                    "recent_games": [],
                    "h2h": [],
                    "latest_prediction_date": None,
                    "latest_predictions": [],
                }
            )

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
                row["AVG"] = round(h / ab, 4) if ab > 0 else 0.0 # pyre-ignore
                row["SLG"] = round(tb / ab, 4) if ab > 0 else 0.0 # pyre-ignore
                row["BB_K"] = round(bb / so, 4) if so > 0 else None

            recent_games = repo.team_recent_games(team=name, season=season, limit=20)
        h2h = repo.team_h2h_rows(team=name, season=season) if repo.table_exists("hitter_game_logs") else []

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
                "h2h": h2h,
                "latest_prediction_date": latest_date,
                "latest_predictions": latest_predictions,
                "notes": [
                    "team recent games and head-to-head are derived from Naver-based hitter_game_logs",
                ],
            }
        )
    except DatabaseError:
        return _error_json("database_error", "failed to load team detail", 500)


@require_GET
def team_schedule(request, team: str):
    season = _parse_int(request.GET.get("season"), _default_season(), min_value=1982, max_value=2100)
    limit = _parse_int(request.GET.get("limit"), 60, min_value=1, max_value=300)
    name = team.strip()

    try:
        schedule_rows = repo.team_schedule_rows(team=name, season=season, limit=limit)
        result_map = repo.team_result_by_game(team=name, season=season)
        items: list[dict[str, Any]] = []
        for row in schedule_rows:
            game_id = str(row.get("game_id") or "").strip()
            merged = {
                "game_date": row.get("game_date"),
                "game_id": game_id or None,
                "away_team": row.get("away_team"),
                "home_team": row.get("home_team"),
                "game_time": row.get("game_time"),
                "stadium": row.get("stadium"),
                "status": row.get("status"),
                "is_home": str(row.get("home_team") or "").strip() == name,
                "opp_team": str(row.get("away_team") or "").strip()
                if str(row.get("home_team") or "").strip() == name
                else str(row.get("home_team") or "").strip(),
                "result": None,
                "team_score": None,
                "opp_score": None,
            }
            if game_id and game_id in result_map:
                src = result_map[game_id]
                merged["result"] = src.get("result")
                merged["team_score"] = src.get("team_score")
                merged["opp_score"] = src.get("opp_score")
                merged["opp_team"] = src.get("opp_team")
            items.append(merged)

        return JsonResponse(
            {
                "season": season,
                "team": name,
                "mode": "SEASON_MATCH" if len(items) > 0 else "NO_DATA",
                "detail": None if len(items) > 0 else "아직 데이터가 없습니다.",
                "rows": items,
                "count": len(items),
            }
        )
    except DatabaseError:
        return _error_json("database_error", "failed to load team schedule", 500)


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
