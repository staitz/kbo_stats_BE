import hashlib
from datetime import datetime
from typing import Any

from django.db import DatabaseError
from django.http import JsonResponse
from django.views.decorators.http import require_GET
from zoneinfo import ZoneInfo

from . import repository as repo


KST = ZoneInfo("Asia/Seoul")
KBO_REGULAR_SEASON_START_MONTH = 3
KBO_REGULAR_SEASON_START_DAY = 28


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
    now_kst = datetime.now(KST)
    current_year = now_kst.year
    has_season_started = (
        now_kst.month > KBO_REGULAR_SEASON_START_MONTH
        or (
            now_kst.month == KBO_REGULAR_SEASON_START_MONTH
            and now_kst.day >= KBO_REGULAR_SEASON_START_DAY
        )
    )
    target_year = current_year if has_season_started else current_year - 1

    latest_at_or_before = repo.logs_latest_season_at_or_before(target_year)
    if latest_at_or_before:
        return latest_at_or_before

    season = repo.default_season()
    if season:
        return season
    return target_year


def _parse_yyyymmdd(value: str) -> datetime | None:
    if not value or len(value) != 8 or not value.isdigit():
        return None
    try:
        return datetime.strptime(value, "%Y%m%d")
    except ValueError:
        return None


def _clamp(value: float, minimum: float, maximum: float) -> float:
    return max(minimum, min(maximum, value))


def _blend_value(anchor: float, current: float, exposure: float, k: float) -> float:
    weight = exposure / (exposure + k) if k > 0 else 1.0
    weight = _clamp(weight, 0.0, 1.0)
    return ((1.0 - weight) * anchor) + (weight * current)


def _team_progress(season: int, team: str) -> float:
    team_games = repo.max_team_games(season, team) if team else 0
    if team_games <= 0:
        return 0.0
    return _clamp(float(team_games) / 144.0, 0.0, 1.0)


def _classify_status(raw_status: str | None, result: str | None) -> str:
    """Classify a raw Naver statusInfo string into a normalized category.

    Returns one of:
      'finished'  – game ended (result data may or may not exist)
      'cancelled'  – rain cancellation, postponement, official cancel
      'suspended'  – suspended/interrupted game
      'scheduled'  – upcoming / not-yet-started game
      'unknown'    – status absent or unrecognized
    """
    # If we have a W/L/D result the game clearly finished
    if result is not None:
        return "finished"

    if not raw_status:
        # No status recorded but no result either → almost certainly a scheduled game
        return "scheduled"

    s = raw_status.replace(" ", "")

    # Cancelled / postponed family
    if any(k in s for k in ["취소", "우천", "강우", "연기", "postpone", "cancel"]):
        return "cancelled"

    # Suspended / interrupted
    if any(k in s for k in ["서스펜", "중단", "suspend"]):
        return "suspended"

    # Already ended (no result row yet → data gap)
    if any(k in s for k in ["종료", "finish", "end"]):
        return "finished"

    # Scheduled / pre-game
    if any(k in s for k in ["경기전", "예정", "scheduled", "before"]):
        return "scheduled"

    return "unknown"


def _estimate_hitter_projection(
    latest_prediction: dict[str, Any] | None,
    current_agg: dict[str, Any] | None,
    current_row: dict[str, Any] | None,
    prior_row: dict[str, Any] | None,
    season: int,
) -> dict[str, Any] | None:
    if not current_agg:
        return latest_prediction

    enriched = dict(latest_prediction or {})
    team = str(enriched.get("team") or "").strip()
    team_games = repo.max_team_games(season, team) if team else 0
    season_games = 144
    pace_factor = float(season_games) / float(team_games) if team_games > 0 else 1.0
    pace_factor = _clamp(pace_factor, 1.0, 2.5)
    progress = _team_progress(season, team)

    hits_to_date = float(current_agg.get("H") or 0)
    rbi_to_date = float(current_agg.get("RBI") or 0)
    pa_to_date = float(enriched.get("pa_to_date") or current_agg.get("PA") or 0)
    prior_hits = float((prior_row or {}).get("H") or 0)
    prior_rbi = float((prior_row or {}).get("RBI") or 0)
    current_hits_full = hits_to_date * pace_factor
    current_rbi_full = rbi_to_date * pace_factor
    predicted_hits = _blend_value(prior_hits, current_hits_full, pa_to_date, 220.0)
    predicted_rbi = _blend_value(prior_rbi, current_rbi_full, pa_to_date, 180.0)
    enriched["predicted_hits_final"] = max(round(predicted_hits), int(round(hits_to_date)))
    enriched["predicted_rbi_final"] = max(round(predicted_rbi), int(round(rbi_to_date)))

    # Award probabilities should reflect only actual model-backed hitter rows.
    # If the player has no model prediction row or the sample is too small,
    # keep the probabilities at zero instead of inferring them from fallbacks.
    # Fallback: if hitter_predictions didn't store pa_to_date, use the
    # actual PA from the season aggregate (hitter_season_totals).
    if pa_to_date == 0 and current_agg:
        pa_to_date = float(current_agg.get("PA") or 0)
    model_source = str(enriched.get("model_source") or "").strip().upper()
    if pa_to_date < 80 or model_source == "":
        enriched["mvp_probability"] = 0.0
        enriched["golden_glove_probability"] = 0.0
        return enriched

    as_of_date = str(enriched.get("as_of_date") or "").strip()
    if not as_of_date:
        enriched["mvp_probability"] = 0.0
        enriched["golden_glove_probability"] = 0.0
        return enriched

    comparison_rows = repo.prediction_rows_for_as_of(season=season, as_of_date=as_of_date)
    qualified = [row for row in comparison_rows if float(row.get("pa_to_date") or 0) >= 80]
    if not qualified:
        enriched["mvp_probability"] = 0.0
        enriched["golden_glove_probability"] = 0.0
        return enriched


    qualified.sort(
        key=lambda row: (
            -float(row.get("predicted_war_final") or 0),
            -float(row.get("predicted_ops_final") or 0),
            str(row.get("player_name") or ""),
        )
    )

    total = len(qualified)
    player_name = str(enriched.get("player_name") or current_agg.get("player_name") or "").strip()
    player_row = next((row for row in qualified if str(row.get("player_name") or "").strip() == player_name), None)
    player_war = float(enriched.get("predicted_war_final") or (player_row or {}).get("predicted_war_final") or 0)
    player_ops = float(enriched.get("predicted_ops_final") or (player_row or {}).get("predicted_ops_final") or 0)
    if player_row:
        rank = next(
            (index for index, row in enumerate(qualified, start=1) if str(row.get("player_name") or "").strip() == player_name),
            total,
        )
    else:
        rank = 1 + sum(
            1
            for row in qualified
            if (
                float(row.get("predicted_war_final") or 0),
                float(row.get("predicted_ops_final") or 0),
                str(row.get("player_name") or ""),
            )
            > (
                player_war,
                player_ops,
                player_name,
            )
        )
    percentile = 1.0 - ((rank - 1) / max(total, 1))
    leader_war = float(qualified[0].get("predicted_war_final") or 0)
    war_ratio = player_war / leader_war if leader_war > 0 else 0.0

    mvp_prob = ((percentile ** 2.6) * 0.42) + (max(0.0, war_ratio - 0.7) * 0.30)
    gg_prob = (percentile * 0.72) + (max(0.0, war_ratio - 0.55) * 0.25)

    season_damp = progress ** 1.35
    enriched["mvp_probability"] = round(_clamp(mvp_prob * season_damp, 0.0, 0.65), 4)
    enriched["golden_glove_probability"] = round(_clamp(gg_prob * season_damp, 0.0, 0.92), 4)
    return enriched


def _estimate_hitter_pace_projection(
    current_agg: dict[str, Any] | None,
    current_row: dict[str, Any] | None,
    prior_row: dict[str, Any] | None,
    season: int,
) -> dict[str, Any] | None:
    if not current_agg:
        return None

    team = str(current_agg.get("team") or current_row.get("team") if current_row else "").strip()
    team_games = repo.max_team_games(season, team) if team else 0
    season_games = 144
    progress = float(team_games) / float(season_games) if team_games > 0 else 0.0
    progress = _clamp(progress, 0.0, 1.0)
    pace_factor = float(season_games) / float(team_games) if team_games > 0 else 1.0
    pace_factor = _clamp(pace_factor, 1.0, 2.5)

    confidence_score = _clamp(0.35 + (progress * 0.55), 0.35, 0.9)
    if confidence_score >= 0.78:
        confidence_level = "HIGH"
    elif confidence_score >= 0.58:
        confidence_level = "MEDIUM"
    else:
        confidence_level = "LOW"

    pa_to_date = float(current_agg.get("PA") or 0)
    hr_to_date = float(current_agg.get("HR") or 0)
    hits_to_date = float(current_agg.get("H") or 0)
    rbi_to_date = float(current_agg.get("RBI") or 0)
    ops_to_date = float(current_agg.get("OPS") or 0)
    war_to_date = float((current_row or {}).get("WAR") or 0)
    prior_hr = float((prior_row or {}).get("HR") or 0)
    prior_hits = float((prior_row or {}).get("H") or 0)
    prior_rbi = float((prior_row or {}).get("RBI") or 0)
    prior_ops = float((prior_row or {}).get("OPS") or 0)
    prior_war = float((prior_row or {}).get("WAR") or 0)
    latest_prediction_date = repo.latest_prediction_date(season) if repo.table_exists("hitter_predictions") else None

    return {
        "player_name": current_agg.get("player_name"),
        "team": team,
        "as_of_date": latest_prediction_date or current_agg.get("latest_game_date"),
        "model_source": "PACE_BASED_HITTER",
        "confidence_score": round(confidence_score, 4),
        "confidence_level": confidence_level,
        "pa_to_date": round(pa_to_date, 1),
        "predicted_hr_final": max(round(_blend_value(prior_hr, hr_to_date * pace_factor, pa_to_date, 180.0)), int(round(hr_to_date))),
        "predicted_ops_final": round(_clamp(_blend_value(prior_ops, ops_to_date, pa_to_date, 220.0), 0.45, 1.05), 3),
        "predicted_war_final": round(_clamp(_blend_value(prior_war, war_to_date * pace_factor, pa_to_date, 260.0), -1.0, 12.0), 3),
        "predicted_hits_final": max(round(_blend_value(prior_hits, hits_to_date * pace_factor, pa_to_date, 220.0)), int(round(hits_to_date))),
        "predicted_rbi_final": max(round(_blend_value(prior_rbi, rbi_to_date * pace_factor, pa_to_date, 180.0)), int(round(rbi_to_date))),
    }


def _estimate_pitcher_projection(
    current_agg: dict[str, Any] | None,
    current_row: dict[str, Any] | None,
    prior_row: dict[str, Any] | None,
    season: int,
) -> dict[str, Any] | None:
    if not current_agg:
        return None

    team = str(current_agg.get("team") or "").strip()
    team_games = repo.max_team_games(season, team) if team else 0
    season_games = 144
    progress = float(team_games) / float(season_games) if team_games > 0 else 0.0
    progress = _clamp(progress, 0.0, 1.0)
    pace_factor = float(season_games) / float(team_games) if team_games > 0 else 1.0
    pace_factor = _clamp(pace_factor, 1.0, 2.5)

    confidence_score = _clamp(0.35 + (progress * 0.55), 0.35, 0.9)
    if confidence_score >= 0.78:
        confidence_level = "HIGH"
    elif confidence_score >= 0.58:
        confidence_level = "MEDIUM"
    else:
        confidence_level = "LOW"

    outs_to_date = float(current_agg.get("OUTS") or 0)
    ip_to_date = float(current_agg.get("IP") or 0)
    wins_to_date = float(current_agg.get("W") or 0)
    losses_to_date = float(current_agg.get("L") or 0)
    saves_to_date = float(current_agg.get("SV") or 0)
    holds_to_date = float(current_agg.get("HLD") or 0)
    so_to_date = float(current_agg.get("SO") or 0)
    h_to_date = float(current_agg.get("H") or 0)
    bb_to_date = float(current_agg.get("BB") or 0)
    er_to_date = float(current_agg.get("ER") or 0)
    prior_ip = float((prior_row or {}).get("IP") or 0)
    prior_w = float((prior_row or {}).get("W") or 0)
    prior_l = float((prior_row or {}).get("L") or 0)
    prior_sv = float((prior_row or {}).get("SV") or 0)
    prior_hld = float((prior_row or {}).get("HLD") or 0)
    prior_so = float((prior_row or {}).get("SO") or 0)
    prior_h = float((prior_row or {}).get("H") or 0)
    prior_bb = float((prior_row or {}).get("BB") or 0)
    prior_er = float((prior_row or {}).get("ER") or 0)
    prior_era = float((prior_row or {}).get("ERA") or 0)
    prior_whip = float((prior_row or {}).get("WHIP") or 0)
    prior_k9 = float((prior_row or {}).get("K9") or 0)
    prior_bb9 = float((prior_row or {}).get("BB9") or 0)
    prior_kbb = float((prior_row or {}).get("KBB") or 0) if (prior_row or {}).get("KBB") is not None else None
    prior_war = float((prior_row or {}).get("WAR") or 0)

    projected_outs = round(_blend_value(prior_ip * 3.0, outs_to_date * pace_factor, outs_to_date, 120.0))
    projected_ip = round(projected_outs / 3.0, 1)
    projected_wins = round(_blend_value(prior_w, wins_to_date * pace_factor, outs_to_date, 90.0))
    projected_losses = round(_blend_value(prior_l, losses_to_date * pace_factor, outs_to_date, 90.0))
    projected_saves = round(_blend_value(prior_sv, saves_to_date * pace_factor, outs_to_date, 75.0))
    projected_holds = round(_blend_value(prior_hld, holds_to_date * pace_factor, outs_to_date, 75.0))
    projected_so = round(_blend_value(prior_so, so_to_date * pace_factor, outs_to_date, 120.0))
    projected_h = round(_blend_value(prior_h, h_to_date * pace_factor, outs_to_date, 120.0))
    projected_bb = round(_blend_value(prior_bb, bb_to_date * pace_factor, outs_to_date, 120.0))
    projected_er = round(_blend_value(prior_er, er_to_date * pace_factor, outs_to_date, 120.0))

    projected_era = _clamp(_blend_value(prior_era, float(current_agg.get("ERA") or 0), outs_to_date, 180.0), 1.5, 8.5)
    projected_whip = _clamp(_blend_value(prior_whip, float(current_agg.get("WHIP") or 0), outs_to_date, 180.0), 0.7, 2.2)
    projected_k9 = _clamp(_blend_value(prior_k9, float(current_agg.get("K9") or 0), outs_to_date, 180.0), 3.0, 14.0)
    projected_bb9 = _clamp(_blend_value(prior_bb9, float(current_agg.get("BB9") or 0), outs_to_date, 180.0), 0.5, 7.0)
    current_kbb = float(current_agg.get("KBB") or 0) if current_agg.get("KBB") is not None else 0.0
    projected_kbb = _clamp(_blend_value(prior_kbb or 0.0, current_kbb, outs_to_date, 180.0), 0.5, 10.0) if (prior_kbb is not None or current_agg.get("KBB") is not None) else None
    projected_war = _clamp(_blend_value(prior_war, float((current_row or {}).get("WAR") or 0) * pace_factor, outs_to_date, 220.0), -1.0, 10.0)

    return {
        "player_name": current_agg.get("player_name"),
        "team": team,
        "role": current_agg.get("role"),
        "as_of_date": current_agg.get("latest_game_date"),
        "model_source": "PACE_BASED_PITCHER",
        "confidence_score": round(confidence_score, 4),
        "confidence_level": confidence_level,
        "wins_to_date": int(round(wins_to_date)),
        "losses_to_date": int(round(losses_to_date)),
        "saves_to_date": int(round(saves_to_date)),
        "holds_to_date": int(round(holds_to_date)),
        "innings_to_date": round(ip_to_date, 1),
        "predicted_wins_final": int(projected_wins),
        "predicted_losses_final": int(projected_losses),
        "predicted_saves_final": int(projected_saves),
        "predicted_holds_final": int(projected_holds),
        "predicted_ip_final": projected_ip,
        "predicted_so_final": int(projected_so),
        "predicted_h_final": int(projected_h),
        "predicted_bb_final": int(projected_bb),
        "predicted_er_final": int(projected_er),
        "predicted_era_final": round(projected_era, 3),
        "predicted_whip_final": round(projected_whip, 3),
        "predicted_k9_final": round(projected_k9, 2),
        "predicted_bb9_final": round(projected_bb9, 2),
        "predicted_kbb_final": round(projected_kbb, 2) if projected_kbb is not None else None,
        "predicted_war_final": round(projected_war, 3),
        "ip_to_date": round(ip_to_date, 3),
        "so_to_date": float(so_to_date or 0),
    }


def _estimate_pitcher_totals_projection(
    current_row: dict[str, Any] | None,
    season: int,
) -> dict[str, Any] | None:
    if not current_row:
        return None

    latest_prediction_date = repo.pitcher_latest_prediction_date(season) if repo.table_exists("pitcher_predictions") else None
    return {
        "player_name": current_row.get("player_name"),
        "team": current_row.get("team"),
        "role": current_row.get("role"),
        "as_of_date": latest_prediction_date or repo.logs_latest_game_date(season),
        "model_source": "SEASON_TOTALS_FALLBACK",
        "confidence_score": 0.9,
        "confidence_level": "HIGH",
        "wins_to_date": int(round(float(current_row.get("W") or 0))),
        "losses_to_date": int(round(float(current_row.get("L") or 0))),
        "saves_to_date": int(round(float(current_row.get("SV") or 0))),
        "holds_to_date": int(round(float(current_row.get("HLD") or 0))),
        "innings_to_date": round(float(current_row.get("IP") or 0), 1),
        "predicted_wins_final": int(round(float(current_row.get("W") or 0))),
        "predicted_losses_final": int(round(float(current_row.get("L") or 0))),
        "predicted_saves_final": int(round(float(current_row.get("SV") or 0))),
        "predicted_holds_final": int(round(float(current_row.get("HLD") or 0))),
        "predicted_ip_final": round(float(current_row.get("IP") or 0), 1),
        "predicted_so_final": int(round(float(current_row.get("SO") or 0))),
        "predicted_h_final": int(round(float(current_row.get("H") or 0))),
        "predicted_bb_final": int(round(float(current_row.get("BB") or 0))),
        "predicted_er_final": int(round(float(current_row.get("ER") or 0))),
        "predicted_era_final": round(float(current_row.get("ERA") or 0), 3),
        "predicted_whip_final": round(float(current_row.get("WHIP") or 0), 3),
        "predicted_k9_final": round(float(current_row.get("K9") or 0), 2),
        "predicted_bb9_final": round(float(current_row.get("BB9") or 0), 2),
        "predicted_kbb_final": round(float(current_row.get("KBB") or 0), 2) if current_row.get("KBB") is not None else None,
        "predicted_war_final": round(float(current_row.get("WAR") or 0), 3),
        "ip_to_date": round(float(current_row.get("IP") or 0), 3),
        "so_to_date": float(current_row.get("SO") or 0),
    }


def _estimate_pitcher_awards(
    latest_prediction: dict[str, Any] | None,
    season: int,
) -> dict[str, Any] | None:
    if not latest_prediction:
        return latest_prediction

    enriched = dict(latest_prediction)
    # Award probabilities should come only from actual pitcher model predictions.
    # Low-sample or fallback-only rows get a truthful zero instead of a heuristic estimate.
    ip_to_date = float(enriched.get("ip_to_date") or enriched.get("innings_to_date") or 0)
    role_hint = str(enriched.get("role") or "").strip().upper()
    role_min_ip = 10.0 if role_hint == "RP" else 20.0
    model_source = str(enriched.get("model_source") or "").strip().upper()
    if ip_to_date < role_min_ip or model_source in {"", "PACE_BASED_PITCHER", "SEASON_TOTALS_FALLBACK"}:
        enriched["mvp_probability"] = 0.0
        enriched["golden_glove_probability"] = 0.0
        return enriched

    as_of_date = str(enriched.get("as_of_date") or "").strip()
    if not as_of_date:
        enriched["mvp_probability"] = 0.0
        enriched["golden_glove_probability"] = 0.0
        return enriched

    comparison_rows = repo.pitcher_prediction_rows_for_as_of(season=season, as_of_date=as_of_date)
    if not comparison_rows:
        enriched["mvp_probability"] = 0.0
        enriched["golden_glove_probability"] = 0.0
        return enriched

    def _ip_threshold(row: dict[str, Any]) -> float:
        role = str(row.get("role") or "").strip().upper()
        return 10.0 if role == "RP" else 20.0

    qualified = [
        row for row in comparison_rows
        if float(row.get("ip_to_date") or 0) >= _ip_threshold(row)
    ]
    if not qualified:
        enriched["mvp_probability"] = 0.0
        enriched["golden_glove_probability"] = 0.0
        return enriched

    qualified.sort(
        key=lambda row: (
            -float(row.get("predicted_war_final") or 0),
            float(row.get("predicted_era_final") or 99.0),
            float(row.get("predicted_whip_final") or 99.0),
            str(row.get("player_name") or ""),
        )
    )

    player_name = str(enriched.get("player_name") or "").strip()
    team = str(enriched.get("team") or "").strip()
    player_row = next(
        (
            row for row in qualified
            if str(row.get("player_name") or "").strip() == player_name
            and str(row.get("team") or "").strip() == team
        ),
        None,
    )
    if not player_row:
        player_row = next(
            (row for row in qualified if str(row.get("player_name") or "").strip() == player_name),
            None,
        )
    total = len(qualified)
    pred_war = float(enriched.get("predicted_war_final") or (player_row or {}).get("predicted_war_final") or 0)
    pred_era = float(enriched.get("predicted_era_final") or (player_row or {}).get("predicted_era_final") or 0)
    pred_whip = float(enriched.get("predicted_whip_final") or (player_row or {}).get("predicted_whip_final") or 0)
    if player_row:
        rank = next(
            (
                index for index, row in enumerate(qualified, start=1)
                if str(row.get("player_name") or "").strip() == str(player_row.get("player_name") or "").strip()
                and str(row.get("team") or "").strip() == str(player_row.get("team") or "").strip()
            ),
            total,
        )
    else:
        rank = 1 + sum(
            1
            for row in qualified
            if (
                float(row.get("predicted_war_final") or 0),
                -(float(row.get("predicted_era_final") or 99.0)),
                -(float(row.get("predicted_whip_final") or 99.0)),
                str(row.get("player_name") or ""),
            )
            > (
                pred_war,
                -pred_era,
                -pred_whip,
                player_name,
            )
        )
    percentile = 1.0 - ((rank - 1) / max(total, 1))

    role = str(enriched.get("role") or (player_row or {}).get("role") or "").strip().upper()
    pred_ip = float(enriched.get("predicted_ip_final") or 0)
    pred_wins = float(enriched.get("predicted_wins_final") or 0)
    pred_saves = float(enriched.get("predicted_saves_final") or 0)
    pred_holds = float(enriched.get("predicted_holds_final") or 0)

    leader_war = max(float(qualified[0].get("predicted_war_final") or 0), 0.01)
    war_ratio = pred_war / leader_war if leader_war > 0 else 0.0

    leader_era = min(
        float(row.get("predicted_era_final") or 99.0)
        for row in qualified
        if float(row.get("predicted_era_final") or 0) > 0
    ) if any(float(row.get("predicted_era_final") or 0) > 0 for row in qualified) else 0.0
    era_bonus = 0.0
    if pred_era > 0 and leader_era > 0:
        era_bonus = _clamp((leader_era / pred_era), 0.0, 1.15)

    if role == "RP":
        player_save_hold = pred_saves + pred_holds
        leader_save_hold = max(
            max(
                float(row.get("predicted_saves_final") or 0) + float(row.get("predicted_holds_final") or 0)
                for row in qualified
            ),
            1.0,
        )
        leverage_ratio = player_save_hold / leader_save_hold
        mvp_prob = (
            (percentile ** 3.0) * 0.18
            + (_clamp(war_ratio, 0.0, 1.2) * 0.22)
            + (_clamp(leverage_ratio, 0.0, 1.2) * 0.12)
            + (_clamp(era_bonus, 0.0, 1.15) * 0.08)
        )
        gg_prob = (
            (percentile * 0.42)
            + (_clamp(war_ratio, 0.0, 1.2) * 0.20)
            + (_clamp(leverage_ratio, 0.0, 1.2) * 0.18)
            + (_clamp(era_bonus, 0.0, 1.15) * 0.16)
        )
    else:
        leader_ip = max(
            max(float(row.get("predicted_ip_final") or 0) for row in qualified),
            1.0,
        )
        leader_wins = max(
            max(float(row.get("predicted_wins_final") or 0) for row in qualified),
            1.0,
        )
        ip_ratio = pred_ip / leader_ip
        win_ratio = pred_wins / leader_wins
        mvp_prob = (
            (percentile ** 2.6) * 0.28
            + (max(0.0, war_ratio - 0.65) * 0.34)
            + (_clamp(ip_ratio, 0.0, 1.2) * 0.11)
            + (_clamp(win_ratio, 0.0, 1.2) * 0.10)
            + (_clamp(era_bonus, 0.0, 1.15) * 0.10)
        )
        gg_prob = (
            (percentile * 0.44)
            + (_clamp(war_ratio, 0.0, 1.2) * 0.18)
            + (_clamp(ip_ratio, 0.0, 1.2) * 0.12)
            + (_clamp(era_bonus, 0.0, 1.15) * 0.20)
        )

    season_damp = _team_progress(season, team) ** 1.35
    enriched["mvp_probability"] = round(_clamp(mvp_prob * season_damp, 0.0, 0.55), 4)
    enriched["golden_glove_probability"] = round(_clamp(gg_prob * season_damp, 0.0, 0.82), 4)
    return enriched


def _season_progress_min_pa(season: int, team: str = "") -> int:
    max_games = repo.max_team_games(season, team)
    return int(max_games * 3.1)


def _season_progress_min_outs(season: int, team: str = "") -> int:
    max_games = repo.max_team_games(season, team)
    return int(max_games * 3)


def _descending_tens_candidates(base_value: int, floor: int = 0) -> list[int]:
    base_value = max(base_value, floor)
    candidates: list[int] = [base_value]

    rounded = (base_value // 10) * 10
    if rounded >= base_value:
        rounded -= 10

    while rounded >= floor:
        candidates.append(rounded)
        rounded -= 10

    if candidates[-1] != floor:
        candidates.append(floor)

    dedup: list[int] = []
    for candidate in candidates:
        if candidate not in dedup:
            dedup.append(candidate)
    return dedup


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

    dedup = _descending_tens_candidates(base_min_pa, floor=0)

    for candidate in dedup:
        if repo.leaderboard_candidate_count(season=season, min_pa=candidate, team=team) >= min_count:
            return candidate

    return dedup[-1]


def _pick_effective_min_outs_for_leaderboard(
    season: int,
    team: str,
    base_min_outs: int,
    auto_relax: bool,
    min_count: int = 20,
) -> int:
    if not auto_relax:
        return base_min_outs
    if not repo.table_exists("pitcher_season_totals"):
        return base_min_outs

    dedup = _descending_tens_candidates(base_min_outs, floor=0)

    for candidate in dedup:
        if repo.pitcher_leaderboard_candidate_count(season=season, min_outs=candidate, team=team) >= min_count:
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


_KNOWN_TEAM_NAMES = {"KIA", "LG", "KT", "NC", "SSG", "두산", "롯데", "삼성", "키움", "한화"}


def _compact_player_name(player_name: str) -> str:
    parts = [part for part in str(player_name or "").strip().split() if part]
    if len(parts) <= 1:
        return str(player_name or "").strip()
    return parts[-1]


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

    if repo.table_exists("pitcher_season_totals"):
        for name in repo.pitcher_distinct_names(season):
            if _virtual_player_id(name) == pid:
                return name

        for name in repo.pitcher_distinct_names(None):
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
        as_of_date = repo.computed_standings_as_of(season)
        if not as_of_date:
            fallback_season = repo.logs_latest_season_at_or_before(requested_season)
            if fallback_season is None:
                return JsonResponse(
                    {
                        "requested_season": requested_season,
                        "effective_season": None,
                        "as_of_date": None,
                        "mode": "NO_DATA",
                        "rows": [],
                    }
                )
            season = fallback_season
            as_of_date = repo.computed_standings_as_of(season)
            if not as_of_date:
                return JsonResponse(
                    {
                        "requested_season": requested_season,
                        "effective_season": None,
                        "as_of_date": None,
                        "mode": "NO_DATA",
                        "rows": [],
                        "available_seasons": repo.available_seasons(),
                    }
                )

        rows = repo.computed_standings_rows(season)
        return JsonResponse(
            {
                "requested_season": requested_season,
                "effective_season": season,
                "as_of_date": as_of_date,
                "mode": "SEASON_MATCH" if season == requested_season else "PRESEASON_FALLBACK",
                "rows": rows,
                "available_seasons": repo.available_seasons(),
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
            auto_relax=True,
            min_count=5,
        )

        base = repo.home_base_totals(season)
        latest_game = repo.latest_game_date(season) if repo.table_exists("hitter_game_logs") else None
        latest_pred = repo.latest_prediction_date(season) if repo.table_exists("hitter_predictions") else None
        top_avg = repo.top_avg_rows(season, effective_min_pa, 5)
        top_ops = repo.top_ops_rows(season, effective_min_pa, 5)
        top_hr = repo.top_hr_rows(season, effective_min_pa, 5)
        effective_min_outs = _season_progress_min_outs(season)
        top_era = repo.top_era_rows(season, 5, min_outs=max(effective_min_outs, 15))
        top_war = repo.top_combined_war_rows(season, effective_min_pa, effective_min_outs, 5)

        standings_as_of = repo.computed_standings_as_of(season)
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
                "effective_min_ip": round(effective_min_outs / 3.0, 1),
                "standings_preview": {
                    "as_of_date": standings_as_of,
                    "rows": standings_preview,
                },
                "notes": [
                    "standings are derived from Naver-based hitter_game_logs by game result aggregation",
                    "pitcher ERA leaderboard is derived from Naver-based pitcher_game_logs when pitcher_season_totals exists",
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
    player_type = str(request.GET.get("player_type", "hitter")).strip().lower()
    min_pa_raw = request.GET.get("min_pa")
    min_ip_raw = request.GET.get("min_ip")
    team = str(request.GET.get("team", "")).strip()
    limit = _parse_int(request.GET.get("limit"), 20, min_value=1, max_value=200)
    offset = _parse_int(request.GET.get("offset"), 0, min_value=0, max_value=100000)

    if player_type not in {"hitter", "pitcher"}:
        player_type = "hitter"

    if player_type == "pitcher":
        missing = _missing_required_tables(["pitcher_season_totals"])
        if missing:
            return _error_json("missing_table", f"required table missing: {', '.join(missing)}", 503)

        if repo.pitcher_leaderboard_candidate_count(season=season, min_outs=0, team=team) == 0:
            return JsonResponse(
                {
                    "season": requested_season,
                    "requested_season": requested_season,
                    "effective_season": None,
                    "mode": "NO_DATA",
                    "player_type": "pitcher",
                    "metric": metric,
                    "requested_min_ip": 0,
                    "effective_min_ip": 0,
                    "effective_min_outs": 0,
                    "min_ip_policy": "AUTO_BY_SEASON_PROGRESS",
                    "team": team or None,
                    "total": 0,
                    "limit": limit,
                    "offset": offset,
                    "rows": [],
                }
            )

        if min_ip_raw is None or str(min_ip_raw).strip() == "":
            requested_min_outs = _season_progress_min_outs(season, team)
            min_ip_policy = "AUTO_BY_SEASON_PROGRESS"
            min_outs = _pick_effective_min_outs_for_leaderboard(
                season=season,
                team=team,
                base_min_outs=requested_min_outs,
                auto_relax=True,
                min_count=20,
            )
        else:
            requested_min_ip = float(request.GET.get("min_ip") or 0)
            requested_min_outs = int(round(requested_min_ip * 3))
            min_outs = requested_min_outs
            min_ip_policy = "MANUAL"

        allowed_pitcher_metrics = {
            "ERA": "ERA",
            "WHIP": "WHIP",
            "K9": "K9",
            "BB9": "BB9",
            "KBB": "KBB",
            "SO": "SO",
            "W": "W",
            "SV": "SV",
            "HLD": "HLD",
            "IP": "IP",
        }
        order_metric = allowed_pitcher_metrics.get(metric, "ERA")

        try:
            total = repo.pitcher_leaderboard_total(season=season, min_outs=min_outs, team=team)
            rows = repo.pitcher_leaderboard_rows(
                season=season,
                min_outs=min_outs,
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
                    "player_type": "pitcher",
                    "metric": order_metric,
                    "requested_min_ip": round(requested_min_outs / 3.0, 1),
                    "effective_min_ip": round(min_outs / 3.0, 1),
                    "effective_min_outs": min_outs,
                    "min_ip_policy": min_ip_policy,
                    "team": team or None,
                    "total": total,
                    "limit": limit,
                    "offset": offset,
                    "rows": rows,
                }
            )
        except DatabaseError:
            return _error_json("database_error", "failed to load pitcher leaderboard", 500)

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
                "player_type": "hitter",
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
            auto_relax=True,
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
                "player_type": "hitter",
                "metric": order_metric,
                "requested_min_pa": requested_min_pa,
                "effective_min_pa": min_pa,
                "min_pa_policy": min_pa_policy,
                "team": team or None,
                "total": total,
                "limit": limit,
                "offset": offset,
                "rows": rows,
                "available_seasons": repo.available_seasons(),
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
        player_id_map = _preferred_player_ids([str(row.get("player_name") or "") for row in rows])
        for row in rows:
            name = str(row.get("player_name") or "").strip()
            row["player_id"] = player_id_map.get(name, _virtual_player_id(name))
        return JsonResponse({"season": season, "q": q, "team": team or None, "rows": rows})
    except DatabaseError:
        return _error_json("database_error", "failed to search players", 500)


@require_GET
def player_detail(request, player_id: str):
    requested_season = _parse_int(request.GET.get("season"), _default_season(), min_value=1982, max_value=2100)
    season = requested_season
    raw_player_type = str(request.GET.get("player_type", "")).strip().lower()
    player_type = raw_player_type if raw_player_type in {"hitter", "pitcher"} else ""
    pid = player_id.strip()

    target_team = None
    resolved_name = _resolve_player_name_from_id(pid, season)
    if resolved_name:
        name = resolved_name
    elif "_" in pid and not pid.startswith("p_"):
        name_part, team_part = pid.rsplit("_", 1)
        name = name_part
        target_team = team_part if team_part in _KNOWN_TEAM_NAMES else None
    else:
        name = pid

    recent_n = _parse_int(request.GET.get("recent_n"), 10, min_value=1, max_value=60)

    def _build_pitcher_response() -> JsonResponse:
        missing = _missing_required_tables(["pitcher_season_totals"])
        if missing:
            return _error_json("missing_table", f"required table missing: {', '.join(missing)}", 503)

        try:
            pitcher_name = name
            season_rows = repo.pitcher_player_season_rows(pitcher_name, team=target_team)
            if not season_rows:
                compact_name = _compact_player_name(pitcher_name)
                if compact_name and compact_name != pitcher_name:
                    compact_rows = repo.pitcher_player_season_rows(compact_name, team=target_team)
                    if compact_rows:
                        pitcher_name = compact_name
                        season_rows = compact_rows
            if not season_rows:
                return _error_json(
                    "player_not_found",
                    f"player not found: {pitcher_name}",
                    404,
                    {"player_id": pid, "player_name": pitcher_name, "player_type": "pitcher"},
                )

            current_rows = [row for row in season_rows if int(row.get("season") or 0) == season]
            effective_season = season
            if not current_rows and season_rows:
                effective_season = int(season_rows[0].get("season") or season)
                current_rows = [row for row in season_rows if int(row.get("season") or 0) == effective_season]

            monthly = []
            current_agg = None
            latest_prediction = None
            totals_prediction = current_rows[0] if current_rows else (season_rows[0] if season_rows else None)
            prior_pitcher_row = next((row for row in season_rows if int(row.get("season") or 0) < effective_season), None)
            if repo.table_exists("pitcher_game_logs"):
                monthly = repo.pitcher_player_monthly_rows(player_name=pitcher_name, season=effective_season, team=target_team)
                current_agg = repo.pitcher_player_current_aggregate(
                    season=effective_season,
                    player_name=pitcher_name,
                    team=target_team,
                )
                if current_agg and not current_agg.get("latest_game_date"):
                    current_agg = None
                if current_agg is not None:
                    current_agg["player_name"] = pitcher_name
                pace_prediction = _estimate_pitcher_projection(
                    current_agg=current_agg,
                    current_row=totals_prediction,
                    prior_row=prior_pitcher_row,
                    season=effective_season,
                )
                if repo.table_exists("pitcher_predictions"):
                    latest_prediction = repo.pitcher_player_latest_prediction(
                        season=effective_season,
                        player_name=pitcher_name,
                        team=target_team,
                    )
                if latest_prediction is None:
                    latest_prediction = pace_prediction
                elif pace_prediction:
                    merged_prediction = dict(pace_prediction)
                    merged_prediction.update({k: v for k, v in latest_prediction.items() if v is not None})
                    latest_prediction = merged_prediction
            totals_projection = _estimate_pitcher_totals_projection(
                current_row=totals_prediction,
                season=effective_season,
            )
            if latest_prediction is None:
                if totals_projection and pace_prediction:
                    merged_prediction = dict(totals_projection)
                    for key, value in pace_prediction.items():
                        if value is not None and key not in {"as_of_date", "model_source", "confidence_score", "confidence_level"}:
                            merged_prediction[key] = value
                    latest_prediction = merged_prediction
                else:
                    latest_prediction = totals_projection or pace_prediction
            elif totals_projection:
                merged_prediction = dict(totals_projection)
                merged_prediction.update({k: v for k, v in latest_prediction.items() if v is not None})
                latest_prediction = merged_prediction
            if latest_prediction and not latest_prediction.get("player_name"):
                latest_prediction["player_name"] = pitcher_name
            latest_prediction = _estimate_pitcher_awards(
                latest_prediction=latest_prediction,
                season=effective_season,
                )

            teams_in_season = sorted(
                {
                    str(row.get("team") or "").strip()
                    for row in current_rows
                    if str(row.get("team") or "").strip()
                }
            )

            return JsonResponse(
                {
                    "season": effective_season,
                    "requested_season": requested_season,
                    "effective_season": effective_season,
                    "player_type": "pitcher",
                    "player_name": pitcher_name,
                    "player_id": pid,
                    "profile": {
                        "teams_in_season": teams_in_season,
                        "birth_date": None,
                        "bats_throws": None,
                    },
                    "season_aggregate": current_agg,
                    "season_rows": current_rows or season_rows[:1],
                    "season_by_year": season_rows,
                    "monthly_splits": monthly,
                    "latest_prediction": latest_prediction,
                }
            )
        except DatabaseError:
            return _error_json("database_error", "failed to load pitcher detail", 500)

    if player_type == "pitcher":
        return _build_pitcher_response()

    missing = _missing_required_tables(["hitter_season_totals"])
    if missing:
        return _error_json("missing_table", f"required table missing: {', '.join(missing)}", 503)

    try:
        season_rows = repo.player_season_rows(name, team=target_team)
        if not season_rows:
            compact_name = _compact_player_name(name)
            if compact_name and compact_name != name:
                compact_rows = repo.player_season_rows(compact_name, team=target_team)
                if compact_rows:
                    name = compact_name
                    season_rows = compact_rows
        if not season_rows:
            if player_type != "hitter" and repo.table_exists("pitcher_season_totals"):
                return _build_pitcher_response()
            return _error_json(
                "player_not_found",
                f"player not found: {name}",
                404,
                {"player_id": pid, "player_name": name},
            )

        current_rows = [row for row in season_rows if int(row.get("season") or 0) == season]
        effective_season = season
        if not current_rows and season_rows:
            effective_season = int(season_rows[0].get("season") or season)
            current_rows = [row for row in season_rows if int(row.get("season") or 0) == effective_season]

        latest_prediction = None
        if repo.table_exists("hitter_predictions"):
            latest_prediction = repo.player_latest_prediction(season=effective_season, player_name=name, team=target_team)

        trend_rows: list[dict[str, Any]] = []
        if repo.table_exists("hitter_daily_snapshots"):
            trend_rows = repo.player_trend_rows(season=effective_season, player_name=name, team=target_team)

        monthly: list[dict[str, Any]] = []
        vs_team: list[dict[str, Any]] = []
        recent_games: list[dict[str, Any]] = []
        if repo.table_exists("hitter_game_logs"):
            tb_expr = _safe_tb_expr("TB")
            monthly = repo.player_monthly_rows(player_name=name, season=effective_season, tb_expr=tb_expr, team=target_team)
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

            vs_team = repo.player_vs_team_rows(
                player_name=name,
                season=effective_season,
                tb_expr=_safe_tb_expr("TB"),
                team=target_team,
            )
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

            recent_games = repo.player_recent_games_rows(
                player_name=name,
                season=effective_season,
                recent_n=recent_n,
                team=target_team,
            )
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
            season=effective_season,
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
        if current_agg and not current_agg.get("latest_game_date"):
            # player_current_aggregate queries hitter_season_totals which has no
            # game_date column. Inject it from the season-wide latest game date so
            # pace projections have a valid as_of_date reference.
            lgd = repo.logs_latest_game_date(effective_season)
            if lgd:
                current_agg["latest_game_date"] = lgd
            else:
                current_agg = None  # No game data exists at all
        if current_agg is not None:
            current_agg["player_name"] = name
            if target_team:
                current_agg["team"] = target_team
            elif current_rows:
                current_agg["team"] = current_rows[0].get("team")
        prior_row = next((row for row in season_rows if int(row.get("season") or 0) < effective_season), None)
        pace_prediction = _estimate_hitter_pace_projection(
            current_agg=current_agg,
            current_row=current_rows[0] if current_rows else (season_rows[0] if season_rows else None),
            prior_row=prior_row,
            season=effective_season,
        )
        if latest_prediction is None:
            latest_prediction = pace_prediction
        elif pace_prediction:
            merged_prediction = dict(pace_prediction)
            merged_prediction.update({k: v for k, v in latest_prediction.items() if v is not None})
            latest_prediction = merged_prediction
        latest_prediction = _estimate_hitter_projection(
            latest_prediction=latest_prediction,
            current_agg=current_agg,
            current_row=current_rows[0] if current_rows else (season_rows[0] if season_rows else None),
            prior_row=prior_row,
            season=effective_season,
        )

        kbreport_splits: dict[str, list[dict[str, Any]]] = {
            "homeaway": [],
            "pitchside": [],
            "opposite": [],
            "month": [],
        }
        if repo.table_exists("kbreport_hitter_splits"):
            ext_rows = repo.player_kbreport_split_rows(season=effective_season, player_name=name, team=target_team)
            for row in ext_rows:
                group = str(row.get("split_group") or "")
                if group in kbreport_splits:
                    kbreport_splits[group].append(row)

        profile_info: dict[str, Any] | None = None
        if repo.table_exists("statiz_players"):
            profile_info = repo.player_profile_info(name)
            
        return JsonResponse(
            {
                "season": effective_season,
                "requested_season": requested_season,
                "effective_season": effective_season,
                "recent_n": recent_n,
                "player_id": _preferred_player_id(name),
                "player_name": name,
                "profile": {
                    "player_id": _preferred_player_id(name),
                    "player_name": name,
                    "teams_in_season": sorted({r["team"] for r in current_rows}) if current_rows else [],
                    "birth_date": profile_info.get("birth_date") if profile_info else None,
                    "bats_throws": profile_info.get("bats_throws") if profile_info else None,
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
            auto_relax=True,
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
                    "leaders": {"ops_top10": [], "hr_top10": [], "era_top10": [], "k9_top10": []},
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
        effective_min_outs = _season_progress_min_outs(season, name)
        leaders_era = repo.team_leaders_era(season=season, team=name, min_outs=max(effective_min_outs, 3), limit=10)
        leaders_k9 = repo.team_leaders_k9(season=season, team=name, min_outs=max(effective_min_outs, 3), limit=10)

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
                "leaders": {"ops_top10": leaders_ops, "hr_top10": leaders_hr, "era_top10": leaders_era, "k9_top10": leaders_k9},
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

            # Derived status fields for the frontend
            sc = _classify_status(merged["status"], merged["result"])
            merged["status_category"] = sc
            merged["result_state"] = (
                "played"          if merged["result"] is not None
                else "not_played" if sc in ("cancelled", "suspended", "scheduled")
                else "missing_result" if sc == "finished"
                else "not_played"
            )
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


from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST
import json
from .models import ErrorReport

@csrf_exempt
@require_POST
def create_error_report(request):
    try:
        body = json.loads(request.body)
        report = ErrorReport.objects.create(
            page=body.get('page', ''),
            tab=body.get('tab', ''),
            issue_type=body.get('issue_type', ''),
            message=body.get('message', ''),
            reported_url=body.get('reported_url', '')
        )
        return JsonResponse({"status": "success", "id": report.id}, status=201)
    except Exception as e:
        return JsonResponse({"status": "error", "message": str(e)}, status=400)
