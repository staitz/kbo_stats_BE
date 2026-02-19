from django.db import connection
from django.http import JsonResponse
from django.views.decorators.http import require_GET


def _query_all(sql: str, params: tuple = ()) -> list[dict]:
    with connection.cursor() as cursor:
        cursor.execute(sql, params)
        cols = [c[0] for c in cursor.description]
        rows = cursor.fetchall()
    return [dict(zip(cols, row)) for row in rows]


@require_GET
def health(_request):
    return JsonResponse({"status": "ok"})


@require_GET
def leaderboard(request):
    season = int(request.GET.get("season", "2025"))
    metric = str(request.GET.get("metric", "OPS")).upper()
    limit = max(1, min(int(request.GET.get("limit", "20")), 100))
    min_pa = max(0, int(request.GET.get("min_pa", "100")))
    allowed_metrics = {"OPS", "HR", "AVG"}
    order_metric = metric if metric in allowed_metrics else "OPS"
    rows = _query_all(
        f"""
        SELECT team, player_name, PA, AB, H, HR, AVG, OPS
        FROM hitter_season_totals
        WHERE season = ? AND PA >= ?
        ORDER BY {order_metric} DESC, PA DESC
        LIMIT ?
        """,
        (season, min_pa, limit),
    )
    return JsonResponse(
        {
            "season": season,
            "metric": order_metric,
            "min_pa": min_pa,
            "rows": rows,
        }
    )


@require_GET
def predictions_latest(request):
    season = int(request.GET.get("season", "2025"))
    latest_rows = _query_all(
        """
        SELECT MAX(as_of_date) AS latest_date
        FROM hitter_predictions
        WHERE season = ?
        """,
        (season,),
    )
    latest_date = latest_rows[0]["latest_date"] if latest_rows else None
    if not latest_date:
        return JsonResponse({"season": season, "latest_date": None, "rows": []})
    rows = _query_all(
        """
        SELECT team, player_name, predicted_hr_final, predicted_ops_final,
               confidence_level, pa_to_date, blend_weight, model_source
        FROM hitter_predictions
        WHERE season = ? AND as_of_date = ?
        ORDER BY predicted_ops_final DESC
        LIMIT 100
        """,
        (season, latest_date),
    )
    return JsonResponse({"season": season, "latest_date": latest_date, "rows": rows})


@require_GET
def player_search(request):
    q = str(request.GET.get("q", "")).strip()
    season = int(request.GET.get("season", "2025"))
    if not q:
        return JsonResponse({"season": season, "q": q, "rows": []})
    rows = _query_all(
        """
        SELECT team, player_name, PA, AB, H, HR, OPS
        FROM hitter_season_totals
        WHERE season = ? AND player_name LIKE ?
        ORDER BY OPS DESC, PA DESC
        LIMIT 30
        """,
        (season, f"%{q}%"),
    )
    return JsonResponse({"season": season, "q": q, "rows": rows})
