"""
orchestrate_daily.py — Hitter Daily E2E Prediction Pipeline
=============================================================

Chains four stages into a single idempotent run:

  Stage 1 [collect]   Update hitter_game_logs from today's KBO results
  Stage 2 [snapshot]  Build/update hitter_daily_snapshots
  Stage 3 [predict]   Run hitter MVP prediction → hitter_predictions DB
  Stage 4 [verify]    Post-run sanity check (row counts / nulls / distribution)

Usage examples
--------------
  # Full daily run (production)
  python -m prediction.orchestrate_daily --season 2025 --as-of-date 2025-09-01

  # Skip data collection (game logs already fresh)
  python -m prediction.orchestrate_daily --season 2025 --as-of-date 2025-09-01 --skip-collect

  # Dry-run: log each stage without writing to DB/files
  python -m prediction.orchestrate_daily --season 2025 --as-of-date 2025-09-01 --dry-run

  # Re-run prediction only (overwrites existing date)
  python -m prediction.orchestrate_daily --season 2025 --as-of-date 2025-09-01 \\
      --skip-collect --skip-snapshot --replace-existing
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
_ROOT = Path(__file__).resolve().parents[1]   # kbo_stat_BE/
_DB_DEFAULT = _ROOT / "kbo_stats.db"

# ---------------------------------------------------------------------------
# Logging helpers
# ---------------------------------------------------------------------------

def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _log(msg: str) -> None:
    print(f"[{_ts()}] {msg}", file=sys.stderr)


def _stage_header(n: int, total: int, name: str) -> None:
    bar = "=" * 60
    _log(f"\n{bar}")
    _log(f"  STAGE {n}/{total}: {name.upper()}")
    _log(f"{bar}")


def _stage_ok(name: str, elapsed: float, **kv: Any) -> None:
    extra = "  ".join(f"{k}={v}" for k, v in kv.items())
    _log(f"  [{name}] OK  elapsed={elapsed:.1f}s  {extra}")


def _stage_fail(name: str, exc: BaseException) -> None:
    _log(f"  [{name}] FAILED: {exc}")


# ---------------------------------------------------------------------------
# Stage 1 — collect
# ---------------------------------------------------------------------------

def stage_collect(season: int, dry_run: bool) -> dict[str, Any]:
    """Run hitter game-log collection for the current season."""
    if dry_run:
        _log("  [collect] DRY-RUN: would call collector.run_range_hitter --auto-start --upsert")
        return {"status": "dry_run"}

    cmd = [sys.executable, "-m", "collector.run_range_hitter", "--auto-start", "--upsert"]
    _log(f"  [collect] {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=_ROOT, capture_output=False)
    if result.returncode != 0:
        raise RuntimeError(f"collector.run_range_hitter exited with code {result.returncode}")
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# Stage 2 — snapshot
# ---------------------------------------------------------------------------

def stage_snapshot(season: int, as_of_date: str, db_path: Path, dry_run: bool) -> dict[str, Any]:
    """Build hitter_daily_snapshots up to as_of_date."""
    # build_hitter_snapshots expects YYYYMMDD format
    as_of_yyyymmdd = as_of_date.replace("-", "")

    if dry_run:
        _log(
            f"  [snapshot] DRY-RUN: would call prediction.build_hitter_snapshots "
            f"--season {season} --as-of {as_of_yyyymmdd} --upsert"
        )
        return {"status": "dry_run"}

    cmd = [
        sys.executable, "-m", "prediction.build_hitter_snapshots",
        "--db", str(db_path),
        "--season", str(season),
        "--as-of", as_of_yyyymmdd,
        "--upsert",
    ]
    _log(f"  [snapshot] {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=_ROOT, capture_output=False)
    if result.returncode != 0:
        raise RuntimeError(f"prediction.build_hitter_snapshots exited with code {result.returncode}")
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# Stage 3 — predict
# ---------------------------------------------------------------------------

def stage_predict(
    season: int,
    as_of_date: str,
    mode: str,
    db_path: Path,
    replace_existing: bool,
    allow_missing_features: bool,
    dry_run: bool,
) -> dict[str, Any]:
    """Run hitter MVP prediction and upsert results to DB."""
    from .mvp_pipeline.config import get_config
    from .mvp_pipeline.db import load_hitter_game_logs
    from .mvp_pipeline.predict import (
        predict_hitter_targets,
        save_predictions,
        upsert_predictions_to_db,
    )

    if dry_run:
        _log(
            f"  [predict] DRY-RUN: would predict season={season} "
            f"as_of={as_of_date} mode={mode}"
        )
        return {"status": "dry_run", "rows": 0, "upserted": 0}

    cfg = get_config()
    data_season = season - 1 if mode == "projection" else season
    game_logs = load_hitter_game_logs(None, data_season)

    _log(f"  [predict] loaded {len(game_logs)} game log rows")
    pred_df = predict_hitter_targets(
        game_logs=game_logs,
        config=cfg,
        as_of_date=as_of_date,
        mode=mode,
        allow_missing_features=allow_missing_features,
    )
    _log(f"  [predict] generated {len(pred_df)} prediction rows")

    # Save CSV artifact
    safe_date = as_of_date.replace("-", "")
    output_path = cfg.prediction_dir / f"hitter_{safe_date}_{mode}.csv"
    save_predictions(pred_df, output_path)
    _log(f"  [predict] saved CSV → {output_path}")

    # Upsert to DB
    upserted = upsert_predictions_to_db(
        pred_df=pred_df,
        db_path=db_path,
        season=season,
        model_season=data_season,
        replace_existing=replace_existing,
        prediction_mode=mode,
    )
    _log(f"  [predict] upserted {upserted} rows → hitter_predictions")

    return {
        "status": "ok",
        "rows_predicted": int(len(pred_df)),
        "rows_upserted": upserted,
        "output_file": str(output_path),
    }


# ---------------------------------------------------------------------------
# Stage 4 — verify
# ---------------------------------------------------------------------------

def stage_verify(season: int, as_of_date: str, mode: str, db_path: Path) -> dict[str, Any]:
    """Sanity-check the freshly written rows in hitter_predictions."""
    if not db_path.exists():
        raise RuntimeError(f"DB not found: {db_path}")

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        # Row count
        row = conn.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM hitter_predictions
            WHERE season = ? AND as_of_date = ? AND prediction_mode = ?
            """,
            (season, as_of_date, mode),
        ).fetchone()
        row_count = int(row["cnt"] if row else 0)
        _log(f"  [verify] row_count={row_count} (season={season} as_of={as_of_date} mode={mode})")

        if row_count == 0:
            raise RuntimeError(
                f"No rows found in hitter_predictions for "
                f"season={season} as_of_date={as_of_date} mode={mode}. "
                "Prediction stage may have failed silently."
            )

        # Null check on key columns
        null_row = conn.execute(
            """
            SELECT
                SUM(CASE WHEN predicted_ops_final IS NULL THEN 1 ELSE 0 END) AS null_ops,
                SUM(CASE WHEN predicted_hr_final  IS NULL THEN 1 ELSE 0 END) AS null_hr,
                SUM(CASE WHEN player_name         IS NULL THEN 1 ELSE 0 END) AS null_name
            FROM hitter_predictions
            WHERE season = ? AND as_of_date = ? AND prediction_mode = ?
            """,
            (season, as_of_date, mode),
        ).fetchone()
        null_ops  = int(null_row["null_ops"]  or 0)
        null_hr   = int(null_row["null_hr"]   or 0)
        null_name = int(null_row["null_name"] or 0)

        if null_ops or null_hr or null_name:
            _log(
                f"  [verify] WARNING: nulls detected — "
                f"predicted_ops_final={null_ops} predicted_hr_final={null_hr} player_name={null_name}"
            )
        else:
            _log("  [verify] null check PASSED — no nulls in key columns")

        # Confidence distribution
        conf_rows = conn.execute(
            """
            SELECT confidence_level, COUNT(*) AS cnt
            FROM hitter_predictions
            WHERE season = ? AND as_of_date = ? AND prediction_mode = ?
            GROUP BY confidence_level
            ORDER BY confidence_level
            """,
            (season, as_of_date, mode),
        ).fetchall()
        conf_dist = {r["confidence_level"]: int(r["cnt"]) for r in conf_rows}
        _log(f"  [verify] confidence_distribution={conf_dist}")

        return {
            "status": "ok",
            "row_count": row_count,
            "null_ops": null_ops,
            "null_hr": null_hr,
            "null_name": null_name,
            "confidence_distribution": conf_dist,
        }
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def stage_validate(
    season: int,
    as_of_date: str,
    mode: str,
    db_path: Path,
) -> dict[str, Any]:
    """Run validate.check_quality and archive the result.

    Validation failures emit WARN log but do NOT abort the pipeline —
    data was already written and callers should decide whether to alert.
    """
    from .validate import check_quality
    result = check_quality(season, as_of_date, mode, db_path)
    severity = "WARN" if not result.passed else "OK"
    fails = [c.name for c in result.checks if c.severity == "FAIL"]
    warns = [c.name for c in result.checks if c.severity == "WARN"]
    if fails:
        _log(f"  [validate] {severity}: FAIL checks={fails}  WARN checks={warns}")
    elif warns:
        _log(f"  [validate] {severity}: WARN checks={warns}")
    else:
        _log("  [validate] OK: all quality checks passed")
    return {
        "status": severity.lower(),
        "passed": result.passed,
        "has_warnings": result.has_warnings,
        "fail_checks": fails,
        "warn_checks": warns,
    }


def run_pipeline(args: argparse.Namespace) -> dict[str, Any]:
    db_path = Path(args.db_path) if args.db_path else _DB_DEFAULT
    season  = args.season
    as_of   = args.as_of_date
    mode    = args.mode

    total_stages = 5 if args.run_validation else 4
    stage_results: dict[str, Any] = {}
    pipeline_start = time.monotonic()

    def _run_stage(n: int, name: str, fn, *fargs, **fkwargs) -> dict[str, Any]:
        _stage_header(n, total_stages, name)
        t0 = time.monotonic()
        try:
            result = fn(*fargs, **fkwargs)
            elapsed = time.monotonic() - t0
            _stage_ok(name, elapsed, **{k: v for k, v in result.items() if k != "status"})
            stage_results[name] = result
            return result
        except Exception as exc:  # noqa: BLE001
            _stage_fail(name, exc)
            stage_results[name] = {"status": "error", "message": str(exc)}
            raise

    try:
        if not args.skip_collect:
            _run_stage(1, "collect", stage_collect, season, args.dry_run)
        else:
            _log("\n[STAGE 1/4: COLLECT — SKIPPED via --skip-collect]")
            stage_results["collect"] = {"status": "skipped"}

        if not args.skip_snapshot:
            _run_stage(2, "snapshot", stage_snapshot, season, as_of, db_path, args.dry_run)
        else:
            _log("\n[STAGE 2/4: SNAPSHOT — SKIPPED via --skip-snapshot]")
            stage_results["snapshot"] = {"status": "skipped"}

        _run_stage(
            3, "predict", stage_predict,
            season, as_of, mode, db_path,
            args.replace_existing, args.allow_missing_features, args.dry_run,
        )

        if not args.skip_verify and not args.dry_run:
            _run_stage(4, "verify", stage_verify, season, as_of, mode, db_path)
        else:
            reason = "--dry-run" if args.dry_run else "--skip-verify"
            _log(f"\n[STAGE 4/{total_stages}: VERIFY — SKIPPED via {reason}]")
            stage_results["verify"] = {"status": "skipped"}

        if args.run_validation and not args.dry_run:
            _stage_header(5, total_stages, "validate (quality)")
            t0 = time.monotonic()
            try:
                val_result = stage_validate(season, as_of, mode, db_path)
                elapsed = time.monotonic() - t0
                _stage_ok("validate", elapsed)
                stage_results["validate"] = val_result
            except Exception as exc:  # noqa: BLE001
                _log(f"  [validate] WARNING exception: {exc} (pipeline not aborted)")
                stage_results["validate"] = {"status": "warn", "message": str(exc)}
        elif args.run_validation and args.dry_run:
            _log(f"\n[STAGE 5/{total_stages}: VALIDATE — SKIPPED via --dry-run]")
            stage_results["validate"] = {"status": "skipped"}

        status = "success"
    except Exception as exc:  # noqa: BLE001
        status = "error"
        failed_stage = next(
            (k for k, v in stage_results.items() if v.get("status") == "error"), "unknown"
        )
        _log(f"\n[PIPELINE FAILED] at stage={failed_stage}: {exc}")

    total_elapsed = time.monotonic() - pipeline_start
    summary = {
        "status": status,
        "season": season,
        "as_of_date": as_of,
        "mode": mode,
        "dry_run": args.dry_run,
        "duration_sec": round(total_elapsed, 1),
        "stages": stage_results,
    }
    return summary


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Hitter daily E2E prediction pipeline: collect → snapshot → predict → verify",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--season", type=int, required=True, help="Target season year (e.g. 2025).")
    parser.add_argument("--as-of-date", required=True, help="Pipeline cutoff date YYYY-MM-DD.")
    parser.add_argument(
        "--mode",
        choices=["prediction", "projection"],
        default="prediction",
        help="Prediction mode (default: prediction).",
    )
    parser.add_argument("--db-path", default=None, help=f"SQLite DB path (default: {_DB_DEFAULT}).")
    parser.add_argument("--replace-existing", action="store_true", help="Overwrite existing prediction rows for this date.")
    parser.add_argument("--allow-missing-features", action="store_true", help="Fill missing schema features with 0 instead of failing.")
    parser.add_argument("--skip-collect",  action="store_true", help="Skip Stage 1 (game-log collection).")
    parser.add_argument("--skip-snapshot", action="store_true", help="Skip Stage 2 (snapshot build).")
    parser.add_argument("--skip-verify",   action="store_true", help="Skip Stage 4 (post-run sanity check).")
    parser.add_argument(
        "--run-validation",
        action="store_true",
        help="Run Stage 5: validate.check_quality after write. Failures are WARN (do not abort).",
    )
    parser.add_argument("--dry-run", action="store_true", help="Log each stage without writing to DB or files.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    _log(f"orchestrate_daily starting: season={args.season} as_of={args.as_of_date} mode={args.mode} dry_run={args.dry_run}")

    summary = run_pipeline(args)

    # Emit JSON summary to stdout for downstream capture
    print(json.dumps(summary, ensure_ascii=False, indent=2))

    if summary["status"] != "success":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
