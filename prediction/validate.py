"""
validate.py — Hitter Prediction Validation Suite
=================================================

Three independent check categories:

  1. check_quality  — Daily data-quality checks on hitter_predictions DB rows
  2. check_backtest — Historical MAE/RMSE evaluation compared to a baseline
  3. check_regression — Smoke-test that load → predict → schema produces no crash

Each check returns a :class:`CheckResult`.

Usage (standalone):
    python -m prediction.validate quality --season 2025 --as-of-date 2025-09-01
    python -m prediction.validate backtest --season 2025 --dates 2025-06-01 2025-09-01
    python -m prediction.validate regression --season 2025
    python -m prediction.validate all --season 2025 --as-of-date 2025-09-01
"""
from __future__ import annotations

import argparse
import json
import sys
import tempfile
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error, mean_squared_error

from db_support import connect_for_path, execute, row_value
from .mvp_pipeline.backtest import evaluate_predictions
from .mvp_pipeline.config import get_config
from .mvp_pipeline.db import load_hitter_game_logs
from .mvp_pipeline.predict import predict_hitter_targets

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
_ROOT = Path(__file__).resolve().parent.parent     # kbo_stat_BE/
_DB_DEFAULT = _ROOT / "kbo_stats.db"
_VALIDATION_DIR = _ROOT / "artifacts" / "validation"
_BASELINE_FILE = _VALIDATION_DIR / "validation_baseline.json"

Severity = Literal["OK", "WARN", "FAIL"]


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class CheckItem:
    name: str
    severity: Severity          # "OK" | "WARN" | "FAIL"
    message: str
    value: Any = None


@dataclass
class CheckResult:
    category: str               # "quality" | "backtest" | "regression"
    passed: bool                # True if no FAIL-level items
    has_warnings: bool          # True if at least one WARN
    checks: list[CheckItem] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "category": self.category,
            "passed": self.passed,
            "has_warnings": self.has_warnings,
            "metadata": self.metadata,
            "checks": [
                {
                    "name": c.name,
                    "severity": c.severity,
                    "message": c.message,
                    "value": c.value,
                }
                for c in self.checks
            ],
        }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _log(msg: str) -> None:
    print(f"[{_ts()}] {msg}", file=sys.stderr)


def _severity_icon(s: Severity) -> str:
    return {"OK": "✓", "WARN": "⚠", "FAIL": "✗"}.get(s, "?")


def _print_result(result: CheckResult) -> None:
    _log(f"\n  [{result.category.upper()}] {'PASSED' if result.passed else 'FAILED'}")
    for c in result.checks:
        _log(f"    {_severity_icon(c.severity)} [{c.severity}] {c.name}: {c.message}")


def _ensure_validation_dir() -> Path:
    _VALIDATION_DIR.mkdir(parents=True, exist_ok=True)
    return _VALIDATION_DIR


def _load_baseline() -> dict[str, Any]:
    if _BASELINE_FILE.exists():
        return json.loads(_BASELINE_FILE.read_text(encoding="utf-8"))
    return {}


def _save_archive(name: str, data: dict[str, Any]) -> Path:
    _ensure_validation_dir()
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    path = _VALIDATION_DIR / f"{date_str}_{name}_report.json"
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# 1. check_quality — Daily quality checks on hitter_predictions
# ---------------------------------------------------------------------------

VALID_CONFIDENCE_LEVELS = {"LOW", "MEDIUM", "HIGH"}
OPS_MIN, OPS_MAX = 0.0, 3.0
HR_MIN = 0.0
BLEND_MIN, BLEND_MAX = 0.0, 1.0


def check_quality(
    season: int,
    as_of_date: str,
    mode: str = "prediction",
    db_path: Path | None = None,
) -> CheckResult:
    """Check data quality of hitter_predictions for a given season + as_of_date."""
    db = db_path or _DB_DEFAULT
    checks: list[CheckItem] = []

    if not db.exists():
        checks.append(CheckItem("db_exists", "FAIL", f"DB not found: {db}"))
        return CheckResult("quality", False, False, checks)

    conn = connect_for_path(db)
    try:
        # ── 1. Row count ────────────────────────────────────────────────────
        row = execute(
            conn,
            "SELECT COUNT(*) AS cnt FROM hitter_predictions WHERE season=? AND as_of_date=? AND prediction_mode=?",
            [season, as_of_date, mode],
        ).fetchone()
        cnt = int(row_value(row, "cnt", 0) or 0)
        if cnt == 0:
            checks.append(CheckItem("row_count", "FAIL", f"0 rows for season={season} as_of={as_of_date} mode={mode}", cnt))
            return CheckResult("quality", False, False, checks)
        checks.append(CheckItem("row_count", "OK", f"{cnt} rows found", cnt))

        # ── 2. NULL checks ──────────────────────────────────────────────────
        null_row = execute(
            conn,
            """
            SELECT
                SUM(CASE WHEN predicted_ops_final IS NULL THEN 1 ELSE 0 END) AS null_ops,
                SUM(CASE WHEN predicted_hr_final  IS NULL THEN 1 ELSE 0 END) AS null_hr,
                SUM(CASE WHEN predicted_war_final IS NULL THEN 1 ELSE 0 END) AS null_war,
                SUM(CASE WHEN player_name         IS NULL THEN 1 ELSE 0 END) AS null_name,
                SUM(CASE WHEN confidence_level    IS NULL THEN 1 ELSE 0 END) AS null_conf
            FROM hitter_predictions
            WHERE season=? AND as_of_date=? AND prediction_mode=?
            """,
            [season, as_of_date, mode],
        ).fetchone()
        for col, key in [("predicted_ops_final", "null_ops"), ("predicted_hr_final", "null_hr"),
                         ("predicted_war_final", "null_war"), ("player_name", "null_name"),
                         ("confidence_level", "null_conf")]:
            n = int(row_value(null_row, key, 0) or 0)
            sev: Severity = "FAIL" if n > 0 else "OK"
            checks.append(CheckItem(f"null_{col}", sev, f"{n} NULL in {col}", n))

        # ── 3. OPS range ────────────────────────────────────────────────────
        range_row = execute(
            conn,
            """
            SELECT
                SUM(CASE WHEN predicted_ops_final < ? OR predicted_ops_final > ? THEN 1 ELSE 0 END) AS bad_ops,
                SUM(CASE WHEN predicted_hr_final  < ? THEN 1 ELSE 0 END) AS neg_hr,
                SUM(CASE WHEN blend_weight < ? OR blend_weight > ? THEN 1 ELSE 0 END) AS bad_blend
            FROM hitter_predictions
            WHERE season=? AND as_of_date=? AND prediction_mode=?
            """,
            [OPS_MIN, OPS_MAX, HR_MIN, BLEND_MIN, BLEND_MAX, season, as_of_date, mode],
        ).fetchone()
        bad_ops  = int(row_value(range_row, "bad_ops", 0) or 0)
        neg_hr   = int(row_value(range_row, "neg_hr", 0) or 0)
        bad_blend = int(row_value(range_row, "bad_blend", 0) or 0)
        checks.append(CheckItem("ops_range",   "FAIL" if bad_ops   > 0 else "OK", f"{bad_ops} OPS outside [{OPS_MIN},{OPS_MAX}]",   bad_ops))
        checks.append(CheckItem("hr_negative", "WARN" if neg_hr    > 0 else "OK", f"{neg_hr} negative HR predictions",               neg_hr))
        checks.append(CheckItem("blend_range", "WARN" if bad_blend > 0 else "OK", f"{bad_blend} blend_weight outside [0,1]",         bad_blend))

        # ── 4. confidence_level values ──────────────────────────────────────
        conf_rows = execute(
            conn,
            "SELECT DISTINCT confidence_level FROM hitter_predictions WHERE season=? AND as_of_date=? AND prediction_mode=?",
            [season, as_of_date, mode],
        ).fetchall()
        bad_conf_vals = [
            row_value(r, "confidence_level", None)
            for r in conf_rows
            if row_value(r, "confidence_level", None) not in VALID_CONFIDENCE_LEVELS
        ]
        checks.append(CheckItem("confidence_values",
                                "FAIL" if bad_conf_vals else "OK",
                                f"invalid confidence_level values: {bad_conf_vals}" if bad_conf_vals else "all values in {LOW,MEDIUM,HIGH}",
                                bad_conf_vals))

        # ── 5. model_version uniqueness ─────────────────────────────────────
        mv_rows = execute(
            conn,
            "SELECT DISTINCT model_version FROM hitter_predictions WHERE season=? AND as_of_date=? AND prediction_mode=?",
            [season, as_of_date, mode],
        ).fetchall()
        mv_list = [row_value(r, "model_version", "") for r in mv_rows]
        checks.append(CheckItem("model_version_unique",
                                "WARN" if len(mv_list) > 1 else "OK",
                                f"multiple model_versions in same batch: {mv_list}" if len(mv_list) > 1 else f"single version: {mv_list}",
                                mv_list))

        # ── 6. as_of_date format ────────────────────────────────────────────
        bad_date_row = execute(
            conn,
            """
            SELECT COUNT(*) AS cnt FROM hitter_predictions
            WHERE season=? AND as_of_date=? AND prediction_mode=?
              AND (LENGTH(as_of_date) != 10 OR as_of_date NOT LIKE '____-__-__')
            """,
            [season, as_of_date, mode],
        ).fetchone()
        bad_dates = int(row_value(bad_date_row, "cnt", 0) or 0)
        checks.append(CheckItem("as_of_date_format", "FAIL" if bad_dates > 0 else "OK",
                                f"{bad_dates} rows with non-YYYY-MM-DD as_of_date", bad_dates))

    finally:
        conn.close()

    passed = all(c.severity != "FAIL" for c in checks)
    has_warnings = any(c.severity == "WARN" for c in checks)
    return CheckResult("quality", passed, has_warnings, checks,
                       metadata={"season": season, "as_of_date": as_of_date, "mode": mode, "row_count": cnt})


# ---------------------------------------------------------------------------
# 2. check_backtest — Historical performance vs. baseline
# ---------------------------------------------------------------------------

def check_backtest(
    season: int,
    as_of_dates: list[str],
    db_path: Path | None = None,
    archive: bool = True,
) -> CheckResult:
    """Run backtest across as_of_dates and compare MAE/RMSE against baseline thresholds."""
    baseline = _load_baseline()
    thresholds = baseline.get("thresholds", {})
    game_logs = load_hitter_game_logs(None, season)
    checks: list[CheckItem] = []
    backtest_rows: list[dict] = []

    for as_of in sorted(as_of_dates):
        try:
            pred_df = predict_hitter_targets(game_logs=game_logs, as_of_date=as_of)
            metrics = evaluate_predictions(pred_df, game_logs)
            metrics["as_of_date"] = as_of
            backtest_rows.append(metrics)
        except Exception as exc:  # noqa: BLE001
            checks.append(CheckItem(f"backtest_{as_of}", "FAIL", f"Exception during prediction: {exc}", None))
            continue

        # Compare each metric against threshold
        for metric, value in metrics.items():
            if metric in ("as_of_date", "n_players"):
                continue
            warn_t = thresholds.get(metric, {}).get("warn")
            fail_t = thresholds.get(metric, {}).get("fail")
            sev: Severity = "OK"
            note = f"{value:.4f}"
            if fail_t is not None and value > fail_t:
                sev = "FAIL"
                note = f"{value:.4f} > FAIL threshold {fail_t}"
            elif warn_t is not None and value > warn_t:
                sev = "WARN"
                note = f"{value:.4f} > WARN threshold {warn_t}"
            checks.append(CheckItem(f"{as_of}/{metric}", sev, note, round(value, 4)))

    # Archive
    report_data: dict[str, Any] = {
        "generated_at": _ts(),
        "season": season,
        "as_of_dates": as_of_dates,
        "baseline_thresholds": thresholds,
        "results": backtest_rows,
        "checks": [asdict(c) for c in checks],
    }
    if archive:
        path = _save_archive("backtest", report_data)
        _log(f"  [backtest] archive saved → {path}")

    passed = all(c.severity != "FAIL" for c in checks)
    has_warnings = any(c.severity == "WARN" for c in checks)
    return CheckResult("backtest", passed, has_warnings, checks,
                       metadata={"season": season, "as_of_dates": as_of_dates, "n_dates": len(as_of_dates)})


# ---------------------------------------------------------------------------
# 3. check_regression — Smoke test: load → predict → output schema
# ---------------------------------------------------------------------------

REQUIRED_PRED_COLS = {
    "player_key", "player_name", "team", "as_of_date",
    "blended_ops_final", "blended_hr_final", "blended_war_final",
    "ops_weight", "prediction_mode",
}


def check_regression(
    season: int,
    db_path: Path | None = None,
) -> CheckResult:
    """Smoke-test: load game logs → predict → verify output shape and schema.

    Does NOT write to any DB.  Uses the last 500 rows of game_logs as a mini
    dataset to keep runtime fast (< 10 s typically).
    """
    checks: list[CheckItem] = []

    # ── Step 1: load game logs ──────────────────────────────────────────────
    try:
        game_logs = load_hitter_game_logs(None, season)
        if len(game_logs) == 0:
            checks.append(CheckItem("load_game_logs", "FAIL", f"0 game log rows for season={season}", 0))
            return CheckResult("regression", False, False, checks)
        checks.append(CheckItem("load_game_logs", "OK", f"loaded {len(game_logs)} rows", len(game_logs)))
    except Exception as exc:  # noqa: BLE001
        checks.append(CheckItem("load_game_logs", "FAIL", str(exc)))
        return CheckResult("regression", False, False, checks)

    # ── Step 2: run predict (latest snapshot, no DB write) ─────────────────
    try:
        pred_df = predict_hitter_targets(game_logs=game_logs)
        checks.append(CheckItem("predict_runs", "OK", f"predict returned {len(pred_df)} rows", len(pred_df)))
    except Exception as exc:  # noqa: BLE001
        checks.append(CheckItem("predict_runs", "FAIL", str(exc)))
        return CheckResult("regression", False, False, checks)

    if len(pred_df) == 0:
        checks.append(CheckItem("predict_nonempty", "FAIL", "predict returned 0 rows", 0))
        return CheckResult("regression", False, False, checks)
    checks.append(CheckItem("predict_nonempty", "OK", f"{len(pred_df)} players", len(pred_df)))

    # ── Step 3: output schema check ─────────────────────────────────────────
    missing_cols = REQUIRED_PRED_COLS - set(pred_df.columns)
    if missing_cols:
        checks.append(CheckItem("output_schema", "FAIL", f"missing columns: {sorted(missing_cols)}", list(missing_cols)))
    else:
        checks.append(CheckItem("output_schema", "OK", f"all {len(REQUIRED_PRED_COLS)} required columns present"))

    # ── Step 4: no nulls in blended outputs ────────────────────────────────
    for col in ["blended_ops_final", "blended_hr_final", "blended_war_final"]:
        if col in pred_df.columns:
            n_null = int(pred_df[col].isna().sum())
            sev = "FAIL" if n_null > 0 else "OK"
            checks.append(CheckItem(f"null_{col}", sev, f"{n_null} NULLs in {col}", n_null))

    # ── Step 5: schema.json loadable ────────────────────────────────────────
    try:
        from .mvp_pipeline.schema import ModelSchema
        cfg = get_config()
        schema_path = cfg.model_dir / "schema.json"
        if schema_path.exists():
            schema = ModelSchema.load(schema_path)
            checks.append(CheckItem("schema_loadable", "OK",
                                    f"schema v{schema.schema_version} model_version={schema.model_version!r}"))
        else:
            checks.append(CheckItem("schema_loadable", "WARN",
                                    f"schema.json not found at {schema_path}. Run train first."))
    except Exception as exc:  # noqa: BLE001
        checks.append(CheckItem("schema_loadable", "WARN", f"schema load warning: {exc}"))

    passed = all(c.severity != "FAIL" for c in checks)
    has_warnings = any(c.severity == "WARN" for c in checks)
    return CheckResult("regression", passed, has_warnings, checks,
                       metadata={"season": season, "n_players": int(len(pred_df))})


# ---------------------------------------------------------------------------
# 4. validate_all — Run all checks + save JSON report
# ---------------------------------------------------------------------------

def validate_all(
    season: int,
    as_of_date: str,
    mode: str = "prediction",
    backtest_dates: list[str] | None = None,
    db_path: Path | None = None,
    skip_backtest: bool = False,
    skip_quality: bool = False,
    skip_regression: bool = False,
) -> dict[str, Any]:
    """Run all enabled checks and save a combined JSON report."""
    results: dict[str, CheckResult] = {}
    started_at = _ts()
    _log("=== validate_all start ===")

    if not skip_quality:
        _log("[quality] running...")
        r = check_quality(season, as_of_date, mode, db_path)
        _print_result(r)
        results["quality"] = r

    if not skip_backtest and backtest_dates:
        _log("[backtest] running...")
        r = check_backtest(season, backtest_dates, db_path)
        _print_result(r)
        results["backtest"] = r

    if not skip_regression:
        _log("[regression] running...")
        r = check_regression(season, db_path)
        _print_result(r)
        results["regression"] = r

    overall_passed = all(r.passed for r in results.values())
    report = {
        "generated_at": started_at,
        "season": season,
        "as_of_date": as_of_date,
        "mode": mode,
        "overall_passed": overall_passed,
        "results": {k: v.to_dict() for k, v in results.items()},
    }

    # Save combined report
    path = _save_archive(f"{as_of_date}_all", report)
    _log(f"  [validate_all] report saved → {path}")
    return report


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _common_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--season", type=int, required=True, help="Season year.")
    parser.add_argument("--db-path", default=None, help="SQLite DB path.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Hitter prediction validation suite",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # quality
    p_quality = sub.add_parser("quality", help="Daily quality check on hitter_predictions.")
    _common_args(p_quality)
    p_quality.add_argument("--as-of-date", required=True, help="YYYY-MM-DD")
    p_quality.add_argument("--mode", default="prediction", choices=["prediction", "projection"])

    # backtest
    p_backtest = sub.add_parser("backtest", help="Historical MAE/RMSE vs. baseline.")
    _common_args(p_backtest)
    p_backtest.add_argument("--dates", nargs="+", required=True, help="YYYY-MM-DD as-of dates.")
    p_backtest.add_argument("--no-archive", action="store_true", help="Skip saving archive file.")

    # regression
    p_reg = sub.add_parser("regression", help="Smoke-test predict pipeline.")
    _common_args(p_reg)

    # all
    p_all = sub.add_parser("all", help="Run all checks.")
    _common_args(p_all)
    p_all.add_argument("--as-of-date", required=True, help="YYYY-MM-DD")
    p_all.add_argument("--mode", default="prediction", choices=["prediction", "projection"])
    p_all.add_argument("--backtest-dates", nargs="*", help="Extra as-of dates for backtest.")
    p_all.add_argument("--skip-backtest",  action="store_true")
    p_all.add_argument("--skip-quality",   action="store_true")
    p_all.add_argument("--skip-regression", action="store_true")

    return parser.parse_args()


def main() -> None:
    args = parse_args()
    db_path = Path(args.db_path) if args.db_path else None

    if args.command == "quality":
        result = check_quality(args.season, args.as_of_date, args.mode, db_path)
        _print_result(result)
        print(json.dumps(result.to_dict(), ensure_ascii=False, indent=2))
        if not result.passed:
            raise SystemExit(1)

    elif args.command == "backtest":
        result = check_backtest(args.season, args.dates, db_path, archive=not args.no_archive)
        _print_result(result)
        print(json.dumps(result.to_dict(), ensure_ascii=False, indent=2))
        if not result.passed:
            raise SystemExit(1)

    elif args.command == "regression":
        result = check_regression(args.season, db_path)
        _print_result(result)
        print(json.dumps(result.to_dict(), ensure_ascii=False, indent=2))
        if not result.passed:
            raise SystemExit(1)

    elif args.command == "all":
        report = validate_all(
            season=args.season,
            as_of_date=args.as_of_date,
            mode=args.mode,
            backtest_dates=args.backtest_dates,
            db_path=db_path,
            skip_backtest=args.skip_backtest,
            skip_quality=args.skip_quality,
            skip_regression=args.skip_regression,
        )
        print(json.dumps(report, ensure_ascii=False, indent=2))
        if not report["overall_passed"]:
            raise SystemExit(1)


if __name__ == "__main__":
    main()
