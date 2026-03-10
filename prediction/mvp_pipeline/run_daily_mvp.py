from __future__ import annotations

import argparse
from pathlib import Path

from .config import get_config
from .db import load_hitter_game_logs
from .predict import (
    MODE_PREDICTION,
    MODE_PROJECTION,
    predict_hitter_targets,
    save_predictions,
    upsert_predictions_to_db,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run daily hitter MVP prediction pipeline.")
    parser.add_argument("--season", type=int, default=2025, help="Season to predict.")
    parser.add_argument("--as-of-date", required=True, help="Prediction cutoff date YYYY-MM-DD.")
    parser.add_argument("--input", type=str, default=None, help="CSV, parquet, or sqlite DB path. If omitted, project DB is used.")
    parser.add_argument("--model-dir", type=str, default=None, help="Directory containing trained models.")
    parser.add_argument("--db-path", type=str, default=None, help="SQLite DB path for upsert. Defaults to project DB.")
    parser.add_argument("--skip-db", action="store_true", help="Skip DB upsert.")
    parser.add_argument("--skip-file", action="store_true", help="Skip CSV/parquet file output.")
    parser.add_argument("--output", type=str, default=None, help="Optional output file path. Defaults to artifacts/predictions/hitter_<date>.csv")
    parser.add_argument("--replace-existing", action="store_true", help="Replace existing rows for the same season/model_version/as_of_date/mode.")
    parser.add_argument(
        "--mode",
        choices=[MODE_PROJECTION, MODE_PREDICTION],
        default=MODE_PREDICTION,
        help=(
            "Prediction mode. "
            '"prediction" (default): in-season, PA-weighted blend. '
            '"projection": pre-season, projection-only (blend_weight forced to 0).'
        ),
    )
    parser.add_argument(
        "--allow-missing-features",
        action="store_true",
        default=False,
        help=(
            "(Schema relaxation) If set, missing schema features are filled with 0. "
            "Default is strict mode (raises SchemaValidationError on any missing feature). "
            "Never use in production."
        ),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    cfg = get_config()

    try:
        data_season = args.season - 1 if args.mode == MODE_PROJECTION else args.season
        game_logs = load_hitter_game_logs(args.input, data_season)
        pred_df = predict_hitter_targets(
            game_logs=game_logs,
            config=cfg,
            model_dir=args.model_dir,
            as_of_date=args.as_of_date,
            mode=args.mode,
            allow_missing_features=args.allow_missing_features,
        )

        output_path = None
        if not args.skip_file:
            output_path = args.output
            if output_path is None:
                safe_date = args.as_of_date.replace("-", "")
                output_path = cfg.prediction_dir / f"hitter_{safe_date}_{args.mode}.csv"
            save_predictions(pred_df, output_path)

        upserted = 0
        if not args.skip_db:
            db_path = args.db_path or (Path(__file__).resolve().parents[2] / "kbo_stats.db")
            model_season = data_season
            upserted = upsert_predictions_to_db(
                pred_df=pred_df,
                db_path=db_path,
                season=args.season,
                model_season=model_season,
                replace_existing=args.replace_existing,
                prediction_mode=args.mode,
            )

        print(
            {
                "status": "success",
                "mode": args.mode,
                "allow_missing_features": args.allow_missing_features,
                "season": args.season,
                "as_of_date": args.as_of_date,
                "rows": int(len(pred_df)),
                "file_output": str(output_path) if output_path else None,
                "db_upserted": upserted,
                "replace_existing": bool(args.replace_existing),
            }
        )
    except Exception as exc:  # noqa: BLE001
        print(
            {
                "status": "error",
                "mode": args.mode,
                "allow_missing_features": args.allow_missing_features,
                "season": args.season,
                "as_of_date": args.as_of_date,
                "message": str(exc),
            }
        )
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
