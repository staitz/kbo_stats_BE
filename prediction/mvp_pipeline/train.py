from __future__ import annotations

import argparse
import json
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error, mean_squared_error

try:
    import lightgbm as lgb
except ModuleNotFoundError as exc:  # pragma: no cover
    raise SystemExit("lightgbm is required. Install it with `pip install lightgbm`.") from exc

from .config import AppConfig, get_config
from .db import load_hitter_game_logs
from .features import HitterFeatureBuilder, make_train_valid_test_split, prepare_model_matrix


TARGET_MAP = {
    "ops": "OPS_final",
    "hr": "HR_final",
    "war": "WAR_final",
}


def build_hitter_model(config: AppConfig) -> lgb.LGBMRegressor:
    train_cfg = config.train
    return lgb.LGBMRegressor(
        objective="regression",
        n_estimators=train_cfg.n_estimators,
        learning_rate=train_cfg.learning_rate,
        num_leaves=train_cfg.num_leaves,
        min_child_samples=train_cfg.min_child_samples,
        subsample=train_cfg.subsample,
        colsample_bytree=train_cfg.colsample_bytree,
        random_state=train_cfg.random_state,
    )


def evaluate_regression(y_true: pd.Series, y_pred: pd.Series) -> dict[str, float]:
    return {
        "mae": float(mean_absolute_error(y_true, y_pred)),
        "rmse": float(np.sqrt(mean_squared_error(y_true, y_pred))),
    }


def train_hitter_targets(
    game_logs: pd.DataFrame,
    config: AppConfig | None = None,
    output_dir: str | Path | None = None,
) -> dict[str, dict]:
    cfg = config or get_config()
    builder = HitterFeatureBuilder(cfg)
    sample_df = builder.build_training_samples(game_logs)
    train_df, valid_df, test_df = make_train_valid_test_split(
        sample_df=sample_df,
        valid_start_date=cfg.data.valid_start_date,
        test_start_date=cfg.data.test_start_date,
    )

    output_path = Path(output_dir) if output_dir else cfg.model_dir
    output_path.mkdir(parents=True, exist_ok=True)

    # Build features once — reuse the FeatureArtifacts from build_training_samples
    # by also capturing feature_cols directly from the first call.
    artifacts = builder.build_daily_features(game_logs)
    feature_cols = artifacts.feature_cols + list(cfg.hitter.categorical_cols)
    categorical_cols = list(cfg.hitter.categorical_cols)
    results: dict[str, dict] = {}

    for target_name, target_col in TARGET_MAP.items():
        x_train, y_train = prepare_model_matrix(train_df, feature_cols, categorical_cols, target_col)
        x_valid, y_valid = prepare_model_matrix(valid_df, feature_cols, categorical_cols, target_col)
        x_test, y_test = prepare_model_matrix(test_df, feature_cols, categorical_cols, target_col)

        model = build_hitter_model(cfg)
        fit_kwargs = {}
        if not valid_df.empty:
            fit_kwargs = {
                "eval_set": [(x_valid, y_valid)],
                "eval_metric": "l1",
                "callbacks": [lgb.early_stopping(cfg.train.early_stopping_rounds), lgb.log_evaluation(50)],
            }
        model.fit(x_train, y_train, categorical_feature=categorical_cols, **fit_kwargs)

        valid_metrics = evaluate_regression(y_valid, model.predict(x_valid)) if not valid_df.empty else {}
        test_metrics = evaluate_regression(y_test, model.predict(x_test)) if not test_df.empty else {}

        model_file = output_path / f"hitter_{target_name}_model.pkl"
        joblib.dump(model, model_file)

        results[target_name] = {
            "target_col": target_col,
            "model_file": str(model_file),
            "valid_metrics": valid_metrics,
            "test_metrics": test_metrics,
            "n_train": int(len(train_df)),
            "n_valid": int(len(valid_df)),
            "n_test": int(len(test_df)),
        }

    meta = {
        "feature_cols": feature_cols,
        "categorical_cols": categorical_cols,
        "splits": {
            "valid_start_date": cfg.data.valid_start_date,
            "test_start_date": cfg.data.test_start_date,
        },
        "results": results,
    }
    meta_file = output_path / "hitter_training_meta.json"
    meta_file.write_text(json.dumps(meta, ensure_ascii=True, indent=2), encoding="utf-8")
    return meta


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train hitter MVP models.")
    parser.add_argument("--input", type=str, default=None, help="CSV, parquet, or sqlite DB path. If omitted, project DB is used.")
    parser.add_argument("--output-dir", type=str, default=None, help="Directory for trained models.")
    parser.add_argument("--season", type=int, default=2025, help="Season to train from when reading sqlite.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    cfg = get_config()
    game_logs = load_hitter_game_logs(args.input, args.season)
    meta = train_hitter_targets(game_logs, cfg, args.output_dir)
    print(json.dumps(meta, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
