from __future__ import annotations

import argparse
import json
from pathlib import Path

import joblib
import pandas as pd

from db_support import connect_for_path
from .config import AppConfig, get_config
from .dataset import build_training_samples, prepare_model_matrix, upsert_predictions
from .train import TARGET_MAP


def _confidence_label(score: float) -> str:
    if score >= 0.8:
        return "HIGH"
    if score >= 0.6:
        return "MEDIUM"
    return "LOW"


def load_models(model_dir: str | Path) -> dict[str, object]:
    path = Path(model_dir)
    return {
        "era": joblib.load(path / "pitcher_era_model.pkl"),
        "whip": joblib.load(path / "pitcher_whip_model.pkl"),
        "war": joblib.load(path / "pitcher_war_model.pkl"),
    }


def predict_latest(
    db_path: str,
    season: int,
    as_of_date: str | None = None,
    config: AppConfig | None = None,
    model_dir: str | Path | None = None,
) -> pd.DataFrame:
    cfg = config or get_config()
    sample_df, artifacts = build_training_samples(db_path=db_path, season=season, config=cfg)
    cutoff_date = as_of_date or str(sample_df["as_of_date"].max())
    eligible_df = sample_df.loc[sample_df["as_of_date"] <= cutoff_date].copy()
    if eligible_df.empty:
        raise RuntimeError("no inference rows for requested as_of_date")

    eligible_df = eligible_df.sort_values(["team", "player_name", "as_of_date", "games_cum"])
    inference_df = (
        eligible_df.groupby(["team", "player_name"], as_index=False)
        .tail(1)
        .copy()
    )
    inference_df["as_of_date"] = cutoff_date

    if inference_df.empty:
        raise RuntimeError("no inference rows after grouping latest pitcher snapshots")

    models = load_models(model_dir or cfg.model_dir)
    categorical_cols = list(cfg.pitcher.categorical_cols)
    x_infer, _ = prepare_model_matrix(
        inference_df.assign(ERA_final=0.0),
        artifacts.feature_cols,
        categorical_cols,
        "ERA_final",
    )

    inference_df["predicted_era_final"] = models["era"].predict(x_infer)
    inference_df["predicted_whip_final"] = models["whip"].predict(x_infer)
    inference_df["predicted_war_final"] = models["war"].predict(x_infer)
    inference_df["confidence_score"] = (
        (inference_df["team_games_cum"] / float(cfg.data.season_length_games)).clip(lower=0.35, upper=0.9)
    )
    inference_df["confidence_level"] = inference_df["confidence_score"].map(_confidence_label)
    inference_df["model_source"] = "pitcher_lgbm_v1"
    return inference_df[
        [
            "season",
            "as_of_date",
            "team",
            "player_name",
            "role",
            "IP",
            "SO_cum",
            "predicted_era_final",
            "predicted_whip_final",
            "predicted_war_final",
            "confidence_score",
            "confidence_level",
            "model_source",
        ]
    ].copy()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Predict pitcher final ERA/WHIP/WAR for a given as_of_date.")
    parser.add_argument("--db", default="kbo_stats.db")
    parser.add_argument("--season", type=int, default=2025)
    parser.add_argument("--as-of-date", default=None, help="YYYY-MM-DD")
    parser.add_argument("--model-dir", default=None)
    parser.add_argument("--upsert-db", action="store_true")
    parser.add_argument("--preview", type=int, default=10)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    cfg = get_config()
    predictions = predict_latest(
        db_path=args.db,
        season=args.season,
        as_of_date=args.as_of_date,
        config=cfg,
        model_dir=args.model_dir,
    )

    if args.upsert_db:
        with connect_for_path(args.db) as conn:
            upserted = upsert_predictions(conn, predictions)
        print(f"upserted={upserted}")

    if args.preview > 0:
        preview_cols = [
            "team",
            "player_name",
            "predicted_era_final",
            "predicted_whip_final",
            "predicted_war_final",
            "confidence_level",
        ]
        print(predictions[preview_cols].head(args.preview).to_json(orient="records", force_ascii=False, indent=2))
    else:
        print(json.dumps({"rows": int(len(predictions))}, ensure_ascii=False))


if __name__ == "__main__":
    main()
