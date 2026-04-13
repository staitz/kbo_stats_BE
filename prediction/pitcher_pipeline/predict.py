from __future__ import annotations

import argparse
import json
from pathlib import Path

import joblib
import pandas as pd

from db_support import connect_for_path
from .config import AppConfig, get_config
from .dataset import (
    DatasetArtifacts,
    _calc_fip,
    _season_age,
    _load_birth_dates,
    build_training_samples,
    estimate_fip_constant,
    load_pitcher_logs,
    load_pitcher_snapshots,
    prepare_model_matrix,
    upsert_predictions,
)
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
    """Run inference using pitcher_daily_snapshots as the primary data source.

    This mirrors the hitter pipeline pattern:
      pitcher_game_logs → pitcher_daily_snapshots → predict_latest → pitcher_predictions

    Training-time features that are not stored in the snapshot (FIP, recent_5_fip, etc.)
    are derived from the snapshot's raw counting columns.
    """
    cfg = config or get_config()

    # ---------------------------------------------------------------------------
    # 1. Load snapshots (primary source — same role as hitter_daily_snapshots)
    # ---------------------------------------------------------------------------
    snapshots = load_pitcher_snapshots(db_path, season)
    if snapshots.empty:
        # Fallback: rebuild on-the-fly from raw game logs (pre-snapshot compatibility)
        sample_df, artifacts = build_training_samples(db_path=db_path, season=season, config=cfg)
        cutoff_date = as_of_date or str(sample_df["as_of_date"].max())
        eligible_df = sample_df.loc[sample_df["as_of_date"] <= cutoff_date].copy()
        if eligible_df.empty:
            raise RuntimeError("no inference rows for requested as_of_date")
        inference_df = (
            eligible_df.sort_values(["team", "player_name", "as_of_date", "games_cum"])
            .groupby(["team", "player_name"], as_index=False)
            .tail(1)
            .copy()
        )
        inference_df["as_of_date"] = cutoff_date
    else:
        # ---------------------------------------------------------------------------
        # 2. Filter to as_of_date and take the latest snapshot per player
        # ---------------------------------------------------------------------------
        cutoff_date = as_of_date or str(snapshots["as_of_date"].max())
        eligible = snapshots.loc[snapshots["as_of_date"] <= cutoff_date].copy()
        if eligible.empty:
            raise RuntimeError("no inference rows for requested as_of_date")

        inference_df = (
            eligible.sort_values(["team", "player_name", "as_of_date"])
            .groupby(["team", "player_name"], as_index=False)
            .tail(1)
            .copy()
        )
        inference_df["as_of_date"] = cutoff_date

        # ---------------------------------------------------------------------------
        # 3. Derive model features from snapshot columns
        #    (mirrors dataset.build_training_samples feature engineering)
        # ---------------------------------------------------------------------------
        # FIP constant: estimated from raw game logs (lightweight pass)
        fip_constant = 3.2
        try:
            logs = load_pitcher_logs(db_path, season)
            if not logs.empty:
                fip_constant = estimate_fip_constant(logs)
        except Exception:
            pass

        # Birth dates for age features
        with connect_for_path(db_path) as conn:
            birth_dates = _load_birth_dates(conn)

        # --- Cumulative features ---
        inference_df["games_cum"] = inference_df["games"].astype(float)
        inference_df["team_games_cum"] = inference_df["games"].astype(float)  # per-player approx
        inference_df["W_cum"] = inference_df["W"].astype(float)
        inference_df["SO_cum"] = inference_df["SO"].astype(float)
        inference_df["SV_cum"] = inference_df["SV"].astype(float)
        inference_df["HLD_cum"] = inference_df["HLD"].astype(float)

        # --- Rate features (already stored in snapshot) ---
        inference_df["K_9"] = inference_df["K9"]
        inference_df["BB_9"] = inference_df["BB9"]

        # --- FIP (computed from cumulative raw counts) ---
        inference_df["FIP"] = inference_df.apply(
            lambda r: _calc_fip(
                float(r["HR"]), float(r["BB"]), float(r["HBP"]),
                float(r["SO"]), float(r["OUTS"]), fip_constant
            ),
            axis=1,
        )
        inference_df["ERA_minus_FIP"] = inference_df["ERA"] - inference_df["FIP"]

        # --- Recent rolling approximations from 7-day window ---
        inference_df["recent_3_era"] = inference_df["ERA_7"]  # 7d ERA is the best proxy
        inference_df["recent_5_fip"] = inference_df.apply(
            lambda r: _calc_fip(
                float(r["HR_7"]), float(r["BB_7"]), float(r["HBP_7"]),
                float(r["SO_7"]), float(r["OUTS_7"]), fip_constant
            ),
            axis=1,
        )
        inference_df["recent_10day_ip"] = inference_df["OUTS_7"] / 3.0  # 7d IP as proxy
        inference_df["rest_days"] = 5.0  # season-average default

        # --- Age features ---
        inference_df["age"] = inference_df["player_name"].map(
            lambda n: _season_age(str(n), season, birth_dates)
        )
        inference_df["age_squared"] = inference_df["age"] ** 2

        # Ensure numeric
        for col in ["FIP", "ERA_minus_FIP", "recent_3_era", "recent_5_fip",
                    "recent_10day_ip", "age", "age_squared"]:
            inference_df[col] = pd.to_numeric(inference_df[col], errors="coerce").fillna(0)

    if inference_df.empty:
        raise RuntimeError("no inference rows after preparing pitcher snapshots")

    # ---------------------------------------------------------------------------
    # 4. Apply models — same as before
    # ---------------------------------------------------------------------------
    models = load_models(model_dir or cfg.model_dir)
    categorical_cols = list(cfg.pitcher.categorical_cols)

    # Determine feature_cols: from snapshot path or from training samples
    if "FIP" in inference_df.columns:
        artifacts = DatasetArtifacts(
            feature_cols=[
                "games_cum", "team_games_cum", "IP", "ERA", "FIP",
                "K_9", "BB_9", "WHIP",
                "recent_3_era", "recent_5_fip", "ERA_minus_FIP",
                "rest_days", "recent_10day_ip",
                "age", "age_squared",
                "W_cum", "SV_cum", "HLD_cum",
            ],
            target_cols=list(cfg.pitcher.target_cols),
            fip_constant=3.2,
        )
    else:
        _, artifacts = build_training_samples(db_path=db_path, season=season, config=cfg)

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

    # Ensure SO_cum exists for output
    if "SO_cum" not in inference_df.columns:
        inference_df["SO_cum"] = inference_df.get("SO", pd.Series(0, index=inference_df.index))

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
