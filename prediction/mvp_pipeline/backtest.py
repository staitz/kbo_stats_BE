from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error, mean_squared_error

from .config import get_config
from .db import load_hitter_game_logs
from .predict import predict_hitter_targets


def evaluate_predictions(pred_df: pd.DataFrame, game_logs: pd.DataFrame) -> dict[str, float]:
    final_df = (
        game_logs.sort_values(["team", "player_name", "game_date"])
        .assign(player_key=lambda x: x["team"].astype(str) + "::" + x["player_name"].astype(str))
        .groupby("player_key", as_index=False)
        .agg(
            player_name=("player_name", "last"),
            team=("team", "last"),
            final_hr=("HR", "sum"),
            final_pa=("PA", "sum"),
            final_tb=("TB", "sum"),
            final_ab=("AB", "sum"),
            final_h=("H", "sum"),
            final_bb=("BB", "sum"),
            final_hbp=("HBP", "sum"),
            final_sf=("SF", "sum"),
        )
    )
    obp_den = (final_df["final_ab"] + final_df["final_bb"] + final_df["final_hbp"] + final_df["final_sf"]).replace(0, np.nan)
    final_df["final_ops"] = (
        ((final_df["final_h"] + final_df["final_bb"] + final_df["final_hbp"]) / obp_den)
        + (final_df["final_tb"] / final_df["final_ab"].replace(0, np.nan))
    ).fillna(0.0)
    final_df["final_war"] = (((final_df["final_ops"] - 0.700) * final_df["final_pa"]) / 70.0).clip(lower=-5.0, upper=15.0)

    merged = pred_df.merge(final_df[["player_key", "final_ops", "final_hr", "final_war"]], on="player_key", how="inner")
    if merged.empty:
        raise ValueError("No rows available for backtest evaluation.")

    return {
        "ops_mae": float(mean_absolute_error(merged["final_ops"], merged["blended_ops_final"])),
        "ops_rmse": float(np.sqrt(mean_squared_error(merged["final_ops"], merged["blended_ops_final"]))),
        "hr_mae": float(mean_absolute_error(merged["final_hr"], merged["blended_hr_final"])),
        "hr_rmse": float(np.sqrt(mean_squared_error(merged["final_hr"], merged["blended_hr_final"]))),
        "war_mae": float(mean_absolute_error(merged["final_war"], merged["blended_war_final"])),
        "war_rmse": float(np.sqrt(mean_squared_error(merged["final_war"], merged["blended_war_final"]))),
        "n_players": int(len(merged)),
    }


def run_backtest(
    game_logs: pd.DataFrame,
    as_of_dates: list[str],
) -> pd.DataFrame:
    rows: list[dict] = []
    for as_of_date in as_of_dates:
        pred_df = predict_hitter_targets(game_logs=game_logs, as_of_date=as_of_date)
        metrics = evaluate_predictions(pred_df, game_logs)
        metrics["as_of_date"] = as_of_date
        rows.append(metrics)
    return pd.DataFrame(rows).sort_values("as_of_date").reset_index(drop=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backtest hitter MVP predictions across as-of dates.")
    parser.add_argument("--input", type=str, default=None, help="CSV, parquet, or sqlite DB path. If omitted, project DB is used.")
    parser.add_argument("--season", type=int, default=2025, help="Season to backtest.")
    parser.add_argument("--dates", nargs="+", required=True, help="As-of dates in YYYY-MM-DD format.")
    parser.add_argument("--output", type=str, default=None, help="Optional CSV output path.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    cfg = get_config()  # ensures artifact directories exist as a side effect
    _ = cfg  # cfg used for side-effect (dir creation); silence linters
    game_logs = load_hitter_game_logs(args.input, args.season)
    result_df = run_backtest(game_logs, args.dates)
    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        result_df.to_csv(output_path, index=False)
    print(result_df.to_json(orient="records", force_ascii=False, indent=2))


if __name__ == "__main__":
    main()
