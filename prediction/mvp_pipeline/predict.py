from __future__ import annotations

import argparse
from pathlib import Path
from datetime import datetime, timezone

import joblib
import pandas as pd

from .config import AppConfig, get_config
from .db import load_hitter_game_logs, open_db
from .features import HitterFeatureBuilder


def blend_projection_and_prediction(projection: pd.Series, prediction: pd.Series, exposure: pd.Series, k: int) -> tuple[pd.Series, pd.Series]:
    weight = exposure / (exposure + float(k))
    blended = ((1.0 - weight) * projection) + (weight * prediction)
    return blended, weight


def load_models(model_dir: str | Path) -> dict[str, object]:
    model_path = Path(model_dir)
    return {
        "ops": joblib.load(model_path / "hitter_ops_model.pkl"),
        "hr": joblib.load(model_path / "hitter_hr_model.pkl"),
        "war": joblib.load(model_path / "hitter_war_model.pkl"),
    }


def make_preseason_projection(snapshot_df: pd.DataFrame) -> pd.DataFrame:
    projection = snapshot_df[["player_key", "player_name", "team", "age"]].copy()
    projection["proj_ops"] = 0.65 * snapshot_df["regressed_ops"] + 0.35 * snapshot_df["OPS_to_date"]
    projection["proj_hr"] = (snapshot_df["HR_cum"] / snapshot_df["PA_cum"].clip(lower=1)) * 550.0
    projection["proj_war"] = (snapshot_df["WAR_to_date"] / snapshot_df["PA_cum"].clip(lower=1)) * 550.0
    return projection


def predict_hitter_targets(
    game_logs: pd.DataFrame,
    config: AppConfig | None = None,
    model_dir: str | Path | None = None,
    as_of_date: str | None = None,
) -> pd.DataFrame:
    cfg = config or get_config()
    builder = HitterFeatureBuilder(cfg)
    artifacts = builder.build_daily_features(game_logs)
    feature_df = artifacts.feature_df.copy()

    if as_of_date is not None:
        cutoff = pd.Timestamp(as_of_date)
        feature_df = feature_df.loc[feature_df["game_date"] <= cutoff].copy()
    else:
        cutoff = pd.to_datetime(feature_df["game_date"]).max()

    latest = feature_df.sort_values(["player_key", "game_date"]).groupby("player_key", as_index=False).tail(1).copy()
    models = load_models(model_dir or cfg.model_dir)
    projection_df = make_preseason_projection(latest)

    feature_cols = artifacts.feature_cols + list(cfg.hitter.categorical_cols)
    x_pred = latest.loc[:, feature_cols].copy()
    for col in cfg.hitter.categorical_cols:
        if col in x_pred.columns:
            x_pred[col] = x_pred[col].astype("category")

    latest["pred_ops_final"] = models["ops"].predict(x_pred)
    latest["pred_hr_final"] = models["hr"].predict(x_pred)
    latest["pred_war_final"] = models["war"].predict(x_pred)
    latest = latest.merge(projection_df, on=["player_key", "player_name", "team", "age"], how="left")

    latest["blended_ops_final"], latest["ops_weight"] = blend_projection_and_prediction(
        latest["proj_ops"], latest["pred_ops_final"], latest["PA_cum"], cfg.hitter.ops_blend_pa_k
    )
    latest["blended_hr_final"], latest["hr_weight"] = blend_projection_and_prediction(
        latest["proj_hr"], latest["pred_hr_final"], latest["PA_cum"], cfg.hitter.hr_blend_pa_k
    )
    latest["blended_war_final"], latest["war_weight"] = blend_projection_and_prediction(
        latest["proj_war"], latest["pred_war_final"], latest["PA_cum"], cfg.hitter.war_blend_pa_k
    )
    latest["player_last_game_date"] = latest["game_date"].dt.strftime("%Y-%m-%d")
    latest["as_of_date"] = pd.Timestamp(cutoff).strftime("%Y-%m-%d")

    result_cols = [
        "player_key",
        "player_name",
        "team",
        "as_of_date",
        "player_last_game_date",
        "PA_cum",
        "OPS_to_date",
        "HR_cum",
        "WAR_to_date",
        "proj_ops",
        "proj_hr",
        "proj_war",
        "pred_ops_final",
        "pred_hr_final",
        "pred_war_final",
        "blended_ops_final",
        "blended_hr_final",
        "blended_war_final",
        "ops_weight",
        "hr_weight",
        "war_weight",
    ]
    return latest.loc[:, result_cols].reset_index(drop=True)



def save_predictions(pred_df: pd.DataFrame, output_path: str | Path) -> None:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.suffix.lower() == ".parquet":
        pred_df.to_parquet(path, index=False)
    else:
        pred_df.to_csv(path, index=False)


def ensure_predictions_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS hitter_predictions (
            season INTEGER NOT NULL,
            as_of_date TEXT NOT NULL,
            team TEXT NOT NULL,
            player_name TEXT NOT NULL,
            predicted_hr_final REAL NOT NULL DEFAULT 0,
            predicted_ops_final REAL NOT NULL DEFAULT 0,
            predicted_war_final REAL NOT NULL DEFAULT 0,
            confidence_level TEXT NOT NULL,
            confidence_score REAL NOT NULL DEFAULT 0,
            model_season INTEGER NOT NULL,
            model_version TEXT NOT NULL,
            created_at TEXT NOT NULL,
            pa_to_date REAL NOT NULL DEFAULT 0,
            blend_weight REAL NOT NULL DEFAULT 0,
            model_source TEXT NOT NULL DEFAULT 'MODEL_ONLY',
            UNIQUE (season, as_of_date, team, player_name)
        )
        """
    )
    existing = {row[1] for row in conn.execute("PRAGMA table_info(hitter_predictions)").fetchall()}
    if "predicted_war_final" not in existing:
        conn.execute(
            "ALTER TABLE hitter_predictions ADD COLUMN predicted_war_final REAL NOT NULL DEFAULT 0"
        )
    conn.commit()


def _confidence_from_pa(pa: float) -> tuple[str, float]:
    if pa < 30:
        return "LOW", min(pa / 60.0, 1.0)
    if pa < 120:
        return "MEDIUM", min(pa / 180.0, 1.0)
    return "HIGH", min(pa / 250.0, 1.0)


def upsert_predictions_to_db(
    pred_df: pd.DataFrame,
    db_path: str | Path,
    season: int,
    model_season: int,
    model_version: str = "mvp_v1",
    model_source: str = "BLENDED",
    replace_existing: bool = False,
) -> int:
    # Use open_db (WAL mode) so concurrent Django API readers don't collide.
    conn = open_db(db_path)
    ensure_predictions_table(conn)

    if replace_existing and not pred_df.empty:
        as_of_values = sorted(set(str(v) for v in pred_df["as_of_date"].tolist()))
        placeholders = ", ".join(["?"] * len(as_of_values))
        conn.execute(
            f"""
            DELETE FROM hitter_predictions
            WHERE season = ?
              AND model_version = ?
              AND as_of_date IN ({placeholders})
            """,
            [season, model_version] + as_of_values,
        )
        conn.commit()

    created_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    rows: list[tuple] = []
    for row in pred_df.itertuples(index=False):
        confidence_level, confidence_score = _confidence_from_pa(float(row.PA_cum))
        rows.append(
            (
                season,
                str(row.as_of_date),  # Store as YYYY-MM-DD (ISO format)
                row.team,
                row.player_name,
                float(row.blended_hr_final),
                float(row.blended_ops_final),
                float(row.blended_war_final),
                confidence_level,
                float(confidence_score),
                model_season,
                model_version,
                created_at,
                float(row.PA_cum),
                float(row.ops_weight),
                model_source,
            )
        )

    cursor = conn.cursor()
    cursor.executemany(
        """
        INSERT INTO hitter_predictions (
            season, as_of_date, team, player_name,
            predicted_hr_final, predicted_ops_final, predicted_war_final,
            confidence_level, confidence_score,
            model_season, model_version, created_at,
            pa_to_date, blend_weight, model_source
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(season, as_of_date, team, player_name) DO UPDATE SET
            predicted_hr_final=excluded.predicted_hr_final,
            predicted_ops_final=excluded.predicted_ops_final,
            predicted_war_final=excluded.predicted_war_final,
            confidence_level=excluded.confidence_level,
            confidence_score=excluded.confidence_score,
            model_season=excluded.model_season,
            model_version=excluded.model_version,
            created_at=excluded.created_at,
            pa_to_date=excluded.pa_to_date,
            blend_weight=excluded.blend_weight,
            model_source=excluded.model_source
        """,
        rows,
    )
    conn.commit()
    conn.close()
    return len(rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Predict hitter season outcomes.")
    parser.add_argument("--input", type=str, default=None, help="CSV, parquet, or sqlite DB path. If omitted, project DB is used.")
    parser.add_argument("--model-dir", type=str, default=None, help="Directory containing trained models.")
    parser.add_argument("--as-of-date", type=str, default=None, help="Prediction cutoff date YYYY-MM-DD.")
    parser.add_argument("--output", type=str, default=None, help="CSV or parquet prediction output path.")
    parser.add_argument("--season", type=int, default=2025, help="Season to predict from when reading sqlite.")
    parser.add_argument("--upsert-db", action="store_true", help="Upsert predictions into hitter_predictions table.")
    parser.add_argument("--db-path", type=str, default=None, help="SQLite DB path for upsert. Defaults to project DB.")
    parser.add_argument("--replace-existing", action="store_true", help="Delete existing rows for the same season/model_version/as_of_date before upsert.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    cfg = get_config()
    game_logs = load_hitter_game_logs(args.input, args.season)
    pred_df = predict_hitter_targets(
        game_logs=game_logs,
        config=cfg,
        model_dir=args.model_dir,
        as_of_date=args.as_of_date,
    )
    if args.output:
        save_predictions(pred_df, args.output)
    if args.upsert_db:
        db_path = args.db_path or (Path(__file__).resolve().parents[2] / "kbo_stats.db")
        count = upsert_predictions_to_db(
            pred_df=pred_df,
            db_path=db_path,
            season=args.season,
            model_season=args.season,
            replace_existing=args.replace_existing,
        )
        print(f"Upserted {count} rows into hitter_predictions")
    print(pred_df.to_json(orient="records", force_ascii=False, indent=2))


if __name__ == "__main__":
    main()
