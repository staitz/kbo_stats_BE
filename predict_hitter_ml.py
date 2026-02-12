import argparse
import json
import os
import pickle
import sqlite3
from datetime import datetime
from typing import Dict, List, Tuple


def safe_col(name: str) -> str:
    if not name:
        return name
    if name[0].isdigit() or "-" in name or " " in name:
        return f'"{name}"'
    return name


def table_columns(conn: sqlite3.Connection, table: str) -> Dict[str, str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {row[1]: (row[2] or "") for row in rows}


def is_numeric(col_type: str) -> bool:
    t = (col_type or "").upper()
    if not t:
        return True
    return "INT" in t or "REAL" in t or "NUM" in t or "DEC" in t or "FLOAT" in t


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
            confidence_level TEXT NOT NULL,
            confidence_score REAL NOT NULL DEFAULT 0,
            model_season INTEGER NOT NULL,
            model_version TEXT NOT NULL,
            created_at TEXT NOT NULL,
            UNIQUE (season, as_of_date, team, player_name)
        )
        """
    )
    conn.commit()


def confidence_for_pa(pa: float) -> Tuple[str, float]:
    if pa < 30:
        level = "LOW"
    elif pa < 120:
        level = "MEDIUM"
    else:
        level = "HIGH"
    score = min(max(pa / 200.0, 0.0), 1.0)
    return level, score


def build_upsert_sql(columns: List[str]) -> str:
    updates = []
    for col in columns:
        if col in {"season", "as_of_date", "team", "player_name"}:
            continue
        updates.append(f"{safe_col(col)}=excluded.{safe_col(col)}")
    return ", ".join(updates)


def main() -> None:
    parser = argparse.ArgumentParser(description="Predict hitter season totals.")
    parser.add_argument("--db", default="db.sqlite3")
    parser.add_argument("--season", required=True, type=int)
    parser.add_argument("--as-of", required=True)
    parser.add_argument("--team")
    parser.add_argument("--model-dir", default="models")
    parser.add_argument("--upsert", action="store_true")
    parser.add_argument("--preview", type=int, default=0)
    args = parser.parse_args()

    model_season = args.season - 1
    hr_path = os.path.join(
        args.model_dir, f"hitter_hr_model_train{model_season}.pkl"
    )
    ops_path = os.path.join(
        args.model_dir, f"hitter_ops_model_train{model_season}.pkl"
    )
    meta_path = os.path.join(
        args.model_dir, f"hitter_model_meta_train{model_season}.json"
    )

    if not os.path.exists(hr_path) or not os.path.exists(ops_path):
        raise SystemExit("Missing model files. Train models first.")

    with open(hr_path, "rb") as f:
        hr_model = pickle.load(f)
    with open(ops_path, "rb") as f:
        ops_model = pickle.load(f)

    meta = {"version": "v1", "feature_columns": None}
    if os.path.exists(meta_path):
        with open(meta_path, "r", encoding="utf-8") as f:
            meta = json.load(f)

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row

    if not conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='hitter_daily_snapshots'"
    ).fetchone():
        raise SystemExit("Missing table: hitter_daily_snapshots")

    ensure_predictions_table(conn)

    snapshot_cols = table_columns(conn, "hitter_daily_snapshots")
    key_cols = {"season", "as_of_date", "team", "player_name"}
    feature_cols = []
    existing_features = []
    missing_features: List[str] = []

    if meta.get("feature_columns"):
        feature_cols = list(meta["feature_columns"])
        for col in feature_cols:
            if col in snapshot_cols:
                existing_features.append(col)
            else:
                missing_features.append(col)
        if missing_features:
            print(
                "Missing snapshot columns for model features (filled with 0): "
                + ", ".join(missing_features)
            )
    else:
        for col, col_type in snapshot_cols.items():
            if col in key_cols:
                continue
            if is_numeric(col_type):
                feature_cols.append(col)
        existing_features = list(feature_cols)

    if not feature_cols:
        raise SystemExit("No feature columns available for prediction.")

    feature_sql = ", ".join(safe_col(c) for c in existing_features)
    filters = ["season = ?", "as_of_date = ?"]
    params: List = [args.season, args.as_of]
    if args.team:
        filters.append("team = ?")
        params.append(args.team)
    sql = f"""
        SELECT team, player_name {',' if feature_sql else ''} {feature_sql}
        FROM hitter_daily_snapshots
        WHERE {' AND '.join(filters)}
    """
    rows = conn.execute(sql, params).fetchall()
    if not rows:
        raise SystemExit("No snapshot rows found for prediction.")

    X = []
    players = []
    for row in rows:
        features = []
        for col in feature_cols:
            if col in row.keys():
                val = row[col]
                features.append(0.0 if val is None else float(val))
            else:
                features.append(0.0)
        X.append(features)
        players.append((row["team"], row["player_name"]))

    hr_preds = hr_model.predict(X)
    ops_preds = ops_model.predict(X)

    insert_cols = [
        "season",
        "as_of_date",
        "team",
        "player_name",
        "predicted_hr_final",
        "predicted_ops_final",
        "confidence_level",
        "confidence_score",
        "model_season",
        "model_version",
        "created_at",
    ]
    insert_cols_sql = ", ".join(safe_col(c) for c in insert_cols)
    placeholders = ", ".join(["?"] * len(insert_cols))
    if args.upsert:
        update_sql = build_upsert_sql(insert_cols)
        insert_sql = f"""
            INSERT INTO hitter_predictions ({insert_cols_sql})
            VALUES ({placeholders})
            ON CONFLICT(season, as_of_date, team, player_name) DO UPDATE SET
                {update_sql}
        """
    else:
        insert_sql = f"""
            INSERT OR IGNORE INTO hitter_predictions ({insert_cols_sql})
            VALUES ({placeholders})
        """

    pa_col = "PA_to_date" if "PA_to_date" in snapshot_cols else None
    created_at = datetime.utcnow().isoformat() + "Z"
    values = []
    for (team, player_name), hr_pred, ops_pred, row in zip(
        players, hr_preds, ops_preds, rows
    ):
        hr_val = max(0.0, float(hr_pred))
        ops_val = min(max(float(ops_pred), 0.0), 2.0)
        pa_val = float(row[pa_col]) if pa_col else 0.0
        level, score = confidence_for_pa(pa_val)
        values.append(
            [
                args.season,
                args.as_of,
                team,
                player_name,
                hr_val,
                ops_val,
                level,
                score,
                model_season,
                meta.get("version", "v1"),
                created_at,
            ]
        )

    cursor = conn.cursor()
    cursor.executemany(insert_sql, values)
    conn.commit()

    print(
        f"Predictions saved for season={args.season}, as_of={args.as_of}, "
        f"team={args.team or 'ALL'}"
    )
    print(f"Rows inserted: {cursor.rowcount}")

    if args.preview and args.preview > 0:
        preview_rows = conn.execute(
            """
            SELECT team, player_name, predicted_ops_final, predicted_hr_final, confidence_level
            FROM hitter_predictions
            WHERE season = ? AND as_of_date = ?
            ORDER BY predicted_ops_final DESC
            LIMIT ?
            """,
            (args.season, args.as_of, args.preview),
        ).fetchall()
        print("Preview top predicted OPS")
        for row in preview_rows:
            print(
                f"{row['team']}\t{row['player_name']}\tOPS={row['predicted_ops_final']:.4f}"
                f"\tHR={row['predicted_hr_final']:.1f}\t{row['confidence_level']}"
            )

    conn.close()


if __name__ == "__main__":
    main()
