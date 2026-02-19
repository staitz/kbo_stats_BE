import argparse
import json
import os
import pickle
import sqlite3
from datetime import datetime
from typing import Any, Dict, List, Tuple


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
            pa_to_date REAL NOT NULL DEFAULT 0,
            blend_weight REAL NOT NULL DEFAULT 0,
            model_source TEXT NOT NULL DEFAULT 'MODEL_ONLY',
            created_at TEXT NOT NULL,
            UNIQUE (season, as_of_date, team, player_name)
        )
        """
    )
    existing = table_columns(conn, "hitter_predictions")
    if "pa_to_date" not in existing:
        conn.execute(
            "ALTER TABLE hitter_predictions ADD COLUMN pa_to_date REAL NOT NULL DEFAULT 0"
        )
    if "blend_weight" not in existing:
        conn.execute(
            "ALTER TABLE hitter_predictions ADD COLUMN blend_weight REAL NOT NULL DEFAULT 0"
        )
    if "model_source" not in existing:
        conn.execute(
            "ALTER TABLE hitter_predictions ADD COLUMN model_source TEXT NOT NULL DEFAULT 'MODEL_ONLY'"
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


def blend_weight(pa_to_date: float, k: float) -> float:
    if k <= 0:
        return 1.0
    pa = max(0.0, float(pa_to_date))
    return pa / (pa + k)


def to_float(value: Any) -> float:
    if value is None:
        return 0.0
    try:
        return float(value)
    except Exception:
        return 0.0


def main() -> None:
    parser = argparse.ArgumentParser(description="Predict hitter season totals.")
    parser.add_argument("--db", default="db.sqlite3")
    parser.add_argument("--season", required=True, type=int)
    parser.add_argument("--as-of", required=True)
    parser.add_argument("--team")
    parser.add_argument("--model-dir", default="models")
    parser.add_argument("--blend-k", type=float)
    parser.add_argument("--upsert", action="store_true")
    parser.add_argument("--preview", type=int, default=0)
    args = parser.parse_args()

    model_season = args.season - 1
    candidate_prev = {
        "season": model_season,
        "hr_path": os.path.join(args.model_dir, f"hitter_hr_model_train{model_season}.pkl"),
        "ops_path": os.path.join(args.model_dir, f"hitter_ops_model_train{model_season}.pkl"),
        "meta_path": os.path.join(args.model_dir, f"hitter_model_meta_train{model_season}.json"),
    }
    candidate_same = {
        "season": args.season,
        "hr_path": os.path.join(args.model_dir, f"hitter_hr_model_train{args.season}.pkl"),
        "ops_path": os.path.join(args.model_dir, f"hitter_ops_model_train{args.season}.pkl"),
        "meta_path": os.path.join(args.model_dir, f"hitter_model_meta_train{args.season}.json"),
    }
    if os.path.exists(candidate_prev["hr_path"]) and os.path.exists(candidate_prev["ops_path"]):
        selected = candidate_prev
    elif os.path.exists(candidate_same["hr_path"]) and os.path.exists(candidate_same["ops_path"]):
        selected = candidate_same
        model_season = args.season
        print(f"[warn] season-1 model missing; using same-season model train{model_season}")
    else:
        selected = candidate_prev

    hr_path = str(selected["hr_path"])
    ops_path = str(selected["ops_path"])
    meta_path = str(selected["meta_path"])

    meta = {"version": "v1", "feature_columns": None, "target_mode": "final_direct", "recommended_blend_k": 60.0}
    if os.path.exists(meta_path):
        with open(meta_path, "r", encoding="utf-8") as f:
            meta = json.load(f)
    blend_k = float(args.blend_k if args.blend_k is not None else meta.get("recommended_blend_k", 60.0))

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row

    if not conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='hitter_daily_snapshots'"
    ).fetchone():
        raise SystemExit("Missing table: hitter_daily_snapshots")
    if not conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='hitter_season_totals'"
    ).fetchone():
        raise SystemExit("Missing table: hitter_season_totals")

    ensure_predictions_table(conn)

    snapshot_cols = table_columns(conn, "hitter_daily_snapshots")
    key_cols = {"season", "as_of_date", "team", "player_name"}

    feature_cols: List[str] = []
    existing_snapshot_features: List[str] = []
    missing_features: List[str] = []
    special_feature_values = {"prev_season_pa", "prev_season_hr", "prev_season_ops"}
    if meta.get("feature_columns"):
        feature_cols = list(meta["feature_columns"])
        for col in feature_cols:
            if col in snapshot_cols:
                existing_snapshot_features.append(col)
            elif col in special_feature_values:
                continue
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
        existing_snapshot_features = list(feature_cols)

    if not feature_cols:
        raise SystemExit("No feature columns available for prediction.")

    # Load models lazily; preseason fallback can run without model files.
    hr_model = None
    ops_model = None
    if os.path.exists(hr_path) and os.path.exists(ops_path):
        with open(hr_path, "rb") as f:
            hr_model = pickle.load(f)
        with open(ops_path, "rb") as f:
            ops_model = pickle.load(f)
    elif meta.get("target_mode") != "ros_to_final":
        raise SystemExit("Missing model files. Train models first.")

    feature_sql = ", ".join(f"s.{safe_col(c)} AS {safe_col(c)}" for c in existing_snapshot_features)
    extra_feature_prefix = ", " if feature_sql else ""
    filters = ["s.season = ?", "s.as_of_date = ?"]
    params: List[Any] = [args.season, args.as_of]
    if args.team:
        filters.append("s.team = ?")
        params.append(args.team)

    rows = conn.execute(
        f"""
        SELECT
            s.team,
            s.player_name,
            COALESCE(s.PA, 0) AS pa_to_date,
            COALESCE(s.HR, 0) AS hr_to_date,
            COALESCE(s.OPS, 0.0) AS ops_to_date,
            COALESCE(p.PA, 0) AS prev_season_pa,
            COALESCE(p.HR, 0) AS prev_season_hr,
            COALESCE(p.OPS, 0.0) AS prev_season_ops
            {extra_feature_prefix}{feature_sql}
        FROM hitter_daily_snapshots s
        LEFT JOIN hitter_season_totals p
            ON p.season = ?
            AND s.team = p.team
            AND s.player_name = p.player_name
        WHERE {" AND ".join(filters)}
        """,
        [args.season - 1] + params,
    ).fetchall()

    records: List[Dict[str, Any]] = [dict(r) for r in rows]
    if not records:
        # Preseason fallback: no snapshots yet -> prior only.
        prior_filters = ["season = ?"]
        prior_params: List[Any] = [args.season - 1]
        if args.team:
            prior_filters.append("team = ?")
            prior_params.append(args.team)
        prior_rows = conn.execute(
            f"""
            SELECT team, player_name, COALESCE(PA, 0) AS prev_season_pa,
                   COALESCE(HR, 0) AS prev_season_hr, COALESCE(OPS, 0.0) AS prev_season_ops
            FROM hitter_season_totals
            WHERE {" AND ".join(prior_filters)}
            """,
            prior_params,
        ).fetchall()
        records = []
        for row in prior_rows:
            d = dict(row)
            d["pa_to_date"] = 0.0
            d["hr_to_date"] = 0.0
            d["ops_to_date"] = 0.0
            for col in feature_cols:
                if col not in d:
                    d[col] = 0.0
            records.append(d)
        if not records:
            raise SystemExit("No snapshot rows or prior-season totals found for prediction.")

    X: List[List[float]] = []
    for row in records:
        features = []
        for col in feature_cols:
            if col in row:
                features.append(to_float(row[col]))
            elif col in special_feature_values:
                features.append(to_float(row.get(col)))
            else:
                features.append(0.0)
        X.append(features)

    hr_model_preds: List[float] = [0.0] * len(records)
    ops_model_preds: List[float] = [0.0] * len(records)
    if hr_model is not None and ops_model is not None:
        hr_model_preds = list(hr_model.predict(X))
        ops_model_preds = list(ops_model.predict(X))

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
        "pa_to_date",
        "blend_weight",
        "model_source",
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

    created_at = datetime.utcnow().isoformat() + "Z"
    target_mode = str(meta.get("target_mode", "final_direct"))
    values = []
    for row, hr_pred, ops_pred in zip(records, hr_model_preds, ops_model_preds):
        pa_val = max(0.0, to_float(row.get("pa_to_date")))
        hr_to_date = max(0.0, to_float(row.get("hr_to_date")))
        ops_to_date = to_float(row.get("ops_to_date"))
        prior_hr = max(0.0, to_float(row.get("prev_season_hr")))
        prior_ops = min(max(to_float(row.get("prev_season_ops")), 0.0), 2.0)

        if hr_model is None or ops_model is None:
            model_hr_final = prior_hr
            model_ops_final = prior_ops
        elif target_mode == "ros_to_final":
            model_hr_final = hr_to_date + float(hr_pred)
            model_ops_final = ops_to_date + float(ops_pred)
        else:
            model_hr_final = float(hr_pred)
            model_ops_final = float(ops_pred)

        model_hr_final = max(0.0, model_hr_final)
        model_ops_final = min(max(model_ops_final, 0.0), 2.0)

        w = blend_weight(pa_val, blend_k)
        final_hr = (1.0 - w) * prior_hr + w * model_hr_final
        final_ops = (1.0 - w) * prior_ops + w * model_ops_final
        final_hr = max(0.0, final_hr)
        final_ops = min(max(final_ops, 0.0), 2.0)
        if w <= 0.0:
            source = "PRIOR_ONLY"
        elif w >= 0.999:
            source = "MODEL_ONLY"
        else:
            source = "BLENDED"

        level, score = confidence_for_pa(pa_val)
        values.append(
            [
                args.season,
                args.as_of,
                row["team"],
                row["player_name"],
                final_hr,
                final_ops,
                level,
                score,
                model_season,
                meta.get("version", "v1"),
                pa_val,
                w,
                source,
                created_at,
            ]
        )

    cursor = conn.cursor()
    cursor.executemany(insert_sql, values)
    conn.commit()

    print(
        f"Predictions saved for season={args.season}, as_of={args.as_of}, "
        f"team={args.team or 'ALL'} target_mode={target_mode} blend_k={blend_k}"
    )
    print(f"Rows inserted: {cursor.rowcount}")

    if args.preview and args.preview > 0:
        preview_rows = conn.execute(
            """
            SELECT team, player_name, predicted_ops_final, predicted_hr_final,
                   confidence_level, pa_to_date, blend_weight, model_source
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
                f"\tHR={row['predicted_hr_final']:.1f}\tPA={row['pa_to_date']:.0f}"
                f"\tw={row['blend_weight']:.3f}\t{row['model_source']}\t{row['confidence_level']}"
            )

    conn.close()


if __name__ == "__main__":
    main()
