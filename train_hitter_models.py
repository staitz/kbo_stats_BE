import argparse
import json
import os
import pickle
import sqlite3
from datetime import datetime
from zoneinfo import ZoneInfo
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


def load_model_class(seed: int | None):
    try:
        from xgboost import XGBRegressor  # type: ignore

        def build():
            return XGBRegressor(
                n_estimators=300,
                learning_rate=0.05,
                max_depth=4,
                subsample=0.9,
                colsample_bytree=0.9,
                objective="reg:squarederror",
                random_state=seed,
            )

        return "xgboost.XGBRegressor", build
    except Exception:
        pass

    try:
        from sklearn.ensemble import GradientBoostingRegressor  # type: ignore

        def build():
            return GradientBoostingRegressor(random_state=seed)

        return "sklearn.GradientBoostingRegressor", build
    except Exception:
        pass

    raise SystemExit(
        "Neither xgboost nor scikit-learn is available. Install one to train models."
    )


def split_by_date(
    rows: List[Tuple], val_after: str | None
) -> Tuple[List[Tuple], List[Tuple]]:
    if not val_after:
        return rows, []
    val_after_date = datetime.strptime(val_after, "%Y%m%d").date()
    train_rows = []
    val_rows = []
    for row in rows:
        as_of_date = datetime.strptime(row[0], "%Y%m%d").date()
        if as_of_date > val_after_date:
            val_rows.append(row)
        else:
            train_rows.append(row)
    return train_rows, val_rows


def rows_to_xy(rows: List[Tuple], feature_len: int) -> Tuple[List[List[float]], List[float], List[float]]:
    X = []
    y_hr = []
    y_ops = []
    for row in rows:
        features = [0.0 if v is None else float(v) for v in row[1 : 1 + feature_len]]
        X.append(features)
        y_hr.append(0.0 if row[1 + feature_len] is None else float(row[1 + feature_len]))
        y_ops.append(0.0 if row[2 + feature_len] is None else float(row[2 + feature_len]))
    return X, y_hr, y_ops


def mae(y_true: List[float], y_pred: List[float]) -> float:
    if not y_true:
        return 0.0
    err = 0.0
    for t, p in zip(y_true, y_pred):
        err += abs(t - p)
    return err / len(y_true)


def main() -> None:
    parser = argparse.ArgumentParser(description="Train hitter models.")
    parser.add_argument("--db", default="db.sqlite3")
    parser.add_argument("--train-season", type=int)
    parser.add_argument("--model-dir", default="models")
    parser.add_argument("--val-after")
    parser.add_argument("--seed", type=int)
    parser.add_argument("--blend-k", type=float, default=60.0)
    args = parser.parse_args()

    if args.train_season is None:
        kst_year = datetime.now(ZoneInfo("Asia/Seoul")).year
        args.train_season = kst_year - 1
        print(f"train_season not provided; using {args.train_season} (KST year-1)")

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row

    if not conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='hitter_training_rows'"
    ).fetchone():
        raise SystemExit("Missing table: hitter_training_rows")

    cols = table_columns(conn, "hitter_training_rows")
    key_cols = {"train_season", "as_of_date", "team", "player_name"}
    target_mode = "final_direct"
    hr_target_col = "y_hr_final"
    ops_target_col = "y_ops_final"
    if "y_hr_ros" in cols and "y_ops_ros" in cols:
        target_mode = "ros_to_final"
        hr_target_col = "y_hr_ros"
        ops_target_col = "y_ops_ros"
    target_cols = {"y_hr_final", "y_ops_final", "y_hr_ros", "y_ops_ros"}
    feature_cols = []
    skipped = []
    for col, col_type in cols.items():
        if col in key_cols or col in target_cols:
            continue
        if not is_numeric(col_type):
            skipped.append(col)
            continue
        feature_cols.append(col)
    if skipped:
        print(f"Skipped non-numeric training columns: {', '.join(skipped)}")
    if not feature_cols:
        raise SystemExit("No feature columns found.")

    feature_sql = ", ".join(safe_col(c) for c in feature_cols)
    sql = f"""
        SELECT as_of_date, {feature_sql}, {safe_col(hr_target_col)}, {safe_col(ops_target_col)}
        FROM hitter_training_rows
        WHERE train_season = ?
    """
    rows = conn.execute(sql, (args.train_season,)).fetchall()
    if not rows:
        raise SystemExit("No training rows found.")
    print(f"Target mode: {target_mode} ({hr_target_col}, {ops_target_col})")

    train_rows, val_rows = split_by_date(rows, args.val_after)
    print(
        f"Split summary: train_rows={len(train_rows)} val_rows={len(val_rows)} "
        f"val_after={args.val_after or 'None'}"
    )
    X_train, y_hr_train, y_ops_train = rows_to_xy(train_rows, len(feature_cols))
    X_val, y_hr_val, y_ops_val = rows_to_xy(val_rows, len(feature_cols))

    model_name, build_model = load_model_class(args.seed)
    hr_model = build_model()
    ops_model = build_model()

    hr_model.fit(X_train, y_hr_train)
    ops_model.fit(X_train, y_ops_train)

    if X_val:
        hr_pred = hr_model.predict(X_val)
        ops_pred = ops_model.predict(X_val)
        print(
            f"Validation MAE: HR={mae(y_hr_val, hr_pred):.3f} "
            f"OPS={mae(y_ops_val, ops_pred):.4f}"
        )
    else:
        print("No validation split used.")

    os.makedirs(args.model_dir, exist_ok=True)
    hr_path = os.path.join(
        args.model_dir, f"hitter_hr_model_train{args.train_season}.pkl"
    )
    ops_path = os.path.join(
        args.model_dir, f"hitter_ops_model_train{args.train_season}.pkl"
    )

    with open(hr_path, "wb") as f:
        pickle.dump(hr_model, f)
    with open(ops_path, "wb") as f:
        pickle.dump(ops_model, f)

    meta = {
        "train_season": args.train_season,
        "model_type": model_name,
        "feature_columns": feature_cols,
        "target_mode": target_mode,
        "hr_target_col": hr_target_col,
        "ops_target_col": ops_target_col,
        "recommended_blend_k": float(args.blend_k),
        "created_at": datetime.utcnow().isoformat() + "Z",
        "version": "v1",
    }
    meta_path = os.path.join(
        args.model_dir, f"hitter_model_meta_train{args.train_season}.json"
    )
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=True, indent=2)

    print(f"Models saved: {hr_path}, {ops_path}")
    print(f"Meta saved: {meta_path}")

    conn.close()


if __name__ == "__main__":
    main()
