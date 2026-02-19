import os
import pickle
import sqlite3
from datetime import datetime

import pandas as pd
import streamlit as st

st.set_page_config(page_title='KBO Hitter Dashboard', layout='wide')

DB_PATH = 'kbo_stats.db'

@st.cache_data(show_spinner=False)
def load_table(query, params=None):
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query(query, conn, params=params or {})
    conn.close()
    return df


def mae(y_true, y_pred):
    if len(y_true) == 0:
        return None
    return float(sum(abs(t - p) for t, p in zip(y_true, y_pred)) / len(y_true))

st.title('KBO Hitter ML Dashboard')

season = st.sidebar.number_input('Season', value=2025, step=1)
min_pa = st.sidebar.slider('Min PA (Season Totals)', min_value=0, max_value=600, value=100, step=10)

st.subheader('Snapshots Summary')
summary = load_table(
    """
    SELECT MIN(as_of_date) AS min_date,
           MAX(as_of_date) AS max_date,
           COUNT(DISTINCT as_of_date) AS distinct_dates,
           COUNT(*) AS total_rows
    FROM hitter_daily_snapshots
    WHERE season = ?
    """,
    params=(season,)
)
st.dataframe(summary, use_container_width=True)

st.subheader('Training Rows Summary')
train_summary = load_table(
    """
    SELECT MIN(as_of_date) AS min_date,
           MAX(as_of_date) AS max_date,
           COUNT(DISTINCT as_of_date) AS distinct_dates,
           COUNT(*) AS total_rows,
           SUM(CASE WHEN y_hr_final IS NULL THEN 1 ELSE 0 END) AS null_hr,
           SUM(CASE WHEN y_ops_final IS NULL THEN 1 ELSE 0 END) AS null_ops
    FROM hitter_training_rows
    WHERE train_season = ?
    """,
    params=(season,)
)
st.dataframe(train_summary, use_container_width=True)

st.subheader('Top OPS (Season Totals)')
ops_df = load_table(
    """
    SELECT team, player_name, OPS, HR, AB, H, PA
    FROM hitter_season_totals
    WHERE season = ?
      AND PA >= ?
    ORDER BY OPS DESC
    LIMIT 20
    """,
    params=(season, min_pa)
)
st.dataframe(ops_df, use_container_width=True)

st.subheader('Player Lookup (Season Totals)')
name_query = st.text_input('Player name contains', value='')
player_df = load_table(
    """
    SELECT team, player_name, PA, AB, H, HR, BB, HBP, SF, OPS
    FROM hitter_season_totals
    WHERE season = ?
    """,
    params=(season,),
)
if name_query.strip():
    mask = player_df["player_name"].astype(str).str.contains(name_query.strip(), na=False)
    match_df = player_df[mask].copy()
    st.dataframe(match_df, use_container_width=True)
else:
    st.caption("Enter a player name to search.")

st.subheader('Validation (Latest Model)')
model_season = season
meta_path = f"models/hitter_model_meta_train{model_season}.json"
hr_path = f"models/hitter_hr_model_train{model_season}.pkl"
ops_path = f"models/hitter_ops_model_train{model_season}.pkl"

if os.path.exists(hr_path) and os.path.exists(ops_path) and os.path.exists(meta_path):
    meta = load_table(
        "SELECT 1",
    )
    with open(meta_path, "r", encoding="utf-8") as f:
        meta_json = f.read()
    feature_cols = None
    try:
        import json

        feature_cols = json.loads(meta_json).get("feature_columns")
    except Exception:
        feature_cols = None

    train_df = load_table(
        """
        SELECT *
        FROM hitter_training_rows
        WHERE train_season = ?
        """,
        params=(model_season,),
    )
    if train_df.empty:
        st.info("No training rows found for validation.")
    else:
        dates = sorted(train_df["as_of_date"].unique().tolist())
        idx = max(int(len(dates) * 0.8) - 1, 0) if dates else 0
        val_after = dates[idx] if dates else None
        if val_after:
            val_df = train_df[train_df["as_of_date"] > val_after].copy()
            if feature_cols is None:
                feature_cols = [
                    c
                    for c in train_df.columns
                    if c
                    not in {
                        "train_season",
                        "as_of_date",
                        "team",
                        "player_name",
                        "y_hr_final",
                        "y_ops_final",
                    }
                    and pd.api.types.is_numeric_dtype(train_df[c])
                ]
            for col in feature_cols:
                if col not in val_df.columns:
                    val_df[col] = 0
            X_val = val_df[feature_cols].fillna(0.0).astype(float).values

            with open(hr_path, "rb") as f:
                hr_model = pickle.load(f)
            with open(ops_path, "rb") as f:
                ops_model = pickle.load(f)

            hr_pred = hr_model.predict(X_val)
            ops_pred = ops_model.predict(X_val)
            hr_mae = mae(val_df["y_hr_final"].astype(float).tolist(), hr_pred.tolist())
            ops_mae = mae(val_df["y_ops_final"].astype(float).tolist(), ops_pred.tolist())

            st.caption(f"val_after (80% time split) = {val_after}")
            st.write(
                {
                    "val_rows": int(len(val_df)),
                    "hr_mae": None if hr_mae is None else round(hr_mae, 4),
                    "ops_mae": None if ops_mae is None else round(ops_mae, 4),
                }
            )
        else:
            st.info("Not enough dates to compute validation split.")
else:
    st.info("Model files not found for validation.")

st.subheader('Predictions (Latest Date)')
latest = load_table(
    """
    SELECT MAX(as_of_date) AS latest_date
    FROM hitter_predictions
    WHERE season = ?
    """,
    params=(season,)
)
latest_date = latest.iloc[0]['latest_date'] if not latest.empty else None
if latest_date:
    pred_df = load_table(
        """
        SELECT team, player_name, predicted_hr_final, predicted_ops_final,
               confidence_level, pa_to_date, blend_weight, model_source
        FROM hitter_predictions
        WHERE season = ? AND as_of_date = ?
        ORDER BY predicted_ops_final DESC
        LIMIT 50
        """,
        params=(season, latest_date)
    )
    st.caption(f'Latest predictions as_of_date={latest_date}')
    if not pred_df.empty:
        pred_view = pred_df.copy()
        pred_view["predicted_hr_final"] = pred_view["predicted_hr_final"].round(2)
        pred_view["predicted_ops_final"] = pred_view["predicted_ops_final"].round(4)
        pred_view["pa_to_date"] = pred_view["pa_to_date"].round(0)
        pred_view["blend_weight"] = pred_view["blend_weight"].round(3)
        st.dataframe(pred_view, use_container_width=True)
    else:
        st.info('No prediction rows for latest date.')
else:
    st.info('No predictions found for this season.')

st.subheader('Confidence Level Distribution')
conf_df = load_table(
    """
    SELECT confidence_level, COUNT(*) AS cnt
    FROM hitter_predictions
    WHERE season = ?
    GROUP BY confidence_level
    ORDER BY cnt DESC
    """,
    params=(season,)
)
if not conf_df.empty:
    st.bar_chart(conf_df.set_index('confidence_level'))
else:
    st.info('No confidence data available.')

st.subheader('Prediction Source Distribution')
source_df = load_table(
    """
    SELECT model_source, COUNT(*) AS cnt
    FROM hitter_predictions
    WHERE season = ?
    GROUP BY model_source
    ORDER BY cnt DESC
    """,
    params=(season,)
)
if not source_df.empty:
    st.bar_chart(source_df.set_index('model_source'))
    st.dataframe(source_df, use_container_width=True)
else:
    st.info('No model source data available.')
