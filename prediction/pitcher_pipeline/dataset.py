from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd

from db_support import connect_for_path, execute, executemany, is_postgres, read_sql_query, row_value
from .config import AppConfig, get_config


DEFAULT_DB = "kbo_stats.db"
PREDICTION_TABLE = "pitcher_predictions"

_PITCHER_LOG_COLUMN_ALIASES = {
    "outs": "OUTS",
    "h": "H",
    "er": "ER",
    "bb": "BB",
    "so": "SO",
    "hr": "HR",
    "hbp": "HBP",
    "w": "W",
    "l": "L",
    "sv": "SV",
    "hld": "HLD",
}

_PITCHER_TOTAL_COLUMN_ALIASES = {
    "w_final": "W_final",
    "l_final": "L_final",
    "sv_final": "SV_final",
    "hld_final": "HLD_final",
    "ip_final": "IP_final",
    "outs_final": "OUTS_final",
    "h_final": "H_final",
    "er_final": "ER_final",
    "bb_final": "BB_final",
    "so_final": "SO_final",
    "era_final": "ERA_final",
    "whip_final": "WHIP_final",
    "k9_final": "K9_final",
    "bb9_final": "BB9_final",
    "kbb_final": "KBB_final",
    "war_final": "WAR_final",
}


@dataclass(slots=True)
class DatasetArtifacts:
    feature_cols: list[str]
    target_cols: list[str]
    fip_constant: float


def _safe_numeric(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    for column in columns:
        if column not in frame.columns:
            frame[column] = 0
        frame[column] = pd.to_numeric(frame[column], errors="coerce").fillna(0)
    return frame


def _load_birth_dates(conn) -> dict[str, str]:
    try:
        rows = execute(
            conn,
            """
            SELECT player_name, birth_date
            FROM statiz_players
            WHERE TRIM(player_name) <> ''
              AND birth_date IS NOT NULL
              AND birth_date <> ''
            """
        ).fetchall()
    except Exception:
        return {}
    out: dict[str, str] = {}
    for row in rows:
        name = str(row_value(row, "player_name", "") or "").strip()
        birth = str(row_value(row, "birth_date", "") or "").strip()
        if name and birth:
            out[name] = birth
    return out


def _season_age(player_name: str, season: int, birth_dates: dict[str, str]) -> float:
    raw = birth_dates.get(player_name)
    if not raw:
        return 27.0
    try:
        birth_year = int(raw[:4])
        return float(season - birth_year)
    except Exception:
        return 27.0


def load_pitcher_logs(db_path: str, season: int) -> pd.DataFrame:
    conn = connect_for_path(db_path)
    try:
        query = """
        SELECT
            substr(game_date, 1, 4) AS season,
            game_date,
            game_id,
            team,
            player_name,
            COALESCE(role, '') AS role,
            COALESCE(OUTS, 0) AS OUTS,
            COALESCE(H, 0) AS H,
            COALESCE(ER, 0) AS ER,
            COALESCE(BB, 0) AS BB,
            COALESCE(SO, 0) AS SO,
            COALESCE(HR, 0) AS HR,
            COALESCE(HBP, 0) AS HBP,
            COALESCE(W, 0) AS W,
            COALESCE(L, 0) AS L,
            COALESCE(SV, 0) AS SV,
            COALESCE(HLD, 0) AS HLD
        FROM pitcher_game_logs
        WHERE substr(game_date, 1, 4) = ?
        ORDER BY game_date ASC, game_id ASC, team ASC, player_name ASC
        """
        frame = read_sql_query(query, conn, params=[str(season)])
        if frame.empty:
            return frame
        frame = frame.rename(columns={k: v for k, v in _PITCHER_LOG_COLUMN_ALIASES.items() if k in frame.columns})
        frame["season"] = frame["season"].astype(int)
        frame["game_date"] = pd.to_datetime(frame["game_date"], format="%Y%m%d")
        numeric_cols = ["OUTS", "H", "ER", "BB", "SO", "HR", "HBP", "W", "L", "SV", "HLD"]
        return _safe_numeric(frame, numeric_cols)
    finally:
        conn.close()


def load_pitcher_final_totals(db_path: str, season: int) -> pd.DataFrame:
    conn = connect_for_path(db_path)
    try:
        query = """
        SELECT
            season,
            team,
            player_name,
            COALESCE(role, '') AS role,
            COALESCE(games, 0) AS games,
            COALESCE(W, 0) AS W_final,
            COALESCE(L, 0) AS L_final,
            COALESCE(SV, 0) AS SV_final,
            COALESCE(HLD, 0) AS HLD_final,
            COALESCE(IP, 0) AS IP_final,
            COALESCE(OUTS, 0) AS OUTS_final,
            COALESCE(H, 0) AS H_final,
            COALESCE(ER, 0) AS ER_final,
            COALESCE(BB, 0) AS BB_final,
            COALESCE(SO, 0) AS SO_final,
            COALESCE(ERA, 0) AS ERA_final,
            COALESCE(WHIP, 0) AS WHIP_final,
            COALESCE(K9, 0) AS K9_final,
            COALESCE(BB9, 0) AS BB9_final,
            COALESCE(KBB, 0) AS KBB_final,
            COALESCE(pitcher_war, WAR, 0) AS WAR_final
        FROM pitcher_season_totals
        WHERE season = ?
        """
        frame = read_sql_query(query, conn, params=[season])
        if frame.empty:
            return frame
        frame = frame.rename(columns={k: v for k, v in _PITCHER_TOTAL_COLUMN_ALIASES.items() if k in frame.columns})

        numeric_cols = [
            "games",
            "W_final",
            "L_final",
            "SV_final",
            "HLD_final",
            "IP_final",
            "OUTS_final",
            "H_final",
            "ER_final",
            "BB_final",
            "SO_final",
            "ERA_final",
            "WHIP_final",
            "K9_final",
            "BB9_final",
            "KBB_final",
            "WAR_final",
        ]
        frame = _safe_numeric(frame, numeric_cols)
        return frame
    finally:
        conn.close()


def estimate_fip_constant(logs: pd.DataFrame) -> float:
    outs = float(logs["OUTS"].sum())
    if outs <= 0:
        return 3.2
    ip = outs / 3.0
    league_era = (float(logs["ER"].sum()) * 9.0) / max(ip, 1.0)
    fip_core = (
        (13.0 * float(logs["HR"].sum()))
        + (3.0 * (float(logs["BB"].sum()) + float(logs["HBP"].sum())))
        - (2.0 * float(logs["SO"].sum()))
    ) / max(ip, 1.0)
    return round(league_era - fip_core, 4)


def _calc_rate(numerator: float, denominator: float, scale: float) -> float:
    return scale * numerator / denominator if denominator > 0 else 0.0


def _calc_fip(hr: float, bb: float, hbp: float, so: float, outs: float, fip_constant: float) -> float:
    ip = outs / 3.0
    if ip <= 0:
        return 0.0
    return ((13.0 * hr) + (3.0 * (bb + hbp)) - (2.0 * so)) / ip + fip_constant


def build_training_samples(
    db_path: str = DEFAULT_DB,
    season: int = 2025,
    config: AppConfig | None = None,
) -> tuple[pd.DataFrame, DatasetArtifacts]:
    cfg = config or get_config()
    logs = load_pitcher_logs(db_path, season)
    finals = load_pitcher_final_totals(db_path, season)
    if logs.empty or finals.empty:
        raise RuntimeError("pitcher logs or pitcher season totals are empty")

    fip_constant = estimate_fip_constant(logs)
    birth_dates: dict[str, str]
    with connect_for_path(db_path) as conn:
        birth_dates = _load_birth_dates(conn)

    final_map = {
        (str(row.team), str(row.player_name)): row
        for row in finals.itertuples(index=False)
    }

    samples: list[dict[str, Any]] = []
    group_cols = ["team", "player_name"]
    for (team, player_name), group in logs.groupby(group_cols, sort=False):
        group = group.sort_values(["game_date", "game_id"]).reset_index(drop=True)
        final_row = final_map.get((str(team), str(player_name)))
        if final_row is None:
            continue

        age = _season_age(str(player_name), season, birth_dates)
        outs_cum = h_cum = er_cum = bb_cum = so_cum = hr_cum = hbp_cum = 0.0
        w_cum = l_cum = sv_cum = hld_cum = 0.0
        team_games_seen = 0
        recent3 = deque(maxlen=3)
        recent5 = deque(maxlen=5)
        recent10 = deque()
        prev_date = None

        for index, row in enumerate(group.itertuples(index=False), start=1):
            outs_cum += float(row.OUTS)
            h_cum += float(row.H)
            er_cum += float(row.ER)
            bb_cum += float(row.BB)
            so_cum += float(row.SO)
            hr_cum += float(row.HR)
            hbp_cum += float(row.HBP)
            w_cum += float(row.W)
            l_cum += float(row.L)
            sv_cum += float(row.SV)
            hld_cum += float(row.HLD)
            team_games_seen += 1

            ip_cum = outs_cum / 3.0
            era_cum = _calc_rate(er_cum, outs_cum, 27.0)
            whip_cum = _calc_rate(h_cum + bb_cum, outs_cum, 3.0)
            k9_cum = _calc_rate(so_cum, outs_cum, 27.0)
            bb9_cum = _calc_rate(bb_cum, outs_cum, 27.0)
            fip_cum = _calc_fip(hr_cum, bb_cum, hbp_cum, so_cum, outs_cum, fip_constant)

            current_date = pd.Timestamp(row.game_date)
            recent3.append({"ER": float(row.ER), "OUTS": float(row.OUTS)})
            recent5.append(
                {
                    "HR": float(row.HR),
                    "BB": float(row.BB),
                    "HBP": float(row.HBP),
                    "SO": float(row.SO),
                    "OUTS": float(row.OUTS),
                }
            )
            recent10.append((current_date, float(row.OUTS)))
            min_date = current_date - pd.Timedelta(days=9)
            while recent10 and recent10[0][0] < min_date:
                recent10.popleft()

            recent3_outs = sum(item["OUTS"] for item in recent3)
            recent3_er = sum(item["ER"] for item in recent3)
            recent5_outs = sum(item["OUTS"] for item in recent5)
            recent5_hr = sum(item["HR"] for item in recent5)
            recent5_bb = sum(item["BB"] for item in recent5)
            recent5_hbp = sum(item["HBP"] for item in recent5)
            recent5_so = sum(item["SO"] for item in recent5)
            recent_3_era = _calc_rate(recent3_er, recent3_outs, 27.0)
            recent_5_fip = _calc_fip(recent5_hr, recent5_bb, recent5_hbp, recent5_so, recent5_outs, fip_constant)
            recent_10day_ip = sum(outs for _, outs in recent10) / 3.0

            rest_days = 5.0
            if prev_date is not None:
                rest_days = float(max((current_date - prev_date).days - 1, 0))
            prev_date = current_date

            if outs_cum < cfg.data.min_outs_threshold:
                continue
            if index % cfg.data.sample_every_n_games != 0 and index != len(group):
                continue

            samples.append(
                {
                    "season": season,
                    "as_of_date": current_date.strftime("%Y-%m-%d"),
                    "team": str(team),
                    "player_name": str(player_name),
                    "role": str(row.role or ""),
                    "games_cum": float(index),
                    "team_games_cum": float(team_games_seen),
                    "IP": round(ip_cum, 3),
                    "ERA": round(era_cum, 4),
                    "FIP": round(fip_cum, 4),
                    "K_9": round(k9_cum, 4),
                    "BB_9": round(bb9_cum, 4),
                    "WHIP": round(whip_cum, 4),
                    "recent_3_era": round(recent_3_era, 4),
                    "recent_5_fip": round(recent_5_fip, 4),
                    "ERA_minus_FIP": round(era_cum - fip_cum, 4),
                    "rest_days": rest_days,
                    "recent_10day_ip": round(recent_10day_ip, 3),
                    "age": age,
                    "age_squared": age * age,
                    "W_cum": float(w_cum),
                    "SO_cum": float(so_cum),
                    "SV_cum": float(sv_cum),
                    "HLD_cum": float(hld_cum),
                    "ERA_final": float(final_row.ERA_final),
                    "WHIP_final": float(final_row.WHIP_final),
                    "WAR_final": float(final_row.WAR_final),
                    "IP_final": float(final_row.IP_final),
                    "SO_final": float(final_row.SO_final),
                }
            )

    sample_df = pd.DataFrame(samples)
    if sample_df.empty:
        raise RuntimeError("no pitcher training samples were generated")

    feature_cols = [
        "games_cum",
        "team_games_cum",
        "IP",
        "ERA",
        "FIP",
        "K_9",
        "BB_9",
        "WHIP",
        "recent_3_era",
        "recent_5_fip",
        "ERA_minus_FIP",
        "rest_days",
        "recent_10day_ip",
        "age",
        "age_squared",
        "W_cum",
        "SV_cum",
        "HLD_cum",
    ]
    target_cols = list(cfg.pitcher.target_cols)
    sample_df = _safe_numeric(sample_df, feature_cols + target_cols + ["IP_final", "SO_final"])
    artifacts = DatasetArtifacts(feature_cols=feature_cols, target_cols=target_cols, fip_constant=fip_constant)
    return sample_df, artifacts


def make_time_split(
    sample_df: pd.DataFrame,
    valid_start_date: str,
    test_start_date: str,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    dates = pd.to_datetime(sample_df["as_of_date"])
    valid_ts = pd.Timestamp(valid_start_date)
    test_ts = pd.Timestamp(test_start_date)
    train_df = sample_df.loc[dates < valid_ts].copy()
    valid_df = sample_df.loc[(dates >= valid_ts) & (dates < test_ts)].copy()
    test_df = sample_df.loc[dates >= test_ts].copy()
    return train_df, valid_df, test_df


def prepare_model_matrix(
    frame: pd.DataFrame,
    feature_cols: list[str],
    categorical_cols: list[str],
    target_col: str,
) -> tuple[pd.DataFrame, pd.Series]:
    x = frame[feature_cols + categorical_cols].copy()
    for column in categorical_cols:
        x[column] = x[column].astype("category")
    y = frame[target_col].astype(float)
    return x, y


def ensure_prediction_table(conn) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {PREDICTION_TABLE} (
            season INTEGER NOT NULL,
            as_of_date TEXT NOT NULL,
            team TEXT NOT NULL,
            player_name TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT '',
            predicted_era_final REAL NOT NULL DEFAULT 0,
            predicted_whip_final REAL NOT NULL DEFAULT 0,
            predicted_war_final REAL NOT NULL DEFAULT 0,
            ip_to_date REAL NOT NULL DEFAULT 0,
            so_to_date REAL NOT NULL DEFAULT 0,
            confidence_score REAL NOT NULL DEFAULT 0,
            confidence_level TEXT NOT NULL DEFAULT '',
            model_source TEXT NOT NULL DEFAULT 'pitcher_lgbm',
            UNIQUE (season, as_of_date, team, player_name)
        )
        """
    )
    conn.commit()


def upsert_predictions(conn, frame: pd.DataFrame) -> int:
    ensure_prediction_table(conn)
    rows = [
        (
            int(row.season),
            str(row.as_of_date),
            str(row.team),
            str(row.player_name),
            str(row.role),
            float(row.predicted_era_final),
            float(row.predicted_whip_final),
            float(row.predicted_war_final),
            float(row.IP),
            float(row.SO_cum if "SO_cum" in frame.columns else row.SO_final if "SO_final" in frame.columns else 0),
            float(row.confidence_score),
            str(row.confidence_level),
            str(row.model_source),
        )
        for row in frame.itertuples(index=False)
    ]
    before = getattr(conn, "total_changes", 0)
    executemany(
        conn,
        f"""
        INSERT INTO {PREDICTION_TABLE} (
            season, as_of_date, team, player_name, role,
            predicted_era_final, predicted_whip_final, predicted_war_final,
            ip_to_date, so_to_date, confidence_score, confidence_level, model_source
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(season, as_of_date, team, player_name) DO UPDATE SET
            role=excluded.role,
            predicted_era_final=excluded.predicted_era_final,
            predicted_whip_final=excluded.predicted_whip_final,
            predicted_war_final=excluded.predicted_war_final,
            ip_to_date=excluded.ip_to_date,
            so_to_date=excluded.so_to_date,
            confidence_score=excluded.confidence_score,
            confidence_level=excluded.confidence_level,
            model_source=excluded.model_source
        """,
        rows,
    )
    conn.commit()
    return (getattr(conn, "total_changes", 0) - before) if not is_postgres(conn) else len(rows)
