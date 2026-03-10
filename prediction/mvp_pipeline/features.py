from __future__ import annotations

import sys
from dataclasses import dataclass
from typing import Iterable

import numpy as np
import pandas as pd

from .config import AppConfig, get_config


def optimize_numeric_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in out.select_dtypes(include=["int64"]).columns:
        out[col] = pd.to_numeric(out[col], downcast="integer")
    for col in out.select_dtypes(include=["float64"]).columns:
        out[col] = pd.to_numeric(out[col], downcast="float")
    return out


def regressed_rate(rate: pd.Series, exposure: pd.Series, league_rate: float, k: int) -> pd.Series:
    return ((exposure * rate) + (k * league_rate)) / (exposure + k)


def _safe_divide(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    return numerator / denominator.replace(0, np.nan)


def _safe_scalar_divide(numerator: float, denominator: float) -> float:
    if denominator == 0:
        return 0.0
    return float(numerator) / float(denominator)


@dataclass(slots=True)
class FeatureArtifacts:
    feature_df: pd.DataFrame
    feature_cols: list[str]
    categorical_cols: list[str]


@dataclass(slots=True)
class TrainingSampleResult:
    """Returned by HitterFeatureBuilder.build_training_samples().

    Attributes:
        sample_df:  The sampled training DataFrame (one row per player-game snapshot).
        artifacts:  FeatureArtifacts from the underlying build_daily_features() call,
                    so callers can reuse ``feature_cols`` without a second build.
    """
    sample_df: pd.DataFrame
    artifacts: FeatureArtifacts


# ---------------------------------------------------------------------------
# Sample report helper
# ---------------------------------------------------------------------------

def _print_sample_report(samples: pd.DataFrame, mode: str, n_games: int, min_pa: int) -> None:
    """Print a sampling statistics report to stdout."""
    total = len(samples)
    print(f"[sampling] mode={mode}  n_games={n_games}  min_pa={min_pa}", file=sys.stderr)
    print(f"[sampling] total_samples={total}", file=sys.stderr)

    if "season" in samples.columns:
        print("[sampling] season_distribution:", file=sys.stderr)
        for season, cnt in samples.groupby("season").size().items():
            print(f"  {season}: {cnt} samples", file=sys.stderr)

    if "game_no" in samples.columns:
        bins = list(range(0, int(samples["game_no"].max()) + 21, 20))
        labels = [f"{bins[i]+1}-{bins[i+1]}" for i in range(len(bins) - 1)]
        cuts = pd.cut(samples["game_no"], bins=bins, labels=labels, right=True)
        dist = cuts.value_counts().sort_index()
        print("[sampling] game_no_distribution:", file=sys.stderr)
        for bracket, cnt in dist.items():
            if cnt > 0:
                print(f"  games [{bracket}]: {cnt} samples", file=sys.stderr)



class HitterFeatureBuilder:
    def __init__(self, config: AppConfig | None = None) -> None:
        self.config = config or get_config()

    def build_daily_features(self, game_logs: pd.DataFrame) -> FeatureArtifacts:
        required = [
            "season",
            "game_date",
            "player_name",
            "team",
            "PA",
            "AB",
            "H",
            "2B",
            "3B",
            "HR",
            "BB",
            "SO",
            "HBP",
            "SF",
            "TB",
        ]
        missing = [col for col in required if col not in game_logs.columns]
        if missing:
            raise ValueError(f"Missing hitter columns: {missing}")

        df = game_logs.copy()
        df["game_date"] = pd.to_datetime(df["game_date"])
        df["player_key"] = df["team"].astype(str) + "::" + df["player_name"].astype(str)

        numeric_base_cols = ["PA", "AB", "H", "2B", "3B", "HR", "BB", "SO", "HBP", "SF", "TB"]
        for col in numeric_base_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        if "age" not in df.columns:
            df["age"] = 27
            df["age_missing"] = 1
        else:
            df["age_missing"] = df["age"].isna().astype("int8")
            df["age"] = pd.to_numeric(df["age"], errors="coerce").fillna(27)

        if "game_ops" not in df.columns:
            obp_den = (df["AB"] + df["BB"] + df["HBP"] + df["SF"]).replace(0, np.nan)
            df["game_ops"] = (((df["H"] + df["BB"] + df["HBP"]) / obp_den) + (df["TB"] / df["AB"].replace(0, np.nan))).fillna(0.0)
        else:
            df["game_ops"] = pd.to_numeric(df["game_ops"], errors="coerce").fillna(0.0)
        if "war_game" not in df.columns:
            df["war_game"] = (((df["game_ops"] - 0.700) * df["PA"]) / 70.0).clip(lower=-0.3, upper=0.6).fillna(0.0)
        else:
            df["war_game"] = pd.to_numeric(df["war_game"], errors="coerce").fillna(0.0)

        optional_defaults = {
            "home_game": 0,
            "park_factor": 1.0,
            "opponent_pitching_strength": 1.0,
            "batting_order": 5,
        }
        for col, default in optional_defaults.items():
            if col not in df.columns:
                df[col] = default

        df = df.sort_values(["player_key", "game_date", "team"]).reset_index(drop=True)
        grp = df.groupby("player_key", sort=False, group_keys=False)
        df["game_no"] = grp.cumcount() + 1

        cum_cols = ["PA", "AB", "H", "2B", "3B", "HR", "BB", "SO", "HBP", "SF", "TB", "war_game"]
        for col in cum_cols:
            df[f"{col}_cum"] = grp[col].cumsum()

        df["OBP_to_date"] = _safe_divide(
            df["H_cum"] + df["BB_cum"] + df["HBP_cum"],
            df["AB_cum"] + df["BB_cum"] + df["HBP_cum"] + df["SF_cum"],
        ).fillna(0.0)
        df["SLG_to_date"] = _safe_divide(df["TB_cum"], df["AB_cum"]).fillna(0.0)
        df["OPS_to_date"] = df["OBP_to_date"] + df["SLG_to_date"]
        df["BB_rate_to_date"] = _safe_divide(df["BB_cum"], df["PA_cum"]).fillna(0.0)
        df["K_rate_to_date"] = _safe_divide(df["SO_cum"], df["PA_cum"]).fillna(0.0)
        df["ISO_to_date"] = df["SLG_to_date"] - _safe_divide(df["H_cum"], df["AB_cum"]).fillna(0.0)
        babip_num = df["H_cum"] - df["HR_cum"]
        babip_den = df["AB_cum"] - df["SO_cum"] - df["HR_cum"] + df["SF_cum"]
        df["BABIP_to_date"] = _safe_divide(babip_num, babip_den).fillna(0.0)
        df["WAR_to_date"] = df["war_game_cum"]
        df["age_squared"] = df["age"] * df["age"]
        df["days_since_season_start"] = (
            df["game_date"] - df.groupby("season")["game_date"].transform("min")
        ).dt.days.astype("int16")

        league_obp = _safe_scalar_divide(
            df["H"].sum() + df["BB"].sum() + df["HBP"].sum(),
            (df["AB"] + df["BB"] + df["HBP"] + df["SF"]).sum(),
        )
        league_slg = _safe_scalar_divide(df["TB"].sum(), df["AB"].sum())
        league_ops = float(league_obp + league_slg)
        league_bb = _safe_scalar_divide(df["BB"].sum(), df["PA"].sum())
        league_k = _safe_scalar_divide(df["SO"].sum(), df["PA"].sum())

        k = self.config.hitter.regression_pa_k
        df["regressed_ops"] = regressed_rate(df["OPS_to_date"], df["PA_cum"], league_ops, k)
        df["regressed_bb_rate"] = regressed_rate(df["BB_rate_to_date"], df["PA_cum"], league_bb, k)
        df["regressed_k_rate"] = regressed_rate(df["K_rate_to_date"], df["PA_cum"], league_k, k)

        for window in self.config.hitter.rolling_windows:
            df[f"recent_{window}_ops"] = (
                df.groupby("player_key")["game_ops"]
                .transform(lambda x, w=window: x.shift(1).rolling(w, min_periods=2).mean())
            )
            bb_roll = (
                df.groupby("player_key")["BB"]
                .transform(lambda x, w=window: x.shift(1).rolling(w, min_periods=2).sum())
            )
            so_roll = (
                df.groupby("player_key")["SO"]
                .transform(lambda x, w=window: x.shift(1).rolling(w, min_periods=2).sum())
            )
            pa_roll = (
                df.groupby("player_key")["PA"]
                .transform(lambda x, w=window: x.shift(1).rolling(w, min_periods=2).sum())
            )
            df[f"recent_{window}_bb_rate"] = _safe_divide(bb_roll, pa_roll)
            df[f"recent_{window}_k_rate"] = _safe_divide(so_roll, pa_roll)

        df["ops_trend"] = df["recent_5_ops"] - df["recent_20_ops"]
        df["bb_trend"] = df["recent_5_bb_rate"] - df["BB_rate_to_date"]
        df["k_trend"] = df["recent_5_k_rate"] - df["K_rate_to_date"]
        df["home_game_ratio"] = grp["home_game"].expanding().mean().reset_index(level=0, drop=True)
        df["park_factor_avg"] = grp["park_factor"].expanding().mean().reset_index(level=0, drop=True)
        df["opponent_pitching_strength_avg"] = grp["opponent_pitching_strength"].expanding().mean().reset_index(level=0, drop=True)

        feature_cols = [
            "game_no",
            "PA_cum",
            "AB_cum",
            "H_cum",
            "HR_cum",
            "BB_cum",
            "SO_cum",
            "OPS_to_date",
            "BB_rate_to_date",
            "K_rate_to_date",
            "ISO_to_date",
            "BABIP_to_date",
            "WAR_to_date",
            "regressed_ops",
            "regressed_bb_rate",
            "regressed_k_rate",
            "recent_5_ops",
            "recent_10_ops",
            "recent_20_ops",
            "recent_5_bb_rate",
            "recent_10_bb_rate",
            "recent_5_k_rate",
            "recent_10_k_rate",
            "ops_trend",
            "bb_trend",
            "k_trend",
            "home_game_ratio",
            "park_factor_avg",
            "opponent_pitching_strength_avg",
            "batting_order",
            "age",
            "age_squared",
            "age_missing",
            "days_since_season_start",
        ]

        for col in self.config.hitter.categorical_cols:
            if col in df.columns:
                df[col] = df[col].astype("category")

        out = optimize_numeric_dtypes(df)
        return FeatureArtifacts(
            feature_df=out,
            feature_cols=feature_cols,
            categorical_cols=list(self.config.hitter.categorical_cols),
        )

    def build_training_samples(
        self,
        game_logs: pd.DataFrame,
        report: bool = True,
    ) -> TrainingSampleResult:
        """Build training samples from raw game logs using the configured sampling policy.

        Sampling policy (controlled via ``AppConfig.data``):

        * ``sampling_mode = "interval"`` (default): one snapshot every
          ``sample_every_n_games`` games for each player.
        * ``sampling_mode = "checkpoints"``: snapshots only at the game counts
          listed in ``sample_game_checkpoints`` (e.g. 20, 40, 60, …).

        In both modes, rows where ``PA_cum < min_pa_threshold`` are excluded.

        Args:
            game_logs:  Raw hitter game log DataFrame.
            report:     If *True*, print a sampling statistics report to stderr.

        Returns:
            :class:`TrainingSampleResult` with ``sample_df`` (the sampled rows)
            and ``artifacts`` (the :class:`FeatureArtifacts` produced by
            :meth:`build_daily_features`, reusable by the caller to avoid a
            redundant second build).
        """
        artifacts = self.build_daily_features(game_logs)
        df = artifacts.feature_df.copy()

        cfg = self.config.data
        mode = cfg.sampling_mode
        min_pa = cfg.min_pa_threshold

        # ── PA gate (applied in both modes) ───────────────────────────────────
        pa_mask = df["PA_cum"] >= min_pa

        if mode == "checkpoints":
            checkpoints = set(cfg.sample_game_checkpoints)
            sample_mask = pa_mask & df["game_no"].isin(checkpoints)
        else:
            # Default: "interval"
            n = cfg.sample_every_n_games
            sample_mask = pa_mask & ((df["game_no"] % n) == 0)

        # ── Final-season stats for labels ─────────────────────────────────────
        final_df = (
            df.groupby("player_key", as_index=False)
            .agg(
                team=("team", "last"),
                player_name=("player_name", "last"),
                OPS_final=("OPS_to_date", "last"),
                HR_final=("HR_cum", "last"),
                WAR_final=("WAR_to_date", "last"),
                PA_final=("PA_cum", "last"),
            )
        )

        samples = df.loc[sample_mask].copy()
        samples = samples.merge(final_df, on=["player_key", "team", "player_name"], how="left")
        samples["sample_date"] = samples["game_date"].dt.strftime("%Y-%m-%d")
        samples["OPS_ros"] = samples["OPS_final"] - samples["OPS_to_date"]
        samples["HR_ros"] = samples["HR_final"] - samples["HR_cum"]
        samples["WAR_ros"] = samples["WAR_final"] - samples["WAR_to_date"]
        samples = optimize_numeric_dtypes(samples)

        if report:
            _print_sample_report(
                samples,
                mode=mode,
                n_games=cfg.sample_every_n_games,
                min_pa=min_pa,
            )

        return TrainingSampleResult(sample_df=samples, artifacts=artifacts)


class PitcherFeatureBuilder:
    def __init__(self, config: AppConfig | None = None) -> None:
        self.config = config or get_config()

    def build_daily_features(self, game_logs: pd.DataFrame) -> FeatureArtifacts:
        required = ["season", "game_date", "player_id", "team", "role", "age", "IP", "ER", "BB", "SO", "H"]
        missing = [col for col in required if col not in game_logs.columns]
        if missing:
            raise ValueError(f"Missing pitcher columns: {missing}")

        df = game_logs.copy()
        df["game_date"] = pd.to_datetime(df["game_date"])
        df = df.sort_values(["player_id", "game_date"]).reset_index(drop=True)
        grp = df.groupby("player_id", sort=False, group_keys=False)

        df["game_no"] = grp.cumcount() + 1
        for col in ["IP", "ER", "BB", "SO", "H"]:
            df[f"{col}_cum"] = grp[col].cumsum()
        df["ERA_to_date"] = (_safe_divide(df["ER_cum"] * 9.0, df["IP_cum"])).fillna(0.0)
        df["WHIP_to_date"] = (_safe_divide(df["BB_cum"] + df["H_cum"], df["IP_cum"])).fillna(0.0)
        df["K9_to_date"] = (_safe_divide(df["SO_cum"] * 9.0, df["IP_cum"])).fillna(0.0)
        df["age_squared"] = df["age"] * df["age"]

        feature_cols = ["game_no", "IP_cum", "ERA_to_date", "WHIP_to_date", "K9_to_date", "age", "age_squared"]
        for col in self.config.pitcher.categorical_cols:
            if col in df.columns:
                df[col] = df[col].astype("category")

        return FeatureArtifacts(
            feature_df=optimize_numeric_dtypes(df),
            feature_cols=feature_cols,
            categorical_cols=list(self.config.pitcher.categorical_cols),
        )


def make_train_valid_test_split(
    sample_df: pd.DataFrame,
    valid_start_date: str,
    test_start_date: str,
    date_col: str = "sample_date",
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    valid_ts = pd.Timestamp(valid_start_date)
    test_ts = pd.Timestamp(test_start_date)
    dates = pd.to_datetime(sample_df[date_col])
    train_df = sample_df.loc[dates < valid_ts].copy()
    valid_df = sample_df.loc[(dates >= valid_ts) & (dates < test_ts)].copy()
    test_df = sample_df.loc[dates >= test_ts].copy()
    return train_df, valid_df, test_df


def prepare_model_matrix(
    df: pd.DataFrame,
    feature_cols: Iterable[str],
    categorical_cols: Iterable[str],
    target_col: str,
) -> tuple[pd.DataFrame, pd.Series]:
    x = df.loc[:, list(feature_cols)].copy()
    for col in categorical_cols:
        if col in x.columns:
            x[col] = x[col].astype("category")
    y = df[target_col].astype("float32")
    return x, y
