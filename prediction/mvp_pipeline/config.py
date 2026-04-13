from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent
ARTIFACT_DIR = ROOT_DIR / "artifacts"
MODEL_DIR = ARTIFACT_DIR / "models"
PREDICTION_DIR = ARTIFACT_DIR / "predictions"


@dataclass(slots=True)
class DataConfig:
    player_id_col: str = "player_id"
    date_col: str = "game_date"
    team_col: str = "team"
    season_col: str = "season"
    age_col: str = "age"
    # ---------------------------------------------------------------------------
    # Sampling policy (used by HitterFeatureBuilder.build_training_samples)
    # ---------------------------------------------------------------------------
    # sampling_mode = "interval"     : sample every sample_every_n_games games
    #               = "checkpoints"  : sample only at sample_game_checkpoints game milestones
    sampling_mode: str = "interval"
    sample_every_n_games: int = 5
    sample_game_checkpoints: tuple[int, ...] = (20, 40, 60, 80, 100, 120)
    min_pa_threshold: int = 20
    # ---------------------------------------------------------------------------
    valid_start_date: str = "2025-08-01"
    test_start_date: str = "2025-09-01"


@dataclass(slots=True)
class HitterFeatureConfig:
    rolling_windows: tuple[int, ...] = (5, 10, 20)
    regression_pa_k: int = 100
    hr_blend_pa_k: int = 140
    hr_projection_pa_k: int = 180
    ops_blend_pa_k: int = 100
    ops_projection_pa_k: int = 220
    war_blend_pa_k: int = 120
    war_projection_pa_k: int = 260
    avg_blend_pa_k: int = 80
    avg_projection_pa_k: int = 160
    ops_final_min: float = 0.45
    ops_final_max: float = 1.05
    hr_final_max: float = 55.0
    war_final_min: float = -1.0
    war_final_max: float = 12.0
    avg_final_min: float = 0.100
    avg_final_max: float = 0.450
    categorical_cols: tuple[str, ...] = ("team",)
    numeric_cols: tuple[str, ...] = (
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
    )
    target_cols: tuple[str, ...] = ("OPS_final", "HR_final", "WAR_final", "AVG_final")


@dataclass(slots=True)
class PitcherFeatureConfig:
    rolling_windows: tuple[int, ...] = (3, 5)
    regression_bf_k: int = 120
    categorical_cols: tuple[str, ...] = ("team", "role")


@dataclass(slots=True)
class TrainConfig:
    random_state: int = 42
    n_estimators: int = 400
    learning_rate: float = 0.05
    num_leaves: int = 31
    min_child_samples: int = 30
    subsample: float = 0.8
    colsample_bytree: float = 0.8
    early_stopping_rounds: int = 50


@dataclass(slots=True)
class AppConfig:
    data: DataConfig = field(default_factory=DataConfig)
    hitter: HitterFeatureConfig = field(default_factory=HitterFeatureConfig)
    pitcher: PitcherFeatureConfig = field(default_factory=PitcherFeatureConfig)
    train: TrainConfig = field(default_factory=TrainConfig)
    artifact_dir: Path = ARTIFACT_DIR
    model_dir: Path = MODEL_DIR
    prediction_dir: Path = PREDICTION_DIR

    def ensure_dirs(self) -> None:
        self.artifact_dir.mkdir(parents=True, exist_ok=True)
        self.model_dir.mkdir(parents=True, exist_ok=True)
        self.prediction_dir.mkdir(parents=True, exist_ok=True)

    def to_dict(self) -> dict:
        data = asdict(self)
        data["artifact_dir"] = str(self.artifact_dir)
        data["model_dir"] = str(self.model_dir)
        data["prediction_dir"] = str(self.prediction_dir)
        return data


def get_config() -> AppConfig:
    config = AppConfig()
    config.ensure_dirs()
    return config
