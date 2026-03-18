from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent
ARTIFACT_DIR = ROOT_DIR / "artifacts"
MODEL_DIR = ARTIFACT_DIR / "models"
PREDICTION_DIR = ARTIFACT_DIR / "predictions"


@dataclass(slots=True)
class DataConfig:
    sample_every_n_games: int = 3
    min_outs_threshold: int = 9
    valid_start_date: str = "2025-08-01"
    test_start_date: str = "2025-09-01"
    season_length_games: int = 144


@dataclass(slots=True)
class PitcherFeatureConfig:
    categorical_cols: tuple[str, ...] = ("team", "role")
    target_cols: tuple[str, ...] = ("ERA_final", "WHIP_final", "WAR_final")


@dataclass(slots=True)
class TrainConfig:
    random_state: int = 42
    n_estimators: int = 300
    learning_rate: float = 0.05
    num_leaves: int = 31
    min_child_samples: int = 20
    subsample: float = 0.8
    colsample_bytree: float = 0.8
    early_stopping_rounds: int = 30


@dataclass(slots=True)
class AppConfig:
    data: DataConfig = field(default_factory=DataConfig)
    pitcher: PitcherFeatureConfig = field(default_factory=PitcherFeatureConfig)
    train: TrainConfig = field(default_factory=TrainConfig)
    artifact_dir: Path = ARTIFACT_DIR
    model_dir: Path = MODEL_DIR
    prediction_dir: Path = PREDICTION_DIR

    def ensure_dirs(self) -> None:
        self.artifact_dir.mkdir(parents=True, exist_ok=True)
        self.model_dir.mkdir(parents=True, exist_ok=True)
        self.prediction_dir.mkdir(parents=True, exist_ok=True)


def get_config() -> AppConfig:
    config = AppConfig()
    config.ensure_dirs()
    return config
