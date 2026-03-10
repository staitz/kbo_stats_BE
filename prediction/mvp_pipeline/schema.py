"""
schema.py — Hitter model schema contract enforcement
=====================================================

This module defines :class:`ModelSchema`, which captures the exact feature
contract produced at training time, and :func:`validate_input`, which
enforces that contract at inference time.

Design principle: "explicit failure + optional relaxation" over "silent fill".
"""
from __future__ import annotations

import json
import sys
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

# ── Versioning ─────────────────────────────────────────────────────────────
SCHEMA_VERSION = "1.0"

# Central model version constant.  Increment this whenever the feature schema
# or model architecture changes in a way that is incompatible with old predictions.
# Both train.py and predict.py import this — it is the SINGLE SOURCE OF TRUTH.
MODEL_VERSION = "hitter_mvp_v2"


# ── Exceptions ─────────────────────────────────────────────────────────────

class SchemaValidationError(ValueError):
    """Raised when inference input is missing required features in strict mode.

    The error message lists the missing column names and the path to the
    schema file so operators can diagnose the root cause quickly.
    """


# ── ModelSchema dataclass ──────────────────────────────────────────────────

@dataclass
class ModelSchema:
    """Immutable snapshot of the feature contract produced at train time.

    Saved to ``schema.json`` alongside the ``.pkl`` model files and loaded
    by the prediction pipeline before every inference run.
    """
    schema_version: str
    model_version: str                 # e.g. "hitter_mvp_v2" — matches DB model_version column
    feature_cols: list[str]            # Ordered list used for model.predict()
    categorical_cols: list[str]        # Subset of feature_cols cast to 'category'
    feature_dtypes: dict[str, str]     # col -> pandas dtype string at training time (reference)
    target_cols: list[str]             # e.g. ["OPS_final", "HR_final", "WAR_final"]
    training_season: int               # Season whose data produced this schema
    trained_at: str                    # ISO-8601 timestamp
    sampling_config: dict[str, Any] = field(default_factory=dict)
    schema_source: str = "mvp_pipeline"

    # ── Persistence ────────────────────────────────────────────────────────

    def save(self, path: str | Path) -> None:
        """Serialise schema to a JSON file."""
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(asdict(self), ensure_ascii=True, indent=2), encoding="utf-8")

    @classmethod
    def load(cls, path: str | Path) -> "ModelSchema":
        """Deserialise schema from a JSON file."""
        p = Path(path)
        if not p.exists():
            raise FileNotFoundError(
                f"[schema] schema.json not found at {p}. "
                "Run `python -m prediction.mvp_pipeline.train` to generate it."
            )
        data = json.loads(p.read_text(encoding="utf-8"))
        return cls(**data)

    @classmethod
    def try_load(cls, path: str | Path) -> "ModelSchema | None":
        """Like :meth:`load` but returns *None* instead of raising on missing file."""
        try:
            return cls.load(path)
        except FileNotFoundError:
            return None


# ── Schema builder (called from train.py) ─────────────────────────────────

def build_schema(
    feature_cols: list[str],
    categorical_cols: list[str],
    target_cols: list[str],
    training_season: int,
    sample_df: pd.DataFrame,
    sampling_config: dict[str, Any],
    model_version: str = MODEL_VERSION,
) -> ModelSchema:
    """Construct a :class:`ModelSchema` from training artefacts."""
    # Collect dtype strings from the training sample matrix
    feature_dtypes = {}
    for col in feature_cols:
        if col in sample_df.columns:
            feature_dtypes[col] = str(sample_df[col].dtype)
        else:
            feature_dtypes[col] = "unknown"

    return ModelSchema(
        schema_version=SCHEMA_VERSION,
        model_version=model_version,
        feature_cols=feature_cols,
        categorical_cols=categorical_cols,
        feature_dtypes=feature_dtypes,
        target_cols=target_cols,
        training_season=training_season,
        trained_at=datetime.now(timezone.utc).isoformat(),
        sampling_config=sampling_config,
    )


# ── Validation (called from predict.py) ───────────────────────────────────

def validate_input(
    x_df: pd.DataFrame,
    schema: ModelSchema,
    allow_missing: bool = False,
    schema_path: str | Path | None = None,
) -> pd.DataFrame:
    """Validate and align inference DataFrame against the training schema.

    Steps
    -----
    1. Detect missing, extra, and dtype-mismatched columns.
    2. Log all findings to stderr.
    3. If any *required* columns are missing and ``allow_missing=False``,
       raise :class:`SchemaValidationError`.
    4. If ``allow_missing=True``, fill missing columns with 0 and continue.
    5. Reindex to ``schema.feature_cols`` (enforces column order).
    6. Cast categorical columns to ``category`` dtype.

    Args:
        x_df:           Inference feature DataFrame (pre-model-predict).
        schema:         Loaded :class:`ModelSchema`.
        allow_missing:  If *True*, fill missing features with 0 and WARN.
                        If *False* (default/strict), raise on any missing feature.
        schema_path:    Optional path displayed in error messages for debuggability.

    Returns:
        Aligned, validated DataFrame ready for ``model.predict()``.
    """
    required = set(schema.feature_cols)
    present  = set(x_df.columns)

    missing = sorted(required - present)
    extra   = sorted(present  - required)

    path_hint = f" (schema: {schema_path})" if schema_path else ""

    # ── 0. Version check ──────────────────────────────────────────────────
    if schema.schema_version != SCHEMA_VERSION:
        print(
            f"[schema] WARNING: schema_version mismatch — "
            f"schema={schema.schema_version!r}, runtime={SCHEMA_VERSION!r}. "
            "Re-train to refresh.",
            file=sys.stderr,
        )

    # ── 1. Missing features ───────────────────────────────────────────────
    if missing:
        print(f"[schema] MISSING features ({len(missing)}): {missing}{path_hint}", file=sys.stderr)
        if not allow_missing:
            raise SchemaValidationError(
                f"{len(missing)} required feature(s) missing from inference input: "
                f"{missing}.\n"
                "  → Use --allow-missing-features to fill with 0 and continue.\n"
                f"  → Or re-run training: python -m prediction.mvp_pipeline.train"
            )
        # Relaxed: fill missing with 0
        print(
            f"[schema] WARNING: MISSING features filled with 0: {missing}\n"
            "[schema]   allow_missing_features=True — prediction quality may be degraded.",
            file=sys.stderr,
        )
        for col in missing:
            x_df = x_df.copy()
            x_df[col] = 0.0

    # ── 2. Extra features ─────────────────────────────────────────────────
    if extra:
        print(
            f"[schema] INFO: {len(extra)} extra column(s) not in schema (will be dropped): {extra}",
            file=sys.stderr,
        )

    # ── 3. dtype drift (informational) ───────────────────────────────────
    dtype_drifts = []
    for col in schema.feature_cols:
        if col in x_df.columns and col in schema.feature_dtypes:
            expected = schema.feature_dtypes[col]
            actual   = str(x_df[col].dtype)
            # Ignore minor float/int width differences (float32 vs float64 etc.)
            if expected.rstrip("0123456789") != actual.rstrip("0123456789"):
                dtype_drifts.append((col, expected, actual))
    if dtype_drifts:
        for col, exp, act in dtype_drifts:
            print(
                f"[schema] dtype drift on '{col}': trained={exp!r}, inference={act!r} (coerced)",
                file=sys.stderr,
            )

    # ── 4. No drift? log OK ───────────────────────────────────────────────
    if not missing and not extra and not dtype_drifts:
        print(
            f"[schema] OK — {len(schema.feature_cols)} features, "
            f"{len(schema.categorical_cols)} categorical, no drift detected.",
            file=sys.stderr,
        )

    # ── 5. Reindex to schema order ────────────────────────────────────────
    aligned = x_df.reindex(columns=schema.feature_cols)

    # ── 6. Cast categoricals ─────────────────────────────────────────────
    for col in schema.categorical_cols:
        if col in aligned.columns:
            aligned[col] = aligned[col].astype("category")

    return aligned
