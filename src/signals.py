"""
Signal generation: composite scoring and regime classification.

Translates engineered features into actionable, interpretable signals
that a portfolio manager could use as a first-pass decision filter.
"""

import logging

import numpy as np
import pandas as pd
from scipy.stats import zscore

logger = logging.getLogger(__name__)


def _safe_zscore(series: pd.Series) -> pd.Series:
    """Z-score that handles constant series gracefully."""
    if series.std() == 0:
        return pd.Series(0.0, index=series.index)
    return pd.Series(zscore(series, nan_policy="omit"), index=series.index)


def score_signals(features_df: pd.DataFrame, weights: dict) -> pd.Series:
    """
    Weighted composite score from normalised features.

    Higher score = more defensive / risk-off environment.
    Components (all z-scored so they're comparable):
      - yield_curve_spread  (inverted: negative spread -> high z)
      - credit_spread       (wider -> higher z)
      - volatility          (higher -> higher z)
      - momentum_30d        (inverted: negative momentum -> high z)
    """
    z_spread = -_safe_zscore(features_df["yield_curve_spread"])
    z_credit = _safe_zscore(features_df["credit_spread"])
    z_vol = _safe_zscore(features_df["volatility"])

    mom_col = "momentum_30d"
    if mom_col in features_df.columns:
        z_mom = -_safe_zscore(features_df[mom_col])
    else:
        z_mom = pd.Series(0.0, index=features_df.index)

    score = (
        weights.get("spread", 0.4) * z_spread
        + weights.get("spread", 0.4) * z_credit
        + weights.get("volatility", 0.3) * z_vol
        + weights.get("momentum", 0.3) * z_mom
    )
    return score.rename("risk_score")


def classify_regime(
    features_df: pd.DataFrame,
    thresholds: dict,
) -> pd.Series:
    """
    Assign a categorical regime label to each observation.

    Priority order (first match wins):
      1. Recession Risk  – yield curve inverted
      2. Credit Stress   – HY spread above stress threshold
      3. Risk On         – positive momentum AND tight credit
      4. Neutral
    """
    yc = features_df["yield_curve_spread"]
    cs = features_df["credit_spread"]
    mom = features_df.get("momentum_30d", pd.Series(0.0, index=features_df.index))

    yc_thresh = thresholds.get("yield_curve_inversion", 0.0)
    cs_thresh = thresholds.get("credit_spread_stress", 5.0)
    mom_thresh = thresholds.get("momentum_positive", 0.0)

    regime = pd.Series("Neutral", index=features_df.index, dtype="object")
    regime[mom > mom_thresh] = "Risk On"
    regime[cs > cs_thresh] = "Credit Stress"
    regime[yc < yc_thresh] = "Recession Risk"

    counts = regime.value_counts()
    logger.info("Regime distribution:\n%s", counts.to_string())
    return regime.rename("regime")


def generate_signals(
    features_df: pd.DataFrame,
    config: dict,
) -> pd.DataFrame:
    """Orchestrate scoring and classification, return enriched DataFrame."""
    sig_cfg = config["signals"]

    score = score_signals(features_df, sig_cfg["weights"])
    regime = classify_regime(features_df, sig_cfg["thresholds"])

    signals_df = features_df.copy()
    signals_df["risk_score"] = score
    signals_df["regime"] = regime

    logger.info("Signal generation complete — %d observations", len(signals_df))
    return signals_df
