"""
Feature engineering for fixed income signal generation.

Computes yield-curve spread, credit-spread proxy, price momentum,
and rolling volatility from the cleaned dataset.
"""

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def compute_yield_curve_spread(df: pd.DataFrame) -> pd.Series:
    """10Y Treasury minus 2Y Treasury (negative = inverted)."""
    return df["treasury_10y"] - df["treasury_2y"]


def compute_credit_spread(df: pd.DataFrame) -> pd.Series:
    """
    High-yield OAS spread.

    If the direct FRED HY series is available use it; otherwise fall back to
    an ETF-implied proxy (HYG total-return vs LQD total-return).
    """
    if "hy_spread" in df.columns and df["hy_spread"].notna().sum() > 0:
        return df["hy_spread"]

    if {"hyg_close", "lqd_close"}.issubset(df.columns):
        hyg_ret = df["hyg_close"].pct_change(30)
        lqd_ret = df["lqd_close"].pct_change(30)
        return (lqd_ret - hyg_ret) * 100  # positive = HY under-performing

    raise KeyError("Need either 'hy_spread' or both 'hyg_close'/'lqd_close'")


def compute_momentum(df: pd.DataFrame, windows: list[int]) -> pd.DataFrame:
    """Rolling price returns for TLT over each window (in trading days)."""
    out = pd.DataFrame(index=df.index)
    col = "tlt_close"
    if col not in df.columns:
        logger.warning("TLT price not available; skipping momentum")
        return out
    for w in windows:
        out[f"momentum_{w}d"] = df[col].pct_change(w) * 100
    return out


def compute_volatility(df: pd.DataFrame, window: int) -> pd.Series:
    """Annualised rolling volatility of TLT daily returns."""
    col = "tlt_close"
    if col not in df.columns:
        logger.warning("TLT price not available; skipping volatility")
        return pd.Series(dtype=float)
    daily_ret = df[col].pct_change()
    return daily_ret.rolling(window).std() * np.sqrt(252) * 100


def build_feature_matrix(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """Assemble all features into a single DataFrame."""
    feat_cfg = config["features"]

    features = df.copy()
    features["yield_curve_spread"] = compute_yield_curve_spread(df)
    features["credit_spread"] = compute_credit_spread(df)

    momentum = compute_momentum(df, feat_cfg["momentum_windows"])
    features = features.join(momentum)

    features["volatility"] = compute_volatility(df, feat_cfg["volatility_window"])

    features.dropna(
        subset=["yield_curve_spread", "credit_spread", "volatility"],
        inplace=True,
    )

    logger.info(
        "Feature matrix: %d rows, columns: %s",
        len(features), list(features.columns),
    )
    return features
