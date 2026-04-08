"""Unit tests for src/features.py"""

import numpy as np
import pandas as pd
import pytest

from src.features import (
    compute_credit_spread,
    compute_momentum,
    compute_volatility,
    compute_yield_curve_spread,
)


@pytest.fixture
def sample_df():
    dates = pd.bdate_range("2024-01-02", periods=100)
    rng = np.random.default_rng(42)
    return pd.DataFrame(
        {
            "treasury_10y": 4.0 + rng.normal(0, 0.1, 100).cumsum() * 0.01,
            "treasury_2y": 4.5 + rng.normal(0, 0.1, 100).cumsum() * 0.01,
            "hy_spread": 4.0 + rng.normal(0, 0.2, 100).cumsum() * 0.01,
            "tlt_close": 90 + rng.normal(0, 0.5, 100).cumsum(),
            "lqd_close": 110 + rng.normal(0, 0.3, 100).cumsum(),
            "hyg_close": 75 + rng.normal(0, 0.4, 100).cumsum(),
        },
        index=dates,
    )


class TestYieldCurveSpread:
    def test_positive_spread(self, sample_df):
        sample_df["treasury_10y"] = 5.0
        sample_df["treasury_2y"] = 3.0
        spread = compute_yield_curve_spread(sample_df)
        assert (spread == 2.0).all()

    def test_negative_spread_inversion(self, sample_df):
        sample_df["treasury_10y"] = 3.0
        sample_df["treasury_2y"] = 4.5
        spread = compute_yield_curve_spread(sample_df)
        assert (spread < 0).all()

    def test_zero_spread(self, sample_df):
        sample_df["treasury_10y"] = 4.0
        sample_df["treasury_2y"] = 4.0
        spread = compute_yield_curve_spread(sample_df)
        assert (spread == 0).all()


class TestCreditSpread:
    def test_uses_hy_spread_when_available(self, sample_df):
        result = compute_credit_spread(sample_df)
        pd.testing.assert_series_equal(result, sample_df["hy_spread"])

    def test_fallback_to_etf_proxy(self, sample_df):
        df = sample_df.drop(columns=["hy_spread"])
        result = compute_credit_spread(df)
        assert len(result) == len(df)

    def test_raises_when_no_data(self, sample_df):
        df = sample_df.drop(columns=["hy_spread", "hyg_close", "lqd_close"])
        with pytest.raises(KeyError):
            compute_credit_spread(df)


class TestMomentum:
    def test_window_output(self, sample_df):
        result = compute_momentum(sample_df, [30, 90])
        assert "momentum_30d" in result.columns
        assert "momentum_90d" in result.columns
        assert len(result) == len(sample_df)

    def test_single_window(self, sample_df):
        result = compute_momentum(sample_df, [10])
        assert "momentum_10d" in result.columns
        assert result["momentum_10d"].iloc[:10].isna().all()

    def test_missing_tlt(self, sample_df):
        df = sample_df.drop(columns=["tlt_close"])
        result = compute_momentum(df, [30])
        assert result.empty


class TestVolatility:
    def test_nonnegative(self, sample_df):
        vol = compute_volatility(sample_df, 60)
        assert (vol.dropna() >= 0).all()

    def test_window_nan_prefix(self, sample_df):
        vol = compute_volatility(sample_df, 60)
        assert vol.iloc[:59].isna().all()

    def test_missing_tlt(self, sample_df):
        df = sample_df.drop(columns=["tlt_close"])
        vol = compute_volatility(df, 60)
        assert vol.empty
