"""Unit tests for src/signals.py"""

import numpy as np
import pandas as pd
import pytest

from src.signals import classify_regime, score_signals


@pytest.fixture
def feature_df():
    dates = pd.bdate_range("2024-01-02", periods=50)
    return pd.DataFrame(
        {
            "yield_curve_spread": np.linspace(1.0, -0.5, 50),
            "credit_spread": np.linspace(3.0, 6.0, 50),
            "momentum_30d": np.linspace(2.0, -3.0, 50),
            "volatility": np.linspace(10.0, 20.0, 50),
        },
        index=dates,
    )


@pytest.fixture
def default_thresholds():
    return {
        "yield_curve_inversion": 0.0,
        "credit_spread_stress": 5.0,
        "momentum_positive": 0.0,
    }


@pytest.fixture
def default_weights():
    return {"momentum": 0.3, "spread": 0.4, "volatility": 0.3}


class TestClassifyRegime:
    def test_recession_risk(self, default_thresholds):
        df = pd.DataFrame(
            {
                "yield_curve_spread": [-0.5],
                "credit_spread": [3.0],
                "momentum_30d": [1.0],
            },
            index=pd.to_datetime(["2024-01-02"]),
        )
        result = classify_regime(df, default_thresholds)
        assert result.iloc[0] == "Recession Risk"

    def test_credit_stress(self, default_thresholds):
        df = pd.DataFrame(
            {
                "yield_curve_spread": [1.0],
                "credit_spread": [6.0],
                "momentum_30d": [1.0],
            },
            index=pd.to_datetime(["2024-01-02"]),
        )
        result = classify_regime(df, default_thresholds)
        assert result.iloc[0] == "Credit Stress"

    def test_risk_on(self, default_thresholds):
        df = pd.DataFrame(
            {
                "yield_curve_spread": [1.5],
                "credit_spread": [3.0],
                "momentum_30d": [2.0],
            },
            index=pd.to_datetime(["2024-01-02"]),
        )
        result = classify_regime(df, default_thresholds)
        assert result.iloc[0] == "Risk On"

    def test_neutral(self, default_thresholds):
        df = pd.DataFrame(
            {
                "yield_curve_spread": [0.5],
                "credit_spread": [3.0],
                "momentum_30d": [-1.0],
            },
            index=pd.to_datetime(["2024-01-02"]),
        )
        result = classify_regime(df, default_thresholds)
        assert result.iloc[0] == "Neutral"

    def test_recession_overrides_credit_stress(self, default_thresholds):
        """Inverted curve takes priority over wide spreads."""
        df = pd.DataFrame(
            {
                "yield_curve_spread": [-0.3],
                "credit_spread": [7.0],
                "momentum_30d": [2.0],
            },
            index=pd.to_datetime(["2024-01-02"]),
        )
        result = classify_regime(df, default_thresholds)
        assert result.iloc[0] == "Recession Risk"


class TestScoreSignals:
    def test_output_length(self, feature_df, default_weights):
        score = score_signals(feature_df, default_weights)
        assert len(score) == len(feature_df)

    def test_score_name(self, feature_df, default_weights):
        score = score_signals(feature_df, default_weights)
        assert score.name == "risk_score"

    def test_higher_risk_scores_late(self, feature_df, default_weights):
        """Later rows have inverted curve + wide spreads -> higher risk."""
        score = score_signals(feature_df, default_weights)
        avg_early = score.iloc[:10].mean()
        avg_late = score.iloc[-10:].mean()
        assert avg_late > avg_early

    def test_constant_features_zero_score(self, default_weights):
        dates = pd.bdate_range("2024-01-02", periods=20)
        df = pd.DataFrame(
            {
                "yield_curve_spread": 1.0,
                "credit_spread": 3.0,
                "momentum_30d": 0.5,
                "volatility": 12.0,
            },
            index=dates,
        )
        score = score_signals(df, default_weights)
        assert (score == 0.0).all()
