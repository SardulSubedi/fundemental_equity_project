"""
Data ingestion from FRED API and Yahoo Finance.

Pulls macroeconomic series (Treasury yields, Fed Funds, CPI, HY spread)
and bond ETF market data, with local CSV caching for offline re-runs.
"""

import logging
import os
import time
from pathlib import Path

import pandas as pd
import yfinance as yf
from fredapi import Fred

logger = logging.getLogger(__name__)

MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds


def _retry(func, *args, retries=MAX_RETRIES, delay=RETRY_DELAY, **kwargs):
    """Execute *func* with exponential-backoff retry."""
    for attempt in range(1, retries + 1):
        try:
            return func(*args, **kwargs)
        except Exception as exc:
            if attempt == retries:
                raise
            wait = delay * (2 ** (attempt - 1))
            logger.warning(
                "Attempt %d/%d failed (%s). Retrying in %ds...",
                attempt, retries, exc, wait,
            )
            time.sleep(wait)


def _get_fred_client() -> Fred:
    api_key = os.getenv("FRED_API_KEY")
    if not api_key:
        raise EnvironmentError(
            "FRED_API_KEY not set. Copy .env.example to .env and add your key."
        )
    return Fred(api_key=api_key)


def fetch_fred_data(config: dict) -> pd.DataFrame:
    """Pull all configured FRED series into a single DataFrame."""
    fred = _get_fred_client()
    series_ids = config["data_sources"]["fred"]["series"]
    start = config["date_range"]["start"]
    end = config["date_range"].get("end")

    frames: dict[str, pd.Series] = {}
    for sid in series_ids:
        logger.info("Fetching FRED series %s", sid)
        data = _retry(
            fred.get_series,
            sid,
            observation_start=start,
            observation_end=end,
        )
        frames[sid] = data

    df = pd.DataFrame(frames)
    df.index.name = "date"
    return df


def fetch_market_data(config: dict) -> pd.DataFrame:
    """Pull bond ETF close prices from Yahoo Finance."""
    tickers = config["data_sources"]["yfinance"]["tickers"]
    start = config["date_range"]["start"]
    end = config["date_range"].get("end")

    logger.info("Fetching Yahoo Finance data for %s", tickers)
    raw = _retry(
        yf.download,
        tickers,
        start=start,
        end=end,
        progress=False,
    )

    if isinstance(raw.columns, pd.MultiIndex):
        prices = raw["Close"]
    else:
        prices = raw[["Close"]].rename(columns={"Close": tickers[0]})

    prices.index.name = "date"
    return prices


def _cache_path(raw_dir: str, name: str) -> Path:
    return Path(raw_dir) / f"{name}.csv"


def _save_cache(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path)
    logger.info("Cached data to %s", path)


def _load_cache(path: Path) -> pd.DataFrame | None:
    if path.exists():
        logger.info("Loading cached data from %s", path)
        return pd.read_csv(path, index_col=0, parse_dates=True)
    return None


def load_all_data(config: dict, use_cache: bool = True) -> pd.DataFrame:
    """
    Fetch and merge FRED + market data.

    When *use_cache* is True the raw pulls are saved as CSV and reused on
    subsequent runs so the pipeline works offline after the first fetch.
    """
    raw_dir = "data/raw"

    fred_cache = _cache_path(raw_dir, "fred_raw")
    market_cache = _cache_path(raw_dir, "market_raw")

    df_fred = (_load_cache(fred_cache) if use_cache else None)
    if df_fred is None:
        df_fred = fetch_fred_data(config)
        _save_cache(df_fred, fred_cache)

    df_market = (_load_cache(market_cache) if use_cache else None)
    if df_market is None:
        df_market = fetch_market_data(config)
        _save_cache(df_market, market_cache)

    df = df_fred.join(df_market, how="inner")
    logger.info(
        "Combined dataset: %d rows x %d cols (%s to %s)",
        len(df), len(df.columns),
        df.index.min().date(), df.index.max().date(),
    )
    return df
