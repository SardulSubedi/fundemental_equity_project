"""
Data cleaning: date alignment, imputation, normalisation, and persistence.
"""

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

COLUMN_MAP = {
    "DGS10": "treasury_10y",
    "DGS2": "treasury_2y",
    "FEDFUNDS": "fed_funds",
    "CPIAUCSL": "cpi",
    "BAMLH0A0HYM2": "hy_spread",
    "TLT": "tlt_close",
    "LQD": "lqd_close",
    "HYG": "hyg_close",
}

CRITICAL_COLS = ["treasury_10y", "treasury_2y"]


def clean_data(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and normalise the raw combined dataset.

    Steps:
      1. Rename columns to snake_case.
      2. Resample to business-day frequency.
      3. Forward-fill gaps (standard for rate data).
      4. Drop rows where critical columns are still NaN.
    """
    df = df_raw.copy()
    df.rename(columns={k: v for k, v in COLUMN_MAP.items() if k in df.columns},
              inplace=True)
    df.index = pd.to_datetime(df.index)
    df = df.asfreq("B")
    df = df.ffill()

    before = len(df)
    existing_critical = [c for c in CRITICAL_COLS if c in df.columns]
    if existing_critical:
        df.dropna(subset=existing_critical, inplace=True)

    logger.info(
        "Cleaned data: %d -> %d rows (dropped %d incomplete)",
        before, len(df), before - len(df),
    )
    return df


def save_processed(df: pd.DataFrame, config: dict) -> None:
    """Write cleaned data to CSV only.

    SQLite `data.db` is written once at the end of the pipeline in `main.save_to_db`.
    Writing the DB here caused partial databases (only `clean_data`) if a later stage
    failed — the dashboard then had no `signals` table.
    """
    csv_path = Path("data/processed/clean_data.csv")
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(csv_path)

    logger.info("Saved cleaned data to %s", csv_path)
