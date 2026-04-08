"""
Fixed Income Signal Engine — pipeline orchestrator.

Usage:
    python main.py              # full run (fetch + process + report)
    python main.py --cached     # re-run from cached data (no API calls)

Not a Streamlit entry — use `streamlit_app.py` or `dashboard/app.py` for the UI.
"""

import argparse
import logging
import os
import sqlite3
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import yaml
from dotenv import load_dotenv

from src.clean import clean_data, save_processed
from src.features import build_feature_matrix
from src.ingest import load_all_data
from src.report import generate_report
from src.signals import generate_signals

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("pipeline")


def load_config(path: str = "config.yaml") -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def save_to_db(
    clean_df, features_df, signals_df, db_path: str,
) -> None:
    """Atomic-ish refresh: drop old tables first (avoids races / 'already exists')."""
    p = Path(db_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(str(p), timeout=60) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        for name in ("signals", "features", "clean_data"):
            conn.execute(f"DROP TABLE IF EXISTS {name}")
        conn.commit()
        clean_df.to_sql("clean_data", conn, if_exists="replace", index=True)
        features_df.to_sql("features", conn, if_exists="replace", index=True)
        signals_df.to_sql("signals", conn, if_exists="replace", index=True)
    logger.info("All tables written to %s", db_path)


def run_pipeline(use_cache: bool = True) -> None:
    # So `config.yaml`, `data/`, and `.env` resolve correctly when imported from
    # Streamlit or any working directory.
    root = Path(__file__).resolve().parent
    os.chdir(root)
    load_dotenv(root / ".env")

    config = load_config()

    logger.info("=== Stage 1/5: Data Ingestion ===")
    raw = load_all_data(config, use_cache=use_cache)

    logger.info("=== Stage 2/5: Data Cleaning ===")
    clean = clean_data(raw)
    save_processed(clean, config)

    logger.info("=== Stage 3/5: Feature Engineering ===")
    features = build_feature_matrix(clean, config)

    logger.info("=== Stage 4/5: Signal Generation ===")
    signals = generate_signals(features, config)

    logger.info("=== Stage 5/5: Report Generation ===")
    report_dir = config["output"]["report_dir"]
    report = generate_report(signals, features, report_dir)

    save_to_db(clean, features, signals, config["output"]["db_path"])

    print("\n" + report)
    logger.info("Pipeline complete.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fixed Income Signal Engine",
    )
    parser.add_argument(
        "--cached",
        action="store_true",
        help="Use locally cached data instead of fetching from APIs",
    )
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Force fresh data fetch, ignoring any cached files",
    )
    args = parser.parse_args()

    use_cache = not args.no_cache
    try:
        run_pipeline(use_cache=use_cache)
    except Exception:
        logger.exception("Pipeline failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
