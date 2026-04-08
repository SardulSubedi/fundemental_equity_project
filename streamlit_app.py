"""
Streamlit UI entry point for Streamlit Community Cloud.

Do **not** set Main file to `main.py` — that file is the CLI *pipeline only*
and will crash when Streamlit tries to run it as an app.

In Cloud: App settings → Main file path → **`streamlit_app.py`**
(alternatively: **`dashboard/app.py`**).
"""
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dashboard.app import main

main()
