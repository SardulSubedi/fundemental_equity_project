"""
Fixed Income Signal Engine — Interactive Dashboard

Launch with:
    streamlit run dashboard/app.py
"""

import os
import sqlite3
import sys
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
import plotly.graph_objects as go
import streamlit as st

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

DB_PATH = ROOT / "data" / "data.db"
CSS_PATH = Path(__file__).resolve().parent / "style.css"

# ── Page config ──────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Fixed Income Signal Engine",
    page_icon="📊",
    layout="wide",
)

if CSS_PATH.exists():
    st.markdown(f"<style>{CSS_PATH.read_text()}</style>", unsafe_allow_html=True)

# ── Data loading ─────────────────────────────────────────────────────────────

REGIME_COLORS = {
    "Recession Risk": "#f85149",
    "Credit Stress":  "#d29922",
    "Risk On":        "#3fb950",
    "Neutral":        "#8b949e",
}

REGIME_CSS = {
    "Recession Risk": "regime-recession",
    "Credit Stress":  "regime-credit",
    "Risk On":        "regime-riskon",
    "Neutral":        "regime-neutral",
}

PLOTLY_LAYOUT = dict(
    template="plotly_dark",
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="Inter, sans-serif", size=12, color="#e6edf3"),
    margin=dict(l=48, r=24, t=40, b=40),
    hovermode="x unified",
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
)


def _normalize_api_key(value: str) -> str:
    """Strip whitespace / quotes often pasted into Streamlit secrets."""
    return value.strip().strip('"').strip("'")


def resolve_fred_api_key() -> str | None:
    """Prefer local `.env`, then Streamlit Cloud secrets (exact name: FRED_API_KEY)."""
    load_dotenv(ROOT / ".env")
    raw = os.environ.get("FRED_API_KEY")
    if raw:
        return _normalize_api_key(raw)
    try:
        if "FRED_API_KEY" in st.secrets:
            key = _normalize_api_key(str(st.secrets["FRED_API_KEY"]))
            os.environ["FRED_API_KEY"] = key
            return key
    except Exception:
        pass
    return None


def database_ready() -> bool:
    """True only if `signals` exists and has rows (avoids partial failed runs)."""
    if not DB_PATH.exists():
        return False
    try:
        with sqlite3.connect(str(DB_PATH)) as conn:
            cur = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='signals'"
            )
            if cur.fetchone() is None:
                return False
            n = conn.execute("SELECT COUNT(*) FROM signals").fetchone()[0]
            return n > 0
    except sqlite3.Error:
        return False


@st.cache_data(ttl=300)
def load_signals() -> pd.DataFrame:
    if not DB_PATH.exists():
        return pd.DataFrame()
    try:
        with sqlite3.connect(str(DB_PATH)) as conn:
            df = pd.read_sql(
                "SELECT * FROM signals",
                conn,
                index_col="date",
                parse_dates=["date"],
            )
    except Exception:
        return pd.DataFrame()
    return df


def ensure_database() -> bool:
    """
    Build `data/data.db` via the full pipeline when missing or broken.

    Older versions wrote a partial DB (only `clean_data`) before the pipeline
    finished — that breaks the dashboard. We delete and rebuild in that case.
    """
    if database_ready():
        return True

    if DB_PATH.exists():
        try:
            DB_PATH.unlink()
        except OSError:
            pass

    key = resolve_fred_api_key()
    if not key:
        return False

    os.environ["FRED_API_KEY"] = key

    from filelock import FileLock
    from main import run_pipeline

    lock_path = ROOT / "data" / ".pipeline.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        with st.spinner(
            "Building dataset from FRED and Yahoo Finance (first run, ~30–60s)..."
        ):
            with FileLock(str(lock_path), timeout=600):
                run_pipeline(use_cache=False)
    except Exception:
        if DB_PATH.exists():
            try:
                DB_PATH.unlink()
            except OSError:
                pass
        raise

    load_signals.clear()
    if not database_ready():
        raise RuntimeError(
            "Pipeline finished but `signals` is empty — check logs and data filters."
        )
    return True


def load_latest_report() -> str:
    output_dir = ROOT / "output"
    reports = sorted(output_dir.glob("report_*.txt"), reverse=True)
    if reports:
        return reports[0].read_text(encoding="utf-8")
    return ""


# ── Main UI ──────────────────────────────────────────────────────────────────

def main():
    try:
        if not database_ready():
            if not ensure_database():
                st.error(
                    "**No usable database and no `FRED_API_KEY` found.**\n\n"
                    "**Streamlit Cloud:** App settings → Secrets → add exactly:\n"
                    "```\nFRED_API_KEY = \"your_key_here\"\n```\n"
                    "(Name must be **`FRED_API_KEY`**, uppercase. Redeploy or clear cache "
                    "after saving.)\n\n"
                    "**Local:** run `python main.py` or put `FRED_API_KEY=...` in `.env`."
                )
                return
    except Exception as exc:
        st.error(
            "The data pipeline failed while fetching or processing. "
            "Common causes: invalid FRED key, or Yahoo Finance temporarily blocked. "
            "See the traceback below."
        )
        st.exception(exc)

    df = load_signals()

    if df.empty:
        st.warning(
            "No data in `signals` table. Try **Reboot app** in Streamlit Cloud, "
            "or run `python main.py` locally."
        )
        return

    latest = df.iloc[-1]
    regime = latest.get("regime", "Neutral")
    regime_css = REGIME_CSS.get(regime, "regime-neutral")

    # ── Header ───────────────────────────────────────────────────────────
    col_title, col_badge = st.columns([3, 1])
    with col_title:
        st.title("Fixed Income Signal Engine")
        st.caption(f"Last observation: **{df.index[-1].strftime('%B %d, %Y')}**")
    with col_badge:
        st.markdown(
            f'<div style="text-align:right;padding-top:24px;">'
            f'<span class="regime-badge {regime_css}">{regime}</span></div>',
            unsafe_allow_html=True,
        )

    st.divider()

    # ── Metrics row ──────────────────────────────────────────────────────
    m1, m2, m3, m4 = st.columns(4)
    yc = latest.get("yield_curve_spread")
    cs = latest.get("credit_spread")
    score = latest.get("risk_score")
    vol = latest.get("volatility")

    with m1:
        delta_yc = _delta(df, "yield_curve_spread")
        st.metric("Yield Curve (10Y-2Y)", f"{yc:+.2f}%", delta=delta_yc)
    with m2:
        delta_cs = _delta(df, "credit_spread")
        st.metric("Credit Spread (HY)", f"{cs:.2f}%", delta=delta_cs)
    with m3:
        delta_sc = _delta(df, "risk_score")
        st.metric("Risk Score", f"{score:+.2f}", delta=delta_sc,
                  delta_color="inverse")
    with m4:
        delta_vol = _delta(df, "volatility")
        st.metric("Annualised Vol", f"{vol:.1f}%", delta=delta_vol,
                  delta_color="inverse")

    st.divider()

    # ── Charts ───────────────────────────────────────────────────────────
    tab_yc, tab_cs, tab_mom, tab_score = st.tabs(
        ["Yield Curve", "Credit Spread", "Momentum & Vol", "Risk Score"]
    )

    with tab_yc:
        fig = go.Figure(layout=PLOTLY_LAYOUT)
        fig.add_trace(go.Scatter(
            x=df.index, y=df["yield_curve_spread"],
            name="10Y-2Y Spread", line=dict(color="#58a6ff", width=2),
        ))
        fig.add_hline(y=0, line_dash="dash", line_color="#f85149",
                      annotation_text="Inversion threshold",
                      annotation_font_color="#f85149")
        fig.update_layout(
            title="Yield Curve Spread Over Time",
            yaxis_title="Spread (%)",
        )
        st.plotly_chart(fig, use_container_width=True)

    with tab_cs:
        fig = go.Figure(layout=PLOTLY_LAYOUT)
        fig.add_trace(go.Scatter(
            x=df.index, y=df["credit_spread"],
            name="HY OAS", line=dict(color="#d29922", width=2),
            fill="tozeroy", fillcolor="rgba(210,153,34,0.08)",
        ))
        fig.add_hline(y=5, line_dash="dash", line_color="#f85149",
                      annotation_text="Stress threshold",
                      annotation_font_color="#f85149")
        fig.update_layout(
            title="High-Yield Credit Spread",
            yaxis_title="Spread (%)",
        )
        st.plotly_chart(fig, use_container_width=True)

    with tab_mom:
        fig = go.Figure(layout=PLOTLY_LAYOUT)
        if "momentum_30d" in df.columns:
            fig.add_trace(go.Scatter(
                x=df.index, y=df["momentum_30d"],
                name="30-Day Momentum", line=dict(color="#3fb950", width=2),
            ))
        if "momentum_90d" in df.columns:
            fig.add_trace(go.Scatter(
                x=df.index, y=df["momentum_90d"],
                name="90-Day Momentum", line=dict(color="#58a6ff", width=2),
            ))
        if "volatility" in df.columns:
            fig.add_trace(go.Scatter(
                x=df.index, y=df["volatility"],
                name="Volatility", line=dict(color="#f85149", width=1.5,
                                             dash="dot"),
                yaxis="y2",
            ))
            fig.update_layout(
                yaxis2=dict(
                    title="Vol (%)", overlaying="y", side="right",
                    showgrid=False,
                ),
            )
        fig.update_layout(
            title="Bond Momentum & Volatility",
            yaxis_title="Return (%)",
        )
        st.plotly_chart(fig, use_container_width=True)

    with tab_score:
        fig = go.Figure(layout=PLOTLY_LAYOUT)
        colors = [REGIME_COLORS.get(r, "#8b949e") for r in df["regime"]]
        fig.add_trace(go.Bar(
            x=df.index, y=df["risk_score"],
            marker_color=colors, name="Risk Score",
        ))
        fig.update_layout(
            title="Composite Risk Score (coloured by regime)",
            yaxis_title="Score (z-units)",
            bargap=0,
        )
        st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # ── Research note ────────────────────────────────────────────────────
    st.subheader("Latest Research Note")
    report_text = load_latest_report()
    if report_text:
        st.code(report_text, language=None)
    else:
        st.info("No report generated yet. Run `python main.py` to create one.")


def _delta(df: pd.DataFrame, col: str, lookback: int = 5) -> str | None:
    """Compute short-term change for metric deltas."""
    if col not in df.columns or len(df) < lookback + 1:
        return None
    curr = df[col].iloc[-1]
    prev = df[col].iloc[-(lookback + 1)]
    diff = curr - prev
    return f"{diff:+.2f}"


if __name__ == "__main__":
    main()
