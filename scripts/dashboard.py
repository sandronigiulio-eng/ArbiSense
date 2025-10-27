#!/usr/bin/env python3
import os
import json
import subprocess
from pathlib import Path
from datetime import datetime

import streamlit as st
import pandas as pd
import plotly.graph_objects as go

# --- Percorsi relativi alla root del repository ---
BASE_DIR = Path(__file__).parent.parent  # scripts/ → root
DATA_FILE = BASE_DIR / "data_sample" / "spread_report_all_pairs_long.csv"
JSON_FILE = BASE_DIR / "reports" / "spread_summary.json"
TRADES_OUT = BASE_DIR / "reports" / "backtest_trades.csv"
METRICS_OUT = BASE_DIR / "reports" / "backtest_metrics.csv"
EQUITY_PNG = BASE_DIR / "reports" / "backtest_equity.png"
BACKTEST_SCRIPT = BASE_DIR / "scripts" / "backtest_signals.py"

# --- Configurazione pagina ---
st.set_page_config(
    page_title="ArbiSense Dashboard",
    page_icon="📊",
    layout="wide"
)
st.title("ArbiSense Dashboard")

# --- Selezione pagina ---
page = st.sidebar.selectbox(
    "Naviga",
    ["Strong Signals", "Backtest"]
)

# ===========================
# PAGINA: STRONG SIGNALS
# ===========================
if page == "Strong Signals":
    st.header("📊 Strong Signals")

    # --- Carica dati ---
    if not DATA_FILE.exists():
        st.error(f"Manca il file dati: {DATA_FILE}")
        st.stop()
    df = pd.read_csv(DATA_FILE)

    if "date" not in df.columns or "pair" not in df.columns:
        st.error("Il CSV deve contenere almeno le colonne 'date' e 'pair'.")
        st.stop()

    # Normalizza colonne (compatibilità: spread_pct vs spread)
    if "spread_pct" in df.columns:
        spread_col = "spread_pct"
    elif "spread" in df.columns:
        spread_col = "spread"
    else:
        st.error("Il CSV non contiene 'spread_pct' né 'spread'.")
        st.stop()

    # parsing date
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])
    df["date_only"] = df["date"].dt.date

    # Carica summary JSON (se presente)
    summary = {}
    if JSON_FILE.exists():
        try:
            with open(JSON_FILE, "r") as f:
                summary = json.load(f)
        except Exception as e:
            st.warning(f"Impossibile leggere summary JSON: {e}")

    # --- Selezione coppia ---
    pairs = sorted(df["pair"].dropna().unique().tolist())
    if not pairs:
        st.info("Nessuna coppia disponibile nei dati.")
        st.stop()

    pair = st.selectbox("Seleziona coppia", pairs)
    df_pair = df[df["pair"] == pair].copy()

    # --- Selezione intervallo date ---
    default_start = df_pair["date_only"].min()
    default_end = df_pair["date_only"].max()
    start_date, end_date = st.date_input(
        "Seleziona intervallo date",
        value=[default_start, default_end]
    )

    mask = (df_pair["date_only"] >= start_date) & (df_pair["date_only"] <= end_date)
    df_pair_filtered = df_pair[mask].copy()

    # --- Strong signals dal summary (se disponibili) ---
    strong_points = pd.DataFrame(columns=df_pair_filtered.columns)
    if pair in summary and isinstance(summary.get(pair, {}), dict) and "strong_signals" in summary[pair]:
        try:
            strong_dates = pd.Series(summary[pair]["strong_signals"])
            strong_dates = pd.to_datetime(strong_dates).dt.date
            strong_mask = df_pair_filtered["date_only"].isin(strong_dates)
            strong_points = df_pair_filtered[strong_mask].copy()
        except Exception:
            pass

    # --- Grafico interattivo ---
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df_pair_filtered["date"],
        y=df_pair_filtered[spread_col],
        mode="lines",
        name="Spread",
        line=dict(color="blue")
    ))
    if not strong_points.empty:
        fig.add_trace(go.Scatter(
            x=strong_points["date"],
            y=strong_points[spread_col],
            mode="markers",
            name="Strong Signal",
            marker=dict(color="red", size=10)
        ))

    y_label = "Spread %" if spread_col == "spread_pct" else "Spread"
    fig.update_layout(
        title=f"Andamento Spread & Strong Signals: {pair}",
        xaxis_title="Date",
        yaxis_title=y_label,
        template="plotly_white"
    )
    st.plotly_chart(fig, use_container_width=True)

    # --- Tabella strong signals con spread ---
    st.subheader("Strong Signals")
    if not strong_points.empty:
        strong_table = strong_points[["date", spread_col]].sort_values("date").copy()
        strong_table.rename(columns={spread_col: "spread_value"}, inplace=True)
        st.dataframe(strong_table, use_container_width=True)
    else:
        st.info("Nessun strong signal disponibile nel periodo selezionato.")

    # --- Statistiche base ---
    st.subheader("Statistiche Spread (raw JSON)")
    if pair in summary:
        st.json(summary[pair])
    else:
        st.info("Nessun summary JSON per la coppia selezionata.")

# ===========================
# PAGINA: BACKTEST
# ===========================
elif page == "Backtest":
    st.header("🔙 Backtest — ArbiSense")

    # --- Parametri ---
    col1, col2, col3 = st.columns(3)
    z_enter = col1.number_input("z-enter (|z| ≥)", value=2.0, step=0.1, format="%.2f")
    z_exit  = col2.number_input("z-exit (|z| ≤)", value=0.5, step=0.1, format="%.2f")
    max_hold = col3.number_input("max-hold (giorni)", value=10, step=1)

    col4, col5, col6 = st.columns(3)
    fee_bps = col4.number_input("fee bps (round-trip)", value=1.0, step=0.5, format="%.1f")
    slippage_bps = col5.number_input("slippage bps (round-trip)", value=1.0, step=0.5, format="%.1f")
    notional = col6.number_input("notional", value=10000.0, step=1000.0, format="%.0f")

    z_window = st.number_input("Rolling z-window (giorni)", value=60, step=5)

    colA, colB = st.columns([1,2])
    run_bt = colA.button("▶ Run backtest", type="primary")
    info = colB.empty()

    # --- Esecuzione backtest ---
    if run_bt:
        if not BACKTEST_SCRIPT.exists():
            info.error(f"Script non trovato: {BACKTEST_SCRIPT}")
        else:
            info.info("Esecuzione backtest in corso…")
            cmd = [
                "python3", str(BACKTEST_SCRIPT),
                "--z-enter", str(z_enter),
                "--z-exit", str(z_exit),
                "--max-hold", str(int(max_hold)),
                "--fee-bps", str(fee_bps),
                "--slippage-bps", str(slippage_bps),
                "--notional", str(notional),
                "--z-window", str(int(z_window)),
            ]
            try:
                out = subprocess.run(cmd, capture_output=True, text=True, check=False)
                if out.returncode == 0:
                    info.success("Backtest completato ✅")
                    if out.stdout:
                        st.code(out.stdout, language="bash")
                else:
                    info.error("Backtest fallito ❌")
                    st.code(out.stderr or out.stdout, language="bash")
            except Exception as e:
                info.error(f"Errore: {e}")

    # --- KPI ---
    st.subheader("📊 KPI")
    if METRICS_OUT.exists():
        try:
            mdf = pd.read_csv(METRICS_OUT)
            st.dataframe(mdf, use_container_width=True)
        except Exception as e:
            st.warning(f"Impossibile leggere metrics: {e}")
    else:
        st.info("Esegui il backtest per generare le metriche.")

    # --- Trades ---
    st.subheader("🧾 Trades")
    if TRADES_OUT.exists():
        try:
            tdf = pd.read_csv(TRADES_OUT)
            st.dataframe(tdf, use_container_width=True, height=400)
        except Exception as e:
            st.warning(f"Impossibile leggere trades: {e}")
    else:
        st.info("Esegui il backtest per generare la lista trades.")

    # --- Equity ---
    st.subheader("📈 Equity curve")
    if EQUITY_PNG.exists():
        st.image(str(EQUITY_PNG), caption="Backtest Equity (net PnL cumulato)")
    else:
        st.info("Esegui il backtest per generare il grafico di equity.")

