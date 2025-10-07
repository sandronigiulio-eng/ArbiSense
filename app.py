#!/usr/bin/env python3
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import json
from pathlib import Path

# --- Percorsi file ---
DATA_FILE = Path("data_sample/spread_report_all_pairs_long.csv")
JSON_FILE = Path("reports/spread_summary.json")

# --- Titolo app ---
st.title("ArbiSense Dashboard - Strong Signals")

# --- Carica dati ---
df = pd.read_csv(DATA_FILE)
df['date'] = pd.to_datetime(df['date'])

with open(JSON_FILE) as f:
    summary = json.load(f)

# --- Selezione coppia ---
pair = st.selectbox("Seleziona coppia", df['pair'].unique())

df_pair = df[df['pair'] == pair]
strong_dates = pd.to_datetime(summary[pair]['strong_signals'])

# --- Grafico ---
st.subheader(f"Andamento Spread & Strong Signals: {pair}")
fig, ax = plt.subplots(figsize=(10,5))
ax.plot(df_pair['date'], df_pair['spread_pct'], label='Spread', color='blue')
ax.scatter(strong_dates, df_pair[df_pair['date'].isin(strong_dates)]['spread_pct'],
           color='red', label='Strong Signal', zorder=5)
ax.set_xlabel("Date")
ax.set_ylabel("Spread %")
ax.legend()
st.pyplot(fig)

# --- Tabella segnali forti ---
st.subheader("Strong Signals")
strong_df = pd.DataFrame({
    "date": strong_dates
})
st.table(strong_df)

# --- Statistiche base ---
st.subheader("Statistiche Spread")
stats = summary[pair]
st.json(stats)

