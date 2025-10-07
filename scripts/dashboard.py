#!/usr/bin/env python3
import streamlit as st  
import pandas as pd
import plotly.graph_objects as go
import json
from pathlib import Path

# --- Percorsi file relativi al repository ---
BASE_DIR = Path(__file__).parent.parent  # dalla cartella scripts alla root del repo
DATA_FILE = BASE_DIR / "data_sample" / "spread_report_all_pairs_long.csv"
JSON_FILE = BASE_DIR / "reports" / "spread_summary.json"
    
# --- Configurazione pagina ---
st.set_page_config(
    page_title="ArbiSense Dashboard",
    page_icon="📊",
    layout="wide"
)
st.title("ArbiSense Dashboard - Strong Signals 📊")

# --- Carica dati ---
df = pd.read_csv(DATA_FILE)
df['date'] = pd.to_datetime(df['date'])
df['date_only'] = df['date'].dt.date  # serve per filtrare strong signals

with open(JSON_FILE) as f:
    summary = json.load(f)
    
# --- Selezione coppia ---
pair = st.selectbox("Seleziona coppia", df['pair'].unique())
df_pair = df[df['pair'] == pair].copy()
    
# --- Selezione intervallo date ---
start_date, end_date = st.date_input(
    "Seleziona intervallo date",
    value=[df_pair['date_only'].min(), df_pair['date_only'].max()]
)
mask = (df_pair['date_only'] >= start_date) & (df_pair['date_only'] <= end_date)
df_pair_filtered = df_pair[mask]

# --- Strong signals ---
strong_dates = pd.Series(summary[pair]['strong_signals'])
strong_dates = pd.to_datetime(strong_dates).dt.date
strong_mask = df_pair_filtered['date_only'].isin(strong_dates)
strong_points = df_pair_filtered[strong_mask]

# --- Grafico interattivo ---
fig = go.Figure()
    
# Linea spread   
fig.add_trace(go.Scatter(
    x=df_pair_filtered['date'],
    y=df_pair_filtered['spread_pct'],
    mode='lines',
    name='Spread',
    line=dict(color='blue')
))
    
# Pallini rossi dei strong signals
fig.add_trace(go.Scatter(
    x=strong_points['date'],                                                                                                         
    y=strong_points['spread_pct'],
    mode='markers',
    name='Strong Signal',
    marker=dict(color='red', size=10)
))
 
fig.update_layout(
    title=f"Andamento Spread & Strong Signals: {pair}",
    xaxis_title="Date",
    yaxis_title="Spread %",
    template="plotly_white"
)

st.plotly_chart(fig, use_container_width=True)

# --- Tabella strong signals con spread ---
st.subheader("Strong Signals")
strong_table = strong_points[['date', 'spread_pct']].copy()
strong_table = strong_table.sort_values('date')
st.dataframe(strong_table)
    
# --- Statistiche base ---
st.subheader("Statistiche Spread")
stats = summary[pair]
st.json(stats)

