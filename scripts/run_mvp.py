#!/usr/bin/env python3
import yfinance as yf
import pandas as pd
import os
import logging
from datetime import datetime
import matplotlib.pyplot as plt

# Configura logging
logging.basicConfig(
    format='%(asctime)s %(levelname)s: %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Cartelle
DATA_DIR = 'data_sample'
REPORT_DIR = 'reports'
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(REPORT_DIR, exist_ok=True)

# Lista ETF da monitorare
pairs = [
    # Cross-listing
    ('CSP1.L', 'IUSA.DE'),        # iShares Core S&P 500 UCITS
    ('VWRL.L', 'VEVE.AS'),        # Vanguard FTSE All-World UCITS
    ('SWDA.L', 'EUNL.DE'),        # iShares MSCI World UCITS
    # Accumulazione vs Distribuzione
    ('SWDA.L', 'IWRD.DE'),        # iShares MSCI World
    ('VWRL.L', 'VEVE.AS'),        # Vanguard FTSE All-World
]

# Soglie segnali (%)
THRESHOLD_ALERT = 0.5
THRESHOLD_STRONG = 1.0

def assign_signal(spread):
    if abs(spread) >= THRESHOLD_STRONG:
        return 'STRONG_SIGNAL'
    elif abs(spread) >= THRESHOLD_ALERT:
        return 'ALERT'
    else:
        return ''

def fetch_and_save(ticker):
    """Scarica prezzi e salva CSV locale"""
    logging.info(f"Fetching {ticker}...")
    data = yf.download(ticker, period="2y", interval="1d", progress=False)['Close']
    path = os.path.join(DATA_DIR, f"{ticker.replace('.', '_')}.csv")
    data.to_csv(path)
    logging.info(f"Saved {path}")
    return path

for t1, t2 in pairs:
    path1 = fetch_and_save(t1)
    path2 = fetch_and_save(t2)

    # Carica dati
    df1 = pd.read_csv(path1, index_col=0, parse_dates=True)
    df2 = pd.read_csv(path2, index_col=0, parse_dates=True)

    df = df1.join(df2, how='inner', lsuffix='_1', rsuffix='_2')
    df.columns = ['Price1', 'Price2']

    # Calcola spread %
    df['spread_pct'] = (df['Price1'] - df['Price2']) / df['Price2'] * 100
    df['signal'] = df['spread_pct'].apply(assign_signal)

    # Salva report
    pair_name = f"{t1.replace('.', '_')}_{t2.replace('.', '_')}"
    report_path = os.path.join(DATA_DIR, f"spread_report_{pair_name}.csv")
    df.to_csv(report_path, float_format='%.3f')
    logging.info(f"Saved report {report_path}")

    # Genera grafico
    plt.figure(figsize=(10,5))
    plt.plot(df.index, df['spread_pct'], label='Spread %', linewidth=1)
    alerts = df[df['signal'] == 'ALERT']
    strongs = df[df['signal'] == 'STRONG_SIGNAL']
    if not alerts.empty:
        plt.scatter(alerts.index, alerts['spread_pct'], color='orange', label='ALERT', zorder=5)
    if not strongs.empty:
        plt.scatter(strongs.index, strongs['spread_pct'], color='red', label='STRONG_SIGNAL', zorder=5)
    plt.axhline(0, color='grey', linewidth=0.6)
    plt.title(f"Spread {t1} vs {t2}")
    plt.xlabel('Date')
    plt.ylabel('Spread (%)')
    plt.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(REPORT_DIR, f"spread_plot_{pair_name}.png"), dpi=150)
    plt.close()
    logging.info(f"Saved plot for {pair_name}")

