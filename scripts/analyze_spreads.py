#!/usr/bin/env python3
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
import json

# --- Config ---
DATA_FILE = Path("data_sample/spread_report_all_pairs_long.csv")
OUTPUT_DIR = Path("reports")
OUTPUT_DIR.mkdir(exist_ok=True)

# Soglia per considerare un segnale "forte" (ad esempio ±2 deviazioni standard)
Z_THRESHOLD = 2

# --- Leggi i dati ---
df = pd.read_csv(DATA_FILE)
df['date'] = pd.to_datetime(df['date'], utc=True)

# --- Analisi per coppia ---
summary = {}
for pair in df['pair'].unique():
    subset = df[df['pair']==pair].copy()
    
    # Statistiche base
    mean = subset['spread_pct'].mean()
    std = subset['spread_pct'].std()
    
    # Evidenzia valori fuori soglia
    subset['strong_signal'] = (subset['spread_pct'] - mean).abs() > Z_THRESHOLD*std
    
    # Salva grafico
    plt.figure(figsize=(10,5))
    plt.plot(subset['date'], subset['spread_pct'], label='Spread')
    plt.scatter(subset['date'][subset['strong_signal']], 
                subset['spread_pct'][subset['strong_signal']], 
                color='red', label='Strong Signal')
    plt.title(f"Spread nel tempo - {pair}")
    plt.xlabel("Date")
    plt.ylabel("Spread %")
    plt.legend()
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / f"spread_plot_{pair}.png")
    plt.close()
    
    # Aggiorna summary
    summary[pair] = {
        "mean": mean,
        "std": std,
        "max": subset['spread_pct'].max(),
        "min": subset['spread_pct'].min(),
        "strong_signals": subset['date'][subset['strong_signal']].dt.strftime('%Y-%m-%d').tolist()
    }

# --- Esporta JSON per dashboard ---
with open(OUTPUT_DIR / "spread_summary.json", "w") as f:
    json.dump(summary, f, indent=2)

print(f"Analisi completata. Grafici e summary JSON salvati in {OUTPUT_DIR}")

