#!/usr/bin/env python3
import json
from pathlib import Path
import pandas as pd

# --- Config ---
JSON_FILE = Path("reports/spread_summary.json")
OUTPUT_CSV = Path("reports/strong_signals.csv")

# --- Leggi il JSON ---
with open(JSON_FILE, "r") as f:
    data = json.load(f)

# --- Costruisci DataFrame ---
rows = []
for pair, stats in data.items():
    for date in stats['strong_signals']:
        rows.append({"pair": pair, "date": date})

df = pd.DataFrame(rows)

# Ordina per coppia e data
df['date'] = pd.to_datetime(df['date'])
df = df.sort_values(['pair', 'date'])

# Salva CSV
df.to_csv(OUTPUT_CSV, index=False)
print(f"Report dei segnali forti salvato in {OUTPUT_CSV}")
print(df)

