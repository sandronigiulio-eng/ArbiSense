#!/usr/bin/env python3
import sys
import pandas as pd
import numpy as np

def load_csv(path):
    return pd.read_csv(path, index_col=0, parse_dates=True)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python scripts/calculate_spread.py ticker1.csv ticker2.csv")
        sys.exit(1)

    a = load_csv(sys.argv[1])
    b = load_csv(sys.argv[2])

    df = a.join(b, how='inner')
    t1 = df.columns[0]
    t2 = df.columns[1]

    # Converti a numerico e rimuovi righe non valide
    df[t1] = pd.to_numeric(df[t1], errors='coerce')
    df[t2] = pd.to_numeric(df[t2], errors='coerce')
    df = df.dropna()

    # Calcola spread
    df['spread_pct'] = (df[t1] - df[t2]) / df[t2] * 100
    threshold = 1.0
    df['signal'] = np.where(df['spread_pct'].abs() > threshold, 'ALERT', '')

    out = 'data_sample/spread_report.csv'
    df.to_csv(out)
    print(f"Saved report to {out}")
    print(df.tail(10))
