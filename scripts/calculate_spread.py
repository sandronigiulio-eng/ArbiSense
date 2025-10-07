#!/usr/bin/env python3
import sys
import pandas as pd
import numpy as np
import logging

# Configura logging
logging.basicConfig(
    format='%(asctime)s %(levelname)s: %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S'
)

def load_csv(path):
    """Carica CSV, converte colonne in float e gestisce valori sporchi."""
    try:
        # Legge CSV senza specificare date_parser per compatibilità futura
        df = pd.read_csv(path, index_col=0, parse_dates=True)
    except Exception as e:
        logging.error(f"Errore caricamento CSV {path}: {e}")
        sys.exit(1)

    # Converte tutte le colonne in float, rimuovendo simboli o spazi
    for col in df.columns:
        df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', '').str.strip(), errors='coerce')

    # Rimuove righe con valori mancanti
    df.dropna(inplace=True)

    if df.empty:
        logging.error(f"CSV {path} non contiene dati validi dopo la pulizia")
        sys.exit(1)

    logging.info(f"Caricati {len(df)} record da {path}")
    return df

def assign_signal(spread, threshold_alert=0.5, threshold_strong=1.0):
    """Assegna segnali di ALERT o STRONG_SIGNAL in base allo spread."""
    if abs(spread) >= threshold_strong:
        return 'STRONG_SIGNAL'
    elif abs(spread) >= threshold_alert:
        return 'ALERT'
    else:
        return ''

if __name__ == "__main__":
    if len(sys.argv) != 3:
        logging.error("Uso corretto: python scripts/calculate_spread.py ticker1.csv ticker2.csv")
        sys.exit(1)

    # Carica dati
    df1 = load_csv(sys.argv[1])
    df2 = load_csv(sys.argv[2])

    # Fai join inner sui due CSV
    df = df1.join(df2, how='inner')
    t1, t2 = df.columns[:2]

    # Calcola spread percentuale
    df['spread_pct'] = (df[t1] - df[t2]) / df[t2] * 100

    # Applica segnali
    df['signal'] = df['spread_pct'].apply(assign_signal)

    # Salva report finale
    df.to_csv('data_sample/spread_report.csv', float_format='%.3f')
    logging.info("Saved improved report to data_sample/spread_report.csv")

