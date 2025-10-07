#!/usr/bin/env python3
import subprocess
import logging
import os

# Configura logging
logging.basicConfig(
    format='%(asctime)s %(levelname)s: %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Lista dei ticker di esempio
tickers = ['SPY', 'IVV']

# Cartella dati
data_folder = 'data_sample'
os.makedirs(data_folder, exist_ok=True)

# Scarica i dati
for ticker in tickers:
    logging.info(f"Fetching {ticker}...")
    cmd = f"python scripts/fetch_prices.py {ticker}"
    subprocess.run(cmd, shell=True, check=True)
    logging.info(f"Saved {data_folder}/{ticker}.csv")

# Calcola lo spread
cmd_calc = f"python scripts/calculate_spread.py {data_folder}/{tickers[0]}.csv {data_folder}/{tickers[1]}.csv"
logging.info("Calcolo spread...")
subprocess.run(cmd_calc, shell=True, check=True)

# Genera il grafico
cmd_plot = "python scripts/plot_spread.py"
logging.info("Generazione grafico...")
subprocess.run(cmd_plot, shell=True, check=True)

logging.info("Tutto completato con successo!")

