#!/usr/bin/env python3
"""
Robust run_mvp.py

Funzioni:
- fetch_and_save(ticker, path): scarica i prezzi con yfinance, salva CSV solo se ci sono dati
- compute_spread(file_a, file_b): legge i CSV, allinea per data, calcola spread_pct
- save_report_and_plot(df_spread, pair_name): salva CSV report e plot PNG
- main: itera sulle coppie (da config/pairs.csv se presente, altrimenti usa PAIRS di default)

Uso:
  python scripts/run_mvp.py            # esegue tutte le coppie (default)
  python scripts/run_mvp.py --pair CSP1.L:IUSA.DE  # esegui solo una coppia
"""

import logging
import time
from pathlib import Path
import argparse
import sys

import pandas as pd
import yfinance as yf
import matplotlib.pyplot as plt

# --- CONFIG ---
DATA_DIR = Path("data_sample")
REPORTS_DIR = Path("reports")
CONFIG_DIR = Path("config")
BLACKLIST_FILE = CONFIG_DIR / "blacklist.txt"
PAIRS_FILE = CONFIG_DIR / "pairs.csv"   # optional: two columns ticker_a,ticker_b (no header or with header)
# default pairs (can be customized)
PAIRS = [
    ("CSP1.L", "IUSA.DE"),
    ("VWRL.L", "VEVE.AS"),
    ("SWDA.L", "EUNL.DE"),
    ("SWDA.L", "IWRD.DE"),  # this one may be delisted -> will be skipped
]

MIN_ROWS_FOR_REPORT = 10  # se dopo l'allineamento ci sono meno di queste righe skip

# --- Logging setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# --- Helper: blacklist loader ---
def load_blacklist(path: Path):
    if not path.exists():
        return set()
    try:
        lines = [l.strip() for l in path.read_text(encoding="utf-8").splitlines()]
        return {l for l in lines if l and not l.startswith("#")}
    except Exception as e:
        logging.warning("Could not read blacklist %s: %s", path, e)
        return set()

# --- Robust fetch helper ---
def fetch_and_save(ticker: str, out_path: Path, period="2y", interval="1d",
                   retries: int = 2, backoff_sec: float = 1.0, blacklist=None) -> bool:
    """
    Scarica i prezzi con yfinance e salva out_path SOLO se ci sono dati validi.
    Ritorna True se il file è stato salvato, False altrimenti.
    """
    if blacklist is None:
        blacklist = set()
    if ticker in blacklist:
        logging.info("Ticker %s in blacklist — skipping", ticker)
        return False

    for attempt in range(1, retries + 1):
        try:
            df = yf.download(ticker, period=period, interval=interval, progress=False, auto_adjust=True)
        except Exception as e:
            logging.warning("Attempt %d: download failed for %s: %s", attempt, ticker, e)
            if attempt < retries:
                time.sleep(backoff_sec * attempt)
                continue
            return False

        if df is None or df.empty:
            logging.warning("No data found for %s (rows=0).", ticker)
            return False

        # prefer 'Close' column when present
        if "Close" in df.columns:
            series = df["Close"]
        else:
            # fallback: first column
            series = df.iloc[:, 0]

        out_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            # Save series as CSV (date index preserved)
            series.to_csv(out_path, header=True)
            logging.info("Saved %s (rows=%d)", out_path, len(series))
            return True
        except Exception as e:
            logging.error("Failed to save %s: %s", out_path, e)
            return False

    return False

# --- Compute spread ---
def compute_spread(file_a: Path, file_b: Path) -> pd.DataFrame:
    """
    Legge due CSV (index date), allinea le date e calcola spread_pct = (price_a / price_b - 1) * 100.
    Ritorna DataFrame con colonne: price_a, price_b, spread_pct
    """
    df_a = pd.read_csv(file_a, index_col=0, parse_dates=True)
    df_b = pd.read_csv(file_b, index_col=0, parse_dates=True)

    # Convert to Series (if DataFrame)
    if isinstance(df_a, pd.DataFrame):
        if "Close" in df_a.columns:
            s_a = df_a["Close"]
        else:
            s_a = df_a.iloc[:, 0]
    else:
        s_a = df_a.squeeze()

    if isinstance(df_b, pd.DataFrame):
        if "Close" in df_b.columns:
            s_b = df_b["Close"]
        else:
            s_b = df_b.iloc[:, 0]
    else:
        s_b = df_b.squeeze()

    # align on intersection of dates
    df = pd.concat([s_a.rename("price_a"), s_b.rename("price_b")], axis=1, join="inner").dropna()

    if df.empty:
        return df

    # Calculate spread_pct as percentage
    df["spread_pct"] = (df["price_a"] / df["price_b"] - 1.0) * 100.0

    # Make sure index name is Date for readability
    df.index.name = "Date"
    return df

# --- Save report and plot ---
def save_report_and_plot(df_spread: pd.DataFrame, pair_name: str) -> None:
    """
    Salva CSV in data_sample/ e plot PNG in reports/
    pair_name: e.g. CSP1_L_IUSA_DE
    """
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    csv_path = DATA_DIR / f"spread_report_{pair_name}.csv"
    png_path = REPORTS_DIR / f"spread_plot_{pair_name}.png"

    # Save CSV
    df_spread.to_csv(csv_path)
    logging.info("Saved report %s (rows=%d)", csv_path, len(df_spread))

    # Plot
    try:
        plt.figure(figsize=(10, 4))
        plt.plot(df_spread.index, df_spread["spread_pct"], label="spread_pct")
        plt.title(f"Spread % — {pair_name}")
        plt.xlabel("Date")
        plt.ylabel("Spread (%)")
        plt.grid(True)
        plt.tight_layout()
        plt.savefig(png_path)
        plt.close()
        logging.info("Saved plot %s", png_path)
    except Exception as e:
        logging.error("Failed to create plot for %s: %s", pair_name, e)

# --- Utility: normalize ticker for filename ---
def pair_name_from(t1: str, t2: str) -> str:
    safe = lambda s: s.replace(".", "_").replace("/", "_").replace(":", "_")
    return f"{safe(t1)}_{safe(t2)}"

# --- Load pairs from config (optional) ---
def load_pairs_from_file(path: Path):
    if not path.exists():
        return []
    try:
        df = pd.read_csv(path, header=None)
        # accept lines like "TICKA,TICKB" or single column with tab/space
        pairs = []
        for _, row in df.iterrows():
            if len(row.dropna()) >= 2:
                a = str(row.iloc[0]).strip()
                b = str(row.iloc[1]).strip()
                pairs.append((a, b))
        return pairs
    except Exception as e:
        logging.warning("Could not read pairs file %s: %s", path, e)
        return []

# --- Main ---
def main(run_pair: str = None):
    # load blacklist and pairs
    blacklist = load_blacklist(BLACKLIST_FILE)
    pairs = []

    if PAIRS_FILE.exists():
        pairs = load_pairs_from_file(PAIRS_FILE)
        logging.info("Loaded %d pairs from %s", len(pairs), PAIRS_FILE)
    if not pairs:
        pairs = PAIRS.copy()
        logging.info("Using default PAIRS (%d pairs)", len(pairs))

    # if a single pair was provided via CLI
    if run_pair:
        if ":" in run_pair:
            t1, t2 = run_pair.split(":", 1)
        elif "," in run_pair:
            t1, t2 = run_pair.split(",", 1)
        else:
            logging.error("Invalid --pair format. Use TICKER1:TICKER2")
            sys.exit(2)
        pairs = [(t1.strip(), t2.strip())]

    any_written = False

    for t1, t2 in pairs:
        logging.info("Processing pair %s - %s", t1, t2)
        file1 = DATA_DIR / f"{t1.replace('.', '_')}.csv"
        file2 = DATA_DIR / f"{t2.replace('.', '_')}.csv"

        ok1 = fetch_and_save(t1, file1, blacklist=blacklist)
        ok2 = fetch_and_save(t2, file2, blacklist=blacklist)

        if not ok1 or not ok2:
            logging.info("Skipping pair %s - %s (ok1=%s ok2=%s)", t1, t2, ok1, ok2)
            # ensure we don't leave empty files: remove if created and size small
            for f in (file1, file2):
                try:
                    if f.exists() and f.stat().st_size < 100:
                        f.unlink()
                except Exception:
                    pass
            continue

        # compute spread
        try:
            df_spread = compute_spread(file1, file2)
        except Exception as e:
            logging.error("Failed to compute spread for %s-%s: %s", t1, t2, e)
            continue

        if df_spread.empty or len(df_spread) < MIN_ROWS_FOR_REPORT:
            logging.info("Spread data for %s-%s too short (rows=%d). skipping saving.", t1, t2, len(df_spread))
            continue

        # save report & plot
        pair_name = pair_name_from(t1, t2)
        try:
            save_report_and_plot(df_spread, pair_name)
            any_written = True
        except Exception as e:
            logging.error("Failed saving report/plot for %s-%s: %s", t1, t2, e)

    if any_written:
        logging.info("run_mvp: completed with new reports.")
    else:
        logging.info("run_mvp: completed — no new reports produced.")

# --- Entry point ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run MVP: fetch prices, compute spreads, save reports")
    parser.add_argument("--pair", help="optional single pair to run, format TICKER1:TICKER2", default=None)
    args = parser.parse_args()
    main(run_pair=args.pair)

