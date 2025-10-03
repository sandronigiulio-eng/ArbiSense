#!/usr/bin/env python3
import sys
import logging
import yfinance as yf
import pandas as pd
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

DATA_DIR = "data_sample"

def fetch_and_save(ticker):
    try:
        logger.info(f"Fetching {ticker}...")
        data = yf.download(ticker, period="2y", interval="1d", progress=False)
        if data.empty:
            logger.warning(f"No data for {ticker}")
            return
        data = data[['Close']].rename(columns={'Close': ticker})
        os.makedirs(DATA_DIR, exist_ok=True)
        filename = f"{DATA_DIR}/{ticker}.csv"
        data.to_csv(filename)
        logger.info(f"Saved {filename}")
    except Exception as e:
        logger.error(f"Error fetching {ticker}: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        logger.error("Usage: python scripts/fetch_prices.py TICKER1 [TICKER2 ...]")
        sys.exit(1)
    for t in sys.argv[1:]:
        fetch_and_save(t)
