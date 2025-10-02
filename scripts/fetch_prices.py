#!/usr/bin/env python3
import sys
import yfinance as yf
import pandas as pd

def fetch_and_save(ticker):
    data = yf.download(ticker, period="2y", interval="1d", progress=False)
    data = data[['Close']].rename(columns={'Close': ticker})
    filename = f"data_sample/{ticker}.csv"
    data.to_csv(filename)
    print(f"Saved {filename}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python scripts/fetch_prices.py TICKER1 [TICKER2 ...]")
        sys.exit(1)
    for t in sys.argv[1:]:
        fetch_and_save(t)
