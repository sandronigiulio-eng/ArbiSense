#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
from pathlib import Path
import numpy as np
import pandas as pd
import yfinance as yf
import yaml
from datetime import datetime, timedelta, timezone

# Prova a usare una sessione curl_cffi (consigliata da yfinance)
CF_SESSION = None
try:
    from curl_cffi import requests as cfrequ
    # impersonate "chrome" rende le richieste più affidabili
    CF_SESSION = cfrequ.Session(impersonate="chrome")
except Exception:
    CF_SESSION = None  # yfinance userà la sua sessione interna

def _choose_close(df):
    if df is None or len(df) == 0:
        return pd.Series(dtype=float)
    if "Close" in df:
        s = df["Close"].dropna()
        if not s.empty:
            return s
    if "Adj Close" in df:
        s = df["Adj Close"].dropna()
        if not s.empty:
            return s
    if isinstance(df, pd.Series):
        return df.dropna()
    return pd.Series(dtype=float)

def fetch_close_series(ticker, lookback_days=10):
    """
    Prova più modalità per recuperare una serie daily recente.
    Ritorna (serie, metodo_usato).
    """
    # yfinance Ticker: passa la sessione solo se è di tipo curl_cffi
    try:
        tkr = yf.Ticker(ticker, session=CF_SESSION) if CF_SESSION is not None else yf.Ticker(ticker)
    except Exception:
        tkr = yf.Ticker(ticker)

    # 1) period=lookback_days
    try:
        h = tkr.history(period=f"{lookback_days}d", auto_adjust=True, actions=False, interval="1d")
        s = _choose_close(h)
        if not s.empty:
            return s, f"TICKER.period={lookback_days}d"
    except Exception:
        pass

    # 2) period=30d
    try:
        h = tkr.history(period="30d", auto_adjust=True, actions=False, interval="1d")
        s = _choose_close(h)
        if not s.empty:
            return s, "TICKER.period=30d"
    except Exception:
        pass

    # 3) start = (oggi - max(90, 3*lookback)) giorni
    try:
        start = (datetime.now(timezone.utc) - timedelta(days=max(lookback_days*3, 90))).date().isoformat()
        h = tkr.history(start=start, auto_adjust=True, actions=False, interval="1d")
        s = _choose_close(h)
        if not s.empty:
            return s, f"TICKER.start={start}"
    except Exception:
        pass

    # 4) fallback con yf.download (accetta sessione curl_cffi se disponibile)
    try:
        h = yf.download(ticker, period="30d", auto_adjust=True, progress=False,
                        interval="1d", session=CF_SESSION) if CF_SESSION is not None \
            else yf.download(ticker, period="30d", auto_adjust=True, progress=False, interval="1d")
        s = _choose_close(h)
        if not s.empty:
            return s, "DOWNLOAD.period=30d"
    except Exception:
        pass

    return pd.Series(dtype=float), "EMPTY"

def last_close(ticker, lookback_days=10):
    """
    Restituisce (timestamp_UTC, last_close_price, currency, metodo).
    """
    s, method = fetch_close_series(ticker, lookback_days=lookback_days)
    if s.empty:
        return None, None, None, method

    ts = pd.Timestamp(s.index[-1])
    if ts.tz is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")

    # valuta
    cur = None
    try:
        tkr = yf.Ticker(ticker, session=CF_SESSION) if CF_SESSION is not None else yf.Ticker(ticker)
        cur = tkr.fast_info.get("currency", None)
        if not cur:
            info = tkr.get_info()
            cur = info.get("currency")
    except Exception:
        cur = None

    return ts.to_pydatetime(), float(s.iloc[-1]), cur, method

def fx_rate(from_cur, to_cur, lookback_days=7):
    """
    (tasso, nota). Gestisce GBp/GBX -> GBP (x0.01) e poi GBP->dest.
    """
    if from_cur is None or to_cur is None:
        return float("nan"), "FX_FAIL missing currency"

    fc_raw = str(from_cur)
    tc_raw = str(to_cur)
    fcU, tcU = fc_raw.upper(), tc_raw.upper()

    if fc_raw in {"GBp", "GBx"} or fcU == "GBX":
        rate_to_dest, note = fx_rate("GBP", tcU, lookback_days=lookback_days)
        return 0.01 * rate_to_dest, f"{fc_raw}->GBP 0.01; {note}"

    if fcU == tcU:
        return 1.0, "1.0"

    pair = f"{fcU}{tcU}=X"
    s, fx_method = fetch_close_series(pair, lookback_days=lookback_days)
    if s.empty:
        return np.nan, f"FX_FAIL {pair}: EMPTY"
    rate = float(s.iloc[-1])
    return rate, f"FX {pair}={rate:.6f} ({fx_method})"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cfg", default="config/pairs_live.yaml")
    ap.add_argument("--outdir", default="data_live")
    ap.add_argument("--lookback_days", type=int, default=10)
    args = ap.parse_args()

    Path(args.outdir).mkdir(parents=True, exist_ok=True)

    with open(args.cfg, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}

    for p in cfg.get("pairs", []):
        pair = p["pair"]; a = p["a"]; b = p["b"]; denom = p.get("denom", "B")

        tsA, priceA, curA, mA = last_close(a, args.lookback_days)
        tsB, priceB, curB, mB = last_close(b, args.lookback_days)

        if not all([tsA, tsB]) or priceA is None or priceB is None or not curA or not curB:
            print(f"[WARN] Missing data for {pair} ({a}/{b})  [methods: A={mA}, B={mB}]")
            continue

        fx_note = "1.0"
        if denom.upper() == "B" and curA != curB:
            rate, fx_note = fx_rate(curA, curB, lookback_days=7)
            if np.isnan(rate):
                print(f"[WARN] FX missing {curA}->{curB} for {pair}; skip  [methods: A={mA}, B={mB}]")
                continue
            priceA = priceA * rate

        ts = max(tsA, tsB)
        row = {
            "date": ts.isoformat(),
            "pair": pair,
            "A_ticker": a, "A_price": priceA,
            "B_ticker": b, "B_price": priceB,
            "fx_used": fx_note, "curA": curA, "curB": curB, "denom": denom,
            "methodA": mA, "methodB": mB
        }

        fp = Path(args.outdir) / f"legs_{pair}.parquet"
        if fp.exists():
            old = pd.read_parquet(fp)
            df = pd.concat([old, pd.DataFrame([row])], ignore_index=True)
            df = df.drop_duplicates(subset=["date"]).sort_values("date")
        else:
            df = pd.DataFrame([row])

        df.to_parquet(fp, index=False)
        print(f"[INGEST] {pair} @ {ts.date()}  A={row['A_price']:.6f}  B={row['B_price']:.6f}  "
              f"({fx_note}; A:{mA} B:{mB}) -> {fp}")

    print("[OK] Ingest done.")

if __name__ == "__main__":
    main()
