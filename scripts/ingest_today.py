import argparse, pandas as pd, numpy as np, yfinance as yf, sys
from pathlib import Path
import datetime as dt
import yaml

ap = argparse.ArgumentParser()
ap.add_argument("--cfg", default="config/pairs_live.yaml")
ap.add_argument("--outdir", default="data_live")
ap.add_argument("--lookback_days", type=int, default=10)
args = ap.parse_args()

Path(args.outdir).mkdir(parents=True, exist_ok=True)

def fx_rate(from_cur, to_cur):
    if from_cur == to_cur: return 1.0, "1.0"
    if from_cur == "GBX":
        rate, note = fx_rate("GBP", to_cur)
        return 0.01*rate, f"GBX->GBP 0.01; {note}"
    pair = f"{from_cur}{to_cur}=X"
    try:
        fx = yf.Ticker(pair).history(period="7d")["Close"].dropna()
        return float(fx.iloc[-1]), f"FX {pair}"
    except Exception as e:
        return np.nan, f"FX_FAIL {pair}: {e}"

def last_close(ticker, lookback_days=10):
    try:
        h = yf.Ticker(ticker).history(period=f"{lookback_days}d")["Close"].dropna()
        if h.empty: return None, None, None
        cur = yf.Ticker(ticker).fast_info.get("currency", None)
        return h.index[-1].tz_convert("UTC").to_pydatetime(), float(h.iloc[-1]), cur
    except Exception:
        return None, None, None

cfg = yaml.safe_load(open(args.cfg))
for p in cfg.get("pairs", []):
    pair, a, b, denom = p["pair"], p["a"], p["b"], p.get("denom","B")
    tsA, priceA, curA = last_close(a, args.lookback_days)
    tsB, priceB, curB = last_close(b, args.lookback_days)
    if not all([tsA, tsB, priceA, priceB, curA, curB]):
        print(f"[WARN] Missing data for {pair} ({a}/{b})"); continue
    fx_note = "1.0"
    if denom.upper()=="B" and curA != curB:
        rate, fx_note = fx_rate(curA, curB)
        if np.isnan(rate):
            print(f"[WARN] FX missing {curA}->{curB} for {pair}; skip"); continue
        priceA = priceA * rate
    ts = max(tsA, tsB)
    row = {
        "date": ts.isoformat(),
        "pair": pair,
        "A_ticker": a, "A_price": priceA,
        "B_ticker": b, "B_price": priceB,
        "fx_used": fx_note, "curA": curA, "curB": curB, "denom": denom
    }
    fp = Path(args.outdir) / f"legs_{pair}.parquet"
    if fp.exists():
        old = pd.read_parquet(fp)
        df  = pd.concat([old, pd.DataFrame([row])], ignore_index=True)
        df  = df.drop_duplicates(subset=["date"]).sort_values("date")
    else:
        df = pd.DataFrame([row])
    df.to_parquet(fp, index=False)
    print(f"[INGEST] {pair} @ {ts.date()}  A={row['A_price']:.6f}  B={row['B_price']:.6f}  ({fx_note}) -> {fp}")
print("[OK] Ingest done.")
