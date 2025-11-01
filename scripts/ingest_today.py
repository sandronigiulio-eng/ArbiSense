import argparse, pandas as pd, numpy as np, yfinance as yf, sys
from pathlib import Path
import yaml

ap = argparse.ArgumentParser()
ap.add_argument("--cfg", default="config/pairs_live.yaml")
ap.add_argument("--outdir", default="data_live")
ap.add_argument("--lookback_days", type=int, default=10)
args = ap.parse_args()

Path(args.outdir).mkdir(parents=True, exist_ok=True)

def fx_rate(from_cur, to_cur):
    # gestisce anche pence (GBp/GBX) -> GBP (x0.01), poi GBP->dest
    if from_cur is None or to_cur is None:
        return float('nan'), "FX_FAIL missing currency"
    fc_raw = str(from_cur)
    tc_raw = str(to_cur)
    fcU, tcU = fc_raw.upper(), tc_raw.upper()

    if fc_raw in {"GBp","GBx"} or fcU == "GBX":
        rate_to_dest, note = fx_rate("GBP", tcU)
        return 0.01 * rate_to_dest, f"{fc_raw}->GBP 0.01; {note}"

    if fcU == tcU:
        return 1.0, "1.0"

    pair = f"{fcU}{tcU}=X"
    try:
        fx = yf.Ticker(pair).history(period="7d")["Close"].dropna()
        rate = float(fx.iloc[-1])
        return rate, f"FX {pair}={rate:.6f}"
    except Exception as e:
        return np.nan, f"FX_FAIL {pair}: {e}"

def last_close(ticker, lookback_days=10):
    try:
        h = yf.Ticker(ticker).history(period=f"{lookback_days}d")["Close"].dropna()
        if h.empty: return None, None, None
        cur = yf.Ticker(ticker).fast_info.get("currency", None)
        ts = h.index[-1].tz_convert("UTC").to_pydatetime()
        return ts, float(h.iloc[-1]), cur
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
    # porta A nella valuta di B se denom==B (tuo caso)
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
