import pandas as pd, yfinance as yf, argparse, pathlib, numpy as np

ap = argparse.ArgumentParser()
ap.add_argument("--pair", required=True)
ap.add_argument("--a", required=True, help="Ticker Yahoo gamba A (es. VUAA.L)")
ap.add_argument("--b", required=True, help="Ticker Yahoo gamba B (es. VUSA.L)")
ap.add_argument("--out", required=True)
args = ap.parse_args()

TRADES = pathlib.Path("reports/wf_trades.csv")
t = pd.read_csv(TRADES, parse_dates=["entry_date","exit_date"])
need = t[t["pair"]==args.pair]
if need.empty:
    raise SystemExit(f"Nessun trade per {args.pair} in {TRADES}")

need_dates = pd.to_datetime(pd.concat([need["entry_date"], need["exit_date"]]).dropna()
            ).dt.tz_convert("UTC").dt.normalize()
start = (need_dates.min() - pd.Timedelta(days=10))
end   = (need_dates.max() + pd.Timedelta(days=10))

def fetch_hist(ticker):
    tk = yf.Ticker(ticker)
    info = tk.fast_info if hasattr(tk, "fast_info") else {}
    currency = getattr(info, "currency", None) if not isinstance(info, dict) else info.get("currency")
    df = tk.history(start=start.date(), end=end.date(), auto_adjust=False, actions=False)
    if df.empty: raise SystemExit(f"Nessun dato Yahoo per {ticker}")
    df = df.reset_index().rename(columns={"Date":"date"})
    col = "Adj Close" if "Adj Close" in df.columns else "Close"
    df["date"] = pd.to_datetime(df["date"], utc=True).dt.normalize()
    return df[["date", col]].rename(columns={col:"price"}), (currency or "USD")

def fx_series(pair):
    # es. "GBPUSD=X", "EURUSD=X", "USDGBP=X"...
    tk = yf.Ticker(pair)
    df = tk.history(start=start.date(), end=end.date(), auto_adjust=False)
    if df.empty: return None
    df = df.reset_index().rename(columns={"Date":"date"})
    col = "Adj Close" if "Adj Close" in df.columns else "Close"
    df["date"] = pd.to_datetime(df["date"], utc=True).dt.normalize()
    return df[["date", col]].rename(columns={col:"fx"})

A, curA = fetch_hist(args.a)
B, curB = fetch_hist(args.b)

# Se valute diverse, converti B in valuta A
if curA != curB:
    # costruiamo ticker FX da Yahoo per B->A
    # mappa semplice: se A=USD, B=GBP -> "GBPUSD=X" (moltiplica B * fx per portare in USD)
    pair = f"{curB}{curA}=X"
    fx = fx_series(pair)
    if fx is None:
        raise SystemExit(f"Nessuna serie FX per {pair}; valuta A={curA}, B={curB}")
    rng = pd.DataFrame({"date": pd.date_range(start=start, end=end, tz="UTC", freq="D")})
    fx = rng.merge(fx, on="date", how="left").ffill()
    B = pd.merge(pd.DataFrame({"date": rng["date"]}), B, on="date", how="left").ffill()
    B["price"] = B["price"] * fx["fx"]  # porta B nella valuta di A
    curB = curA  # allineato

# costruisci serie giornaliere continue per ffill
rng = pd.DataFrame({"date": pd.date_range(start=start, end=end, tz="UTC", freq="D")})
A = rng.merge(A, on="date", how="left").ffill()
B = rng.merge(B, on="date", how="left").ffill()

need = pd.DataFrame({"date": sorted(need_dates.unique())})
out = need.merge(A, on="date", how="left").rename(columns={"price":"A_price"}) \
          .merge(B, on="date", how="left").rename(columns={"price":"B_price"})
out["A_ticker"], out["B_ticker"] = args.a, args.b

outp = pathlib.Path(args.out); outp.parent.mkdir(parents=True, exist_ok=True)
out[["date","A_ticker","A_price","B_ticker","B_price"]].to_csv(outp, index=False)
print(f"[WROTE] {outp} rows={len(out)}  (A={args.a} {curA}, B={args.b} {curB})")
