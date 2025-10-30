import pandas as pd, yfinance as yf, argparse, pathlib

ap = argparse.ArgumentParser()
ap.add_argument("--pair", required=True)
ap.add_argument("--a", required=True)
ap.add_argument("--b", required=True)
ap.add_argument("--out", required=True)
args = ap.parse_args()

TR = pathlib.Path("reports/wf_trades.csv")
t = pd.read_csv(TR, parse_dates=["entry_date","exit_date"])
t = t[t["pair"]==args.pair]
if t.empty: raise SystemExit(f"Nessun trade per {args.pair} in {TR}")
dates = pd.to_datetime(pd.concat([t["entry_date"], t["exit_date"]])
         ).dt.tz_convert("UTC").dt.normalize().dropna()
start=(dates.min()-pd.Timedelta(days=300)).date()  # più storia per OLS
end  =(dates.max()+pd.Timedelta(days=10)).date()

def hist(tk):
    x = yf.Ticker(tk)
    info = x.fast_info if hasattr(x,"fast_info") else {}
    cur  = getattr(info,"currency",None) if not isinstance(info,dict) else info.get("currency","USD")
    df = x.history(start=start, end=end, auto_adjust=False, actions=False)
    if df.empty: raise SystemExit(f"Nessun dato per {tk}")
    df = df.reset_index().rename(columns={"Date":"date"})
    col = "Adj Close" if "Adj Close" in df.columns else "Close"
    df["date"]=pd.to_datetime(df["date"], utc=True).dt.normalize()
    return df[["date",col]].rename(columns={col:"price"}), cur

A, curA = hist(args.a)
B, curB = hist(args.b)

if curA != curB:
    fx_tk = f"{curB}{curA}=X"   # converti B -> valuta di A
    fx = yf.Ticker(fx_tk).history(start=start, end=end, auto_adjust=False)
    if fx.empty: raise SystemExit(f"Nessun FX {fx_tk}")
    fx = fx.reset_index().rename(columns={"Date":"date"})
    col = "Adj Close" if "Adj Close" in fx.columns else "Close"
    fx["date"]=pd.to_datetime(fx["date"], utc=True).dt.normalize()
    fx = fx[["date",col]].rename(columns={col:"fx"})
    B = B.merge(fx, on="date", how="left").ffill()
    B["price"] = B["price"] * B["fx"]

rng = pd.DataFrame({"date": pd.date_range(start=start, end=end, tz="UTC", freq="D")})
A = rng.merge(A, on="date", how="left").ffill()
B = rng.merge(B[["date","price"]], on="date", how="left").ffill()

out = rng.merge(A, on="date", how="left").rename(columns={"price":"A_price"}) \
         .merge(B, on="date", how="left").rename(columns={"price":"B_price"})
out["A_ticker"], out["B_ticker"] = args.a, args.b
path=pathlib.Path(args.out); path.parent.mkdir(parents=True, exist_ok=True)
out[["date","A_ticker","A_price","B_ticker","B_price"]].to_csv(path, index=False)
print(f"[WROTE] {path} rows={len(out)} (FULL series; A={args.a} {curA}, B={args.b} -> {curA})")
