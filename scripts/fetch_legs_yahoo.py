import pandas as pd, numpy as np, yfinance as yf, pathlib

PAIR   = "IWDA_AS_EUNL_DE"
A_TICK = "IWDA.AS"   # iShares Core MSCI World UCITS (Euronext Amsterdam)
B_TICK = "EUNL.DE"   # iShares Core MSCI World UCITS (XETRA)
TRADES = pathlib.Path("reports/wf_trades.csv")
OUT    = pathlib.Path("data_sample/legs_IWDA_EUNL.csv")

t = pd.read_csv(TRADES, parse_dates=["entry_date","exit_date"])
t = t[t["pair"]==PAIR].copy()
if t.empty:
    raise SystemExit(f"Nessun trade per {PAIR} in {TRADES}")

need_dates = pd.to_datetime(pd.concat([t["entry_date"], t["exit_date"]]).dropna()
                ).dt.tz_convert("UTC").dt.normalize()
start = (need_dates.min() - pd.Timedelta(days=10)).date()
end   = (need_dates.max() + pd.Timedelta(days=10)).date()

def fetch_hist(ticker):
    df = yf.Ticker(ticker).history(start=start, end=end, auto_adjust=False, actions=False)
    if df.empty:
        raise SystemExit(f"Nessun dato Yahoo per {ticker}")
    df = df.reset_index().rename(columns={"Date":"date"})
    price_col = "Adj Close" if "Adj Close" in df.columns else "Close"
    df["date"] = pd.to_datetime(df["date"], utc=True).dt.normalize()
    return df[["date", price_col]].rename(columns={price_col:"price"})

a = fetch_hist(A_TICK)
b = fetch_hist(B_TICK)

# Serie giornaliera continua per ffill (nearest-previous)
all_days = pd.DataFrame({"date": pd.date_range(start=start, end=end, tz="UTC", freq="D")})
a2 = all_days.merge(a, on="date", how="left").sort_values("date"); a2["price"] = a2["price"].ffill()
b2 = all_days.merge(b, on="date", how="left").sort_values("date"); b2["price"] = b2["price"].ffill()

need = pd.DataFrame({"date": sorted(need_dates.unique())})
legs = need.merge(a2, on="date", how="left").rename(columns={"price":"A_price"})
legs = legs.merge(b2, on="date", how="left").rename(columns={"price":"B_price"})
legs["A_ticker"] = "IWDA_AS"
legs["B_ticker"] = "EUNL_DE"

# Fallback "nearest" se ancora mancano prezzi (es. date fuoricalendario)
if legs["A_price"].isna().any():
    avail_a = a[["date","price"]].set_index("date").sort_index()
    for i, r in legs[legs["A_price"].isna()].iterrows():
        dn = avail_a.index[np.argmin(np.abs((avail_a.index - r["date"]))) ]
        legs.at[i,"A_price"] = avail_a.loc[dn,"price"]
if legs["B_price"].isna().any():
    avail_b = b[["date","price"]].set_index("date").sort_index()
    for i, r in legs[legs["B_price"].isna()].iterrows():
        dn = avail_b.index[np.argmin(np.abs((avail_b.index - r["date"]))) ]
        legs.at[i,"B_price"] = avail_b.loc[dn,"price"]

OUT.parent.mkdir(parents=True, exist_ok=True)
legs = legs[["date","A_ticker","A_price","B_ticker","B_price"]].copy()
legs["date"] = legs["date"].dt.strftime("%Y-%m-%d 00:00:00+00:00")
legs.to_csv(OUT, index=False)
print(f"[WROTE] {OUT} rows={len(legs)}  (A={A_TICK}, B={B_TICK})")
