import pandas as pd
t = pd.read_csv("reports/wf_trades.true.csv")
g = t[t["pair"]=="IWDA_AS_EUNL_DE"].dropna(subset=["net_pnl"]).copy()
if g.empty:
    raise SystemExit("Nessun trade valido IWDA in wf_trades.true.csv")
NOTIONAL=250000.0
g["bps"] = g["net_pnl"]/NOTIONAL*1e4
print("== IWDA reason_exit ==")
print(g["reason_exit"].value_counts(dropna=False).to_string())
print("\n== IWDA bps stats ==")
print(g["bps"].describe(percentiles=[.5,.9,.95]).to_string())
print("\n== IWDA folds attivi ==", g["fold"].nunique())
