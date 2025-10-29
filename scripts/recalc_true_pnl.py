import pandas as pd, numpy as np, os

N = 250_000.0  # notional usato nel WF

trades_path = "reports/wf_trades.csv"
norm_path   = "data_sample/spread_report_all_pairs_long.normalized.csv"

if not os.path.exists(trades_path):
    raise SystemExit("wf_trades.csv non trovato.")
if not os.path.exists(norm_path):
    raise SystemExit("normalized CSV non trovato.")

t = pd.read_csv(trades_path)
if t.empty:
    raise SystemExit("wf_trades.csv vuoto.")

# Parse date columns
for c in ["entry_date","exit_date"]:
    if c in t.columns:
        t[c] = pd.to_datetime(t[c], utc=True, errors="coerce")
    else:
        raise SystemExit(f"Manca colonna {c} in wf_trades.csv")

# Carica normalizzato
df = pd.read_csv(norm_path)
date_col = "date" if "date" in df.columns else ("timestamp" if "timestamp" in df.columns else None)
if not date_col:
    raise SystemExit("Il file normalizzato deve avere 'date' o 'timestamp'")
df[date_col] = pd.to_datetime(df[date_col], utc=True, errors="coerce")
df = df.dropna(subset=[date_col, "pair", "spread_raw", "spread_scale"]).copy()
df["spread_eff_norm"] = pd.to_numeric(df["spread_raw"], errors="coerce") * pd.to_numeric(df["spread_scale"], errors="coerce")
df = df.dropna(subset=["spread_eff_norm"])
df = df.sort_values([ "pair", date_col ])

# Funzione per fare merge 'nearest' per pair
def nearest_merge(trades, side_col):
    # side_col: "entry" oppure "exit"
    key = f"{side_col}_date"
    m = trades[["pair", key]].rename(columns={key: "ts"})
    res_list=[]
    for pair, g in m.groupby("pair"):
        g = g.sort_values("ts")
        ref = df[df["pair"]==pair][[date_col, "spread_eff_norm"]].rename(columns={date_col:"ts"})
        if ref.empty:
            # nessun dato per questa pair
            tmp = g.copy()
            tmp[f"{side_col}_eff_true"] = np.nan
            res_list.append(tmp)
            continue
        # merge_asof nearest con tolleranza 3 giorni
        mg = pd.merge_asof(g, ref.sort_values("ts"), on="ts", direction="nearest", tolerance=pd.Timedelta("3D"))
        mg[f"{side_col}_eff_true"] = mg["spread_eff_norm"]
        res_list.append(mg[["pair","ts",f"{side_col}_eff_true"]].rename(columns={"ts":key}))
    out = pd.concat(res_list, ignore_index=True)
    return out

e = nearest_merge(t, "entry")
x = nearest_merge(t, "exit")
tt = t.merge(e, on=["pair","entry_date"], how="left").merge(x, on=["pair","exit_date"], how="left")

# Calcolo PnL vero
def pnl_true(r):
    e = float(r["entry_eff_true"]) if pd.notna(r["entry_eff_true"]) else np.nan
    x = float(r["exit_eff_true"])  if pd.notna(r["exit_eff_true"])  else np.nan
    if np.isnan(e) or np.isnan(x):
        return np.nan
    if r["direction"]=="SHORT_SPREAD":
        return N * (e - x)
    else:  # LONG_SPREAD
        return N * (x - e)

tt["net_pnl_true"] = tt.apply(pnl_true, axis=1)
ok = tt["net_pnl_true"].notna().sum()
miss = len(tt) - ok

print(f"Trades totali: {len(tt)} | allineati: {ok} | non allineati: {miss}")
print("SUM PnL orig :", round(float(tt["net_pnl"].sum()), 2) if "net_pnl" in tt.columns else "N/D")
print("SUM PnL TRUE :", round(float(tt["net_pnl_true"].sum()), 2))
print("\nPnL TRUE per pair:")
print(tt.groupby("pair")["net_pnl_true"].sum().sort_values(ascending=False).round(2).to_string())

os.makedirs("reports", exist_ok=True)
tt.to_csv("reports/wf_trades.true.csv", index=False)
print("\n[WROTE] reports/wf_trades.true.csv")
