import pandas as pd, sys
from pathlib import Path

m = pd.read_csv("reports/wf_metrics.csv")
# preferisci il TRUE se presente
tpath_true = Path("reports/wf_trades.true.csv")
t = pd.read_csv(tpath_true) if tpath_true.exists() else pd.read_csv("reports/wf_trades.csv")

# Totali OOS per coppia
agg = t.groupby("pair").agg(
    trades=("net_pnl","size"),
    pnl_net=("net_pnl","sum")
).reset_index()

# Folds >= 0 dalle metrics
folds = m[m["net_pnl_total"].notna()].copy()
folds["pos"] = (folds["net_pnl_total"] >= 0).astype(int)
folds_pos = folds.groupby("pair")["pos"].sum().reset_index(name="folds_pos_ge0")

out = agg.merge(folds_pos, on="pair", how="left").fillna(0)

print("\n== OOS summary by pair (TRUE if available) ==")
print(out.sort_values("pnl_net", ascending=False).to_string(index=False))

# Sanity check in bps (pnl / notional * 1e4) per coppia
NOTIONAL = 250000.0
t["pnl_bps"] = t["net_pnl"] / NOTIONAL * 1e4
bps = t.groupby("pair")["pnl_bps"].agg(["count","mean","median","std","min","max"]).reset_index()
print("\n== PnL per trade (bps) ==")
print(bps.to_string(index=False))

# Focus SWDA
sw = t[t["pair"]=="SWDA_L_EUNL_DE"].copy()
if not sw.empty:
    print("\n== SWDA exits reasons ==")
    if "reason_exit" in sw.columns:
        print(sw["reason_exit"].value_counts(dropna=False).to_string())
    else:
        print("(no reason_exit column)")
    print("\n== SWDA totals ==")
    print(f"trades={len(sw)}  pnl_net={sw['net_pnl'].sum():.2f}  median_bps={sw['pnl_bps'].median():.2f}")
