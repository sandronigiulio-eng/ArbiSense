import pandas as pd, json
from pathlib import Path

TR = Path("reports/wf_trades.true.csv")
ME = Path("reports/wf_metrics.csv")
OUT = Path("reports/presets.json")
NOTIONAL = 250000.0

t = pd.read_csv(TR)
m = pd.read_csv(ME)
ok = []

for pair, g in t.groupby("pair"):
    # usa solo trade con net_pnl non NaN (validi)
    gv = g.dropna(subset=["net_pnl"]).copy()
    folds_active = gv["fold"].nunique()
    if gv.empty or folds_active < 3:
        continue
    pnl = gv["net_pnl"].sum()
    # sanity in bps
    gv["bps"] = gv["net_pnl"]/NOTIONAL*1e4
    med_abs = gv["bps"].abs().median()
    max_abs = gv["bps"].abs().max()
    # regole di plausibilità: mediana <= 300 bps, max <= 1000 bps
    if med_abs <= 300 and max_abs <= 1000 and pnl >= 0:
        # parametri minimi (qui puoi arricchire dai best_params se vuoi)
        ok.append({
            "pair": pair,
            "created": pd.Timestamp.utcnow().isoformat(timespec="seconds"),
            "folds_active": int(folds_active),
            "oos_total_pnl": float(pnl),
            "params": {"pair": pair, "spread_scale":"auto"}
        })

with open(OUT, "w") as f: json.dump(ok, f, indent=2)
print(f"[WROTE] {OUT} presets={len(ok)}")
for p in ok:
    print(f"- {p['pair']} OOS_TRUE {p['oos_total_pnl']:.2f} folds {p['folds_active']}")
