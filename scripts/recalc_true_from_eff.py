import pandas as pd, numpy as np
from pathlib import Path

NOTIONAL = 250000.0
INP = Path("reports/wf_trades.csv")
OUT = Path("reports/wf_trades.true.csv")

t = pd.read_csv(INP)
need = {"entry_spread_eff","exit_spread_eff","direction","cost","pair","fold"}
missing = need - set(t.columns)
if missing:
    raise SystemExit(f"Mancano colonne nel wf_trades.csv: {missing}")

# direzione: LONG_SPREAD guadagna se (exit - entry) > 0; SHORT_SPREAD se < 0
dir_sign = np.where(t["direction"].astype(str).str.upper().str.contains("LONG"), +1.0, -1.0)

delta = t["exit_spread_eff"].astype(float) - t["entry_spread_eff"].astype(float)
gross = dir_sign * delta * NOTIONAL
net   = gross - t["cost"].astype(float)

t["net_pnl_true"] = net
t["net_pnl"] = net  # sovrascrivo per pipeline a valle

# clip outlier assurdi (> ±2,500 bps)
bps = t["net_pnl"] / NOTIONAL * 1e4
mask_ok = bps.abs().le(2500)
t.loc[~mask_ok, ["net_pnl","net_pnl_true"]] = np.nan

t.to_csv(OUT, index=False)
print(f"[WROTE] {OUT}  rows={len(t)}  nan_clipped={(~mask_ok).sum()}")
