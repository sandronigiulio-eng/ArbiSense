import pandas as pd, json, numpy as np
from pathlib import Path
from collections import Counter

TR = Path("reports/wf_trades.true.csv")
ME = Path("reports/wf_metrics.csv")
OUT = Path("reports/presets.json")
NOTIONAL = 250000.0

t = pd.read_csv(TR)
m = pd.read_csv(ME) if ME.exists() else pd.DataFrame()

presets = []

for pair, g0 in t.groupby("pair"):
    g = g0.dropna(subset=["net_pnl"]).copy()
    folds_active = g["fold"].nunique()
    if g.empty or folds_active < 3:
        continue

    # bps realistici per trade
    g["bps"] = g["net_pnl"] / NOTIONAL * 1e4

    # statistiche robuste per-coppia
    med_abs = g["bps"].abs().median()
    q90_abs = g["bps"].abs().quantile(0.90)
    q95_abs = g["bps"].abs().quantile(0.95)
    max_abs = g["bps"].abs().max()
    pnl = g["net_pnl"].sum()

    # qualità delle uscite
    if "reason_exit" in g.columns:
        c = g["reason_exit"].fillna("NA").value_counts()
        mrv = int(c.get("MEAN_REVERT", 0))
        tmo = int(c.get("TIMEOUT", 0))
        mr_ratio = mrv / max(1, (mrv + tmo))
    else:
        mr_ratio = 1.0  # se non c'è la colonna, non penalizziamo

    # GATE "seri" ma data-driven:
    # - profitto e copertura fold
    ok_profit = (pnl >= 0) and (folds_active >= 3)
    # - scala plausibile (calcolata sui trade della coppia)
    ok_bps = (med_abs <= 900) and (q95_abs <= 1500) and (max_abs <= 1800)
    # - dinamica sana: almeno metà uscite per mean-revert
    ok_exit = (mr_ratio >= 0.5)

    if ok_profit and ok_bps and ok_exit:
        # parametri: se hai wf_best_params.csv, qui puoi consolidare,
        # altrimenti salviamo un preset minimale lato coppia
        params = {"pair": pair, "spread_scale": "auto"}
        presets.append({
            "pair": pair,
            "created": pd.Timestamp.utcnow().isoformat(timespec="seconds"),
            "folds_active": int(folds_active),
            "oos_total_pnl": float(pnl),
            "stats": {
                "trades": int(len(g)),
                "median_abs_bps": float(med_abs),
                "q95_abs_bps": float(q95_abs),
                "max_abs_bps": float(max_abs),
                "mr_ratio": float(mr_ratio)
            },
            "params": params
        })

with open(OUT, "w") as f:
    json.dump(presets, f, indent=2)

print(f"[WROTE] {OUT} presets={len(presets)}")
for p in presets:
    s = p["stats"]
    print(f"- {p['pair']}  OOS_TRUE {p['oos_total_pnl']:.2f}  folds {p['folds_active']}  "
          f"med|bps| {s['median_abs_bps']:.1f}  q95 {s['q95_abs_bps']:.1f}  max {s['max_abs_bps']:.1f}  MR% {s['mr_ratio']:.0%}")
