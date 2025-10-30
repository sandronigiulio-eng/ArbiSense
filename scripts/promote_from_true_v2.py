import pandas as pd, json
from collections import Counter
from pathlib import Path

TRADES_TRUE = Path("reports/wf_trades.true.csv")
METRICS_FP  = Path("reports/wf_metrics.csv")
BEST_FP     = Path("reports/wf_best_params.csv")
PRESETS_OUT = Path("reports/presets.json")

if not TRADES_TRUE.exists():
    raise SystemExit("Manca reports/wf_trades.true.csv (esegui prima recalc_true_pnl_v2.py)")

t = pd.read_csv(TRADES_TRUE)
m = pd.read_csv(METRICS_FP) if METRICS_FP.exists() else pd.DataFrame()

pairs = []
for pair, g in t.groupby("pair"):
    # totale TRUE
    oos_total = g["net_pnl"].sum(skipna=True)
    # folds attivi = fold con >=1 trade (TRUE)
    folds_active = g["fold"].nunique()

    if oos_total >= 0 and folds_active >= 3:
        # params di maggioranza da wf_best_params.csv (se c'è)
        params = {"pair": pair, "spread_scale": "auto"}
        if BEST_FP.exists():
            bp = pd.read_csv(BEST_FP)
            bp = bp[bp["pair"]==pair].copy()
            if not bp.empty:
                # prendi i parametri più frequenti tra i fold
                for k in ["z_enter","z_exit","z_stop","max_hold","latency","z_window","side","notional"]:
                    if k in bp.columns:
                        vals = bp[k].dropna().tolist()
                        if vals:
                            try:
                                params[k] = Counter(vals).most_common(1)[0][0]
                            except Exception:
                                pass
        pairs.append({
            "pair": pair,
            "created": pd.Timestamp.utcnow().isoformat(timespec="seconds"),
            "folds_active": int(folds_active),
            "oos_total_pnl": float(oos_total),
            "params": params
        })

with open(PRESETS_OUT, "w") as f:
    json.dump(pairs, f, indent=2)
print(f"[WROTE] {PRESETS_OUT}  presets={len(pairs)}")
for p in pairs:
    print(f"- {p['pair']}  OOS_TRUE {p['oos_total_pnl']:.2f}  folds {p['folds_active']}")
