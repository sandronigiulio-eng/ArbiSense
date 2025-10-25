#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ArbiSense — Grid Search dei parametri del backtest

Esegue backtest_signals.py molte volte con combinazioni di:
- z-enter, z-exit, z-stop, max-hold, latency-days
e salva i risultati in:
  reports/opt_results.csv   (tutte le combinazioni)
  reports/opt_best.json     (migliore combinazione)
Criterio: massimizza Sharpe; tie-break per net_pnl_total e MaxDD (più alto Sharpe, più alto PnL, minore MaxDD).
"""

import os, sys, json, itertools, subprocess
from pathlib import Path
import pandas as pd

BASE_DIR = Path(__file__).parent.parent
BACKTEST = BASE_DIR / "scripts" / "backtest_signals.py"
METRICS  = BASE_DIR / "reports" / "backtest_metrics.csv"
RESULTS  = BASE_DIR / "reports" / "opt_results.csv"
BESTJSON = BASE_DIR / "reports" / "opt_best.json"

def run_bt(pairs_file, params):
    cmd = [
        "python3", str(BACKTEST),
        "--pairs-file", str(pairs_file),
        "--z-enter",  str(params["z_enter"]),
        "--z-exit",   str(params["z_exit"]),
        "--z-stop",   str(params["z_stop"]),
        "--latency-days", str(params["latency_days"]),
        "--max-hold", str(params["max_hold"]),
        "--fee-bps",  str(params["fee_bps"]),
        "--slippage-bps", str(params["slippage_bps"]),
        "--notional", str(params["notional"]),
        "--z-window", str(params["z_window"]),
        "--spread-scale", str(params["spread_scale"])
    ]
    out = subprocess.run(cmd, capture_output=True, text=True)
    if out.returncode != 0:
        raise RuntimeError(out.stderr or out.stdout or "Backtest failed")
    if not METRICS.exists():
        raise RuntimeError("metrics file not found after backtest")
    m = pd.read_csv(METRICS).iloc[0].to_dict()
    return m, out.stdout

def main():
    import argparse
    ap = argparse.ArgumentParser("ArbiSense Optimizer")
    ap.add_argument("--pairs-file", default=str(BASE_DIR / "reports" / "selected_pairs.csv"))
    ap.add_argument("--notional", type=float, default=250_000)
    ap.add_argument("--z-window", type=int, default=60)
    ap.add_argument("--spread-scale", default="auto")
    ap.add_argument("--fee-bps", type=float, default=1.0)
    ap.add_argument("--slippage-bps", type=float, default=1.0)
    # griglie
    ap.add_argument("--z-enter",  default="2.5,3.0,3.5")
    ap.add_argument("--z-exit",   default="0.5,0.75,1.0")
    ap.add_argument("--z-stop",   default="3.0,3.5,4.0")
    ap.add_argument("--max-hold", default="5,10,15")
    ap.add_argument("--latency-days", default="1")
    args = ap.parse_args()

    grids = {
        "z_enter":      [float(x) for x in str(args.z_enter).split(",") if x],
        "z_exit":       [float(x) for x in str(args.z_exit).split(",") if x],
        "z_stop":       [float(x) for x in str(args.z_stop).split(",") if x],
        "max_hold":     [int(x)   for x in str(args.max_hold).split(",") if x],
        "latency_days": [int(x)   for x in str(args.latency_days).split(",") if x],
    }

    fixed = {
        "fee_bps": args.fee_bps,
        "slippage_bps": args.slippage_bps,
        "notional": args.notional,
        "z_window": args.z_window,
        "spread_scale": args.spread_scale,
    }

    combos = list(itertools.product(*grids.values()))
    print(f"[INFO] Running {len(combos)} combinations...", flush=True)

    rows = []
    for (ze, zx, zs, mh, lat) in combos:
        params = dict(z_enter=ze, z_exit=zx, z_stop=zs, max_hold=mh, latency_days=lat, **fixed)
        try:
            m, _ = run_bt(args.pairs_file, params)
            m.update(params)
            rows.append(m)
            print(f"[OK] zE={ze} zX={zx} zS={zs} H={mh} L={lat}  ->  Sharpe={m.get('Sharpe'):0.3f}  PnL={m.get('net_pnl_total'):0.0f}")
        except Exception as e:
            print(f"[FAIL] {params}: {e}", file=sys.stderr)

    if not rows:
        print("[ERROR] Nessun risultato.", file=sys.stderr); sys.exit(1)

    df = pd.DataFrame(rows)
    RESULTS.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(RESULTS, index=False)

    # criterio: ordina per Sharpe desc, poi PnL desc, poi MaxDD asc (più vicino a 0 è meglio)
    df["_ord"] = (-df["Sharpe"].fillna(0), -df["net_pnl_total"].fillna(-1e18), df["MaxDD"].fillna(-1))
    best = df.sort_values(list(df["_ord"].columns) if hasattr(df["_ord"], "columns") else ["_ord"]).iloc[0].to_dict()
    best.pop("_ord", None)
    with open(BESTJSON, "w") as f:
        json.dump(best, f, indent=2)

    print("\n=== BEST PARAMS ===")
    for k in ["z_enter","z_exit","z_stop","max_hold","latency_days"]:
        print(f"{k}: {best[k]}")
    print(f"Sharpe: {best['Sharpe']:.3f} | PnL: {best['net_pnl_total']:.0f} | MaxDD: {best['MaxDD']:.3f}")
    print(f"\n[WROTE] {RESULTS}")
    print(f"[WROTE] {BESTJSON}")

if __name__ == "__main__":
    main()

