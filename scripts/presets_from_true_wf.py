import argparse, json, pandas as pd, os, datetime as dt

ap=argparse.ArgumentParser()
ap.add_argument("--min-folds", type=int, default=3)
ap.add_argument("--true-trades", default="reports/wf_trades.true.csv")
ap.add_argument("--best", default="reports/wf_best_params.csv")
ap.add_argument("--out", default="reports/presets.json")
args=ap.parse_args()

t = pd.read_csv(args.true_trades)
if "net_pnl_true" not in t.columns and "net_pnl" in t.columns:
    t["net_pnl_true"] = t["net_pnl"]
if t.empty:
    open(args.out, "w").write("[]"); print("[INFO] nessun trade TRUE."); raise SystemExit(0)

b = pd.read_csv(args.best) if os.path.exists(args.best) else pd.DataFrame()

per_pair = t.groupby("pair").agg(oos_total_pnl=("net_pnl_true","sum"),
                                 folds_active =("fold","nunique")).reset_index()
keep = per_pair[(per_pair["oos_total_pnl"]>=0) & (per_pair["folds_active"]>=args.min_folds)]
if keep.empty:
    open(args.out, "w").write("[]"); print("[INFO] nessuna pair passa i filtri."); raise SystemExit(0)

allowed = {"z_enter","z_exit","z_stop","max_hold","latency",
           "notional","start","end","train_days","test_days","step_days","z_window","side","pair"}

presets=[]
for _,row in keep.iterrows():
    pair=row["pair"]; params={}
    if not b.empty and "pair" in b.columns:
        bb=b[b["pair"]==pair]
        if not bb.empty:
            # prendi la prima riga (o, se hai metrica migliore, selezionala)
            for k,v in bb.iloc[0].to_dict().items():
                if k in allowed:
                    params[k]=v
    params.setdefault("pair", pair)
    params.setdefault("side", "short")
    params.setdefault("z_window", 40.0)
    params.setdefault("notional", 250000.0)
    # importante: scala auto
    params["spread_scale"] = "auto"

    presets.append({
        "pair": pair,
        "created": dt.datetime.now().isoformat(timespec="seconds"),
        "folds_active": int(row["folds_active"]),
        "oos_total_pnl": float(row["oos_total_pnl"]),
        "params": params
    })

os.makedirs("reports", exist_ok=True)
with open(args.out,"w") as f: json.dump(presets, f, indent=2)
print(f"[OK] {args.out} con {len(presets)} preset")
for p in presets:
    print(f"- {p['pair']} OOS_TRUE {round(p['oos_total_pnl'],2)} folds {p['folds_active']}")
