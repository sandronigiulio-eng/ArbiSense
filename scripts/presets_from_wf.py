import pandas as pd, json, os, datetime as dt

m=pd.read_csv("reports/wf_metrics.csv")
b=pd.read_csv("reports/wf_best_params.csv")

# consideriamo solo fold con trade (ed escludiamo eventuali SKIP)
if "reason" in m.columns:
    m_eff = m[(m["trades"]>0) & (~m["reason"].astype(str).str.contains("SKIP", na=False))]
else:
    m_eff = m[(m["trades"]>0)]

presets=[]
for pair, g in m_eff.groupby("pair"):
    folds_active = int(g["fold"].nunique()) if "fold" in g.columns else len(g)
    oos_total    = float(g["net_pnl_total"].sum()) if "net_pnl_total" in g.columns else 0.0
    if (folds_active >= 3) and (oos_total >= 0):
        df = b[b["pair"]==pair].merge(g[["pair","fold","net_pnl_total"]], on=["pair","fold"], how="inner")
        def pick(col):
            if col not in df.columns or df.empty: return None
            agg=df.groupby(col, dropna=False)["net_pnl_total"].sum().sort_values(ascending=False)
            ix=agg.index[0]
            try: return float(ix)
            except: return ix
        params={}
        for col in ["z_enter","z_exit","z_stop","max_hold","latency","spread_scale","notional","z_window","sign"]:
            v=pick(col)
            if v is not None: params[col]=v
        params["pair"]=pair
        params["side"]="short"
        presets.append({
            "pair": pair,
            "created": dt.datetime.now().isoformat(timespec="seconds"),
            "folds_active": folds_active,
            "oos_total_pnl": oos_total,
            "params": params
        })

os.makedirs("reports", exist_ok=True)
with open("reports/presets.json","w",encoding="utf-8") as f:
    json.dump(presets,f,ensure_ascii=False,indent=2)
print(f"[OK] reports/presets.json con {len(presets)} preset")
for p in presets:
    print("-", p["pair"], "OOS", round(p["oos_total_pnl"],2), "folds", p["folds_active"])
