import pandas as pd, numpy as np, argparse
from pathlib import Path

ap = argparse.ArgumentParser()
ap.add_argument("--pair", required=True)
ap.add_argument("--legs", required=True)
ap.add_argument("--denom", default="B", choices=["A","B"])
ap.add_argument("--hedge", default="ols", choices=["ols","one"])
ap.add_argument("--tol", default="6h")
args = ap.parse_args()

TRADES = Path("reports/wf_trades.csv")
legs = pd.read_csv(args.legs)
for c in ["date","A_price","B_price"]:
    if c not in legs.columns: raise SystemExit(f"{args.legs} manca '{c}'")
legs["date"] = pd.to_datetime(legs["date"], utc=True, errors="coerce")
legs = legs.dropna(subset=["date"]).sort_values("date")

t = pd.read_csv(TRADES)
need = ["pair","fold","entry_date","exit_date","direction","cost"]
for c in need:
    if c not in t.columns: raise SystemExit(f"{TRADES} manca '{c}'")
t["entry_date"] = pd.to_datetime(t["entry_date"], utc=True, errors="coerce")
t["exit_date"]  = pd.to_datetime(t["exit_date"],  utc=True, errors="coerce")
tg = t[t["pair"]==args.pair].copy()
if tg.empty: raise SystemExit(f"Nessun trade per {args.pair}")
tg["_row_id"] = tg.index

def h_ols(a,b):
    a,b = pd.Series(a), pd.Series(b)
    if len(a.dropna())<30 or len(b.dropna())<30:  # serve un po' di storia
        return 1.0
    varB = np.var(b)
    if varB==0: return 1.0
    covAB = np.cov(a,b)[0,1]
    return float(covAB/varB)

rows=0
t2=t.copy()
for fold,gf in tg.groupby("fold"):
    test_start = pd.to_datetime(gf["entry_date"].min(), utc=True)
    if pd.isna(test_start): continue
    train_end   = test_start - pd.Timedelta(seconds=1)
    train_start = train_end - pd.Timedelta(days=240)
    dfh = legs[(legs["date"]>=train_start)&(legs["date"]<=train_end)].dropna(subset=["A_price","B_price"])
    h = h_ols(dfh["A_price"], dfh["B_price"]) if args.hedge=="ols" else 1.0

    e = pd.merge_asof(
        gf.sort_values("entry_date"),
        legs.rename(columns={"date":"_t_entry","A_price":"A_ent","B_price":"B_ent"}),
        left_on="entry_date", right_on="_t_entry",
        direction="nearest", tolerance=pd.Timedelta(args.tol)
    )
    e = pd.merge_asof(
        e.sort_values("exit_date"),
        legs.rename(columns={"date":"_t_exit","A_price":"A_ex","B_price":"B_ex"}),
        left_on="exit_date", right_on="_t_exit",
        direction="nearest", tolerance=pd.Timedelta(args.tol)
    )

    spread_ent = e["A_ent"] - h*e["B_ent"]
    spread_ex  = e["A_ex"]  - h*e["B_ex"]
    den_ent    = e["B_ent"] if args.denom=="B" else e["A_ent"]
    den_ex     = e["B_ex"]  if args.denom=="B" else e["A_ex"]
    entry_eff  = spread_ent/den_ent
    exit_eff   = spread_ex/den_ex

    t2.loc[e["_row_id"], "entry_spread_eff"] = entry_eff.values
    t2.loc[e["_row_id"], "exit_spread_eff"]  = exit_eff.values
    rows += int((entry_eff.notna() & exit_eff.notna()).sum())

t2.to_csv(TRADES, index=False)
print(f"[OK] {args.pair}: eff aggiornati (validi) {rows}")
