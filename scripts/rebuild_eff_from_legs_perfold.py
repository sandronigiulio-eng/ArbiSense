import pandas as pd, numpy as np
from pathlib import Path

PAIR      = "IWDA_AS_EUNL_DE"
LEGS_FP   = Path("data_sample/legs_IWDA_EUNL.csv")
TRADES_FP = Path("reports/wf_trades.csv")
TOL       = pd.Timedelta("3h")
TRAIN_D   = 240  # giorni di train per fold (coerente col WF)
DENOM     = "B"  # eff = spread / B_price

def hedge_ratio_ols(a, b):
    a, b = pd.Series(a), pd.Series(b)
    if len(a.dropna()) < 5 or len(b.dropna()) < 5:  # dati pochi -> fallback
        return 1.0
    varB = np.var(b)
    if varB == 0: return 1.0
    covAB = np.cov(a, b)[0,1]
    return float(covAB / varB)

# ---- input ----
if not LEGS_FP.exists(): raise SystemExit(f"Manca {LEGS_FP}")
if not TRADES_FP.exists(): raise SystemExit(f"Manca {TRADES_FP}")

legs = pd.read_csv(LEGS_FP)
for c in ["date","A_price","B_price"]:
    if c not in legs.columns: raise SystemExit(f"{LEGS_FP} manca '{c}'")
legs["date"] = pd.to_datetime(legs["date"], utc=True, errors="coerce")
legs = legs.dropna(subset=["date"]).sort_values("date")

t = pd.read_csv(TRADES_FP)
need_cols = ["pair","fold","entry_date","exit_date","direction","cost"]
for c in need_cols:
    if c not in t.columns: raise SystemExit(f"{TRADES_FP} manca '{c}'")
t["entry_date"] = pd.to_datetime(t["entry_date"], utc=True, errors="coerce")
t["exit_date"]  = pd.to_datetime(t["exit_date"],  utc=True, errors="coerce")

tg = t[t["pair"]==PAIR].copy()
if tg.empty: raise SystemExit(f"Nessun trade per {PAIR}")
tg["_row_id"] = tg.index

# ---- per fold: stima h sul TRAIN e ricostruisci eff ----
rows_done = 0
t2 = t.copy()

for fold, gf in tg.groupby("fold"):
    # approx. test_start come prima entry del fold
    test_start = pd.to_datetime(gf["entry_date"].min(), utc=True)
    if pd.isna(test_start): continue
    train_end   = test_start - pd.Timedelta(seconds=1)
    train_start = train_end - pd.Timedelta(days=TRAIN_D)

    dfh = legs[(legs["date"]>=train_start) & (legs["date"]<=train_end)].dropna(subset=["A_price","B_price"])
    h = hedge_ratio_ols(dfh["A_price"], dfh["B_price"]) if not dfh.empty else 1.0

    # allineamento ENTRY/EXIT
    legs_ent = legs[["date","A_price","B_price"]].rename(columns={"date":"_t_entry","A_price":"A_ent","B_price":"B_ent"})
    legs_ex  = legs[["date","A_price","B_price"]].rename(columns={"date":"_t_exit","A_price":"A_ex","B_price":"B_ex"})

    e = pd.merge_asof(
        gf.sort_values("entry_date"), legs_ent,
        left_on="entry_date", right_on="_t_entry",
        direction="nearest", tolerance=TOL
    )
    e = pd.merge_asof(
        e.sort_values("exit_date"), legs_ex,
        left_on="exit_date", right_on="_t_exit",
        direction="nearest", tolerance=TOL
    )

    spread_ent = e["A_ent"] - h * e["B_ent"]
    spread_ex  = e["A_ex"]  - h * e["B_ex"]
    den_ent    = e["B_ent"] if DENOM=="B" else e["A_ent"]
    den_ex     = e["B_ex"]  if DENOM=="B" else e["A_ex"]

    entry_eff = spread_ent / den_ent
    exit_eff  = spread_ex  / den_ex

    valid = entry_eff.notna() & exit_eff.notna()
    t2.loc[e["_row_id"], "entry_spread_eff"] = entry_eff.values
    t2.loc[e["_row_id"], "exit_spread_eff"]  = exit_eff.values
    rows_done += int(valid.sum())

bak = TRADES_FP.with_suffix(".bak.csv")
if not bak.exists(): bak.write_text(TRADES_FP.read_text())
t2.to_csv(TRADES_FP, index=False)

print(f"[OK] Per-fold eff ricostruiti per {PAIR}. Righe aggiornate (valide): {rows_done}")
