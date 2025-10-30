import pandas as pd, numpy as np
from pathlib import Path

# --- Config ---
PAIR      = "IWDA_AS_EUNL_DE"
LEGS_FP   = Path("data_sample/legs_IWDA_EUNL.csv")
TRADES_FP = Path("reports/wf_trades.csv")
TOL       = pd.Timedelta("3h")   # usa 'h' minuscolo
DENOM     = "B"                  # eff = spread / B_price (altrimenti "A")
HEDGE     = "ols"                # "ols" oppure "one" (h=1 fisso)
# ---------------

if not LEGS_FP.exists(): raise SystemExit(f"Manca {LEGS_FP}")
if not TRADES_FP.exists(): raise SystemExit(f"Manca {TRADES_FP}")

# Carica prezzi delle gambe
legs = pd.read_csv(LEGS_FP)
for c in ["date","A_price","B_price"]:
    if c not in legs.columns:
        raise SystemExit(f"{LEGS_FP} manca colonna '{c}'")
legs["date"] = pd.to_datetime(legs["date"], utc=True, errors="coerce")
legs = legs.dropna(subset=["date"]).sort_values("date")

# Carica trades WF
t = pd.read_csv(TRADES_FP)
need_cols = ["pair","entry_date","exit_date","direction","cost"]
for c in need_cols:
    if c not in t.columns:
        raise SystemExit(f"{TRADES_FP} manca colonna '{c}'")
t["entry_date"] = pd.to_datetime(t["entry_date"], utc=True, errors="coerce")
t["exit_date"]  = pd.to_datetime(t["exit_date"],  utc=True, errors="coerce")

tg = t[t["pair"]==PAIR].copy()
if tg.empty:
    raise SystemExit(f"Nessun trade per {PAIR}")
tg["_row_id"] = tg.index  # per scrivere indietro nelle stesse righe

# Hedge ratio h (A ~ h*B) su tutta la serie disponibile
dfh = legs.dropna(subset=["A_price","B_price"]).copy()
if dfh.empty:
    raise SystemExit("Serie prezzi vuota per calcolare l'hedge ratio")
if HEDGE == "ols":
    varB = np.var(dfh["B_price"])
    covAB = np.cov(dfh["A_price"], dfh["B_price"])[0,1]
    h = float(covAB / (varB if varB!=0 else 1.0))
else:
    h = 1.0

# Allinea ENTRY (nearest) e EXIT (nearest)
legs_ent = legs[["date","A_price","B_price"]].rename(columns={"date":"_t_entry","A_price":"A_ent","B_price":"B_ent"})
legs_ex  = legs[["date","A_price","B_price"]].rename(columns={"date":"_t_exit","A_price":"A_ex","B_price":"B_ex"})

e = pd.merge_asof(
    tg.sort_values("entry_date"),
    legs_ent,
    left_on="entry_date",
    right_on="_t_entry",
    direction="nearest",
    tolerance=TOL
)
e = pd.merge_asof(
    e.sort_values("exit_date"),
    legs_ex,
    left_on="exit_date",
    right_on="_t_exit",
    direction="nearest",
    tolerance=TOL
)

# Calcola spread e 'eff' dimensionless
spread_ent = e["A_ent"] - h * e["B_ent"]
spread_ex  = e["A_ex"]  - h * e["B_ex"]
den_ent    = e["B_ent"] if DENOM=="B" else e["A_ent"]
den_ex     = e["B_ex"]  if DENOM=="B" else e["A_ex"]

entry_eff = spread_ent / den_ent
exit_eff  = spread_ex  / den_ex

valid = entry_eff.notna() & exit_eff.notna()
n_all = len(e); n_ok = int(valid.sum()); n_miss = n_all - n_ok

# Scrivi nei trade originali SOLO le righe della coppia
t2 = t.copy()
t2.loc[e["_row_id"], "entry_spread_eff"] = entry_eff.values
t2.loc[e["_row_id"], "exit_spread_eff"]  = exit_eff.values

# Backup e salvataggio
bak = TRADES_FP.with_suffix(".bak.csv")
if not bak.exists():
    bak.write_text(TRADES_FP.read_text())
t2.to_csv(TRADES_FP, index=False)

print(f"[OK] Eff ricostruiti per {PAIR}: match {n_ok}/{n_all}, missing {n_miss}, h={h:.6f}, denom={DENOM}")
