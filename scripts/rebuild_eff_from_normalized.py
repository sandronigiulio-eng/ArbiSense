import pandas as pd, numpy as np
from pathlib import Path

PAIR = "IWDA_AS_EUNL_DE"  # coppia target
DATA = Path("data_sample/spread_report_all_pairs_long.normalized.csv")
TRIN = Path("reports/wf_trades.csv")
TROUT= Path("reports/wf_trades.csv")   # sovrascrive in-place (salviamo backup)
TOL  = pd.Timedelta("6H")              # tolleranza merge_asof

assert DATA.exists(), f"Manca {DATA}"
assert TRIN.exists(), f"Manca {TRIN}"

# individua colonna tempo nel dataset
date_cols = ["date","datetime","timestamp","time"]
df0 = pd.read_csv(DATA)
time_col = next((c for c in date_cols if c in df0.columns), None)
if time_col is None:
    raise SystemExit(f"Colonna tempo non trovata (cercate: {date_cols})")

# dati normalizzati (spread_raw, spread_scale)
df = df0[["pair", time_col, "spread_raw", "spread_scale"]].copy()
df[time_col] = pd.to_datetime(df[time_col], utc=True, errors="coerce")
df = df.sort_values(["pair", time_col])

# trades WF
t = pd.read_csv(TRIN)
for c in ["pair","entry_date","exit_date"]:
    if c not in t.columns:
        raise SystemExit(f"wf_trades.csv manca colonna '{c}'")
t["entry_date"] = pd.to_datetime(t["entry_date"], utc=True, errors="coerce")
t["exit_date"]  = pd.to_datetime(t["exit_date"],  utc=True, errors="coerce")

mask = t["pair"].eq(PAIR)
tg = t.loc[mask].copy()
if tg.empty:
    raise SystemExit(f"Nessun trade per {PAIR} in wf_trades.csv")
tg["_row_id"] = tg.index

dg = df[df["pair"].eq(PAIR)].copy()
if dg.empty:
    raise SystemExit(f"Nessun dato per {PAIR} nel dataset normalizzato")

# merge_asof per entry
e = pd.merge_asof(
    tg.sort_values("entry_date"),
    dg[[time_col,"spread_raw","spread_scale"]].rename(columns={
        time_col:"_t_entry","spread_raw":"sr_entry","spread_scale":"ss_entry"
    }),
    left_on="entry_date", right_on="_t_entry",
    direction="backward", tolerance=TOL
)

# merge_asof per exit
e = pd.merge_asof(
    e.sort_values("exit_date"),
    dg[[time_col,"spread_raw","spread_scale"]].rename(columns={
        time_col:"_t_exit","spread_raw":"sr_exit","spread_scale":"ss_exit"
    }),
    left_on="exit_date", right_on="_t_exit",
    direction="backward", tolerance=TOL
)

# calcola eff = spread_raw * spread_scale (fallback 1.0)
entry_eff = e["sr_entry"].astype(float)
exit_eff  = e["sr_exit"].astype(float)

valid = entry_eff.notna() & exit_eff.notna()
n_all = len(e); n_ok = int(valid.sum()); n_miss = n_all - n_ok

# scrivi nei trade originali solo le righe IWDA
t2 = t.copy()
t2.loc[e["_row_id"], "entry_spread_eff"] = entry_eff.values
t2.loc[e["_row_id"], "exit_spread_eff"]  = exit_eff.values

# backup + overwrite
bak = TRIN.with_suffix(".bak.csv")
if not bak.exists():
    bak.write_text(TRIN.read_text())
t2.to_csv(TROUT, index=False)

print(f"[OK] Ricostruiti eff per {PAIR}: {n_ok}/{n_all} match; {n_miss} senza match. File aggiornato: {TROUT.name} (backup: {bak.name}).")
