import pandas as pd, numpy as np
from pathlib import Path

NOTIONAL = 250000.0
DATA_FP = "data_sample/spread_report_all_pairs_long.normalized.csv"
TRADES_IN = "reports/wf_trades.csv"
TRADES_OUT = "reports/wf_trades.true.csv"

d = pd.read_csv(DATA_FP)
t = pd.read_csv(TRADES_IN)

# autodetect colonna tempo nel dataset
time_col = None
for c in ["date","datetime","timestamp","time"]:
    if c in d.columns:
        time_col = c; break
if time_col is None:
    raise SystemExit("Colonna tempo non trovata nel dataset (attese: date/datetime/timestamp/time)")

# parsing date
d[time_col] = pd.to_datetime(d[time_col], utc=True, errors="coerce")
t["entry_date"] = pd.to_datetime(t["entry_date"], utc=True, errors="coerce")
t["exit_date"]  = pd.to_datetime(t["exit_date"],  utc=True, errors="coerce")

d = d.sort_values([ "pair", time_col ])

out_rows = []
for pair, tg in t.groupby("pair", sort=False):
    dg = d[d["pair"]==pair].copy()
    if dg.empty:
        # niente dati: copia trade con net_pnl NaN
        tg = tg.copy()
        tg["net_pnl"] = np.nan
        tg["net_pnl_true"] = np.nan
        out_rows.append(tg)
        continue

    dg = dg.sort_values(time_col)
    # asof entry
    e = pd.merge_asof(
        tg.sort_values("entry_date"),
        dg[[time_col,"spread_raw","spread_scale"]].rename(columns={time_col:"_t_entry"}),
        left_on="entry_date", right_on="_t_entry", direction="backward", tolerance=pd.Timedelta("3D")
    )
    # asof exit
    e = pd.merge_asof(
        e.sort_values("exit_date"),
        dg[[time_col,"spread_raw","spread_scale"]].rename(columns={time_col:"_t_exit","spread_raw":"spread_raw_exit","spread_scale":"spread_scale_exit"}),
        left_on="exit_date", right_on="_t_exit", direction="backward", tolerance=pd.Timedelta("3D")
    )

    # spread scalati (se manca la scale, usa 1.0)
    s_ent = e["spread_raw"].astype(float) * e["spread_scale"].fillna(1.0).astype(float)
    s_ex  = e["spread_raw_exit"].astype(float) * e["spread_scale_exit"].fillna(1.0).astype(float)

    # direzione: SHORT_SPREAD guadagna se il differenziale SCENDE
    dir_sign = np.where(e["direction"].eq("LONG_SPREAD"), +1.0, -1.0)

    gross_true = dir_sign * (s_ex - s_ent) * NOTIONAL
    net_true   = gross_true - e["cost"].astype(float)

    e = e.copy()
    # SOVRASCRIVI net_pnl con il TRUE e salva anche colonna dedicata
    e["net_pnl_true"] = net_true
    e["net_pnl"] = net_true

    # sanity check: elimina outlier assurdi (>|2500 bps|)
    pnl_bps = e["net_pnl"] / NOTIONAL * 1e4
    mask_ok = pnl_bps.abs() <= 2500
    e.loc[~mask_ok, "net_pnl"] = np.nan
    e.loc[~mask_ok, "net_pnl_true"] = np.nan

    out_rows.append(e)

t_true = pd.concat(out_rows, ignore_index=True)
t_true.to_csv(TRADES_OUT, index=False)
print(f"[WROTE] {TRADES_OUT}  rows={len(t_true)}")
