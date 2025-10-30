import argparse, pandas as pd, numpy as np
from pathlib import Path

ap = argparse.ArgumentParser()
ap.add_argument("--input", default="reports/strong_signals.csv")
ap.add_argument("--out",   default="reports/strong_signals.csv")
ap.add_argument("--data",  default="data_sample/spread_report_all_pairs_long.normalized.csv")
ap.add_argument("--pair-quality", default="reports/pair_quality.csv")
ap.add_argument("--regime-zvol-max", type=float, default=None)
ap.add_argument("--regime-zvol-window", type=int, default=20)
ap.add_argument("--regime-adf-max", type=float, default=None)
ap.add_argument("--z-window", type=int, default=40, help="rolling window per z sugli spread normalizzati")
args = ap.parse_args()

sig_fp = Path(args.input); out_fp = Path(args.out)
df = pd.read_csv(sig_fp) if sig_fp.exists() else pd.DataFrame()
if df.empty:
    print(f"[INFO] Nessun segnale in {sig_fp}; nulla da filtrare.")
    if out_fp != sig_fp: df.to_csv(out_fp, index=False)
    raise SystemExit(0)

need = {"timestamp","pair"}
missing = need - set(df.columns)
if missing:
    raise SystemExit(f"[ERR] Mancano colonne in signals: {missing}")

# --- dataset normalizzato per ricavare z e z-vol per pair ---
raw = pd.read_csv(args.data)
date_cols = [c for c in ["date","datetime","timestamp","time"] if c in raw.columns]
if not date_cols:
    raise SystemExit("[ERR] Colonna tempo non trovata nel dataset normalizzato")
tcol = date_cols[0]
raw[tcol] = pd.to_datetime(raw[tcol], utc=True, errors="coerce")
raw = raw.sort_values(["pair", tcol])

if not {"pair","spread_raw","spread_scale"}.issubset(raw.columns):
    raise SystemExit("[ERR] Il dataset normalizzato deve avere pair, spread_raw, spread_scale")

# z = standardizzazione rolling dello spread_eff = spread_raw * spread_scale
raw["spread_eff"] = raw["spread_raw"].astype(float) * raw["spread_scale"].fillna(1.0).astype(float)
raw["z"] = np.nan
for p, g in raw.groupby("pair"):
    se = g["spread_eff"].astype(float)
    mu = se.rolling(args.z_window, min_periods=max(5, args.z_window//2)).mean()
    sd = se.rolling(args.z_window, min_periods=max(5, args.z_window//2)).std(ddof=0)
    raw.loc[g.index, "z"] = (se - mu) / sd.replace(0, np.nan)

# mappa adf_p da pair_quality (se richiesto)
adf_map = {}
if args.regime_adf_max is not None and Path(args.pair_quality).exists():
    pq = pd.read_csv(args.pair_quality)
    if {"pair","adf_p"}.issubset(pq.columns):
        adf_map = dict(zip(pq["pair"], pq["adf_p"]))

# --- filtra segnali ---
df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
df = df.sort_values(["pair","timestamp"]).reset_index(drop=True)

def zvol_ok(pair, ts):
    if args.regime_zvol_max is None: return True
    g = raw[raw["pair"]==pair]
    if g.empty: return True
    g = g[g[tcol] <= ts].tail(args.regime_zvol_window)
    if len(g) < max(5, args.regime_zvol_window//2):  # pochi punti -> non blocco
        return True
    zvol = g["z"].std(ddof=0)
    return bool(pd.notna(zvol) and zvol <= args.regime_zvol_max)

def adf_ok(pair):
    if args.regime_adf_max is None: return True
    pv = adf_map.get(pair, None)
    if pv is None: return True  # se non lo so, non blocco
    return pv <= args.regime_adf_max

keep = []
for i, r in df.iterrows():
    if not zvol_ok(r["pair"], r["timestamp"]): 
        continue
    if not adf_ok(r["pair"]):
        continue
    keep.append(i)

out = df.loc[keep].copy()
out.to_csv(out_fp, index=False)
print(f"[OK] Regime filter: {len(df)} -> {len(out)} (zvol_max={args.regime_zvol_max}, win={args.regime_zvol_window}, adf_max={args.regime_adf_max})")
