import pandas as pd, sys
from pathlib import Path
IN = Path(sys.argv[1]) if len(sys.argv)>1 else Path("reports/strong_signals.csv")
OUT= Path(sys.argv[2]) if len(sys.argv)>2 else IN
LOOKBACK_DAYS = int(sys.argv[3]) if len(sys.argv)>3 else 120  # finestra entro cui considerare ENTER valido

df = pd.read_csv(IN)
if df.empty:
    print(f"[INFO] Nessun segnale in {IN}"); 
    (df if OUT==IN else df.to_csv(OUT, index=False))
    raise SystemExit(0)

need = {"timestamp","pair","action"}
if not need.issubset(df.columns): 
    raise SystemExit(f"[ERR] Mancano colonne: {need - set(df.columns)}")

df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
df = df.sort_values(["pair","timestamp"]).reset_index(drop=True)

keep = []
last_enter = {}  # pair -> timestamp dell’ultimo ENTER_SHORT
for i,r in df.iterrows():
    p, a, t = r["pair"], str(r["action"]).upper(), r["timestamp"]
    if a.startswith("ENTER_"):
        last_enter[p] = t
        keep.append(i)
    elif a.startswith("EXIT_"):
        ok = p in last_enter and (t - last_enter[p]).days <= LOOKBACK_DAYS
        if ok: keep.append(i)
    else:
        keep.append(i)

out = df.loc[keep].copy()
out.to_csv(OUT, index=False)
print(f"[OK] Position guard: {len(df)} -> {len(out)} righe (entro {LOOKBACK_DAYS} giorni)")
