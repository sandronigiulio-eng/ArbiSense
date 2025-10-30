import pandas as pd, sys
from pathlib import Path

IN  = Path(sys.argv[1]) if len(sys.argv)>1 else Path("reports/strong_signals.csv")
OUT = Path(sys.argv[2]) if len(sys.argv)>2 else IN
COOLDOWN_DAYS = int(sys.argv[3]) if len(sys.argv)>3 else 3

df = pd.read_csv(IN)
if df.empty:
    print(f"[INFO] Nessun segnale in {IN}")
    if OUT != IN: df.to_csv(OUT, index=False)
    raise SystemExit(0)

if "timestamp" not in df.columns or "pair" not in df.columns:
    raise SystemExit("Mancano colonne 'timestamp' o 'pair'")

df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
df = df.sort_values(["pair","timestamp"]).reset_index(drop=True)

keep_idx = []
last_ts = {}
for i, r in df.iterrows():
    p = r["pair"]
    t = r["timestamp"]
    if p not in last_ts or (t - last_ts[p]).days >= COOLDOWN_DAYS:
        keep_idx.append(i)
        last_ts[p] = t

out = df.loc[keep_idx].copy()
out.to_csv(OUT, index=False)
print(f"[OK] Postfilter: {len(df)} -> {len(out)} righe (cooldown {COOLDOWN_DAYS} giorni)")
