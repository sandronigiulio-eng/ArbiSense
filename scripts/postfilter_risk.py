import pandas as pd, sys
from pathlib import Path
IN  = Path(sys.argv[1]) if len(sys.argv)>1 else Path("reports/strong_signals.csv")
OUT = Path(sys.argv[2]) if len(sys.argv)>2 else IN
MAX_PER_DAY  = int(sys.argv[3]) if len(sys.argv)>3 else 10
MAX_PER_PAIR = int(sys.argv[4]) if len(sys.argv)>4 else 6
if not IN.exists() or IN.stat().st_size==0: print("[INFO] No signals"); sys.exit(0)
df = pd.read_csv(IN)
if df.empty: print("[INFO] No rows"); sys.exit(0)
if "timestamp" not in df.columns or "pair" not in df.columns:
    print("[ERR] Missing columns 'timestamp' or 'pair'"); sys.exit(1)
df["date"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce").dt.date
df["rank_pair"] = df.groupby(["pair"]).cumcount()+1
df = df[df["rank_pair"] <= MAX_PER_PAIR]
df["rank_day"]  = df.groupby(["date"]).cumcount()+1
df = df[df["rank_day"]  <= MAX_PER_DAY]
df.drop(columns=["rank_pair","rank_day"], inplace=True)
df.to_csv(OUT, index=False)
print(f"[OK] Risk cap -> {len(df)} rows (<= {MAX_PER_PAIR}/pair, <= {MAX_PER_DAY}/day)")
