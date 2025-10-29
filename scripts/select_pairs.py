import argparse, pandas as pd, sys, os, re

ap=argparse.ArgumentParser()
ap.add_argument("--quality-csv", default="reports/pair_quality.csv")
ap.add_argument("--out", default="reports/selected_pairs.csv")
ap.add_argument("--k", type=int, default=2)
ap.add_argument("--exclude", default="CSP1")  # regex
args=ap.parse_args()

if not os.path.exists(args.quality_csv):
    print(f"[ERR] {args.quality_csv} non trovato. Esegui quality_metrics.py prima.", file=sys.stderr); sys.exit(1)

q=pd.read_csv(args.quality_csv)
# colonne attese: pair, samples, adf_p, half_life, vol_full, vol_recent, quality_score
if "quality_score" not in q.columns:
    # fallback grezzo se manca: più half-life breve e vol_recent bassa
    q["quality_score"]=(-q["half_life"].fillna(1e9)) + (q["vol_recent"].rank(ascending=True, method="average")*0.0)

mask=~q["pair"].astype(str).str.contains(args.exclude, regex=True, na=False)
q=q[mask].copy()

q=q.sort_values(["quality_score","vol_recent"], ascending=[False,True])
sel=q["pair"].head(args.k).tolist()
os.makedirs("reports", exist_ok=True)
pd.DataFrame({"pair": sel}).to_csv(args.out, index=False)
print(f"[OK] {args.out} ->", ", ".join(sel))
