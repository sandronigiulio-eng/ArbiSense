import argparse, os, numpy as np, pandas as pd

# opzionale: per ADF, se disponibile
try:
    from statsmodels.tsa.stattools import adfuller
except Exception:
    adfuller = None

ap=argparse.ArgumentParser()
ap.add_argument("--input", required=True, help="CSV normalizzato con colonne: date,pair,spread_raw,spread_scale")
ap.add_argument("--window", type=int, default=60, help="window per vol_recent")
ap.add_argument("--min-samples", type=int, default=300, help="min righe per tenere la coppia")
ap.add_argument("--exclude", nargs="*", default=["CSP1"], help="pattern da escludere (substring match)")
ap.add_argument("--out", default="reports/pair_quality.csv")
args=ap.parse_args()

df=pd.read_csv(args.input)
# Normalizziamo lo spread su scala ~1
spread_col = "spread_raw" if "spread_raw" in df.columns else ("spread" if "spread" in df.columns else None)
if spread_col is None: raise SystemExit("Manca colonna spread_raw/spread")
scale_col  = "spread_scale" if "spread_scale" in df.columns else None
if scale_col is None: raise SystemExit("Manca colonna spread_scale")

df["date"]=pd.to_datetime(df["date"] if "date" in df.columns else df["timestamp"], utc=True, errors="coerce")
df=df.dropna(subset=["date","pair",spread_col,scale_col]).copy()
df["s"]=pd.to_numeric(df[spread_col], errors="coerce")*pd.to_numeric(df[scale_col], errors="coerce")
df=df.dropna(subset=["s"])

pairs=[]
for pair, g in df.groupby("pair"):
    if any(x in pair for x in args.exclude):
        continue
    g=g.sort_values("date")
    s=g["s"].astype(float).values
    if len(s) < args.min_samples:
        continue
    samples=len(s)

    # ADF p-value (se disponibile)
    if adfuller is not None:
        try:
            p_adf=float(adfuller(s, maxlag=1, autolag="AIC")[1])
        except Exception:
            p_adf=np.nan
    else:
        p_adf=np.nan

    # half-life da AR(1) su s
    try:
        s1=s[1:]; s0=s[:-1]
        s0m=s0 - s0.mean()
        phi=( (s0m*(s1 - s1.mean())).sum() / ((s0m**2).sum()+1e-12) )
        if 0 < phi < 1:
            import math
            half_life = -math.log(2)/math.log(phi)
        else:
            half_life = np.inf
    except Exception:
        half_life=np.nan

    vol_full   = float(np.nanstd(s, ddof=0))
    recent     = s[-args.window:] if len(s) >= args.window else s
    vol_recent = float(np.nanstd(recent, ddof=0))

    # Quality score (0..1 circa): più alto = meglio (bassa p_adf, half-life breve, vol_recent moderata)
    p = 1 - np.clip(p_adf if not np.isnan(p_adf) else 0.5, 0, 1)   # ADF meglio se alto qui (perché 1-p)
    hl = 1.0/(1.0 + (half_life if np.isfinite(half_life) else 1e6))
    vr = 1.0/(1.0 + vol_recent)
    quality = 0.5*p + 0.3*hl + 0.2*vr

    pairs.append({
        "pair": pair,
        "samples": samples,
        "adf_p": p_adf,
        "half_life": half_life,
        "vol_full": vol_full,
        "vol_recent": vol_recent,
        "quality_score": quality
    })

out=pd.DataFrame(pairs).sort_values("quality_score", ascending=False)
os.makedirs("reports", exist_ok=True)
out.to_csv(args.out, index=False)
print(f"[OK] Quality salvata in {args.out}")
print(out.head(10).to_string(index=False))
