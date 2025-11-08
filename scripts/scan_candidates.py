import argparse, pandas as pd, numpy as np
from pathlib import Path

ap = argparse.ArgumentParser()
ap.add_argument("--infile", default="data_sample/spread_report_all_pairs_long.normalized.csv")
ap.add_argument("--z_enter", type=float, default=2.2)
ap.add_argument("--z_window", type=int, default=40)
ap.add_argument("--lookback_days", type=int, default=720)   # ~2 anni trading days ~500, teniamoci larghi
ap.add_argument("--topk", type=int, default=12)
ap.add_argument("--outfile", default="reports/candidate_scan.csv")
args = ap.parse_args()

df = pd.read_csv(args.infile)
assert "pair" in df.columns, "Manca la colonna 'pair' nel dataset"

# Heuristics per individuare la colonna numerica se 'z' non c'è
num_cols = [c for c in df.columns if c not in {"pair","date","timestamp"} and pd.api.types.is_numeric_dtype(df[c])]
if "z" in df.columns:
    val_col = "z"
elif "spread" in df.columns:
    val_col = "spread"
elif num_cols:
    val_col = num_cols[0]
else:
    raise SystemExit("Non trovo colonne numeriche utili (z/spread).")

# ordina temporalmente se possibile
time_col = "date" if "date" in df.columns else ("timestamp" if "timestamp" in df.columns else None)
if time_col:
    df[time_col] = pd.to_datetime(df[time_col], errors="coerce", utc=True)
    df = df.sort_values([ "pair", time_col ])

rows = []
bad = {"IWDA_AS_EUNL_DE","SWDA_L_EUNL_DE","VUAA_L_VUSA_L","VWRL_L_VEVE_AS"}
for pair, g in df.groupby("pair"):
    if pair in bad or "CSP1" in pair:
        continue
    g = g.copy()
    # lookback
    if time_col:
        g = g.tail(args.lookback_days)
    else:
        g = g.tail(args.lookback_days)

    x = g[val_col].astype(float)
    if x.size < max(120, args.z_window+5):   # serie troppo corta
        continue

    # se non è già z, normalizza tipo z con rolling
    if val_col != "z":
        mu = x.rolling(args.z_window, min_periods=args.z_window//2).mean()
        sd = x.rolling(args.z_window, min_periods=args.z_window//2).std(ddof=0)
        z = (x - mu) / sd
    else:
        z = x

    z = z.dropna()
    if z.empty: 
        continue

    # z-vol (rolling std su 20)
    zvol = z.rolling(20, min_periods=10).std(ddof=0)
    zvol_p75 = np.nanpercentile(zvol.dropna(), 75) if zvol.dropna().size else np.nan

    # conteggio episodi ENTER (short-only, crossing da sotto a sopra z_enter)
    cross = (z.shift(1) < args.z_enter) & (z >= args.z_enter)
    # consolida episodi contigui (debounce 3 giorni)
    enter_idx = list(np.where(cross)[0])
    enters = 0
    last_i = -999
    for i in enter_idx:
        if i - last_i > 3:
            enters += 1
            last_i = i

    # proxy di stazionarietà: varianza limitata e zvol contenuta
    rows.append({
        "pair": pair,
        "enters_oos": int(enters),
        "zvol_p75": float(zvol_p75),
        "last_date": str(g[time_col].iloc[-1]) if time_col else "",
    })

out = pd.DataFrame(rows)
if out.empty:
    print("[INFO] Nessuna candidata trovata con i criteri attuali.")
    Path(args.outfile).parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.outfile, index=False)
    raise SystemExit(0)

# filtri "rigidi" coerenti col tuo regime filter
out = out[(out["enters_oos"] >= 3) & (out["zvol_p75"] <= 1.0)].copy()
if out.empty:
    print("[INFO] Candidate presenti ma non passano filtri (enters>=3, zvol_p75<=1.0). Prova lookback_days più lunghi.")
    Path(args.outfile).parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.outfile, index=False)
    raise SystemExit(0)

# mapping a ticker Yahoo dai suffissi del nome pair A_MKT_B_MKT
suf = {"L":".L","DE":".DE","AS":".AS","MI":".MI","SW":".SW","PA":".PA","IR":".IR","VX":".VX","F":".F"}
def map_yh(p):
    parts = p.split("_")
    if len(parts)!=4: return "",""
    a, ma, b, mb = parts
    return f"{a}{suf.get(ma,'')}", f"{b}{suf.get(mb,'')}"

out["A_yh"], out["B_yh"] = zip(*out["pair"].map(map_yh))
out["onboard_cmd"] = out.apply(lambda r: f"bin/pair_onboard.sh {r.pair} {r.A_yh} {r.B_yh}", axis=1)

out = out.sort_values(["enters_oos","zvol_p75"], ascending=[False, True]).head(args.topk)
Path(args.outfile).parent.mkdir(parents=True, exist_ok=True)
out.to_csv(args.outfile, index=False)
print(out.to_string(index=False))
