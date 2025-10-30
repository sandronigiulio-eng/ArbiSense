import argparse, json, pandas as pd, numpy as np

def zscore(x, win):
    m = x.rolling(win, min_periods=max(5, win//4)).mean()
    v = x.rolling(win, min_periods=max(5, win//4)).std(ddof=0)
    return (x - m) / v

ap = argparse.ArgumentParser()
ap.add_argument("--input", required=True)
ap.add_argument("--preset", default="reports/preset_best.json")
ap.add_argument("--out", default="reports/strong_signals.csv")
ap.add_argument("--lookback", type=int, default=5, help="ultimi N punti su cui cercare crossing")
# >>> NEW: heads-up vicino alle soglie
ap.add_argument("--emit-near", action="store_true", help="emetti NEAR_ENTER/NEAR_EXIT quando z è vicino alle soglie")
ap.add_argument("--near-delta", type=float, default=0.2, help="distanza massima dalla soglia per generare NEAR_* (default 0.2)")
args = ap.parse_args()

preset = json.load(open(args.preset))
p = preset["params"]
pair = p.get("pair","SWDA_L_EUNL_DE")
side = p.get("side","short")
sign = int(p.get("sign", 1))
z_enter = float(p.get("z_enter",3.0))
z_exit  = float(p.get("z_exit",2.0))
z_window = int(p.get("z_window",60))

df = pd.read_csv(args.input)
date_col = "date" if "date" in df.columns else ("timestamp" if "timestamp" in df.columns else None)
if not date_col:
    raise SystemExit("Input deve avere 'date' o 'timestamp'")
df[date_col] = pd.to_datetime(df[date_col], utc=True, errors="coerce")
df = df[(df["pair"]==pair)].dropna(subset=[date_col]).sort_values(date_col)

# scegli colonna spread canonica
spread_col = (
    "spread_raw" if "spread_raw" in df.columns else
    ("spread" if "spread" in df.columns else
     ("spread_pct" if "spread_pct" in df.columns else None))
)
if not spread_col:
    raise SystemExit("Manca colonna spread_raw/spread/spread_pct")

s = pd.to_numeric(df[spread_col], errors="coerce")
s = s * sign  # applica segno del preset
z = zscore(s.astype(float), z_window)

# segnali sugli ultimi N punti (crossing + near)
N = min(args.lookback, len(z))
if N <= 1:
    open(args.out,"w").write("")
    print("No data for signals")
    raise SystemExit(0)

sub = df.tail(N).copy()
sub["z"] = z.tail(N).values
sub["z_prev"] = z.shift(1).tail(N).values

rows=[]
delta = float(args.near_delta)

for _,r in sub.iterrows():
    ts = r[date_col]
    zt = float(r["z"]) if pd.notna(r["z"]) else None
    zp = float(r["z_prev"]) if pd.notna(r["z_prev"]) else None
    if zt is None or zp is None:
        continue

    crossed = False

    # ENTRY short: crossing sopra z_enter
    if side=="short" and zp < z_enter <= zt:
        rows.append({"timestamp": ts.isoformat(), "pair": pair, "side": "short",
                     "action": "ENTER_SHORT", "z": zt, "z_enter": z_enter, "z_exit": z_exit})
        crossed = True

    # EXIT short: crossing sotto z_exit
    if side=="short" and zp > z_exit >= zt:
        rows.append({"timestamp": ts.isoformat(), "pair": pair, "side": "short",
                     "action": "EXIT_SHORT", "z": zt, "z_enter": z_enter, "z_exit": z_exit})
        crossed = True

    # NEAR_* (solo se richiesto e se non c'è già stato crossing alla stessa barra)
    if args.emit_near and not crossed:
        # NEAR_ENTER: z è poco sotto la soglia (sta per incrociare verso l'alto)
        # richiediamo anche zt > zp per evitare spam quando si allontana
        if (z_enter - delta) <= zt < z_enter and zt > zp:
            rows.append({"timestamp": ts.isoformat(), "pair": pair, "side": "short",
                         "action": "NEAR_ENTER", "z": zt, "z_enter": z_enter, "z_exit": z_exit,
                         "near_delta": delta})
        # NEAR_EXIT: z è poco sopra la soglia di exit (sta per incrociare verso il basso)
        # richiediamo anche zt < zp per evitare spam quando si allontana
        if z_exit < zt <= (z_exit + delta) and zt < zp:
            rows.append({"timestamp": ts.isoformat(), "pair": pair, "side": "short",
                         "action": "NEAR_EXIT", "z": zt, "z_enter": z_enter, "z_exit": z_exit,
                         "near_delta": delta})

out_cols = ["timestamp","pair","side","action","z","z_enter","z_exit","near_delta"]
out = pd.DataFrame(rows, columns=out_cols) if rows else pd.DataFrame(columns=out_cols)
out.to_csv(args.out, index=False)
print(f"[WROTE] {args.out} rows={len(out)}")
if len(out): print(out.tail(10).to_string(index=False))
