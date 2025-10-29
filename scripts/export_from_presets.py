import argparse, json, pandas as pd
from datetime import datetime

ap=argparse.ArgumentParser()
ap.add_argument("--input", required=True)
ap.add_argument("--presets", default="reports/presets.json")
ap.add_argument("--out", default="reports/strong_signals.csv")
ap.add_argument("--lookback", type=int, default=10)
ap.add_argument("--emit-near", action="store_true")
ap.add_argument("--near-delta", type=float, default=0.3)
args=ap.parse_args()

presets=json.load(open(args.presets))
all_rows=[]

def run_one(preset):
    import subprocess, json, tempfile, os, pandas as pd
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w") as tmpjs, \
         tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmpout:
        json.dump(preset, tmpjs); tmpjs.flush()
        cmd=[
            "python3","scripts/export_from_preset.py",
            "--input", args.input,
            "--preset", tmpjs.name,
            "--out", tmpout.name,
            "--lookback", str(args.lookback)
        ]
        if args.emit_near: cmd.append("--emit-near")
        cmd += ["--near-delta", str(args.near_delta)]
        subprocess.run(cmd, check=True)
        import os, pandas as pd
        if (not os.path.exists(tmpout.name)) or os.path.getsize(tmpout.name)==0:
            return pd.DataFrame()
        try:
            df=pd.read_csv(tmpout.name)
        except pd.errors.EmptyDataError:
            df=pd.DataFrame()
        os.unlink(tmpjs.name); os.unlink(tmpout.name)
        return df

for p in presets:
    try:
        df=run_one({"params": p["params"]})
        if not df.empty: all_rows.append(df)
    except Exception as e:
        print("[WARN] export fallito per", p.get("pair"), e)

out=pd.concat(all_rows, ignore_index=True) if all_rows else pd.DataFrame()
# assicurati di avere sempre l'header
cols = ["timestamp","pair","side","action","z","z_enter","z_exit"]
if args.emit_near:
    cols.append("near_delta")
import pandas as pd
if out.empty:
    out = pd.DataFrame(columns=cols)
else:
    # aggiungi eventuali colonne mancanti e riordina
    for c in cols:
        if c not in out.columns:
            out[c] = pd.NA
    out = out[cols]
out.to_csv(args.out, index=False)
print(f"[WROTE] {args.out} rows={len(out)}")
if len(out):
    try:
        print(out.tail(10).to_string(index=False))
    except Exception:
        pass
if len(out): print(out.tail(10).to_string(index=False))
