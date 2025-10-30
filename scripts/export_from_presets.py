import argparse, json, pandas as pd, sys, os, tempfile, subprocess

ap = argparse.ArgumentParser()
ap.add_argument("--input", required=True)
ap.add_argument("--presets", default="reports/presets.json")
ap.add_argument("--out", default="reports/strong_signals.csv")
ap.add_argument("--lookback", type=int, default=10)
ap.add_argument("--emit-near", action="store_true")
ap.add_argument("--near-delta", type=float, default=0.3)
args = ap.parse_args()

repo = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
export_script = os.path.join(repo, "scripts", "export_from_preset.py")
py = sys.executable  # usa il Python del venv corrente

presets = json.load(open(args.presets))
all_rows = []

def safe_read_csv(path):
    try:
        if os.path.getsize(path) == 0:
            return pd.DataFrame()
        return pd.read_csv(path)
    except Exception:
        # file con solo newline/whitespace → nessuna colonna
        return pd.DataFrame()

def run_one(preset_params):
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w") as tmpjs, \
         tempfile.NamedTemporaryFile(suffix=".csv",  delete=False, mode="w") as tmpout:
        json.dump({"params": preset_params}, tmpjs); tmpjs.flush()
        cmd = [
            py, export_script,
            "--input", args.input,
            "--preset", tmpjs.name,
            "--out", tmpout.name,
            "--lookback", str(args.lookback)
        ]
        if args.emit_near:
            cmd += ["--emit-near", "--near-delta", str(args.near_delta)]

        subprocess.run(cmd, check=True)
        df = safe_read_csv(tmpout.name)
        os.unlink(tmpjs.name); os.unlink(tmpout.name)
        return df

for p in presets:
    try:
        df = run_one(p["params"])
        if not df.empty:
            all_rows.append(df)
    except Exception as e:
        print("[WARN] export fallito per", p.get("pair", p.get("params", {}).get("pair")), e)

out_cols = ["timestamp","pair","side","action","z","z_enter","z_exit","near_delta"]
out = pd.concat(all_rows, ignore_index=True) if all_rows else pd.DataFrame(columns=out_cols)
out.to_csv(args.out, index=False)
print(f"[WROTE] {args.out} rows={len(out)}")
if len(out):
    print(out.tail(10).to_string(index=False))
