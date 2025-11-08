#!/usr/bin/env python3
import os, json, sys
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd

def main():
    input_path = Path(os.getenv("PUBLIC_INPUT", "reports/strong_signals.csv"))
    outdir = Path(os.getenv("PUBLIC_DIR", "public"))
    fields_env = os.getenv("PUBLIC_FIELDS", "timestamp,pair,side,action,z,z_enter,z_exit,date,near_delta")
    fields = [f.strip() for f in fields_env.split(",") if f.strip()]

    if not input_path.exists() or input_path.stat().st_size == 0:
        print(f"[PUBLIC] no data: {input_path} missing or empty")
        return 0

    df = pd.read_csv(input_path)
    if df.empty:
        print("[PUBLIC] input empty")
        return 0

    cols = [c for c in fields if c in df.columns]
    if not cols:
        print("[PUBLIC] no overlapping fields")
        return 0

    outdir.mkdir(parents=True, exist_ok=True)
    df_out = df.loc[:, cols].copy()

    # arrotonda numerici
    for c in ("z","z_enter","z_exit","near_delta"):
        if c in df_out.columns:
            df_out[c] = pd.to_numeric(df_out[c], errors="coerce").round(3)

    # sort e ISO8601
    if "timestamp" in df_out.columns:
        try:
            df_out["timestamp"] = pd.to_datetime(df_out["timestamp"], utc=True, errors="coerce")
            df_out.sort_values("timestamp", inplace=True)
            df_out["timestamp"] = df_out["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        except Exception as e:
            print("[PUBLIC] timestamp parse warning:", e)

    latest_csv  = outdir / "latest.csv"
    latest_json = outdir / "latest.json"

    df_out.to_csv(latest_csv, index=False)
    data = json.loads(df_out.to_json(orient="records"))

    # meta + summary
    summary = (df.groupby("pair")["action"].count().sort_values(ascending=False).to_dict()
               if "pair" in df.columns and not df.empty else {})
    meta = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": str(input_path),
        "count": int(len(df_out)),
        "summary_by_pair": summary,
    }

    with open(latest_json, "w") as f:
        json.dump({"data": data, "meta": meta}, f, ensure_ascii=False, indent=2)
    with open(outdir / "meta.json", "w") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    print(f"[PUBLIC] wrote {latest_csv} and {latest_json} (rows={len(df_out)})")
    return 0

if __name__ == "__main__":
    sys.exit(main())

