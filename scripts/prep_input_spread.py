import argparse, pandas as pd, numpy as np, os, sys

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True)
    ap.add_argument("--out", dest="outp", required=True)
    ap.add_argument("--prefer", choices=["raw","pct"], default="auto",
        help="Forza interpretazione. 'auto' prova a capire (bps vs pct).")
    args = ap.parse_args()

    df = pd.read_csv(args.inp)
    cols = {c.lower(): c for c in df.columns}

    # Trova una colonna spread
    spread_col = None
    is_pct = False
    for k in ["spread_raw","spread","spread_pct"]:
        if k in cols:
            spread_col = cols[k]
            is_pct = (k == "spread_pct")
            break
    if spread_col is None:
        sys.exit("Nessuna colonna spread_raw|spread|spread_pct trovata.")

    s = pd.to_numeric(df[spread_col], errors="coerce")
    med = float(np.nanmedian(np.abs(s)))

    # Heuristics:
    # - se è 'pct' ma i valori sono > 10 → sono quasi certamente bps
    # - se è 'raw' e i valori sono ~O(1e4) → probabilmente bps
    # - prefer 'auto' salvo override manuale
    out = df.copy()
    if args.prefer == "pct":
        out = out.rename(columns={spread_col: "spread_pct"})
        out["spread_scale"] = 1.0
    elif args.prefer == "raw":
        out = out.rename(columns={spread_col: "spread_raw"})
        # bps tipico: scala 1e-4
        out["spread_scale"] = 1e-4
    else:
        # auto
        if is_pct and med > 10:
            # era marcata pct ma in realtà bps
            out = out.rename(columns={spread_col: "spread_raw"})
            out["spread_scale"] = 1e-4
        elif (not is_pct) and med > 500:  # 5k–10k bps molto comuni
            out = out.rename(columns={spread_col: "spread_raw"})
            out["spread_scale"] = 1e-4
        else:
            # valori piccoli (fractions) → pct
            out = out.rename(columns={spread_col: "spread_pct"})
            out["spread_scale"] = 1.0

    # keeping only one canonical name: prefer raw if possible
    if "spread_pct" in out.columns and "spread_raw" not in out.columns:
        # lasciamo pct com'è
        pass

    out.to_csv(args.outp, index=False)
    print(f"[OK] Wrote {args.outp}")
    print(f"  median_abs={med}, cols={list(out.columns)[:10]} ...")

if __name__ == "__main__":
    main()
