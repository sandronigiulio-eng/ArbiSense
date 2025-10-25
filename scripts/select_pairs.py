#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ArbiSense — Selezione automatica coppie A-list da reports/pair_quality.csv

Modalità:
- strict  : ADF ≤ 0.05, half-life ∈ [3,20], min-samples 120
- relaxed : ADF ≤ 0.35, half-life ∈ [3,60], min-samples 120   ← consigliata per sbloccare test
- topn    : ignora i filtri duri, prende i Top-N per quality_score

Stampa diagnostica: quante coppie scartate e perché.
Salva: reports/selected_pairs.csv (colonna 'pair').
"""

import os, sys, argparse
import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
QUALITY_CSV = BASE_DIR / "reports" / "pair_quality.csv"
OUT_CSV = BASE_DIR / "reports" / "selected_pairs.csv"

def parse_args():
    ap = argparse.ArgumentParser("ArbiSense select pairs")
    ap.add_argument("--quality", default=str(QUALITY_CSV))
    ap.add_argument("--mode", choices=["strict","relaxed","topn"], default="relaxed")
    ap.add_argument("--top", type=int, default=10, help="quante coppie tenere (per tutte le modalità)")
    # override manuali (facoltativi)
    ap.add_argument("--adf-max", type=float, default=None)
    ap.add_argument("--hl-min", type=float, default=None)
    ap.add_argument("--hl-max", type=float, default=None)
    ap.add_argument("--min-samples", type=int, default=None)
    ap.add_argument("--out", default=str(OUT_CSV))
    return ap.parse_args()

def main():
    args = parse_args()
    qpath = Path(args.quality)
    if not qpath.exists():
        print(f"[ERROR] quality non trovato: {qpath}", file=sys.stderr); sys.exit(1)

    df = pd.read_csv(qpath)
    df.columns = [c.strip().lower() for c in df.columns]
    need = {"pair","samples","adf_p","half_life","quality_score"}
    if not need.issubset(df.columns):
        print(f"[ERROR] quality deve avere {need}", file=sys.stderr); sys.exit(1)

    # preset per modalità
    presets = {
        "strict":  {"adf_max": 0.05, "hl_min": 3.0, "hl_max": 20.0, "min_samples": 120},
        "relaxed": {"adf_max": 0.35, "hl_min": 3.0, "hl_max": 60.0, "min_samples": 120},
        "topn":    {"adf_max": None, "hl_min": None, "hl_max": None, "min_samples": 0},
    }
    p = presets[args.mode]

    # consenti override manuali
    adf_max = args.adf_max if args.adf_max is not None else p["adf_max"]
    hl_min  = args.hl_min  if args.hl_min  is not None else p["hl_min"]
    hl_max  = args.hl_max  if args.hl_max  is not None else p["hl_max"]
    min_samples = args.min_samples if args.min_samples is not None else p["min_samples"]

    total = len(df)
    sel = df.copy()

    # diagnostica scarti
    reasons = {}

    # filtro samples
    if min_samples and min_samples > 0:
        before = len(sel)
        sel = sel[sel["samples"] >= min_samples]
        reasons["samples"] = before - len(sel)

    # filtro ADF
    if adf_max is not None:
        before = len(sel)
        sel = sel[sel["adf_p"] <= adf_max]
        reasons["adf"] = before - len(sel)

    # filtro half-life
    if (hl_min is not None) and (hl_max is not None):
        before = len(sel)
        sel = sel[sel["half_life"].between(hl_min, hl_max)]
        reasons["half_life"] = before - len(sel)

    # ordinamento e top N
    sel = sel.sort_values(["quality_score","pair"], ascending=[False, True]).head(args.top)

    out = sel[["pair"]].reset_index(drop=True)
    os.makedirs(Path(args.out).parent, exist_ok=True)
    out.to_csv(args.out, index=False)

    # stampa esito
    print(f"[MODE] {args.mode}  |  top={args.top}  |  adf_max={adf_max}  hl=[{hl_min},{hl_max}]  min_samples={min_samples}")
    print(f"[IN]   coppie totali: {total}")
    if args.mode != "topn":
        for k in ["samples","adf","half_life"]:
            if k in reasons:
                print(f"[DROP] {k:10s}: {reasons[k]} scartate")
    print(f"[OK]   selezionate: {len(out)} -> {args.out}")
    if len(out):
        print(out.to_string(index=False))

if __name__ == "__main__":
    main()

