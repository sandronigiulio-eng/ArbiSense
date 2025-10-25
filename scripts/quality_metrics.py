#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ArbiSense — Quality Metrics per coppie:
- Usa 'spread_pct' se disponibile, altrimenti 'spread'
- Calcola zscore rolling (se assente)
- Calcola ADF p-value (stazionarietà), half-life, volatilità (full & recente)
- Score composito con pesi configurabili
- Salva reports/pair_quality.csv
"""

import sys, os, argparse, math
from pathlib import Path
import numpy as np
import pandas as pd
from statsmodels.tsa.stattools import adfuller

BASE_DIR = Path(__file__).parent.parent
DEFAULT_INPUT = BASE_DIR / "data_sample" / "spread_report_all_pairs_long.csv"
OUT_CSV = BASE_DIR / "reports" / "pair_quality.csv"

def adf_pvalue(series: pd.Series) -> float:
    s = pd.Series(series).dropna()
    if len(s) < 30:
        return float("nan")
    try:
        return float(adfuller(s, autolag="AIC")[1])
    except Exception:
        return float("nan")

def half_life(series: pd.Series) -> float:
    s = pd.Series(series).dropna()
    if len(s) < 30:
        return float("nan")
    x = s.shift(1).dropna()
    y = (s - s.shift(1)).dropna()
    x, y = x.align(y, join="inner")
    vx = x.var()
    if not np.isfinite(vx) or vx <= 0:
        return float("nan")
    beta = np.cov(x, y, bias=True)[0, 1] / vx
    if beta >= 0 or not np.isfinite(beta):
        return float("inf")
    # half-life in giorni: -ln(2)/beta (beta ~ phi-1 per AR(1) nello spazio delle differenze)
    return float(-np.log(2.0) / beta)

def parse_args():
    ap = argparse.ArgumentParser("ArbiSense quality metrics")
    ap.add_argument("--input", default=str(DEFAULT_INPUT))
    ap.add_argument("--window", type=int, default=60, help="rolling window per zscore")
    ap.add_argument("--recent", type=int, default=90, help="giorni recent per vol_recent")
    ap.add_argument("--min-samples", type=int, default=120)
    # pesi score (0..1, sommano ~1)
    ap.add_argument("--w-adf", type=float, default=0.45)
    ap.add_argument("--w-hl", type=float, default=0.45)
    ap.add_argument("--w-vol", type=float, default=0.10)
    ap.add_argument("--out", default=str(OUT_CSV))
    return ap.parse_args()

def main():
    args = parse_args()
    df = pd.read_csv(args.input)
    df.columns = [c.strip().lower() for c in df.columns]
    if "pair" not in df.columns or "date" not in df.columns:
        print("[ERROR] input deve avere 'pair' e 'date'", file=sys.stderr); sys.exit(1)

    spread_col = "spread_pct" if "spread_pct" in df.columns else ("spread" if "spread" in df.columns else None)
    if not spread_col:
        print("[ERROR] mancano 'spread_pct' e 'spread'", file=sys.stderr); sys.exit(1)

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).sort_values(["pair", "date"]).reset_index(drop=True)
    df["spread_val"] = pd.to_numeric(df[spread_col], errors="coerce")
    df = df.dropna(subset=["spread_val"])

    win, mp = int(args.window), max(10, int(args.window // 2))
    recent_n = int(args.recent)

    out_rows = []
    for pair, g in df.groupby("pair"):
        g = g.copy()
        s = g["spread_val"]
        # zscore rolling (per completezza/consistenza con resto pipeline)
        m = s.rolling(win, min_periods=mp).mean()
        v = s.rolling(win, min_periods=mp).std(ddof=0).replace(0, np.nan)
        z = (s - m) / v
        samples = int(s.notna().sum())

        # metriche
        pval = adf_pvalue(s)
        hl = half_life(s)
        vol_full = float(np.nanstd(s))
        vol_recent = float(np.nanstd(s.tail(recent_n))) if samples >= recent_n else float("nan")

        # normalizzazioni per score:
        # adf: 1 quando p=0, 0 quando p>=0.05
        adf_score = 0.0 if not np.isfinite(pval) else max(0.0, min(1.0, (0.05 - pval) / 0.05))
        # half-life: 1 quando hl=3, 0 quando hl>=60 (clippato)
        if np.isfinite(hl) and hl > 0:
            hl_score = max(0.0, min(1.0, (60.0 - hl) / (60.0 - 3.0)))
        else:
            hl_score = 0.0
        # vol: preferiamo spread meno rumoroso → punteggio = 1/(1+scaled vol)
        vol_ref = np.nanmedian(np.abs(df["spread_val"])) or 1.0
        vol_base = vol_recent if np.isfinite(vol_recent) else vol_full
        vol_score = 1.0 / (1.0 + abs(vol_base / (vol_ref if vol_ref != 0 else 1.0)))

        score = args.w_adf * adf_score + args.w_hl * hl_score + args.w_vol * vol_score

        out_rows.append({
            "pair": pair,
            "samples": samples,
            "adf_p": pval,
            "half_life": hl,
            "vol_full": vol_full,
            "vol_recent": vol_recent,
            "quality_score": float(score)
        })

    qual = pd.DataFrame(out_rows).sort_values(["quality_score","pair"], ascending=[False, True])
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    qual.to_csv(args.out, index=False)
    print(f"[OK] Quality salvata in {args.out}")
    print(qual.head(12).to_string(index=False))

if __name__ == "__main__":
    main()

