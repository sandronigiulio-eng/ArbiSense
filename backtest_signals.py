#!/usr/bin/env python3
"""
ArbiSense — backtest_signals.py (drop‑in)

Backtest single‑run a partire da un CSV di spread (long o wide) per una o più coppie.
Genera trades in base a soglie z‑score (z_enter / z_exit / z_stop), supporta side short/long/both,
latency in giorni, max_hold in giorni, costi in bps e PnL con segno corretto.

Output:
  - reports/backtest_trades.csv
  - reports/backtest_metrics.csv
  - reports/backtest_equity.png

Dipendenze: pandas, numpy, matplotlib (Agg)
"""
from __future__ import annotations
import argparse, os, sys, math, json
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from datetime import timedelta

# ------------------------------ utils ------------------------------

def ensure_dir(p: str):
    os.makedirs(p, exist_ok=True)


def parse_args():
    ap = argparse.ArgumentParser("ArbiSense Backtest (single run)")
    ap.add_argument("--input", default="data_sample/spread_report_all_pairs_long.csv",
                    help="CSV con colonne almeno: date/timestamp, pair, spread o spread_pct")
    ap.add_argument("--pair", default=None, help="Coppia da filtrare (es. SWDA_L_EUNL_DE). Se None usa tutte")
    ap.add_argument("--side", choices=["short", "long", "both"], default="short")
    ap.add_argument("--z-enter", type=float, default=3.0)
    ap.add_argument("--z-exit", type=float, default=2.0)
    ap.add_argument("--z-stop", type=float, default=99.0)
    ap.add_argument("--max-hold", type=int, default=5, help="giorni max di holding")
    ap.add_argument("--latency-days", type=int, default=0)
    ap.add_argument("--z-window", type=int, default=60, help="finestra rolling per zscore")
    ap.add_argument("--notional", type=float, default=250_000.0)
    ap.add_argument("--spread-scale", default="auto", help="auto oppure numero (fattore moltiplicativo)")
    ap.add_argument("--fee-bps", type=float, default=0.0)
    ap.add_argument("--slippage-bps", type=float, default=0.0)
    ap.add_argument("--outdir", default="reports")
    return ap.parse_args()


def _infer_date_col(df: pd.DataFrame) -> str:
    for c in ["date", "timestamp", "Date", "Datetime"]:
        if c in df.columns:
            return c
    # fallback: prima colonna datetime‑like
    for c in df.columns:
        try:
            pd.to_datetime(df[c])
            return c
        except Exception:
            pass
    raise KeyError("Nessuna colonna data/timestamp trovata")


def zscore(s: pd.Series, win: int) -> pd.Series:
    m = s.rolling(win, min_periods=max(5, win//4)).mean()
    v = s.rolling(win, min_periods=max(5, win//4)).std(ddof=0)
    return (s - m) / v


def pick_spread_cols(df: pd.DataFrame):
    cols = df.columns
    if {"spread"}.issubset(cols):
        return "spread", False
    if {"spread_pct"}.issubset(cols):
        return "spread_pct", True
    # compatibilità: spread_raw
    if {"spread_raw"}.issubset(cols):
        return "spread_raw", False
    raise KeyError("Servono colonne 'spread' o 'spread_pct' (o 'spread_raw')")


def compute_pnl(direction: str, entry_spread: float, exit_spread: float, *,
                 is_pct: bool, spread_scale: float, notional: float,
                 fee_bps: float, slippage_bps: float) -> float:
    """ PnL con segno corretto. SHORT beneficia se exit<entry. """
    delta = exit_spread - entry_spread
    dir_sign = -1.0 if direction == "SHORT_SPREAD" else +1.0
    if is_pct:
        gross = dir_sign * delta * notional
    else:
        gross = dir_sign * delta * spread_scale * notional
    costs = (fee_bps + slippage_bps) * 1e-4 * notional
    return gross - costs


# ------------------------------ core backtest ------------------------------

def backtest_pair(df: pd.DataFrame, args: argparse.Namespace) -> tuple[pd.DataFrame, pd.DataFrame]:
    date_col = _infer_date_col(df)
    df = df.sort_values([date_col]).reset_index(drop=True)

    spread_col, is_pct = pick_spread_cols(df)

    # scala spread
    if args.spread_scale == "auto":
        # 1bp del livello medio come scala (robusto a outlier):
        lvl = df[spread_col].abs().median()
        spread_scale = (lvl if lvl and not math.isnan(lvl) else 1.0) * 1e-4
        if is_pct:
            spread_scale = 1.0  # pct già scalare
    else:
        spread_scale = float(args.spread_scale)

    # zscore
    df["z"] = zscore(df[spread_col].astype(float), args.z_window)

    # latency
    if args.latency_days > 0:
        df["z_lag"] = df["z"].shift(args.latency_days)
        df["spread_lag"] = df[spread_col].shift(args.latency_days)
    else:
        df["z_lag"] = df["z"]
        df["spread_lag"] = df[spread_col]

    rows = []  # trades
    in_pos = False
    dir_now: str | None = None
    entry_idx = None

    def try_open(i):
        nonlocal in_pos, dir_now, entry_idx
        z = df.at[i, "z_lag"]
        if np.isnan(z):
            return
        if args.side in ("short", "both") and z >= args.z_enter and not in_pos:
            in_pos = True; dir_now = "SHORT_SPREAD"; entry_idx = i
        elif args.side in ("long", "both") and z <= -args.z_enter and not in_pos:
            in_pos = True; dir_now = "LONG_SPREAD"; entry_idx = i

    def must_exit(i) -> tuple[bool, str]:
        # condizioni di uscita
        if not in_pos:
            return False, ""
        z_now = df.at[i, "z_lag"]
        if np.isnan(z_now):
            return False, ""
        # exit per mean_revert
        if dir_now == "SHORT_SPREAD" and z_now <= args.z_exit:
            return True, "MEAN_REVERT"
        if dir_now == "LONG_SPREAD" and z_now >= -args.z_exit:
            return True, "MEAN_REVERT"
        # stop
        if dir_now == "SHORT_SPREAD" and z_now >= args.z_stop:
            return True, "STOP"
        if dir_now == "LONG_SPREAD" and z_now <= -args.z_stop:
            return True, "STOP"
        # max hold
        if entry_idx is not None and (i - entry_idx) >= args.max_hold:
            return True, "TIMEOUT"
        return False, ""

    n = len(df)
    for i in range(n):
        if not in_pos:
            try_open(i)
        if in_pos:
            exit_now, reason = must_exit(i)
            if exit_now and entry_idx is not None:
                # chiusura trade
                entry_row = df.loc[entry_idx]
                exit_row  = df.loc[i]
                direction = dir_now
                entry_spread = float(entry_row["spread_lag"]) if not is_pct else float(entry_row[spread_col])
                exit_spread  = float(exit_row["spread_lag"])  if not is_pct else float(exit_row[spread_col])
                net = compute_pnl(direction, entry_spread, exit_spread,
                                  is_pct=is_pct, spread_scale=spread_scale,
                                  notional=args.notional, fee_bps=args.fee_bps,
                                  slippage_bps=args.slippage_bps)
                rows.append({
                    "pair": df.loc[i, "pair"] if "pair" in df.columns else "UNKNOWN",
                    "direction": direction,
                    "entry_idx": int(entry_idx),
                    "exit_idx": int(i),
                    "entry_date": pd.to_datetime(entry_row[date_col]).date(),
                    "exit_date":  pd.to_datetime(exit_row[date_col]).date(),
                    "entry_z": float(entry_row["z_lag"]),
                    "exit_z":  float(exit_row["z_lag"]),
                    "reason_exit": reason,
                    "entry_spread_raw": float(entry_row[spread_col]) if not is_pct else np.nan,
                    "exit_spread_raw":  float(exit_row[spread_col])  if not is_pct else np.nan,
                    "entry_spread_pct": float(entry_row[spread_col]) if is_pct else np.nan,
                    "exit_spread_pct":  float(exit_row[spread_col])  if is_pct else np.nan,
                    "spread_scale": spread_scale,
                    "net_pnl": float(net),
                    "fold": None,  # compilato da WF, qui resta None
                })
                in_pos = False; dir_now = None; entry_idx = None

    trades = pd.DataFrame(rows)

    # metriche semplici
    if trades.empty:
        metrics = pd.DataFrame([{ "trades": 0, "net_pnl_total": 0.0, "Sharpe": 0.0, "MaxDD": 0.0, "hit_rate": 0.0 }])
    else:
        eq = trades["net_pnl"].cumsum()
        ret = trades["net_pnl"]
        sharpe = ret.mean() / (ret.std(ddof=0) + 1e-9) * np.sqrt(252/args.max_hold)
        # max drawdown discreto sui trade
        roll_max = eq.cummax()
        dd = (eq - roll_max)
        maxdd = dd.min() if len(dd) else 0.0
        hit = (trades["net_pnl"] > 0).mean()
        metrics = pd.DataFrame([{ "trades": len(trades), "net_pnl_total": trades["net_pnl"].sum(),
                                  "Sharpe": sharpe, "MaxDD": float(maxdd), "hit_rate": float(hit) }])

    return trades, metrics


# ------------------------------ main ------------------------------

def main():
    args = parse_args()
    ensure_dir(args.outdir)

    df = pd.read_csv(args.input)
    if args.pair:
        df = df[df["pair"] == args.pair].copy()
        if df.empty:
            sys.exit(f"Pair {args.pair} non trovata in {args.input}")

    # normalizza date
    date_col = _infer_date_col(df)
    df[date_col] = pd.to_datetime(df[date_col])

    # run per ciascuna pair
    trades_all = []
    metrics_all = []
    for pair, g in (df.groupby("pair") if "pair" in df.columns else [("UNKNOWN", df)]):
        t, m = backtest_pair(g.copy(), args)
        if len(t):
            t["pair"] = pair
        metrics = m.copy()
        metrics["pair"] = pair
        trades_all.append(t)
        metrics_all.append(metrics)

    trades_all = pd.concat(trades_all, ignore_index=True) if trades_all else pd.DataFrame(columns=["pair","net_pnl"])
    metrics_all = pd.concat(metrics_all, ignore_index=True) if metrics_all else pd.DataFrame()

    trades_path = os.path.join(args.outdir, "backtest_trades.csv")
    metrics_path = os.path.join(args.outdir, "backtest_metrics.csv")
    eq_path = os.path.join(args.outdir, "backtest_equity.png")

    trades_all.to_csv(trades_path, index=False)
    metrics_all.to_csv(metrics_path, index=False)

    # equity plot
    plt.figure(figsize=(9,4.2))
    if not trades_all.empty:
        plt.plot(trades_all["net_pnl"].cumsum().values)
    plt.title("ArbiSense — Backtest Equity")
    plt.xlabel("Trade #")
    plt.ylabel("PnL cum")
    plt.tight_layout()
    plt.savefig(eq_path, dpi=120)

    print(f"[WROTE] {metrics_path}\n[WROTE] {trades_path}\n[WROTE] {eq_path}")

if __name__ == "__main__":
    main()

