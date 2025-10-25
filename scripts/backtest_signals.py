#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ArbiSense — Backtest segnali di mean-reversion sullo spread (robusto).

Funzioni principali
- --pairs-file: limita i pair da backtestare (CSV con colonna 'pair')
- --latency-days: esecuzione ordini a T+latency (default 1)
- --z-stop: stop-loss su |z| durante la posizione
- --side: both | long | short (seleziona il lato dei segnali)
- --spread-scale: corregge le unità dello spread per il PnL (auto/fattore)
  (lo z-score non cambia con lo scaling; impatta SOLO il PnL)

Output
  reports/backtest_trades.csv
  reports/backtest_metrics.csv
  reports/backtest_equity.png
"""

import os, sys, argparse
import numpy as np
import pandas as pd

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

BASE_DIR      = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DEFAULT_INPUT = os.path.join(BASE_DIR, "data_sample", "spread_report_all_pairs_long.csv")
TRADES_OUT    = os.path.join(BASE_DIR, "reports", "backtest_trades.csv")
METRICS_OUT   = os.path.join(BASE_DIR, "reports", "backtest_metrics.csv")
EQUITY_PNG    = os.path.join(BASE_DIR, "reports", "backtest_equity.png")

# ----------------------------- CLI ---------------------------------
def parse_args():
    ap = argparse.ArgumentParser(description="ArbiSense Backtest")
    ap.add_argument("--input", default=DEFAULT_INPUT)
    ap.add_argument("--pairs", default="", help="Lista di coppie separate da virgole (vuoto=tutte)")
    ap.add_argument("--pairs-file", default="", help="CSV con colonna 'pair' per limitare il backtest")
    ap.add_argument("--z-enter", type=float, default=2.0, help="soglia ingresso (|z| ≥ z_enter)")
    ap.add_argument("--z-exit",  type=float, default=0.5, help="soglia uscita (|z| ≤ z_exit)")
    ap.add_argument("--z-stop",  type=float, default=3.5, help="stop su |z| (≥ z_stop) se in posizione")
    ap.add_argument("--latency-days", type=int, default=1, help="ritardo esecuzione ordini in giorni (T+latency)")
    ap.add_argument("--max-hold", type=int, default=10, help="giorni max in posizione")
    ap.add_argument("--fee-bps", type=float, default=1.0, help="bps round-trip (entrata+uscita)")
    ap.add_argument("--slippage-bps", type=float, default=1.0, help="bps round-trip")
    ap.add_argument("--notional", type=float, default=10000.0, help="taglia nozionale per trade")
    ap.add_argument("--start", default="", help="YYYY-MM-DD (opz.)")
    ap.add_argument("--end",   default="", help="YYYY-MM-DD (opz.)")
    ap.add_argument("--z-window", type=int, default=60, help="finestra rolling per zscore se assente")
    ap.add_argument("--spread-scale", default="auto",
                    help="Fattore per scalare lo spread ai fini del PnL (es. 0.0001 se in bps). 'auto' prova a stimarlo.")
    ap.add_argument("--side", choices=["both","long","short"], default="both",
                    help="Quali segnali prendere: both | long | short")
    return ap.parse_args()

# --------------------------- DATA IO --------------------------------
def load_data(path, pairs_filter, start, end, z_window=60, pairs_file=""):
    if not os.path.exists(path):
        print(f"[ERROR] Input non trovato: {path}", file=sys.stderr); sys.exit(1)

    df = pd.read_csv(path)
    df.columns = [c.strip().lower() for c in df.columns]
    if "pair" not in df.columns or "date" not in df.columns:
        print(f"[ERROR] Il CSV deve avere almeno 'pair' e 'date'. Trovate: {df.columns.tolist()}", file=sys.stderr); sys.exit(1)

    # accetta spread o spread_pct
    if "spread" in df.columns:
        spread_col = "spread"
    elif "spread_pct" in df.columns:
        spread_col = "spread_pct"
    else:
        print(f"[ERROR] Manca 'spread' o 'spread_pct'. Trovate: {df.columns.tolist()}", file=sys.stderr); sys.exit(1)

    # normalizza
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])
    df["spread"] = pd.to_numeric(df[spread_col], errors="coerce")
    df = df.dropna(subset=["spread"])
    df = df.sort_values(["pair", "date"]).reset_index(drop=True)

    # filtro manuale 'pairs'
    if pairs_filter:
        wanted = [p.strip() for p in pairs_filter.split(",") if p.strip()]
        df = df[df["pair"].astype(str).isin(wanted)]

    # filtro da file CSV (se fornito)
    if pairs_file:
        try:
            pf = pd.read_csv(pairs_file)
            pf.columns = [c.strip().lower() for c in pf.columns]
            allowed = set(pf["pair"].dropna().astype(str))
            df = df[df["pair"].astype(str).isin(allowed)]
        except Exception as e:
            print(f"[WARN] Impossibile leggere pairs-file {pairs_file}: {e}", file=sys.stderr)

    # filtri temporali
    if start:
        df = df[df["date"] >= pd.to_datetime(start)]
    if end:
        df = df[df["date"] <= pd.to_datetime(end)]

    # zscore: se manca, calcolalo con transform (vectorizzato)
    if "zscore" not in df.columns:
        win = int(z_window); mp = max(10, win // 2)
        g = df.groupby("pair", group_keys=False)
        roll_mean = g["spread"].transform(lambda s: s.rolling(win, min_periods=mp).mean())
        roll_std  = g["spread"].transform(lambda s: s.rolling(win, min_periods=mp).std(ddof=0))
        df["zscore"] = (df["spread"] - roll_mean) / roll_std.replace(0, np.nan)
    else:
        df["zscore"] = pd.to_numeric(df["zscore"], errors="coerce")

    return df[["pair", "date", "spread", "zscore"]]

# ----------------------- SCALING HELPERS ----------------------------
def infer_spread_scale(series: pd.Series) -> float:
    """
    Stima un fattore di scala per portare lo spread su un ordine di grandezza ragionevole.
    Heuristics:
      - se il 95° percentile > 1000 -> probabile bps: usa 1e-4
      - elif > 10 -> probabile percento (1=1%): usa 1e-2
      - altrimenti 1.0
    """
    s = series.dropna().astype(float)
    if s.empty:
        return 1.0
    q95 = s.abs().quantile(0.95)
    if q95 > 1000:
        return 1e-4
    elif q95 > 10:
        return 1e-2
    return 1.0

# ------------------------- STRATEGY LOGIC ----------------------------
def backtest_pair(g, z_enter, z_exit, z_stop, latency_days, max_hold,
                  fee_bps, slippage_bps, notional, spread_scale, side="both"):
    """
    Enter:
      - SHORT spread se z >= z_enter (se side consente 'short')
      - LONG  spread se z <= -z_enter (se side consente 'long')
    Exit:
      - quando |z| <= z_exit  oppure dopo max_hold giorni  oppure |z| >= z_stop (stop)
    Esecuzione ordini: T+latency_days (default 1)
    PnL usa lo **spread scalato**: eff_spread = spread * spread_scale
    """
    rows = []
    in_pos = False
    entry_i = None
    entry_spread_eff = None
    sign = 0

    g = g.copy().reset_index(drop=True)
    g["spread"] = pd.to_numeric(g["spread"], errors="coerce")
    g["zscore"] = pd.to_numeric(g["zscore"], errors="coerce")
    n = len(g)

    def exec_price(idx):
        j = min(idx + latency_days, n - 1)
        return float(g.loc[j, "spread"]), g.loc[j, "date"]

    for i in range(n):
        z = g.loc[i, "zscore"]
        spr = g.loc[i, "spread"]
        if not np.isfinite(z) or not np.isfinite(spr):
            continue

        if not in_pos:
            go_short = (z >= z_enter) and (side in ("both","short"))
            go_long  = (z <= -z_enter) and (side in ("both","long"))

            if go_short:
                px, d_exec = exec_price(i)
                in_pos, entry_i, entry_spread_eff, sign = True, i, px * spread_scale, -1
                entry_date = pd.to_datetime(d_exec).date().isoformat()
            elif go_long:
                px, d_exec = exec_price(i)
                in_pos, entry_i, entry_spread_eff, sign = True, i, px * spread_scale, +1
                entry_date = pd.to_datetime(d_exec).date().isoformat()
        else:
            held = i - entry_i
            exit_reason = None
            if abs(z) <= z_exit:
                exit_reason = "MEAN_REVERT"
            elif held >= max_hold:
                exit_reason = "TIMEOUT"
            elif abs(z) >= z_stop:
                exit_reason = "STOP_Z"

            if exit_reason:
                px, d_exec = exec_price(i)
                exit_spread_eff = px * spread_scale
                exit_date = pd.to_datetime(d_exec).date().isoformat()
                pnl  = (exit_spread_eff - entry_spread_eff) * (-sign) * notional
                cost = (fee_bps + slippage_bps) / 10000.0 * notional
                net  = pnl - cost
                rows.append({
                    "pair": g.loc[i, "pair"],
                    "entry_date": entry_date,
                    "exit_date": exit_date,
                    "entry_spread_raw": float(g.loc[entry_i, "spread"]),
                    "exit_spread_raw":  float(g.loc[i, "spread"]),
                    "entry_spread_eff": float(entry_spread_eff),
                    "exit_spread_eff":  float(exit_spread_eff),
                    "direction": "SHORT_SPREAD" if sign==-1 else "LONG_SPREAD",
                    "days_held": int(max(1, (pd.to_datetime(exit_date) - pd.to_datetime(entry_date)).days)),
                    "gross_pnl": float(pnl),
                    "cost": float(cost),
                    "net_pnl": float(net),
                    "entry_z": float(g.loc[entry_i, "zscore"]) if np.isfinite(g.loc[entry_i, "zscore"]) else float("nan"),
                    "exit_z":  float(z),
                    "reason_exit": exit_reason,
                    "spread_scale": float(spread_scale),
                })
                in_pos, entry_i, entry_spread_eff, sign = False, None, None, 0

    return pd.DataFrame(rows)

# ------------------------- METRICS & PLOTS --------------------------
def equity_and_metrics(trades: pd.DataFrame):
    if trades.empty:
        return pd.DataFrame(), {
            "trades": 0, "net_pnl_total": 0.0,
            "CAGR": 0.0, "vol_annualized": 0.0, "Sharpe": 0.0, "MaxDD": 0.0, "hit_rate": 0.0
        }

    trades["exit_date"] = pd.to_datetime(trades["exit_date"])
    daily = trades.groupby("exit_date")["net_pnl"].sum().sort_index()
    eq = daily.cumsum()
    eq_df = eq.rename("equity").to_frame()

    start = daily.index.min()
    end   = daily.index.max()
    idx = pd.date_range(start, end, freq="D")
    capital0 = 100000.0
    capital = capital0 + eq.reindex(idx, fill_value=0).cumsum()
    rets = capital.pct_change().fillna(0.0)

    ann = 252.0
    mu = rets.mean() * ann
    sigma = rets.std(ddof=0) * (ann ** 0.5)
    sharpe = (mu / sigma) if sigma > 1e-12 else 0.0

    roll_max = capital.cummax()
    dd = (capital - roll_max) / roll_max
    max_dd = float(dd.min())

    # CAGR robusto
    days = max((end - start).days, 1)
    years = max(days / 365.25, 1/365.25)
    cap0 = float(capital.iloc[0]); capN = float(capital.iloc[-1])
    if not np.isfinite(cap0) or cap0 <= 0 or not np.isfinite(capN) or capN <= 0:
        cagr = 0.0
    else:
        try:
            cagr = (capN / cap0) ** (1.0 / years) - 1.0
            cagr = float(cagr) if np.isfinite(cagr) else 0.0
        except Exception:
            cagr = 0.0

    metrics = {
        "trades": int(len(trades)),
        "start": start.date().isoformat(),
        "end":   end.date().isoformat(),
        "net_pnl_total": float(trades["net_pnl"].sum()),
        "CAGR": cagr,
        "vol_annualized": float(sigma),
        "Sharpe": float(sharpe),
        "MaxDD": max_dd,
        "hit_rate": float((trades["net_pnl"] > 0).mean()),
    }

    # Plot
    plt.figure(figsize=(10,5))
    eq.plot()
    plt.title("ArbiSense — Backtest Equity (net PnL cum.)")
    plt.xlabel("Date")
    plt.ylabel("Equity (baseline: 0)")
    plt.tight_layout()
    os.makedirs(os.path.dirname(EQUITY_PNG), exist_ok=True)
    plt.savefig(EQUITY_PNG, dpi=150)
    plt.close()

    return eq_df, metrics

# ------------------------------ MAIN --------------------------------
def main():
    args = parse_args()
    df = load_data(
        args.input, args.pairs, args.start, args.end,
        z_window=args.z_window, pairs_file=args.pairs_file
    )

    # determina il fattore di scala
    if isinstance(args.spread_scale, str) and args.spread_scale.strip().lower() == "auto":
        scale = infer_spread_scale(df["spread"])
        print(f"[INFO] spread-scale=auto → uso {scale}", flush=True)
    else:
        try:
            scale = float(args.spread_scale)
        except Exception:
            print(f"[WARN] spread-scale non valido ({args.spread_scale}), uso 1.0", file=sys.stderr)
            scale = 1.0

    all_trades = []
    for pair, g in df.groupby("pair"):
        t = backtest_pair(
            g[["pair","date","spread","zscore"]].copy(),
            z_enter=args.z_enter,
            z_exit=args.z_exit,
            z_stop=args.z_stop,
            latency_days=max(0, int(args.latency_days)),
            max_hold=args.max_hold,
            fee_bps=args.fee_bps,
            slippage_bps=args.slippage_bps,
            notional=args.notional,
            spread_scale=scale,
            side=args.side
        )
        if not t.empty:
            all_trades.append(t)

    trades = pd.concat(all_trades, ignore_index=True) if all_trades else pd.DataFrame()
    os.makedirs(os.path.dirname(TRADES_OUT), exist_ok=True)
    trades.to_csv(TRADES_OUT, index=False)

    _, metrics = equity_and_metrics(trades)
    pd.DataFrame([metrics]).to_csv(METRICS_OUT, index=False)

    if trades.empty:
        print("[INFO] Nessun trade generato con i parametri attuali.")
    else:
        print("[OK] Backtest completato.")
        print(f"  - Trades: {len(trades)} -> {TRADES_OUT}")
        print(f"  - Metrics: {METRICS_OUT}")
        print(f"  - Equity PNG: {EQUITY_PNG}")

if __name__ == "__main__":
    main()

