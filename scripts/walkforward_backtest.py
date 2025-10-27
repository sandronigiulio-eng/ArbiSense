#!/usr/bin/env python3
"""
ArbiSense — Walk-Forward Backtest (full replacement)

- Rolling TRAIN/TEST con griglia parametri
- Selezione automatica del SEGNO (+1/-1) su TRAIN (massimizza PnL)
- Filtri minimi di trade su TRAIN/TEST
- Gestione robusta dello spread (raw/pct + heuristics bps)
- Date tz-aware (UTC) per evitare errori tz-naive/aware
- Output:
    reports/wf_best_params.csv
    reports/wf_metrics.csv
    reports/wf_trades.csv
    reports/wf_equity.png

Dipendenze: pandas, numpy, matplotlib (Agg)
"""
from __future__ import annotations
import argparse, os, sys, math
from dataclasses import dataclass
from typing import List, Tuple, Dict, Any, Optional
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# -------------------- CLI --------------------

def parse_args():
    ap = argparse.ArgumentParser("ArbiSense Walk-Forward")
    ap.add_argument("--input", default="data_sample/spread_report_all_pairs_long.csv",
                    help="CSV long con colonne: pair, date/timestamp e spread_raw|spread|spread_pct")
    ap.add_argument("--pairs", default=None,
                    help="Lista coppie separate da virgola")
    ap.add_argument("--pairs-file", default=None,
                    help="CSV con colonna 'pair' (filtra le coppie)")
    ap.add_argument("--side", choices=["both","long","short"], default="short")
    ap.add_argument("--start", default=None, help="YYYY-MM-DD inclusiva")
    ap.add_argument("--end",   default=None, help="YYYY-MM-DD inclusiva")
    ap.add_argument("--train-days", type=int, default=240)
    ap.add_argument("--test-days",  type=int, default=60)
    ap.add_argument("--step-days",  type=int, default=60,
                    help="di quanto far scorrere la finestra")

    ap.add_argument("--notional", type=float, default=250_000.0)
    ap.add_argument("--fee-bps", type=float, default=0.0)
    ap.add_argument("--slippage-bps", type=float, default=0.0)
    ap.add_argument("--z-window", type=int, default=60)
    ap.add_argument("--spread-scale", default="auto", help="auto o numero (fattore)")

    ap.add_argument("--grid-z-enter", default="2.6,2.8,3.0,3.2,3.4")
    ap.add_argument("--grid-z-exit",  default="2.4,2.6,2.8")
    ap.add_argument("--grid-z-stop",  default="4.0,99")
    ap.add_argument("--grid-max-hold", default="5,7")
    ap.add_argument("--latency-days",  default="0")

    ap.add_argument("--min-trades-train", type=int, default=2,
                    help="Scarta combinazioni con pochi trade sul TRAIN")
    ap.add_argument("--min-trades-test", type=int, default=1,
                    help="Scarta fold TEST con meno di N trade")

    ap.add_argument("--outdir", default="reports")
    return ap.parse_args()

# -------------------- utils --------------------

def ensure_dir(p: str):
    os.makedirs(p, exist_ok=True)


def infer_date_col(df: pd.DataFrame) -> str:
    for c in ["date","timestamp","Date","Datetime"]:
        if c in df.columns:
            return c
    for c in df.columns:
        try:
            pd.to_datetime(df[c])
            return c
        except Exception:
            pass
    raise KeyError("Nessuna colonna data/timestamp trovata")


def pick_spread_col(df: pd.DataFrame) -> Tuple[str, bool]:
    cols = set(df.columns)
    if "spread_raw" in cols:
        return "spread_raw", False
    if "spread" in cols:
        return "spread", False
    if "spread_pct" in cols:
        return "spread_pct", True
    raise KeyError("Servono colonne spread_raw|spread|spread_pct")


def zscore(x: pd.Series, win: int) -> pd.Series:
    m = x.rolling(win, min_periods=max(5, win//4)).mean()
    v = x.rolling(win, min_periods=max(5, win//4)).std(ddof=0)
    return (x - m) / v


def dir_sign(direction: str) -> float:
    if direction == "SHORT_SPREAD": return -1.0
    if direction == "LONG_SPREAD":  return +1.0
    raise ValueError(f"Direzione sconosciuta: {direction}")


def compute_pnl(direction: str, entry_spread: float, exit_spread: float, *,
                 is_pct: bool, spread_scale: float, notional: float,
                 fee_bps: float, slippage_bps: float) -> float:
    delta = exit_spread - entry_spread
    gross = dir_sign(direction) * delta * (notional if is_pct else (spread_scale * notional))
    costs = (fee_bps + slippage_bps) * 1e-4 * notional
    return gross - costs

@dataclass
class BTParams:
    z_enter: float
    z_exit: float
    z_stop: float
    max_hold: int
    latency: int

@dataclass
class BTContext:
    is_pct: bool
    spread_scale: float
    notional: float
    fee_bps: float
    slippage_bps: float
    side: str
    z_window: int

# -------------------- backtest engine --------------------

def backtest_on_series(dates: pd.Series, spread: pd.Series, params: BTParams, ctx: BTContext) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """Ritorna (trades_df, metrics_dict) per una singola serie."""
    z = zscore(spread.astype(float), ctx.z_window)
    z_lag = z.shift(params.latency) if params.latency > 0 else z
    s_lag = spread.shift(params.latency) if params.latency > 0 else spread

    in_pos = False
    direction: Optional[str] = None
    entry_i: Optional[int] = None
    rows = []

    def try_open(i):
        nonlocal in_pos, direction, entry_i
        zi = z_lag.iat[i]
        if np.isnan(zi):
            return
        if not in_pos and ctx.side in ("short","both") and zi >= params.z_enter:
            in_pos, direction, entry_i = True, "SHORT_SPREAD", i
        elif not in_pos and ctx.side in ("long","both") and zi <= -params.z_enter:
            in_pos, direction, entry_i = True, "LONG_SPREAD", i

    def must_exit(i) -> Tuple[bool,str]:
        if not in_pos:
            return False, ""
        zi = z_lag.iat[i]
        if np.isnan(zi):
            return False, ""
        if direction == "SHORT_SPREAD" and zi <= params.z_exit:
            return True, "MEAN_REVERT"
        if direction == "LONG_SPREAD" and zi >= -params.z_exit:
            return True, "MEAN_REVERT"
        if direction == "SHORT_SPREAD" and zi >= params.z_stop:
            return True, "STOP"
        if direction == "LONG_SPREAD" and zi <= -params.z_stop:
            return True, "STOP"
        if entry_i is not None and (i - entry_i) >= params.max_hold:
            return True, "TIMEOUT"
        return False, ""

    n = len(spread)
    for i in range(n):
        if not in_pos:
            try_open(i)
        if in_pos:
            exit_now, reason = must_exit(i)
            if exit_now:
                entry_spread = float(s_lag.iat[entry_i] if not np.isnan(s_lag.iat[entry_i]) else spread.iat[entry_i])
                exit_spread  = float(s_lag.iat[i] if not np.isnan(s_lag.iat[i]) else spread.iat[i])
                net = compute_pnl(direction, entry_spread, exit_spread,
                                   is_pct=ctx.is_pct, spread_scale=ctx.spread_scale,
                                   notional=ctx.notional, fee_bps=ctx.fee_bps,
                                   slippage_bps=ctx.slippage_bps)
                rows.append({
                    "entry_date": pd.to_datetime(dates.iat[entry_i]).date(),
                    "exit_date":  pd.to_datetime(dates.iat[i]).date(),
                    "entry_spread_eff": float(entry_spread),
                    "exit_spread_eff":  float(exit_spread),
                    "direction": direction,
                    "days_held": int(i - entry_i),
                    "net_pnl": float(net),
                    "entry_z": float(z_lag.iat[entry_i]),
                    "exit_z":  float(z_lag.iat[i]),
                    "reason_exit": reason,
                })
                in_pos, direction, entry_i = False, None, None

    trades = pd.DataFrame(rows)
    if trades.empty:
        metrics = {
            "trades": 0,
            "net_pnl_total": 0.0,
            "CAGR": 0.0,
            "vol_annualized": 0.0,
            "Sharpe": 0.0,
            "MaxDD": 0.0,
            "hit_rate": 0.0,
        }
        return trades, metrics

    eq = trades["net_pnl"].cumsum()
    ret = trades["net_pnl"]
    vol_ann = ret.std(ddof=0) * np.sqrt(252 / max(1, params.max_hold)) if len(ret) > 1 else 0.0
    sharpe = (ret.mean() / ret.std(ddof=0)) * np.sqrt(252 / max(1, params.max_hold)) if ret.std(ddof=0) > 0 else 0.0
    roll_max = eq.cummax(); dd = (eq - roll_max); maxdd = float(dd.min()) if len(dd) else 0.0
    hit = float((trades["net_pnl"] > 0).mean())
    metrics = {
        "trades": int(len(trades)),
        "net_pnl_total": float(ret.sum()),
        "CAGR": 0.0,
        "vol_annualized": float(vol_ann),
        "Sharpe": float(sharpe),
        "MaxDD": maxdd,
        "hit_rate": hit,
    }
    return trades, metrics

# -------------------- WF core --------------------

def choose_sign_on_train(dates: pd.Series, spread: pd.Series, params: BTParams, ctx: BTContext) -> int:
    # prova +spread e -spread e sceglie quello con PnL TRAIN maggiore
    t_pos, m_pos = backtest_on_series(dates,  spread, params, ctx)
    t_neg, m_neg = backtest_on_series(dates, -spread, params, ctx)
    pnl_pos = m_pos.get("net_pnl_total", 0.0)
    pnl_neg = m_neg.get("net_pnl_total", 0.0)
    return 1 if pnl_pos >= pnl_neg else -1


def parse_grid_floats(s: str) -> List[float]:
    return [float(x.strip()) for x in str(s).split(",") if str(x).strip()]

def parse_grid_ints(s: str) -> List[int]:
    return [int(float(x.strip())) for x in str(s).split(",") if str(x).strip()]


# -------------------- main --------------------

def main():
    args = parse_args()
    ensure_dir(args.outdir)

    # carica input
    df = pd.read_csv(args.input)
    if "pair" not in df.columns:
        sys.exit("Input privo della colonna 'pair'")

    # filtra coppie se richiesto
    pairs: List[str] = []
    if args.pairs_file and os.path.exists(args.pairs_file):
        pf = pd.read_csv(args.pairs_file)
        if "pair" not in pf.columns:
            sys.exit("pairs-file: manca colonna 'pair'")
        pairs += [str(x) for x in pf["pair"].dropna().unique().tolist()]
    if args.pairs:
        pairs += [p.strip() for p in str(args.pairs).split(",") if p.strip()]
    pairs = list(dict.fromkeys(pairs))  # dedup
    if pairs:
        df = df[df["pair"].isin(pairs)].copy()
        if df.empty:
            sys.exit("Nessuna riga per le coppie richieste")

    # normalizza date a UTC tz-aware, poi filtra per range globale
    date_col = infer_date_col(df)
    df[date_col] = pd.to_datetime(df[date_col], utc=True, errors="coerce")
    df = df.dropna(subset=[date_col]).sort_values(["pair", date_col]).reset_index(drop=True)

    gstart = pd.to_datetime(args.start, utc=True) if args.start else None
    gend   = pd.to_datetime(args.end,   utc=True) if args.end   else None
    if gstart is not None:
        df = df[df[date_col] >= gstart]
    if gend is not None:
        df = df[df[date_col] <= gend]

    if df.empty:
        sys.exit("Input vuoto dopo il filtro date")

    # spread column & heuristic scale
    spread_col, is_pct_guess = pick_spread_col(df)

    # griglie parametri
    grid_z_enter = parse_grid_floats(args.grid_z_enter)
    grid_z_exit  = parse_grid_floats(args.grid_z_exit)
    grid_z_stop  = parse_grid_floats(args.grid_z_stop)
    grid_maxhold = parse_grid_ints(args.grid_max_hold)
    grid_latency = parse_grid_ints(args.latency_days)

    best_rows = []
    metrics_rows = []
    trades_rows = []

    # per ogni pair, esegui WF
    for pair, g in df.groupby("pair"):
        g = g.copy()
        dates = g[date_col]
        s = pd.to_numeric(g[spread_col], errors="coerce")

        # heuristics: se 'pct' ma mediana grande → trattala come bps
        med = float(np.nanmedian(np.abs(s)))
        if is_pct_guess and med > 10:
            is_pct = False
            auto_scale = 1e-4
        else:
            is_pct = is_pct_guess
            if args.spread_scale == "auto":
                lvl = float(np.nanmedian(np.abs(s)))
                auto_scale = (lvl if lvl and not math.isnan(lvl) else 1.0) * 1e-4
                if is_pct:
                    auto_scale = 1.0
            else:
                auto_scale = float(args.spread_scale)

        # costruisci i fold
        if gstart is not None:
            start_ts = gstart
        else:
            start_ts = dates.min()
        if gend is not None:
            end_ts = gend
        else:
            end_ts = dates.max()

        # rolling window
        cur = pd.to_datetime(start_ts, utc=True)
        fold_id = 0
        while cur + pd.Timedelta(days=args.train_days + args.test_days) <= pd.to_datetime(end_ts, utc=True) + pd.Timedelta(days=1):
            fold_id += 1
            tr_start = cur
            tr_end   = cur + pd.Timedelta(days=args.train_days - 1)
            te_start = tr_end + pd.Timedelta(days=1)
            te_end   = tr_end + pd.Timedelta(days=args.test_days)

            mask_tr = (dates >= tr_start) & (dates <= tr_end)
            mask_te = (dates >= te_start) & (dates <= te_end)
            s_tr = s[mask_tr].reset_index(drop=True)
            s_te = s[mask_te].reset_index(drop=True)
            d_tr = dates[mask_tr].reset_index(drop=True)
            d_te = dates[mask_te].reset_index(drop=True)

            if len(s_tr) < max(30, args.z_window//2) or len(s_te) == 0:
                cur += pd.Timedelta(days=args.step_days)
                continue

            # contesto
            ctx = BTContext(
                is_pct=is_pct,
                spread_scale=auto_scale,
                notional=args.notional,
                fee_bps=args.fee_bps,
                slippage_bps=args.slippage_bps,
                side=args.side,
                z_window=args.z_window,
            )

            # grid search su TRAIN
            best_score = -1e18
            best_candidate: Optional[Tuple[BTParams, int, Dict[str, Any]]] = None

            for ze in grid_z_enter:
                for zx in grid_z_exit:
                    for zs in grid_z_stop:
                        for mh in grid_maxhold:
                            for lt in grid_latency:
                                params = BTParams(ze, zx, zs, mh, lt)
                                # scegli segno su TRAIN
                                sign = choose_sign_on_train(d_tr, s_tr, params, ctx)
                                t_train, m_train = backtest_on_series(d_tr, sign * s_tr, params, ctx)
                                tr_trades = int(m_train.get("trades", 0))
                                if tr_trades < args.min_trades_train:
                                    continue  # scarta combinazione povera di trade
                                score = float(m_train.get("net_pnl_total", 0.0))
                                # tie-breaker su Sharpe e trades
                                score += 1e-6 * float(m_train.get("Sharpe", 0.0))
                                score += 1e-9 * tr_trades
                                if score > best_score:
                                    best_score = score
                                    best_candidate = (params, sign, m_train)

            if best_candidate is None:
                # nessun candidato valido per questo fold
                metrics_rows.append({
                    "pair": pair, "fold": fold_id,
                    "test_start": str(te_start.date()), "test_end": str(te_end.date()),
                    "trades": 0, "net_pnl_total": 0.0,
                    "CAGR": 0.0, "vol_annualized": 0.0,
                    "Sharpe": 0.0, "MaxDD": 0.0, "hit_rate": 0.0,
                    "reason": "SKIP_NO_VALID_PARAM"
                })
                cur += pd.Timedelta(days=args.step_days)
                continue

            params, sign, m_train = best_candidate

            # TEST con i best params + segno fisso
            t_test, m_test = backtest_on_series(d_te, sign * s_te, params, ctx)
            te_trades = int(m_test.get("trades", 0))
            if te_trades < args.min_trades_test:
                metrics_rows.append({
                    "pair": pair, "fold": fold_id,
                    "test_start": str(te_start.date()), "test_end": str(te_end.date()),
                    "trades": 0, "net_pnl_total": 0.0,
                    "CAGR": 0.0, "vol_annualized": 0.0,
                    "Sharpe": 0.0, "MaxDD": 0.0, "hit_rate": 0.0,
                    "reason": "SKIP_MIN_TRADES_TEST"
                })
                cur += pd.Timedelta(days=args.step_days)
                continue

            # annota trades (TEST) con fold/pair
            if not t_test.empty:
                t_test = t_test.copy()
                t_test["pair"] = pair
                t_test["fold"] = fold_id
                t_test["sign"] = sign
                t_test["z_enter"] = params.z_enter
                t_test["z_exit"]  = params.z_exit
                t_test["z_stop"]  = params.z_stop
                t_test["max_hold"] = params.max_hold
                t_test["latency"] = params.latency
                t_test["spread_scale"] = ctx.spread_scale
                trades_rows.append(t_test)

            # metrics TEST
            metrics_row = {
                "pair": pair, "fold": fold_id,
                "test_start": str(te_start.date()), "test_end": str(te_end.date()),
                **m_test
            }
            metrics_rows.append(metrics_row)

            # best params row (per fold & pair)
            best_rows.append({
                "pair": pair, "fold": fold_id,
                "z_enter": params.z_enter, "z_exit": params.z_exit, "z_stop": params.z_stop,
                "max_hold": params.max_hold, "latency": params.latency,
                "notional": args.notional, "start": str(tr_start.date()), "end": str(te_end.date()),
                "train_days": args.train_days, "test_days": args.test_days, "step_days": args.step_days,
                "spread_scale": ctx.spread_scale, "side": args.side, "sign": sign, "z_window": args.z_window,
            })

            cur += pd.Timedelta(days=args.step_days)

    # salva output
    best_df    = pd.DataFrame(best_rows)
    metrics_df = pd.DataFrame(metrics_rows)
    trades_df  = pd.concat(trades_rows, ignore_index=True) if trades_rows else pd.DataFrame(columns=["pair","net_pnl","fold"])

    out_best = os.path.join(args.outdir, "wf_best_params.csv")
    out_metr = os.path.join(args.outdir, "wf_metrics.csv")
    out_trad = os.path.join(args.outdir, "wf_trades.csv")
    out_png  = os.path.join(args.outdir, "wf_equity.png")

    ensure_dir(args.outdir)
    best_df.to_csv(out_best, index=False)
    metrics_df.to_csv(out_metr, index=False)
    trades_df.to_csv(out_trad, index=False)

    # equity plot (cum PnL TEST ordinato per data di uscita)
    plt.figure(figsize=(10,4))
    if not trades_df.empty:
        trades_df = trades_df.sort_values(["fold","exit_date"])  # richiede exit_date
        plt.plot(trades_df["net_pnl"].cumsum().values)
    plt.title("ArbiSense — WF Equity (TEST cum PnL)")
    plt.xlabel("Trade # (ordinati per fold)")
    plt.ylabel("PnL cum")
    plt.tight_layout()
    plt.savefig(out_png, dpi=120)

    print(f"[WROTE] {out_best}\n[WROTE] {out_metr}\n[WROTE] {out_trad}\n[WROTE] {out_png}")


if __name__ == "__main__":
    main()

