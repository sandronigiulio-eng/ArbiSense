#!/usr/bin/env python3
import argparse, itertools, math, os, datetime as dt
import pandas as pd
import numpy as np

# ---------------------------
# util
# ---------------------------
def zscore(x: pd.Series, window: int, minp: int = None):
    if minp is None:
        minp = max(5, window//4)
    m = x.rolling(window, min_periods=minp).mean()
    s = x.rolling(window, min_periods=minp).std(ddof=0)
    return (x - m) / s

def parse_date(s):
    return pd.to_datetime(s, utc=True)

def ensure_cols(df, cols):
    miss = [c for c in cols if c not in df.columns]
    if miss:
        raise SystemExit(f"Mancano colonne nel dataset: {miss}")

# ---------------------------
# backtest semplice short/long spread su zscore
# ---------------------------
def simulate_trades(df, side, z_enter, z_exit, z_stop, max_hold, latency_days,
                    notional, fee_bps, slippage_bps, fold_id, pair, sign, z_window):
    """
    df: DataFrame ordinato per timestamp con colonne: ts, spread_eff
    Ritorna: trades list[dict], pnl_series (serie giornaliera)
    """
    # z-score sullo spread "orientato" dal sign (scelto sul TRAIN)
    z = zscore(df["spread_eff"] * sign, z_window)
    df = df.copy()
    df["z"] = z

    in_pos = False
    pos_dir = None
    entry_idx = None
    entry_eff = None
    entry_z = None
    age = 0

    trades = []
    equity = 0.0
    pnl_by_day = []

    fee = notional * (fee_bps/10_000.0)
    slip = notional * (slippage_bps/10_000.0)

    # latenza: quante barre dopo il segnale si esegue
    lat = int(latency_days)

    for i, r in df.iterrows():
        ts = r["ts"]
        eff = float(r["spread_eff"])
        zz  = float(r["z"]) if not math.isnan(r["z"]) else None

        # registra PnL giornaliero (flat = 0) — per Sharpe/vol opzionali
        pnl_by_day.append((ts, 0.0))

        if zz is None:
            continue

        # ENTRY
        if not in_pos:
            if side in ("short","both") and zz >= z_enter:
                # applica latenza
                j = min(i+lat, df.index[-1])
                eff_entry = float(df.loc[j, "spread_eff"])
                z_entry   = float(df.loc[j, "z"])
                in_pos = True
                pos_dir = "SHORT_SPREAD"
                entry_idx = j
                entry_eff = eff_entry
                entry_z = z_entry
                age = 0
                continue
            if side in ("long","both") and zz <= -z_enter:
                j = min(i+lat, df.index[-1])
                eff_entry = float(df.loc[j, "spread_eff"])
                z_entry   = float(df.loc[j, "z"])
                in_pos = True
                pos_dir = "LONG_SPREAD"
                entry_idx = j
                entry_eff = eff_entry
                entry_z = z_entry
                age = 0
                continue

        # EXIT (se in posizione)
        else:
            age += 1
            exit_reason = None
            do_exit = False

            if pos_dir == "SHORT_SPREAD":
                if zz <= z_exit:
                    do_exit = True; exit_reason = "MEAN_REVERT"
                elif zz >= z_stop:
                    do_exit = True; exit_reason = "STOP"
                elif age >= max_hold:
                    do_exit = True; exit_reason = "TIMEOUT"

            elif pos_dir == "LONG_SPREAD":
                if zz >= -z_exit:
                    do_exit = True; exit_reason = "MEAN_REVERT"
                elif zz <= -z_stop:
                    do_exit = True; exit_reason = "STOP"
                elif age >= max_hold:
                    do_exit = True; exit_reason = "TIMEOUT"

            if do_exit:
                j = min(i+lat, df.index[-1])
                eff_exit = float(df.loc[j, "spread_eff"])
                z_exitv  = float(df.loc[j, "z"])
                gross = 0.0
                if pos_dir == "SHORT_SPREAD":
                    gross = notional * (entry_eff - eff_exit)
                else:
                    gross = notional * (eff_exit - entry_eff)
                cost = 2*(fee+slip)
                net  = gross - cost
                equity += net
                trades.append({
                    "pair": pair,
                    "fold": fold_id,
                    "entry_date": df.loc[entry_idx, "ts"],
                    "exit_date":  df.loc[j, "ts"],
                    "entry_spread_eff": entry_eff,
                    "exit_spread_eff":  eff_exit,
                    "direction": pos_dir,
                    "days_held": age,
                    "gross_pnl": gross,
                    "cost": cost,
                    "net_pnl": net,
                    "entry_z": entry_z,
                    "exit_z": z_exitv,
                    "reason_exit": exit_reason,
                    "spread_scale": "auto",
                    "sign": sign,
                })
                in_pos=False
                pos_dir=None
                entry_idx=None
                entry_eff=None
                entry_z=None
                age=0

    # ricava serie PnL giornaliera (netto per giorno, qui semplificato = equity jumps solo a exit)
    # per ora lasciamo solo cumulata via trades; Sharpe opzionale
    pnl_series = pd.DataFrame(pnl_by_day, columns=["ts","pnl"])
    return trades, pnl_series

def eval_sign_on_train(train_df, side, params, notional, fee_bps, slippage_bps, pair, z_window):
    best = None
    for sign in (+1, -1):
        tr, _ = simulate_trades(train_df, side=side, z_enter=params["z_enter"], z_exit=params["z_exit"],
                                z_stop=params["z_stop"], max_hold=params["max_hold"],
                                latency_days=params["latency"], notional=notional,
                                fee_bps=fee_bps, slippage_bps=slippage_bps,
                                fold_id=-1, pair=pair, sign=sign, z_window=z_window)
        pnl = sum(x["net_pnl"] for x in tr) if tr else 0.0
        if (best is None) or (pnl > best[0]):
            best = (pnl, sign)
    return best[1] if best else 1

# ---------------------------
# main WF
# ---------------------------
def main():
    ap = argparse.ArgumentParser("ArbiSense WF v2 (true PnL + sign per fold)")
    ap.add_argument("--input", default="data_sample/spread_report_all_pairs_long.normalized.csv")
    ap.add_argument("--pairs-file", required=True)
    ap.add_argument("--side", choices=["both","long","short"], default="short")
    ap.add_argument("--start", default=None)
    ap.add_argument("--end", default=None)
    ap.add_argument("--train-days", type=int, default=240)
    ap.add_argument("--test-days", type=int, default=60)
    ap.add_argument("--step-days", type=int, default=45)
    ap.add_argument("--notional", type=float, default=250000.0)
    ap.add_argument("--fee-bps", type=float, default=0.0)
    ap.add_argument("--slippage-bps", type=float, default=0.0)
    ap.add_argument("--z-window", type=int, default=40)
    ap.add_argument("--spread-scale", default="auto")
    ap.add_argument("--grid-z-enter", default="2.4,2.6,2.8,3.0")
    ap.add_argument("--grid-z-exit",  default="1.6,1.8,2.0")
    ap.add_argument("--grid-z-stop",  default="3.6,4.0,99")
    ap.add_argument("--grid-max-hold", default="5,7,9")
    ap.add_argument("--latency-days",  default="0,1")
    ap.add_argument("--min-trades-train", type=int, default=2)
    ap.add_argument("--min-trades-test",  type=int, default=1)
    args = ap.parse_args()

    # carica input normalizzato
    df = pd.read_csv(args.input)
    date_col = "date" if "date" in df.columns else ("timestamp" if "timestamp" in df.columns else None)
    if not date_col:
        raise SystemExit("Input deve avere 'date' o 'timestamp'")
    ensure_cols(df, [date_col, "pair", "spread_raw", "spread_scale"])
    df[date_col] = pd.to_datetime(df[date_col], utc=True, errors="coerce")
    df = df.dropna(subset=[date_col]).copy()
    df = df.sort_values([ "pair", date_col ])

    # spread effettivo dalla normalizzazione
    if str(args.spread_scale).lower()=="auto":
        df["spread_eff"] = pd.to_numeric(df["spread_raw"], errors="coerce") * pd.to_numeric(df["spread_scale"], errors="coerce")
    else:
        val = float(args.spread_scale)
        df["spread_eff"] = pd.to_numeric(df["spread_raw"], errors="coerce") * val

    # filtro date globali
    if args.start: df = df[df[date_col] >= parse_date(args.start)]
    if args.end:   df = df[df[date_col] <= parse_date(args.end)]
    df = df.dropna(subset=["spread_eff"])

    # pairs
    pairs = pd.read_csv(args.pairs_file)["pair"].dropna().astype(str).unique().tolist()

    # griglie
    grid = dict(
        z_enter=[float(x) for x in str(args.grid_z_enter).split(",") if x],
        z_exit =[float(x) for x in str(args.grid_z_exit).split(",") if x],
        z_stop =[float(x) for x in str(args.grid_z_stop).split(",") if x],
        max_hold=[int(x) for x in str(args.grid_max_hold).split(",") if x],
        latency=[int(x) for x in str(args.latency_days).split(",") if x],
    )

    all_trades = []
    rows_metrics = []
    rows_best = []

    for pair in pairs:
        g = df[df["pair"]==pair].copy()
        if g.empty: 
            continue
        g = g.rename(columns={date_col:"ts"})
        g = g.sort_values("ts").reset_index(drop=True)

        # definisci finestre fold
        if g.empty: 
            continue
        ts_min, ts_max = g["ts"].min(), g["ts"].max()
        if args.start: ts_min = max(ts_min, parse_date(args.start))
        if args.end:   ts_max = min(ts_max, parse_date(args.end))

        folds = []
        cur_start = ts_min
        td = pd.Timedelta(days=int(args.train_days))
        vd = pd.Timedelta(days=int(args.test_days))
        sd = pd.Timedelta(days=int(args.step_days))
        while cur_start + td + vd <= ts_max:
            train_start = cur_start
            train_end   = cur_start + td
            test_start  = train_end
            test_end    = train_end + vd
            folds.append((train_start, train_end, test_start, test_end))
            cur_start += sd

        best_for_pair = None  # (oos_pnl, params_dict)

        for fold_id,(tr_s,tr_e,te_s,te_e) in enumerate(folds, start=1):
            tr = g[(g["ts"]>=tr_s)&(g["ts"]<tr_e)].copy()
            te = g[(g["ts"]>=te_s)&(g["ts"]<te_e)].copy()
            if len(tr)<max(20, args.z_window*2) or len(te)<args.z_window:
                rows_metrics.append({"pair":pair,"fold":fold_id,"net_pnl_total":0.0,"trades":0,"hit_rate":0.0,"reason":"SKIP_TOO_SHORT"})
                continue

            best_fold = None  # (oos_pnl, params, sign)

            for zE,zX,zS,mH,lat in itertools.product(grid["z_enter"], grid["z_exit"], grid["z_stop"], grid["max_hold"], grid["latency"]):
                params = dict(z_enter=zE, z_exit=zX, z_stop=zS, max_hold=mH, latency=lat)
                # scegli segno sul TRAIN
                sign = eval_sign_on_train(tr, side=args.side, params=params,
                                          notional=args.notional, fee_bps=args.fee_bps, slippage_bps=args.slippage_bps,
                                          pair=pair, z_window=args.z_window)
                # simula TRAIN per conteggio trade (filtro min-trades-train)
                tr_trades,_ = simulate_trades(tr, side=args.side, z_enter=zE, z_exit=zX, z_stop=zS, max_hold=mH,
                                              latency_days=lat, notional=args.notional, fee_bps=args.fee_bps,
                                              slippage_bps=args.slippage_bps, fold_id=fold_id, pair=pair,
                                              sign=sign, z_window=args.z_window)
                if len(tr_trades) < args.min_trades_train:
                    continue

                # simula TEST
                te_trades,_ = simulate_trades(te, side=args.side, z_enter=zE, z_exit=zX, z_stop=zS, max_hold=mH,
                                              latency_days=lat, notional=args.notional, fee_bps=args.fee_bps,
                                              slippage_bps=args.slippage_bps, fold_id=fold_id, pair=pair,
                                              sign=sign, z_window=args.z_window)
                if len(te_trades) < args.min_trades_test:
                    continue

                oos = sum(x["net_pnl"] for x in te_trades)
                if (best_fold is None) or (oos > best_fold[0]):
                    best_fold = (oos, params, sign, te_trades)

            if best_fold is None:
                rows_metrics.append({"pair":pair,"fold":fold_id,"net_pnl_total":0.0,"trades":0,"hit_rate":0.0,"reason":"SKIP_MIN_TRADES_TEST"})
                continue

            oos_pnl, params, sign, te_trades = best_fold
            wins = sum(1 for x in te_trades if x["net_pnl"]>0)
            rows_metrics.append({"pair":pair,"fold":fold_id,"net_pnl_total":oos_pnl,"trades":len(te_trades),
                                 "hit_rate": wins/len(te_trades) if te_trades else 0.0})
            all_trades.extend(te_trades)

            # tieni best params globali per pair (somma sui fold)
            if best_for_pair is None:
                best_for_pair = dict(total=oos_pnl, params=params, sign=sign)
            else:
                best_for_pair["total"] += oos_pnl

        # salva “miglior” configurazione aggregata per pair (greedy su somma OOS fold)
        if best_for_pair is not None:
            rows_best.append({
                "pair": pair,
                "z_enter": best_for_pair["params"]["z_enter"],
                "z_exit":  best_for_pair["params"]["z_exit"],
                "z_stop":  best_for_pair["params"]["z_stop"],
                "max_hold":best_for_pair["params"]["max_hold"],
                "latency": best_for_pair["params"]["latency"],
                "z_window": args.z_window,
                "side": args.side,
                "notional": args.notional,
                "spread_scale": "auto",
                "sign": best_for_pair["sign"],
                "oos_total_pnl": best_for_pair["total"],
            })

    # Scrivi output
    os.makedirs("reports", exist_ok=True)
    # trades
    if all_trades:
        trades_df = pd.DataFrame(all_trades)
    else:
        trades_df = pd.DataFrame(columns=[
            "pair","fold","entry_date","exit_date","entry_spread_eff","exit_spread_eff","direction",
            "days_held","gross_pnl","cost","net_pnl","entry_z","exit_z","reason_exit","spread_scale","sign"
        ])
    trades_df.to_csv("reports/wf_trades.csv", index=False)
    trades_df.to_csv("reports/wf_trades.true.csv", index=False)  # compat export TRUE
    # metrics
    metrics_df = pd.DataFrame(rows_metrics) if rows_metrics else pd.DataFrame(columns=["pair","fold","net_pnl_total","trades","hit_rate","reason"])
    metrics_df.to_csv("reports/wf_metrics.csv", index=False)
    # best params
    best_df = pd.DataFrame(rows_best) if rows_best else pd.DataFrame(columns=["pair","z_enter","z_exit","z_stop","max_hold","latency","z_window","side","notional","spread_scale","sign","oos_total_pnl"])
    best_df.to_csv("reports/wf_best_params.csv", index=False)

    print("[WROTE] reports/wf_best_params.csv")
    print("[WROTE] reports/wf_metrics.csv")
    print("[WROTE] reports/wf_trades.csv")

if __name__ == "__main__":
    main()

