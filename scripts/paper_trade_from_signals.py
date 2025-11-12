#!/usr/bin/env python3
import argparse, os, sys, json
from datetime import datetime
import pandas as pd, numpy as np, yfinance as yf, yaml

def load_pairs(cfg_path):
    with open(cfg_path, "r") as f:
        y = yaml.safe_load(f) or {}
    root = y.get("pairs", y)  # supporta sia top-level 'pairs' che root diretto

    pairs = {}

    def extract_leg(obj):
        # obj può essere: string (ticker) o dict {ticker: ..., fx_to_eur: ...}
        if obj is None:
            return None, None
        if isinstance(obj, str):
            return obj, None
        if isinstance(obj, dict):
            t = obj.get("ticker") or obj.get("symbol") or obj.get("code") or obj.get("id")
            fx = obj.get("fx_to_eur") or obj.get("fx") or obj.get("fx_eur")
            return t, fx
        return None, None

    if isinstance(root, list):
        for p in root:
            if not isinstance(p, dict): 
                continue
            name = p.get("name") or p.get("pair") or p.get("id")
            if not name and len(p)==1:
                # caso: [{ IWDA_AS_EUNL_DE: { ... }}, ...]
                name, p = next(iter(p.items()))
            la = p.get("leg_a") or p.get("A") or p.get("legA") or p.get("a")
            lb = p.get("leg_b") or p.get("B") or p.get("legB") or p.get("b")
            A, fxA = extract_leg(la)
            B, fxB = extract_leg(lb)
            if not A: A = p.get("ticker_a") or p.get("A_ticker")
            if not B: B = p.get("ticker_b") or p.get("B_ticker")
            notional = p.get("notional", 250000)
            max_hold = p.get("max_hold", 5)
            if name and A and B:
                pairs[name] = {"A":A,"B":B,"fxA":fxA,"fxB":fxB,"notional":notional,"max_hold":max_hold}
    elif isinstance(root, dict):
        # caso: pairs: { IWDA_AS_EUNL_DE: {leg_a:{ticker:..}, leg_b:{ticker:..}, ...}, ... }
        for name, p in root.items():
            if not isinstance(p, dict): 
                continue
            la = p.get("leg_a") or p.get("A") or p.get("legA") or p.get("a")
            lb = p.get("leg_b") or p.get("B") or p.get("legB") or p.get("b")
            A, fxA = extract_leg(la)
            B, fxB = extract_leg(lb)
            if not A: A = p.get("ticker_a") or p.get("A_ticker")
            if not B: B = p.get("ticker_b") or p.get("B_ticker")
            notional = p.get("notional", 250000)
            max_hold = p.get("max_hold", 5)
            if A and B:
                pairs[name] = {"A":A,"B":B,"fxA":fxA,"fxB":fxB,"notional":notional,"max_hold":max_hold}

    if not pairs:
        raise ValueError(f"Nessuna pair valida trovata in {cfg_path}.")
    print("[INFO] Pairs caricate:", ", ".join(sorted(pairs.keys())))
    return pairs

def fetch_series(ticker, start, end):
    df = yf.download(ticker, start=start, end=end, progress=False, auto_adjust=True)
    if df.empty or "Close" not in df:
        return pd.Series(dtype=float)
    s = df["Close"].copy()
    s.index = pd.to_datetime(s.index).tz_localize("UTC")
    return s

def to_eur(series, fx):
    if not fx: 
        return series
    fxs = fetch_series(fx, (series.index.min() - pd.Timedelta(days=2)).date().isoformat(),
                          (series.index.max() + pd.Timedelta(days=2)).date().isoformat())
    # es. GBPEUR=X → prezzi_in_valuta * FX (converti in EUR)
    if isinstance(fxs, pd.Series) and not fxs.empty:
        series = series.reindex(fxs.index).fillna(method="ffill")
        return series * fxs
    return series

def simulate(signals_path, cfg_pairs, out_trades, cash_per_trade=10000, fees_bps=1.5):
    if not os.path.exists(signals_path):
        print(f"[INFO] Nessun segnale: file {signals_path} assente.")
        pd.DataFrame().to_csv(out_trades, index=False)
        return

    try:
        sig = pd.read_csv(signals_path)
    except Exception:
        print("[INFO] Nessun segnale: CSV vuoto/illeggibile.")
        pd.DataFrame().to_csv(out_trades, index=False)
        return

    if sig.empty:
        print("[INFO] Nessun segnale: CSV vuoto.")
        pd.DataFrame().to_csv(out_trades, index=False)
        return

    pairs = load_pairs(cfg_pairs)

    # tieni solo ENTER/EXIT e ordina
    sig["timestamp"] = pd.to_datetime(sig["timestamp"], utc=True)
    sig = sig.sort_values(["pair","timestamp"]).reset_index(drop=True)

    trades = []
    for pair, grp in sig.groupby("pair", sort=False):
        meta = pairs.get(pair)
        if not meta:
            print(f"[WARN] pair {pair} non trovata in config, salto.")
            continue
        A, B = meta["A"], meta["B"]
        fxA, fxB = meta.get("fxA"), meta.get("fxB")
        max_hold = int(meta.get("max_hold", 5))

        # finestra prezzi ampia
        start = (grp["timestamp"].min()-pd.Timedelta(days=5)).date().isoformat()
        end   = (grp["timestamp"].max()+pd.Timedelta(days=max_hold+5)).date().isoformat()
        sA = to_eur(fetch_series(A, start, end), fxA)
        sB = to_eur(fetch_series(B, start, end), fxB)

        open_time = None
        qtyA = qtyB = 0.0
        side = None
        entry_pxA = entry_pxB = None

        for _, r in grp.iterrows():
            ts = pd.to_datetime(r["timestamp"]).tz_convert("UTC")
            action = str(r["action"])
            fill_day = (ts + pd.Timedelta(days=1)).normalize()  # fill il giorno dopo

            if "ENTER" in action and open_time is None:
                side = str(r["side"]).strip().lower()
                if fill_day not in sA.index or fill_day not in sB.index:
                    continue
                entry_pxA, entry_pxB = float(sA.loc[fill_day]), float(sB.loc[fill_day])
                half = cash_per_trade/2.0
                if side == "short":
                    qtyA = -(half/entry_pxA)  # short A
                    qtyB = +(half/entry_pxB)  # long B
                else:
                    qtyA = +(half/entry_pxA)
                    qtyB = -(half/entry_pxB)
                open_time = fill_day
                fees = (abs(qtyA)*entry_pxA + abs(qtyB)*entry_pxB)*(fees_bps/10000.0)
                trades.append(dict(pair=pair, enter=fill_day, exit=pd.NaT,
                                   side=side, pxA_in=entry_pxA, pxB_in=entry_pxB,
                                   pxA_out=np.nan, pxB_out=np.nan, pnl_eur=-fees))
            elif "EXIT" in action and open_time is not None:
                planned = (ts + pd.Timedelta(days=1)).normalize()
                cap_day = min(planned, (open_time + pd.Timedelta(days=max_hold)))
                out_idx = sA.index[sA.index>=cap_day]
                if len(out_idx)==0: 
                    continue
                out_day = out_idx[0]
                exit_pxA, exit_pxB = float(sA.loc[out_day]), float(sB.loc[out_day])
                gross = (qtyA*exit_pxA + qtyB*exit_pxB) - (qtyA*entry_pxA + qtyB*entry_pxB)
                fees = (abs(qtyA)*exit_pxA + abs(qtyB)*exit_pxB)*(fees_bps/10000.0)
                pnl = gross - fees
                trades.append(dict(pair=pair, enter=open_time, exit=out_day,
                                   side=side, pxA_in=entry_pxA, pxB_in=entry_pxB,
                                   pxA_out=exit_pxA, pxB_out=exit_pxB, pnl_eur=pnl))
                open_time=None; qtyA=qtyB=0.0; entry_pxA=entry_pxB=None; side=None

        # chiusura forzata a max_hold se rimane aperto
        if open_time is not None:
            cap_day = (open_time + pd.Timedelta(days=max_hold))
            out_idx = sA.index[sA.index>=cap_day]
            if len(out_idx)>0:
                out_day = out_idx[0]
                exit_pxA, exit_pxB = float(sA.loc[out_day]), float(sB.loc[out_day])
                gross = (qtyA*exit_pxA + qtyB*exit_pxB) - (qtyA*entry_pxA + qtyB*entry_pxB)
                fees = (abs(qtyA)*exit_pxA + abs(qtyB)*exit_pxB)*(fees_bps/10000.0)
                pnl = gross - fees
                trades.append(dict(pair=pair, enter=open_time, exit=out_day,
                                   side="forced_exit", pxA_in=entry_pxA, pxB_in=entry_pxB,
                                   pxA_out=exit_pxA, pxB_out=exit_pxB, pnl_eur=pnl))

    df = pd.DataFrame(trades)
    df.to_csv(out_trades, index=False)
    if df.empty:
        print("[INFO] Nessuna operazione simulata.")
        return

    df["holding_days"] = (pd.to_datetime(df["exit"]) - pd.to_datetime(df["enter"])).dt.days
    summary = df.groupby("pair")["pnl_eur"].agg(n_trades="count", pnl_eur="sum", avg_pnl="mean")
    print("\n=== PAPER RESULTS PER PAIR ===")
    print(summary.round(2).to_string())
    print("\nTotal PnL EUR:", round(df["pnl_eur"].sum(),2))

if __name__=="__main__":
    ap=argparse.ArgumentParser()
    ap.add_argument("--signals", default="reports/strong_signals.csv")
    ap.add_argument("--pairs", default="config/pairs_live.yaml")
    ap.add_argument("--out", default="reports/paper_trades.csv")
    ap.add_argument("--cash-per-trade", type=float, default=10000)
    ap.add_argument("--fees-bps", type=float, default=1.5)
    args=ap.parse_args()
    simulate(args.signals, args.pairs, args.out, args.cash_per_trade, args.fees_bps)
