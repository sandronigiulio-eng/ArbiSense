#!/usr/bin/env python3
# scripts/paper_trade_from_signals.py
import argparse, os, sys, json, time
from datetime import datetime, timedelta, timezone
import pandas as pd, numpy as np, yfinance as yf, yaml

def load_pairs(cfg_path):
    with open(cfg_path, "r") as f:
        y = yaml.safe_load(f)
    # atteso: mapping pair -> dict con tickers A/B e (opz.) FX base
    # fallback: deduci da nome pair tipo SWDA_L_EUNL_DE
    pairs = {}
    for p in y.get("pairs", []):
        name = p["name"]
        pairs[name] = {
            "A": p["leg_a"]["ticker"],
            "B": p["leg_b"]["ticker"],
            "fxA": p["leg_a"].get("fx_to_eur"),  # es. "GBPEUR=X"
            "fxB": p["leg_b"].get("fx_to_eur"),
            "notional": p.get("notional", 250000),
            "max_hold": p.get("max_hold", 5)
        }
    return pairs

def fetch_series(ticker, start, end):
    df = yf.download(ticker, start=start, end=end, progress=False, auto_adjust=True)
    if df.empty or "Close" not in df:
        return pd.Series(dtype=float)
    s = df["Close"].copy()
    s.index = pd.to_datetime(s.index).tz_localize("UTC")
    return s

def to_eur(series, fx):
    if not fx: return series
    fxs = fetch_series(fx, series.index.min()-pd.Timedelta(days=2), series.index.max()+pd.Timedelta(days=2))
    if fx.endswith("EUR=X"):
        # es. GBPEUR=X: moltiplica prezzi in GBP * GBPEUR
        series = series.reindex(fxs.index).fillna(method="ffill")
        return series * fxs
    # estendi qui altri casi se servono
    return series

def simulate(signals_path, cfg_pairs, out_trades, cash_per_trade=10000, fees_bps=1.5):
    sig = pd.read_csv(signals_path)
    if sig.empty:
        print("[INFO] Nessun segnale: esco.")
        pd.DataFrame().to_csv(out_trades, index=False)
        return

    pairs = load_pairs(cfg_pairs)

    # Considera solo ENTER/EXIT ordinati per timestamp
    sig["timestamp"] = pd.to_datetime(sig["timestamp"], utc=True)
    sig = sig.sort_values(["pair","timestamp"]).reset_index(drop=True)

    trades = []
    for pair, grp in sig.groupby("pair"):
        meta = pairs.get(pair)
        if not meta: 
            print(f"[WARN] pair {pair} non presente in config, salto.")
            continue
        A, B = meta["A"], meta["B"]
        fxA, fxB = meta.get("fxA"), meta.get("fxB")
        max_hold = int(meta.get("max_hold", 5))

        # Carica finestra ampia prezzi
        start = (grp["timestamp"].min()-pd.Timedelta(days=5)).date().isoformat()
        end   = (grp["timestamp"].max()+pd.Timedelta(days=max_hold+5)).date().isoformat()
        sA = to_eur(fetch_series(A, start, end), fxA)
        sB = to_eur(fetch_series(B, start, end), fxB)

        open_time = None
        qtyA = qtyB = 0.0
        side = None  # supportiamo 'short' (short A, long B) come nel tuo preset
        entry_pxA = entry_pxB = None

        for _, r in grp.iterrows():
            ts = pd.to_datetime(r["timestamp"]).tz_convert("UTC")
            action = str(r["action"])
            # esecuzione “realistica”: usa il prezzo di CHIUSURA del giorno successivo
            fill_day = (ts + pd.Timedelta(days=1)).normalize()

            if "ENTER" in action and open_time is None:
                side = str(r["side"]).strip().lower()
                # prezzi di fill
                if fill_day not in sA.index or fill_day not in sB.index:
                    continue
                entry_pxA, entry_pxB = float(sA.loc[fill_day]), float(sB.loc[fill_day])
                # allocazione 50/50 del cash sullo spread
                half = cash_per_trade/2.0
                # quantità: valore/ prezzo
                if side == "short":
                    qtyA = -(half/entry_pxA)  # short A
                    qtyB = +(half/entry_pxB)  # long B
                else:
                    qtyA = +(half/entry_pxA)
                    qtyB = -(half/entry_pxB)
                open_time = fill_day
                # commissioni in bps su nozionale
                fees = (abs(qtyA)*entry_pxA + abs(qtyB)*entry_pxB)*(fees_bps/10000.0)
                trades.append(dict(pair=pair, enter=fill_day, exit=pd.NaT,
                                   side=side, pxA_in=entry_pxA, pxB_in=entry_pxB,
                                   pxA_out=np.nan, pxB_out=np.nan, pnl_eur=-fees))
            elif "EXIT" in action and open_time is not None:
                # chiusura al giorno successivo all'EXIT (o max_hold se prima)
                planned = (ts + pd.Timedelta(days=1)).normalize()
                # rispetto max_hold: se troppo tardi, usa open_time+max_hold
                cap_day = min(planned, (open_time + pd.Timedelta(days=max_hold)))
                # trova il primo giorno di prezzi disponibile >= cap_day
                out_idx = sA.index[sA.index>=cap_day]
                if len(out_idx)==0: 
                    continue
                out_day = out_idx[0]
                exit_pxA, exit_pxB = float(sA.loc[out_day]), float(sB.loc[out_day])
                # PnL: (qtyA*pxA_out + qtyB*pxB_out) - (qtyA*pxA_in + qtyB*pxB_in) - fees chiusura
                gross = (qtyA*exit_pxA + qtyB*exit_pxB) - (qtyA*entry_pxA + qtyB*entry_pxB)
                fees = (abs(qtyA)*exit_pxA + abs(qtyB)*exit_pxB)*(fees_bps/10000.0)
                pnl = gross - fees
                trades.append(dict(pair=pair, enter=open_time, exit=out_day,
                                   side=side, pxA_in=entry_pxA, pxB_in=entry_pxB,
                                   pxA_out=exit_pxA, pxB_out=exit_pxB, pnl_eur=pnl))
                # reset
                open_time=None; qtyA=qtyB=0.0; entry_pxA=entry_pxB=None; side=None

        # se rimane aperto senza EXIT: chiudi a max_hold
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
    if df.empty:
        print("[INFO] Nessuna operazione simulata.")
        df.to_csv(out_trades, index=False)
        return
    df["holding_days"] = (pd.to_datetime(df["exit"]) - pd.to_datetime(df["enter"])).dt.days
    summary = df.groupby("pair")["pnl_eur"].agg(["count","sum","mean"]).rename(columns={"count":"n_trades","sum":"pnl_eur","mean":"avg_pnl"})
    print("\n=== PAPER RESULTS PER PAIR ===")
    print(summary.to_string())
    print("\nTotal PnL EUR:", round(df["pnl_eur"].sum(),2))
    df.to_csv(out_trades, index=False)

if __name__=="__main__":
    ap=argparse.ArgumentParser()
    ap.add_argument("--signals", default="reports/strong_signals.csv")
    ap.add_argument("--pairs", default="config/pairs_live.yaml")
    ap.add_argument("--out", default="reports/paper_trades.csv")
    ap.add_argument("--cash-per-trade", type=float, default=10000)
    ap.add_argument("--fees-bps", type=float, default=1.5)
    args=ap.parse_args()
    simulate(args.signals, args.pairs, args.out, args.cash_per_trade, args.fees_bps)

