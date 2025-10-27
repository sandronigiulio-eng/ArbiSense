import argparse, os, sys
import pandas as pd

def pick_spreads(row):
    cols = row.index
    # preferisci spread "raw" se disponibile
    if "entry_spread_raw" in cols and "exit_spread_raw" in cols:
        return float(row["entry_spread_raw"]), float(row["exit_spread_raw"]), False
    if "entry_spread" in cols and "exit_spread" in cols:
        return float(row["entry_spread"]), float(row["exit_spread"]), False
    if "entry_spread_pct" in cols and "exit_spread_pct" in cols:
        return float(row["entry_spread_pct"]), float(row["exit_spread_pct"]), True
    raise KeyError("Colonne spread non trovate (entry/exit). Servono *_spread_raw | *_spread | *_spread_pct.")

def dir_sign(direction: str) -> float:
    if direction == "SHORT_SPREAD": return -1.0
    if direction == "LONG_SPREAD":  return +1.0
    raise ValueError(f"Direzione sconosciuta: {direction}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--trades", default="reports/wf_trades.csv")
    ap.add_argument("--out", default="reports/wf_trades_fixed.csv")
    ap.add_argument("--notional", type=float, default=250000.0)
    ap.add_argument("--fee-bps", type=float, default=0.0)
    ap.add_argument("--slippage-bps", type=float, default=0.0)
    ap.add_argument("--spread-sign", type=float, default=1.0, help="+1 o -1 se vuoi forzare un segno su tutto (finché non implementi il sign per fold)")
    ap.add_argument("--inplace", action="store_true", help="sovrascrive il file originale")
    args = ap.parse_args()

    if not os.path.exists(args.trades):
        sys.exit(f"File non trovato: {args.trades}")

    t = pd.read_csv(args.trades)
    required = {"direction"}
    if not required.issubset(set(t.columns)):
        sys.exit(f"Colonne richieste mancanti: {required - set(t.columns)}")

    # calcola net_pnl_corretto
    def compute_row(row):
        entry, exit, is_pct = pick_spreads(row)
        d_spread = (exit - entry) * float(args.spread_sign)
        s_dir = dir_sign(str(row["direction"]))
        spread_scale = float(row["spread_scale"]) if "spread_scale" in row.index else 1.0

        if is_pct:
            gross = s_dir * d_spread * args.notional
        else:
            gross = s_dir * d_spread * spread_scale * args.notional

        total_bps = (args.fee_bps + args.slippage_bps) * 1e-4
        costs = total_bps * args.notional
        return gross - costs

    t["net_pnl_fixed"] = t.apply(compute_row, axis=1)

    # diagnostica di coerenza segno su SHORT con exit_z < entry_z (se colonne presenti)
    info = {}
    if {"entry_z","exit_z"}.issubset(t.columns):
        short = t[t["direction"]=="SHORT_SPREAD"].copy()
        short["delta_z"] = short["exit_z"] - short["entry_z"]
        info["short_revert_total"] = int((short["delta_z"] < 0).sum())
        info["short_revert_pnl_pos"] = int(((short["delta_z"] < 0) & (short["net_pnl_fixed"] > 0)).sum())
        info["short_revert_pnl_neg"] = int(((short["delta_z"] < 0) & (short["net_pnl_fixed"] < 0)).sum())

    # output
    out_path = args.trades if args.inplace else args.out
    t.to_csv(out_path, index=False)
    total = float(t["net_pnl_fixed"].sum())
    folds = int(t["fold"].nunique()) if "fold" in t.columns else None

    print(f"[WROTE] {out_path}")
    print(f"TOTAL_PNL_FIXED={total:.6f}, FOLDS={folds}")
    if info:
        print(f"SHORT revert check: total={info['short_revert_total']}, pnl>0={info['short_revert_pnl_pos']}, pnl<0={info['short_revert_pnl_neg']}")

if __name__ == "__main__":
    main()
