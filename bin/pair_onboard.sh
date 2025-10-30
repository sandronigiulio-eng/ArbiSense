#!/usr/bin/env bash
set -euo pipefail
PAIR="$1"           # es. VWRL_L_VEVE_AS
A_TK="$2"           # Yahoo gamba A (es. VWRL.L)
B_TK="$3"           # Yahoo gamba B (es. VEVE.AS)
LEGS="data_sample/legs_${PAIR}.full.csv"

printf "pair\n%s\n" "$PAIR" > reports/selected_pairs.csv

# WF conservativo ma non troppo restrittivo
python3 scripts/walkforward_backtest_v2.py \
  --input data_sample/spread_report_all_pairs_long.normalized.csv \
  --pairs-file reports/selected_pairs.csv \
  --start 2023-01-01 --end 2025-10-31 \
  --train-days 240 --test-days 60 --step-days 45 \
  --grid-z-enter "2.2,2.4,2.6,2.8,3.0" \
  --grid-z-exit  "1.0,1.2,1.4" \
  --grid-z-stop  "4,99" \
  --grid-max-hold "3,5" \
  --latency-days "0,1" \
  --min-trades-train 2 --min-trades-test 1 \
  --notional 250000 --spread-scale auto --z-window 40 \
  --fee-bps 2 --slippage-bps 2

# Se non ci sono trade, esci subito
if ! awk -F, -v p="$PAIR" 'NR>1 && $1==p{found=1} END{exit !(found)}' reports/wf_trades.csv; then
  echo "[SKIP] Nessun trade per $PAIR con questi parametri."
  exit 0
fi

# Gambe FULL con FX auto
python3 scripts/fetch_legs_yahoo_full.py --pair "$PAIR" --a "$A_TK" --b "$B_TK" --out "$LEGS"

# Eff per-fold (hedge OLS, denom=B)
python3 scripts/rebuild_eff_from_legs_generic.py \
  --pair "$PAIR" --legs "$LEGS" --denom B --hedge ols --tol 6h

# TRUE + riepilogo + promozione
python3 scripts/recalc_true_from_eff.py
python3 scripts/summary_pairs.py
python3 scripts/promote_from_true_v4.py

echo "[DONE] Onboarding tentato per $PAIR. Controlla reports/presets.json."
