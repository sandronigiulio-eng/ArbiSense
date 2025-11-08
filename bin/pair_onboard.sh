#!/usr/bin/env bash
set -euo pipefail

# --- Load .env (repo-root) ---
SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

# Carica .env in modo robusto per zsh/bash
if [ -f ".env" ]; then
  if [ -n "${ZSH_VERSION:-}" ]; then
    setopt allexport
    source ".env"
    unsetopt allexport
  else
    set -a
    . ".env"
    set +a
  fi
fi
# --- end .env ---
export PYTHONPATH="$PWD${PYTHONPATH:+:$PYTHONPATH}"

# Zittisci eventuali filtri warnings malformati ereditati dall'ambiente
# (es. vecchie impostazioni PYTHONWARNINGS che causano "Invalid -W option ignored")
if [ "${PYTHONWARNINGS:-}" != "" ] && printf '%s' "$PYTHONWARNINGS" | grep -q "urllib3\.exceptions"; then
  export PYTHONWARNINGS=""
fi

# --- Args & usage ---
if [ $# -ne 3 ]; then
  echo "Uso: $0 <PAIR_ID> <A_TICKER> <B_TICKER>"
  echo "Esempio: $0 VUAA_L_VUSA_L VUAA.L VUSA.L"
  exit 1
fi
PAIR="$1"   # es. VUAA_L_VUSA_L
A_TK="$2"   # es. VUAA.L
B_TK="$3"   # es. VUSA.L
LEGS="data_sample/legs_${PAIR}.full.csv"

mkdir -p reports data_sample

# Seleziona solo questa coppia per il walkforward
printf "pair\n%s\n" "$PAIR" > reports/selected_pairs.csv

# --- Snapshot dei presets PRIMA della promozione (per non perderli se la nuova lista è vuota) ---
if [ -f reports/presets.json ]; then
  cp reports/presets.json reports/presets.json.prev
else
  echo "[]" > reports/presets.json.prev
fi

# --- Walk-forward: griglia leggermente ampia, requisiti minimi di trades ---
python3 scripts/walkforward_backtest_v2.py \
  --input data_sample/spread_report_all_pairs_long.normalized.csv \
  --pairs-file reports/selected_pairs.csv \
  --start 2023-01-01 --end 2025-10-31 \
  --train-days 240 --test-days 60 --step-days 45 \
  --grid-z-enter "1.6,1.8,2.0,2.2,2.4,2.6,2.8" \
  --grid-z-exit  "0.4,0.6,0.8,1.0,1.2" \
  --grid-z-stop  "3.2,3.6,4,99" \
  --grid-max-hold "3,5,7" \
  --latency-days "0,1" \
  --min-trades-train 1 --min-trades-test 1 \
  --notional 250000 --spread-scale auto --z-window 30 \
  --fee-bps 2 --slippage-bps 2

# Se non ci sono trade per questa pair nel WF, termina senza toccare i presets
if ! awk -F, -v p="$PAIR" 'NR>1 && $1==p{found=1} END{exit !(found)}' reports/wf_trades.csv; then
  echo "[SKIP] Nessun trade per $PAIR con questi parametri."
  # Ripristina eventuale snapshot (non necessario perché non abbiamo toccato i presets)
  exit 0
fi

# --- Serie FULL + ricostruzione efficienze ---
python3 scripts/fetch_legs_yahoo_full.py --pair "$PAIR" --a "$A_TK" --b "$B_TK" --out "$LEGS"
python3 scripts/rebuild_eff_from_legs_generic.py \
  --pair "$PAIR" --legs "$LEGS" --denom B --hedge ols --tol 6h

# --- TRUE, summary e promozione ---
python3 scripts/recalc_true_from_eff.py
python3 scripts/summary_pairs.py
python3 scripts/promote_from_true_v4.py

# --- Merge di sicurezza con i presets precedenti (richiede jq) ---
if command -v jq >/dev/null 2>&1; then
  # se il file nuovo è vuoto o mancante, normalizzalo a []
  [ -s reports/presets.json ] || echo "[]" > reports/presets.json
  # unisci vecchi + nuovi, deduplica per 'pair'
  jq -s 'add | unique_by(.pair)' reports/presets.json.prev reports/presets.json > reports/presets.json.merged \
    || cp reports/presets.json.prev reports/presets.json.merged
  mv reports/presets.json.merged reports/presets.json
  rm -f reports/presets.json.prev
else
  echo "[WARN] jq non installato: salto merge di sicurezza dei presets. (Installa con: brew install jq)"
  # Se il nuovo file è vuoto, ripristina quello precedente
  if [ ! -s reports/presets.json ]; then
    mv reports/presets.json.prev reports/presets.json
  else
    rm -f reports/presets.json.prev
  fi
fi

# --- Recap veloce ---
if command -v jq >/dev/null 2>&1; then
  echo "== Preset attivo (se presente) per $PAIR =="
  jq -C ".[] | select(.pair==\"$PAIR\")" reports/presets.json || true
fi

echo "[DONE] Onboarding completato per $PAIR. Controlla reports/presets.json."

