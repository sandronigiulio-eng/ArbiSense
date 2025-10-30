#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

# venv + variabili
[ -d "venv" ] && source venv/bin/activate
source .env 2>/dev/null || true

INPUT="data_sample/spread_report_all_pairs_long.normalized.csv"
OUT="reports/strong_signals.csv"
LOOKBACK_DAYS=90

mkdir -p reports

# 1) Export segnali
python3 scripts/export_from_presets.py \
  --input "$INPUT" \
  --presets reports/presets.json \
  --out "$OUT" \
  --lookback $LOOKBACK_DAYS

# 2) Filtro di regime (z-vol + ADF)
python3 scripts/filter_regime.py \
  --input "$OUT" \
  --out "$OUT" \
  --data "$INPUT" \
  --pair-quality reports/pair_quality.csv \
  --regime-zvol-max 1.2 \
  --regime-zvol-window 20 \
  --regime-adf-max 0.30

# 3) Cooldown anti-spam (3 giorni)
python3 scripts/postfilter_signals.py "$OUT" "$OUT" 3

# 4) Invio alert
python3 scripts/send_alerts.py \
  --token "${TELEGRAM_TOKEN:-}" \
  --chat-id "${TELEGRAM_CHAT_ID:-}"

echo "[OK] Daily alerts done."
