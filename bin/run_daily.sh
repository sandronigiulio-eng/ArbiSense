#!/usr/bin/env bash
set -euo pipefail

REPO="/Users/giulio/ArbiSense"
cd "$REPO"

# Esporta variabili da .env (se presente)
if [ -f .env ]; then
  set -a
  . ./.env
  set +a
fi

# Python del venv (assoluto)
PY="$REPO/venv/bin/python3"
if [ ! -x "$PY" ]; then
  echo "[ERROR] venv non trovato: $PY" >&2
  exit 90
fi

INPUT="$REPO/data_sample/spread_report_all_pairs_long.normalized.csv"
OUT="$REPO/reports/strong_signals.csv"
LOOKBACK_DAYS=7

mkdir -p "$REPO/reports"

if [ -s "$REPO/reports/presets.json" ]; then
  echo "[INFO] Using reports/presets.json"
  "$PY" "$REPO/scripts/export_from_presets.py" \
    --input "$INPUT" \
    --presets "$REPO/reports/presets.json" \
    --out "$OUT" \
    --lookback $LOOKBACK_DAYS
elif [ -s "$REPO/reports/preset_best.json" ]; then
  echo "[INFO] Using reports/preset_best.json"
  "$PY" "$REPO/scripts/export_from_preset.py" \
    --input "$INPUT" \
    --preset "$REPO/reports/preset_best.json" \
    --out "$OUT" \
    --lookback $LOOKBACK_DAYS
else
  echo "[WARN] Nessun preset trovato."
  exit 0
fi

"$PY" "$REPO/scripts/send_alerts.py" \
  --token "${TELEGRAM_TOKEN:-}" \
  --chat-id "${TELEGRAM_CHAT_ID:-}"

echo "[OK] Daily alerts done."
