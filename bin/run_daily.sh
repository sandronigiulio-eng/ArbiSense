#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
[ -d "venv" ] && source venv/bin/activate
source .env 2>/dev/null || true

INPUT="data_sample/spread_report_all_pairs_long.normalized.csv"
OUT="reports/strong_signals.csv"
LOOKBACK_DAYS=90
mkdir -p reports

# 1) Export
python3 scripts/export_from_presets.py \
  --input "$INPUT" \
  --presets reports/presets.json \
  --out "$OUT" \
  --lookback $LOOKBACK_DAYS

# 2) Filtro regime (z-vol + ADF)
python3 scripts/filter_regime.py \
  --input "$OUT" \
  --out "$OUT" \
  --data "$INPUT" \
  --pair-quality reports/pair_quality.csv \
  --regime-zvol-max 1.0 \
  --regime-zvol-window 20 \
  --regime-adf-max 0.30

# 3) Position guard (no EXIT senza ENTER recente)
python3 scripts/position_guard.py "$OUT" "$OUT" 120

# 4) Cooldown 3 giorni
python3 scripts/postfilter_signals.py "$OUT" "$OUT" 3

# 5) Alert Telegram
python3 scripts/send_alerts.py \
  --token "${TELEGRAM_TOKEN:-}" \
  --chat-id "${TELEGRAM_CHAT_ID:-}"

# 6) Summary
if [ -s "$OUT" ]; then
  echo "[SUMMARY] Segnali per coppia (ultimi $LOOKBACK_DAYS gg):"
  python3 - <<'PY'
import pandas as pd
df = pd.read_csv("reports/strong_signals.csv")
print(df.groupby("pair")["action"].count().sort_values(ascending=False).to_string())
PY
else
  echo "[SUMMARY] Nessun segnale."
fi
echo "[OK] Daily alerts done."
