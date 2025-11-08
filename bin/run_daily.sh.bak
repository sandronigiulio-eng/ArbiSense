#!/usr/bin/env bash
# ArbiSense - daily runner
# Esegue: ingest EOD, export segnali, filtri regime/posizioni, cooldown, risk-cap, invio Telegram, summary.
# Usa variabili da .env con fallback sicuri.

# -------- Shell & repo setup --------
set -euo pipefail
[ -n "${ARBISENSE_DEBUG:-}" ] && set -x

# Repo root
SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

# -------- Load .env (repo-root) --------
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

# Fallback timezone e dirs se mancanti
export TZ="${TZ:-Europe/Rome}"
REPORTS_DIR="${REPORTS_DIR:-reports}"
DATA_LIVE_DIR="${DATA_LIVE_DIR:-data_live}"
DATA_SAMPLE_DIR="${DATA_SAMPLE_DIR:-data_sample}"

# Parametri con fallback
LOOKBACK_DAYS="${LOOKBACK_DAYS:-90}"
WF_ZVOL_MAX="${WF_ZVOL_MAX:-1.0}"
WF_ADF_MAX="${WF_ADF_MAX:-0.30}"
WF_REGIME_WIN="${WF_REGIME_WIN:-20}"
POSITION_GUARD_DAYS="${POSITION_GUARD_DAYS:-120}"
POST_COOLDOWN_DAYS="${POST_COOLDOWN_DAYS:-3}"
RISK_CAP_MAX_PER_PAIR="${RISK_CAP_MAX_PER_PAIR:-6}"
RISK_CAP_MAX_PER_DAY="${RISK_CAP_MAX_PER_DAY:-10}"
MAX_LOG_SIZE_MB="${MAX_LOG_SIZE_MB:-5}"

# -------- Log setup --------
mkdir -p "$REPORTS_DIR"
LOG="$REPORTS_DIR/daily.log"

# Rotazione log se >= MAX_LOG_SIZE_MB
if [ -f "$LOG" ]; then
  SZ_MB=$(( $(wc -c <"$LOG") / 1024 / 1024 ))
  if [ "$SZ_MB" -ge "$MAX_LOG_SIZE_MB" ]; then
    mv "$LOG" "$REPORTS_DIR/daily.log.$(date +%Y%m%d%H%M)"
    : > "$LOG"
    echo "[OK] Rotated daily.log" >> "$LOG"
  fi
fi

# Se in esecuzione interattiva (tty), duplica output anche su $LOG
if [ -t 1 ]; then
  exec > >(tee -a "$LOG") 2>&1
fi

echo "[START] run_daily.sh @ $(date) TZ=$TZ"

# -------- Weekday guard (Mon–Fri) --------
if [ -z "${ARBISENSE_FORCE:-}" ] && [ "$(date +%u)" -ge 6 ]; then
  echo "[SKIP] Weekend ($(date))"
  echo "[DONE] (weekend guard)"
  exit 0
fi

# -------- venv --------
if [ -d "venv" ]; then
  # shellcheck disable=SC1091
  . "venv/bin/activate"
fi

# -------- Step 0: Ingest EOD legs (non-fatal) --------
# Anche se fallisce qualche ticker, continuiamo la pipeline sui dati esistenti.
python3 scripts/ingest_today.py --cfg config/pairs_live.yaml --outdir "$DATA_LIVE_DIR" || echo "[WARN] ingest_today ha dato errori, continuo."

# File IO
INPUT="$DATA_SAMPLE_DIR/spread_report_all_pairs_long.normalized.csv"
OUT="$REPORTS_DIR/strong_signals.csv"
PAIR_QUALITY="$REPORTS_DIR/pair_quality.csv"

# -------- Step 1: Export segnali da presets --------
python3 scripts/export_from_presets.py \
  --input "$INPUT" \
  --presets "$REPORTS_DIR/presets.json" \
  --out "$OUT" \
  --lookback "$LOOKBACK_DAYS"

# -------- Step 2: Filtro regime (z-vol + ADF) --------
python3 scripts/filter_regime.py \
  --input "$OUT" \
  --out "$OUT" \
  --data "$INPUT" \
  --pair-quality "$PAIR_QUALITY" \
  --regime-zvol-max "$WF_ZVOL_MAX" \
  --regime-zvol-window "$WF_REGIME_WIN" \
  --regime-adf-max "$WF_ADF_MAX"

# -------- Step 3: Position guard (no EXIT orfani) --------
python3 scripts/position_guard.py "$OUT" "$OUT" "$POSITION_GUARD_DAYS"

# -------- Step 4: Cooldown --------
python3 scripts/postfilter_signals.py "$OUT" "$OUT" "$POST_COOLDOWN_DAYS"

# -------- Step 5: Risk cap (per day / per pair) --------
python3 scripts/postfilter_risk.py "$OUT" "$OUT" "$RISK_CAP_MAX_PER_DAY" "$RISK_CAP_MAX_PER_PAIR"

# -------- Step 6: Invio Telegram aggregato --------
python3 scripts/send_alerts_aggregate.py

# -------- Step 7: Summary --------
if [ -s "$OUT" ]; then
  echo "[SUMMARY] Segnali per coppia (ultimi ${LOOKBACK_DAYS} gg):"
  python3 - <<'PY'
import pandas as pd
df = pd.read_csv("reports/strong_signals.csv")
print(df.groupby("pair")["action"].count().sort_values(ascending=False).to_string())
PY
else
  echo "[SUMMARY] Nessun segnale."
fi

echo "[DONE] Daily alerts done. $(date)"

# -------- Step 8: Export pubblico (CSV/JSON) --------
python3 scripts/make_public_reports.py || echo "[WARN] public export failed"
# -------- Step 9: Health-check Telegram --------
if [ -n "${TELEGRAM_BOT_TOKEN:-}" ] && [ -n "${TELEGRAM_CHAT_ID:-}" ]; then
python3 - <<'PY'
import os, requests, pathlib

paths = ["reports/strong_signals.csv", "public/latest.json", "public/latest.csv"]
missing = [p for p in paths if not pathlib.Path(p).exists() or pathlib.Path(p).stat().st_size == 0]
ok = len(missing) == 0

msg = "✅ ArbiSense OK" if ok else "⚠️ ArbiSense con avvisi"
if missing:
    msg += "\nMancano/sono vuoti:\n- " + "\n- ".join(missing)

token = os.environ.get("TELEGRAM_BOT_TOKEN")
chat  = os.environ.get("TELEGRAM_CHAT_ID")

try:
    r = requests.post(
        f"https://api.telegram.org/bot{token}/sendMessage",
        data={"chat_id": chat, "text": msg},
        timeout=10,
    )
    print("[HEALTH] telegram", r.status_code)
except Exception as e:
    print("[HEALTH] telegram error:", e)
PY
fi
# --- Fail-alerts: notifica su Telegram se la run fallisce ---
set -E

arbisense_fail_alert() {
  local tail_lines=60
  local body
  if [ -f "$LOG" ]; then
    body="$(tail -n "$tail_lines" "$LOG")"
  else
    body="(nessun log trovato)"
  fi

  if [ -n "${TELEGRAM_BOT_TOKEN:-}" ] && [ -n "${TELEGRAM_CHAT_ID:-}" ]; then
    ARBISENSE_FAIL_MSG="❌ ArbiSense FAILED ($(date +'%Y-%m-%d %H:%M %Z')) su $(hostname)"
    ARBISENSE_FAIL_BODY="$body" python3 - <<'PY'
import os, requests, textwrap
token = os.getenv("TELEGRAM_BOT_TOKEN")
chat  = os.getenv("TELEGRAM_CHAT_ID")
msg   = os.getenv("ARBISENSE_FAIL_MSG","ArbiSense FAILED")
body  = os.getenv("ARBISENSE_FAIL_BODY","")
text  = f"{msg}\n\n```\n{body[:3500]}\n```"
try:
    r = requests.post(
        f"https://api.telegram.org/bot{token}/sendMessage",
        data={"chat_id": chat, "text": text, "parse_mode": "Markdown"},
        timeout=10,
    )
    print("[ALERT] fail telegram", r.status_code)
except Exception as e:
    print("[ALERT] telegram error:", e)
PY
  fi
}

trap 'arbisense_fail_alert' ERR
# --- fine fail-alerts ---

# --- FAIL alert su errori (trap) ---
fail_alert() {
  local line="$1"
  if [ -n "${TELEGRAM_BOT_TOKEN:-}" ] && [ -n "${TELEGRAM_CHAT_ID:-}" ]; then
    curl -s -X POST "https://api.telegram.org/bot$TELEGRAM_BOT_TOKEN/sendMessage" \
      -d chat_id="$TELEGRAM_CHAT_ID" \
      -d text="❌ ArbiSense FAILED in run_daily.sh (line $line)" >/dev/null || true
  fi
}
trap 'fail_alert "$LINENO"; exit 1' ERR
# --- end FAIL alert ---

# --- Test di errore iniettato (facoltativo) ---
if [ -n "${ARBISENSE_INJECT_FAIL:-}" ]; then
  echo "[TEST] ARBISENSE_INJECT_FAIL set -> forcing error"
  false
fi
# --- end test injection ---

# --- Auto-publish to GitHub Pages (gh-pages via ghp-import) ---
# Requisiti: repo git con 'origin' configurato e gh-pages attivo come sorgente Pages.
# Disattiva con ARBISENSE_SKIP_PUBLISH=1
if [ "${ARBISENSE_SKIP_PUBLISH:-0}" != "1" ]; then
  if [ -d "public" ] && [ -s "public/latest.json" ]; then
    if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
      if ! command -v ghp-import >/dev/null 2>&1; then
        echo "[PUBLISH] installing ghp-import..."
        python3 -m pip install -q ghp-import || true
      fi
      if command -v ghp-import >/dev/null 2>&1; then
        echo "[PUBLISH] pushing ./public to gh-pages…"
        ghp-import -n -p -f -m "pages: $(date -u +%FT%TZ)" public
        echo "[PUBLISH] done."
      else
        echo "[WARN] ghp-import non disponibile; salto publish."
      fi
    else
      echo "[WARN] Non è una repo git; salto publish."
    fi
  else
    echo "[WARN] public/ mancante o vuoto; salto publish."
  fi
fi
# --- end auto-publish ---
# Silenzia warning LibreSSL di urllib3 in tutti i sotto-processi Python
export PYTHONWARNINGS="${PYTHONWARNINGS:-ignore}"

