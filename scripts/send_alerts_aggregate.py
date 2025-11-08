#!/usr/bin/env python3
import os, sys, json, requests
from pathlib import Path
from datetime import datetime
try:
    import pandas as pd
except Exception as e:
    print("[WARN] pandas non disponibile:", e)

REPO_ROOT = Path(__file__).resolve().parents[1]
ENV_FILE = REPO_ROOT / ".env"

def load_env_file():
    # Fallback: carica .env se non già in ambiente
    if ENV_FILE.exists():
        for line in ENV_FILE.read_text().splitlines():
            line=line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k,v = line.split("=",1)
            v = v.strip().strip('"').strip("'")
            os.environ.setdefault(k.strip(), v)

def build_message():
    reports = REPO_ROOT / "reports"
    csv_path = reports / "strong_signals.csv"
    tz = os.getenv("TZ","UTC")
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    header = f"ArbiSense • Daily signals ({now} {tz})"

    if not csv_path.exists() or csv_path.stat().st_size == 0:
        return header + "\nNessun segnale oggi."

    try:
        import pandas as pd
        df = pd.read_csv(csv_path)
        if df.empty:
            return header + "\nNessun segnale oggi."
        # Riepilogo per coppia
        counts = df.groupby("pair")["action"].count().sort_values(ascending=False)
        lines = [header, "Segnali per coppia (ultimi 90 gg):"]
        for pair, cnt in counts.items():
            lines.append(f"• {pair}: {cnt}")
        return "\n".join(lines)
    except Exception as e:
        return header + f"\n[WARN] Impossibile leggere il CSV ({e})"

def send(text):
    token = os.getenv("TELEGRAM_BOT_TOKEN","").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID","").strip()
    if not token or not chat_id or "PUT_YOUR" in token:
        print("[WARN] Telegram disabilitato: token/chat_id mancanti o placeholder")
        return 0

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        r = requests.post(url, data={"chat_id": chat_id, "text": text}, timeout=20)
        print(f"[OK] Aggregate telegram sent ({r.status_code})")
        if r.status_code != 200:
            # diagnostica utile se capitano 401/400
            try:
                print("[DEBUG] Response JSON:", json.dumps(r.json(), ensure_ascii=False)[:800])
            except Exception:
                print("[DEBUG] Response TEXT:", r.text[:800])
        return r.status_code
    except requests.RequestException as e:
        print("[ERR] Telegram request failed:", e)
        return -1

if __name__ == "__main__":
    # Carica .env (fallback) e invia
    load_env_file()
    msg = build_message()
    code = send(msg)
    sys.exit(0 if code==200 else 1)
