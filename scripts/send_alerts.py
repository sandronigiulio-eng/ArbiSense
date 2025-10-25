import os
import json
import requests
from datetime import datetime

def send_telegram_message(token, chat_id, message):
    """Invia un messaggio Telegram tramite il bot token"""
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": message, "parse_mode": "Markdown"}
    response = requests.post(url, data=payload)
    response.raise_for_status()

def main():
    token = os.getenv("TELEGRAM_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    if not token or not chat_id:
        print("⚠️ Telegram credentials missing. Skipping alert.")
        return

    signals_path = "data_sample/strong_signals.json"
    if not os.path.exists(signals_path):
        print("⚠️ No strong_signals.json found.")
        return

    with open(signals_path, "r") as f:
        signals = json.load(f)

    if not signals:
        print("ℹ️ Nessun segnale forte trovato.")
        return

    message_lines = [
        f"📊 *ArbiSense — Segnali del {datetime.utcnow().strftime('%d/%m/%Y %H:%M UTC')}*",
        ""
    ]

    for s in signals:
        message_lines.append(
            f"- {s['pair']} → spread: {s['spread']:.4f}, z-score: {s['zscore']:.2f}"
        )

    message = "\n".join(message_lines)
    send_telegram_message(token, chat_id, message)
    print("✅ Alert Telegram inviato con successo!")

if __name__ == "__main__":
    main()
# (qui incolla tutto lo script Python che ti ho dato)
