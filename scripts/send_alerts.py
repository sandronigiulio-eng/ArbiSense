#!/usr/bin/env python3
import pandas as pd
from pathlib import Path
import smtplib
from email.message import EmailMessage

# --- Config ---
STRONG_SIGNALS_FILE = Path("reports/strong_signals.csv")
NOTIFIED_FILE = Path("reports/notified_signals.csv")

EMAIL_FROM = "tuo@email.com"
EMAIL_TO = "destinatario@email.com"
SMTP_SERVER = "smtp.tuo-provider.com"
SMTP_PORT = 587
SMTP_USER = "tuo@email.com"
SMTP_PASSWORD = "password"

# --- Leggi segnali forti ---
df = pd.read_csv(STRONG_SIGNALS_FILE)
df['date'] = pd.to_datetime(df['date'])

# --- Leggi segnali già notificati ---
if NOTIFIED_FILE.exists():
    notified = pd.read_csv(NOTIFIED_FILE)
    notified['date'] = pd.to_datetime(notified['date'])
else:
    notified = pd.DataFrame(columns=['pair', 'date'])

# --- Filtra nuovi segnali ---
new_signals = df.merge(notified, on=['pair','date'], how='outer', indicator=True)
new_signals = new_signals[new_signals['_merge'] == 'left_only'][['pair','date']]

if not new_signals.empty:
    # --- Invia email ---
    msg = EmailMessage()
    msg['Subject'] = f"Nuovi strong signals ArbiSense ({len(new_signals)})"
    msg['From'] = EMAIL_FROM
    msg['To'] = EMAIL_TO
    body = "\n".join([f"{row['pair']}: {row['date'].date()}" for idx,row in new_signals.iterrows()])
    msg.set_content(body)

    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.starttls()
        server.login(SMTP_USER, SMTP_PASSWORD)
        server.send_message(msg)
    print(f"Inviata email per {len(new_signals)} nuovi segnali.")

    # --- Aggiorna storico notifiche ---
    updated_notified = pd.concat([notified, new_signals], ignore_index=True)
    updated_notified.to_csv(NOTIFIED_FILE, index=False)
else:
    print("Nessun nuovo strong signal oggi.")

