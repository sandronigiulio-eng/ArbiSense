#!/usr/bin/env python3
import os, sys, re, math, json, time
from datetime import datetime
import requests
import pandas as pd

CSV_PATH = "reports/strong_signals.csv"

def env(v): 
    return (os.getenv(v) or "").strip()

TOKEN = env("TELEGRAM_TOKEN")
CHAT  = env("TELEGRAM_CHAT_ID")
API   = f"https://api.telegram.org/bot{TOKEN}"

def fail(msg, code=1):
    print(f"[ERROR] {msg}", file=sys.stderr); sys.exit(code)

def preflight():
    if not TOKEN or not re.match(r'^\d{7,12}:[A-Za-z0-9_-]{35,}$', TOKEN):
        fail("TELEGRAM_TOKEN mancante o non valido.")
    if not CHAT or not re.match(r'^-?\d{5,}$', CHAT):
        fail("TELEGRAM_CHAT_ID mancante o non numerico.")
    try:
        r = requests.get(f"{API}/getMe", timeout=10)
        r.raise_for_status()
        if not r.json().get("ok"):
            fail(f"getMe ok:false: {r.text}")
    except requests.RequestException as e:
        fail(f"Check token/getMe fallito: {e}")

def load_signals(path=CSV_PATH):
    if not os.path.exists(path):
        return pd.DataFrame()
    df = pd.read_csv(path)
    # Normalizza nomi colonne più comuni
    df.columns = [c.strip().lower() for c in df.columns]
    # Aspettati almeno pair + date
    if "pair" not in df.columns or "date" not in df.columns:
        return pd.DataFrame()
    # Converte date in YYYY-MM-DD
    try:
        df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    except Exception:
        pass
    # Z-score/Spread opzionali
    for col in ["zscore", "z", "spread", "delta", "signal_value"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df

def fmt_row(row):
    base = f"- {row.get('pair','?')} {row.get('date','?')} →"
    # priorità: zscore, poi spread/delta/signal_value
    if not math.isnan(row.get("zscore", float("nan"))):
        return f"{base} z={row['zscore']:.2f}"
    if not math.isnan(row.get("z", float("nan"))):
        return f"{base} z={row['z']:.2f}"
    for alt in ["spread","delta","signal_value"]:
        val = row.get(alt, None)
        if val is not None and not (isinstance(val, float) and math.isnan(val)):
            try:
                return f"{base} {alt}={float(val):.4f}"
            except Exception:
                return f"{base} {alt}={val}"
    # se nulla disponibile, lascia freccia senza valore
    return base + " (valore n/d)"

def chunk_text(lines, header, max_len=3500):
    """
    Telegram max 4096, restiamo larghi. Costruiamo più messaggi se necessario.
    """
    out, cur = [], header
    for ln in lines:
        add = ("\n" if cur else "") + ln
        if len(cur) + len(add) > max_len:
            out.append(cur)
            cur = header + "\n" + ln
        else:
            cur = cur + add if cur else ln
    if cur:
        out.append(cur)
    return out

def send_message(text):
    try:
        r = requests.post(f"{API}/sendMessage", data={
            "chat_id": CHAT,
            "text": text
        }, timeout=15)
        r.raise_for_status()
        ok = r.json().get("ok", False)
        if not ok:
            print(f"[WARN] sendMessage ok:false → {r.text}", file=sys.stderr)
        return ok
    except requests.RequestException as e:
        print(f"[WARN] sendMessage errore rete → {e}", file=sys.stderr)
        return False

def main():
    preflight()
    df = load_signals()
    if df.empty or len(df) == 0:
        send_message("ArbiSense — 0 strong signal(s)")
        print("[INFO] Nessun segnale da inviare.")
        return

    # Ordina per data desc, poi pair
    if "date" in df.columns:
        try:
            df["_d"] = pd.to_datetime(df["date"])
            df = df.sort_values(by=["_d","pair"], ascending=[False, True]).drop(columns=["_d"])
        except Exception:
            df = df.sort_values(by=["pair"])

    # Riformatta pair come mostrato negli esempi (evita spazi)
    df["pair"] = df["pair"].astype(str).str.replace(r"\s+", "_", regex=True)

    lines = [fmt_row(row) for _, row in df.iterrows()]
    header = f"ArbiSense — {len(lines)} strong signal(s)"
    messages = chunk_text(lines, header)

    for i, msg in enumerate(messages, 1):
        ok = send_message(msg)
        print(f"[INFO] Sent chunk {i}/{len(messages)}: {ok}")
        # breve sleep per rate limit
        time.sleep(0.2)

if __name__ == "__main__":
    main()

