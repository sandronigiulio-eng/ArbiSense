#!/usr/bin/env python3
import os, sys, json, argparse
from datetime import datetime
import pandas as pd, pytz, requests

def send(text):
    tok=os.getenv('TELEGRAM_BOT_TOKEN'); chat=os.getenv('TELEGRAM_CHAT_ID')
    print(text)  # sempre a console
    if not tok or not chat: return
    try:
        r = requests.post(f"https://api.telegram.org/bot{tok}/sendMessage",
                          json={"chat_id": chat, "text": text,
                                "parse_mode": "HTML", "disable_web_page_preview": True}, timeout=15)
        print("[TELEGRAM]", r.status_code, r.text if r.status_code>=300 else "OK")
    except Exception as e:
        print("[TELEGRAM] errore:", e)

def load_presets(path):
    try:
        data=json.load(open(path))
        # supporta sia {pair: {...}} sia [ {...,"pair":...}, ... ]
        if isinstance(data, dict):
            return data
        d={}
        for row in data:
            if isinstance(row, dict) and "pair" in row:
                d[row["pair"]]=row
        return d
    except Exception:
        return {}

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--reports", default="reports")
    ap.add_argument("--presets", default="reports/presets.json")
    ap.add_argument("--tz", default=os.getenv("TZ","Europe/Rome"))
    args=ap.parse_args()

    tz=pytz.timezone(args.tz)
    today=datetime.now(tz).strftime("%Y-%m-%d")

    csv=os.path.join(args.reports, "strong_signals.csv")
    if not os.path.exists(csv):
        send(f"ArbiSense — {today}\nNessun segnale (file assente).")
        return

    try:
        df=pd.read_csv(csv)
    except Exception:
        send(f"ArbiSense — {today}\nNessun segnale (CSV vuoto/illeggibile).")
        return

    if df.empty:
        send(f"ArbiSense — {today}\nNessun segnale.")
        return

    presets=load_presets(args.presets)
    cap_day=int(os.getenv("RISK_CAP_PER_DAY", "10"))

    # calcola open/close per coppia
    df["is_enter"]=df["action"].astype(str).str.startswith("ENTER")
    df["is_exit"]=df["action"].astype(str).str.startswith("EXIT")
    lines=[f"ArbiSense — {today}", f"Segnali totali: {len(df)} (cap giorno {cap_day})"]

    for pair, grp in df.groupby("pair", sort=False):
        open_n=int(grp["is_enter"].sum())
        close_n=int(grp["is_exit"].sum())
        p=presets.get(pair, {})
        zenter=p.get("z_enter","-"); zexit=p.get("z_exit","-"); zstop=p.get("z_stop","-"); mh=p.get("max_hold","-")
        lines.append(f"• {pair}: open={open_n} close={close_n} | z_enter={zenter} z_exit={zexit} stop={zstop} | max_hold={mh}d")

    lines.append("Warning: nessuna")
    lines.append("Artefatti: gh-pages latest.csv/json")
    send("\n".join(lines))

if __name__ == "__main__":
    main()
