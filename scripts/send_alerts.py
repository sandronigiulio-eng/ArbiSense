#!/usr/bin/env python3
import os, sys, argparse, json, pandas as pd, requests, hashlib

STATE_FILE="reports/alerts_state.json"

def load_env_fallback(env_path="reports/telegram.env"):
    if not os.path.exists(env_path): return {}
    kv={}
    for line in open(env_path,"r",encoding="utf-8"):
        line=line.strip()
        if not line or line.startswith("#") or "=" not in line: continue
        k,v=line.split("=",1); kv[k.strip()]=v.strip()
    return kv

def get_creds(args):
    token = args.token or os.environ.get("TELEGRAM_TOKEN")
    chat  = args.chat_id or os.environ.get("TELEGRAM_CHAT_ID")
    if (not token) or (not chat):
        kv=load_env_fallback()
        token = token or kv.get("TELEGRAM_TOKEN")
        chat  = chat  or kv.get("TELEGRAM_CHAT_ID")
    return token, chat

def send_message(token, chat_id, text):
    url=f"https://api.telegram.org/bot{token}/sendMessage"
    r=requests.post(url, json={"chat_id": chat_id, "text": text}, timeout=20)
    r.raise_for_status(); return r.json()

def load_state():
    if os.path.exists(STATE_FILE):
        try: return json.load(open(STATE_FILE))
        except: return {}
    return {}

def save_state(s): os.makedirs("reports", exist_ok=True); json.dump(s, open(STATE_FILE,"w"), indent=2)

def main():
    ap=argparse.ArgumentParser("ArbiSense Telegram alerts")
    ap.add_argument("--signals", default="reports/strong_signals.csv")
    ap.add_argument("--token", default=None)
    ap.add_argument("--chat-id", default=None)
    args=ap.parse_args()

    token, chat_id=get_creds(args)
    if not token or not chat_id:
        print("[ERROR] TELEGRAM_TOKEN o CHAT_ID mancanti (CLI/ENV/.env).", file=sys.stderr); sys.exit(1)

    # valida token
    gm=requests.get(f"https://api.telegram.org/bot{token}/getMe", timeout=10)
    gm.raise_for_status()
    if not gm.json().get("ok", False):
        print("[ERROR] Token non valido.", file=sys.stderr); sys.exit(1)

    if not os.path.exists(args.signals) or os.path.getsize(args.signals)==0:
        print("[INFO] Nessun segnale da inviare."); return
    t=pd.read_csv(args.signals)
    if t.empty:
        print("[INFO] Nessun segnale da inviare."); return

    state=load_state()
    sent=0
    for _,r in t.iterrows():
        key=f"{r.get('timestamp','')}-{r.get('pair','')}-{r.get('action','')}"
        if state.get(key): 
            continue  # già inviato
        msg=(f"[ArbiSense] {r.get('action','SIGNAL')} — {r.get('pair','?')}\n"
             f"z={r.get('z','?')} (enter={r.get('z_enter','?')}, exit={r.get('z_exit','?')})\n"
             f"ts={r.get('timestamp','')}")
        try:
            send_message(token, chat_id, msg); state[key]=True; sent+=1
        except Exception as e:
            print("[ERROR] Invio fallito:", e, file=sys.stderr)
    save_state(state)
    print(f"[OK] Inviati {sent} nuovi segnali.")
