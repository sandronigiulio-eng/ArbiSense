# ArbiSense — MVP Signals Runner

ArbiSense genera segnali mean-reversion su coppie ETF/indici, applica filtri di regime/rischio, invia alert **aggregati su Telegram** e pubblica un export leggero in `public/`.

## Quick start

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
# Apri .env e inserisci:
# TELEGRAM_BOT_TOKEN=...
# TELEGRAM_CHAT_ID=...

# Test Telegram (zsh):
setopt allexport; source .env; unsetopt allexport
curl -s "https://api.telegram.org/bot$TELEGRAM_BOT_TOKEN/getMe" | jq .ok   # deve stampare true

# Prima run forzata (anche nel weekend):
ARBISENSE_FORCE=1 ./bin/run_daily.sh
# Risultati: Telegram ✅, file in reports/ e public/

md

## 📊 Snapshot risultati (paper)
Vedi gli ultimi artefatti in `docs/samples/` (CSV e, se presente, grafico del PnL cumulato).

