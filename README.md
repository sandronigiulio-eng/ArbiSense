# ArbiSense - MVP

Breve: piattaforma che segnala disallineamenti di prezzo tra ETF/bond per wealth manager e family office.

## Requisiti
Python 3.8+
Attivare virtualenv: source venv/bin/activate
Installare dipendenze: pip install -r requirements.txt

## Esempio rapido
1. python scripts/fetch_prices.py SPY IVV
2. python scripts/calculate_spread.py data_sample/SPY.csv data_sample/IVV.csv
