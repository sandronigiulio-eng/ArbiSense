#!/usr/bin/env python3
import pandas as pd
import matplotlib.pyplot as plt
import os

# Carica il report generato da calculate_spread.py
path = 'data_sample/spread_report.csv'
df = pd.read_csv(path, index_col=0, parse_dates=True)

# Se non esiste la colonna 'signal' o spread, esci con avviso
if 'spread_pct' not in df.columns:
    raise SystemExit("File senza colonna spread_pct. Esegui prima calculate_spread.py")

# Crea cartella per output grafici
os.makedirs('reports', exist_ok=True)

plt.figure(figsize=(10,5))
plt.plot(df.index, df['spread_pct'], label='Spread %', linewidth=1)
# Evidenzia gli ALERT
if 'signal' in df.columns:
    alerts = df[df['signal'] == 'ALERT']
    if not alerts.empty:
        plt.scatter(alerts.index, alerts['spread_pct'], color='red', label='ALERT', zorder=5)

plt.axhline(0, color='grey', linewidth=0.6)
plt.title('Spread % nel tempo')
plt.xlabel('Date')
plt.ylabel('Spread (%)')
plt.legend()
plt.tight_layout()
out = 'reports/spread_plot.png'
plt.savefig(out, dpi=150)
print(f"Saved plot to {out}")
#!/usr/bin/env python3
import pandas as pd
import matplotlib.pyplot as plt
import logging

# Configura logging
logging.basicConfig(
    format='%(asctime)s %(levelname)s: %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Percorso del report generato da calculate_spread.py
report_path = 'data_sample/spread_report.csv'

# Leggi il report
try:
    df = pd.read_csv(report_path, index_col=0, parse_dates=True)
except Exception as e:
    logging.error(f"Errore nel caricamento del report {report_path}: {e}")
    exit(1)

logging.info(f"Caricati {len(df)} record da {report_path}")

# Crea il grafico
plt.figure(figsize=(12, 6))
plt.plot(df.index, df['spread_pct'], label='Spread %', linewidth=1.5)

# Evidenzia segnali
if 'signal' in df.columns:
    alerts = df[df['signal'] == 'ALERT']
    strong = df[df['signal'] == 'STRONG_SIGNAL']

    if not alerts.empty:
        plt.scatter(alerts.index, alerts['spread_pct'], color='orange', label='ALERT', zorder=5)
    if not strong.empty:
        plt.scatter(strong.index, strong['spread_pct'], color='red', label='STRONG_SIGNAL', zorder=5)

plt.axhline(0, color='grey', linewidth=0.6)
plt.title('Spread % nel tempo con segnali')
plt.xlabel('Date')
plt.ylabel('Spread (%)')
plt.legend()
plt.tight_layout()

# Salva il grafico
out = 'reports/spread_plot.png'
plt.savefig(out, dpi=150)
logging.info(f"Saved plot to {out}")
plt.show()

