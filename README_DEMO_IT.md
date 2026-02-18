# ArbiSense — Demo (audit-grade)

Questa demo NON esegue trade reali.
È un “assistente operativo simulato” che trasforma segnali/strategie in:
- piano operativo,
- eseguiti simulati (con costi),
- P&L e curva equity,
- traccia completa (runs/<RUN_ID>, metadata, ledger).

## File principali (public/)
- latest.html / latest.json / latest.csv
  Output “utente”: segnali/azioni filtrati (non live).
- ops_plan.csv
  Piano operativo del giorno (cosa fare e perché).
- fills_simulated.csv
  Eseguiti simulati per le legs (con slippage/commissioni).
- mtm_daily.csv, mtm_by_fill.csv
  Mark-to-market e dettaglio per fill.
- pnl_daily.csv
  P&L del giorno (include costi).
- position_state.csv
  Stato posizione dopo le operazioni (flat/long/short).
- equity_curve.csv (+ equity_curve.png)
  Curva equity cumulata e drawdown (su più date se si usa demo_wow_curve).
- strategy_demo.yaml (+ sha256)
  Snapshot della strategia usata nella run (auditabilità).
- metadata.json
  Commit/config hash + dettagli run.
