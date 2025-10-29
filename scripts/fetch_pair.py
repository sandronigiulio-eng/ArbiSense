import argparse, os, numpy as np, pandas as pd
from datetime import datetime
try:
    import yfinance as yf
except ImportError:
    raise SystemExit("Installare yfinance (pip install yfinance)")

ap=argparse.ArgumentParser()
ap.add_argument("--pair", required=True, help="nome coppia es. VUAA_L_VUSA_L")
ap.add_argument("--a", required=True, help="ticker A (yfinance)")
ap.add_argument("--b", required=True, help="ticker B (yfinance)")
ap.add_argument("--start", default="2018-01-01")
ap.add_argument("--end",   default=None)
ap.add_argument("--out", default="data_sample/spread_pair.csv")
ap.add_argument("--append-to", default="data_sample/spread_report_all_pairs_long.normalized.csv")
ap.add_argument("--auto-adjust", type=int, default=1, help="1=usa prezzi Close aggiustati; 0=usa 'Adj Close'")
args=ap.parse_args()

auto_adjust = bool(int(args.auto_adjust))

print(f"[INFO] Download {args.a}, {args.b}… (auto_adjust={auto_adjust})")
df = yf.download([args.a, args.b], start=args.start, end=args.end, auto_adjust=auto_adjust, progress=False)

if df is None or len(df)==0:
    raise SystemExit("Download vuoto: controlla i ticker o la rete.")

def extract_prices(frame: pd.DataFrame) -> pd.DataFrame:
    # Restituisce DataFrame con colonne ['A','B'] di prezzi
    if isinstance(frame.columns, pd.MultiIndex):
        lvl0 = set(frame.columns.get_level_values(0))
        # Se auto_adjust=True, 'Adj Close' non c'è: usa 'Close'
        price_key = "Adj Close" if ("Adj Close" in lvl0 and not auto_adjust) else ("Close" if "Close" in lvl0 else None)
        if price_key is None:
            raise SystemExit(f"Colonna prezzi non trovata (presenti: {sorted(lvl0)}).")
        sub = frame[price_key]
        # assicura che i due ticker ci siano
        missing=[]
        for tk in [args.a, args.b]:
            if tk not in sub.columns:
                missing.append(tk)
        if missing:
            raise SystemExit(f"Mancano colonne per: {missing}. Tickers errati?")
        px = sub[[args.a, args.b]].copy()
        px.columns = ["A","B"]
        return px
    else:
        # SingleIndex: prova a prendere 'Close' o 'Adj Close' come singola serie: NON supportiamo 2 ticker in single-index
        raise SystemExit("Download inatteso (SingleIndex) per 2 tickers. Riprova senza auto_adjust o con tickers diversi.")

px = extract_prices(df).dropna().copy()
if px.empty:
    raise SystemExit("Prezzi vuoti dopo dropna (storico insufficiente?)")

# OLS A = alpha + beta * B
A = px["A"].values
B = px["B"].values
varB = np.var(B, ddof=0)
beta = (np.cov(A, B, ddof=0)[0,1] / varB) if varB>0 else 0.0
alpha = float(A.mean() - beta * B.mean())
spread = A - (alpha + beta*B)

med = np.median(np.abs(spread))
scale = (1.0/med) if med>0 else 1e-6

out = pd.DataFrame({
    "date": pd.to_datetime(px.index).tz_localize("UTC", nonexistent="shift_forward", ambiguous="NaT"),
    "pair": args.pair,
    "spread_raw": spread,
    "spread_scale": scale
})

os.makedirs(os.path.dirname(args.out), exist_ok=True)
out.to_csv(args.out, index=False)
print(f"[WROTE] {args.out} rows={len(out)}  scale={scale:.6g}  beta={beta:.6g}  alpha={alpha:.6g}")

# append al master normalized (se esiste)
if args.append_to:
    os.makedirs(os.path.dirname(args.append_to), exist_ok=True)
    if os.path.exists(args.append_to):
        base = pd.read_csv(args.append_to)
        base["date"] = pd.to_datetime(base["date"], utc=True, errors="coerce")
        merged = pd.concat([base, out], ignore_index=True)
        merged = merged.drop_duplicates(subset=["date","pair"]).sort_values("date")
        merged.to_csv(args.append_to, index=False)
        print(f"[APPENDED] {args.append_to} → total rows={len(merged)}")
    else:
        out.to_csv(args.append_to, index=False)
        print(f"[INIT] {args.append_to}")
