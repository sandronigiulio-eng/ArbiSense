import pandas as pd
from pathlib import Path

CACHE_DIR = Path("data_cache/fx")
CACHE_DIR.mkdir(parents=True, exist_ok=True)

def _cache_path(base, quote):
    return CACHE_DIR / f"fx_{base}_{quote}.csv"

def _load_cache(base, quote, start):
    fp = _cache_path(base, quote)
    if fp.exists():
        try:
            s = pd.read_csv(fp, parse_dates=["date"]).set_index("date")["FX"].to_frame()
            s.index = pd.to_datetime(s.index)
            s = s.loc[s.index >= pd.to_datetime(start)]
            if len(s):
                return s, f"CACHE:{fp.name}"
        except Exception:
            pass
    return None, None

def _save_cache(base, quote, s):
    if s is None or s.empty:
        return
    out = s.copy()
    out["date"] = out.index
    out = out[["date", "FX"]]
    out.to_csv(_cache_path(base, quote), index=False)

def _yf_try(pair, start):
    try:
        import yfinance as yf
        df = yf.download(pair, start=pd.to_datetime(start).date(), progress=False, auto_adjust=False, interval="1d", threads=False)
        if df is None or df.empty or "Adj Close" not in df:
            return None
        s = df["Adj Close"].rename("FX").to_frame()
        s.index = pd.to_datetime(s.index)
        s = s.sort_index().ffill()
        return s
    except Exception:
        return None

def _ecb_hist():
    # ECB storico (EUR base). Zip ufficiale.
    url = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist.zip"
    ecb = pd.read_csv(url, compression="zip")
    ecb = ecb.rename(columns={"Date": "date"})
    ecb["date"] = pd.to_datetime(ecb["date"])
    ecb = ecb.set_index("date").sort_index()
    # colonna valuta => quante UNITA' DI QUELLA VALUTA per 1 EUR (cioè EUR->CCY)
    return ecb

def _from_ecb(base, quote, start):
    base = base.upper(); quote = quote.upper()
    ecb = _ecb_hist()
    # Entrambe le valute devono essere (quando necessario) presenti tra le colonne ECB.
    # ECB ha sempre base EUR. Costruiamo:
    # EUR->X = ecb[X]
    # X->EUR = 1/ecb[X]
    # X->Y   = (EUR->Y)/(EUR->X) = ecb[Y]/ecb[X]
    if base == "EUR" and quote in ecb.columns:
        s = ecb[quote].rename("FX").to_frame()
    elif quote == "EUR" and base in ecb.columns:
        s = (1.0 / ecb[base]).rename("FX").to_frame()
    elif base in ecb.columns and quote in ecb.columns:
        s = (ecb[quote] / ecb[base]).rename("FX").to_frame()
    else:
        return None, "ECB_FAIL"

    s = s.sort_index().ffill()
    s = s.loc[s.index >= pd.to_datetime(start)]
    return s, "ECB"

def fx_series(base, quote, start):
    base = base.upper().strip()
    quote = quote.upper().strip()
    if base == quote:
        return None, "1.0"

    # Cache
    s, note = _load_cache(base, quote, start)
    if s is not None:
        return s, note

    # Yahoo diretto
    s = _yf_try(f"{base}{quote}=X", start)
    if s is not None:
        _save_cache(base, quote, s)
        return s.loc[s.index >= pd.to_datetime(start)], f"YF:{base}{quote}=X"

    # Yahoo inverso
    s_inv = _yf_try(f"{quote}{base}=X", start)
    if s_inv is not None:
        s = (1.0 / s_inv["FX"]).rename("FX").to_frame()
        s = s.sort_index().ffill()
        _save_cache(base, quote, s)
        return s.loc[s.index >= pd.to_datetime(start)], f"YF_INV:{quote}{base}=X"

    # ECB fallback
    s, tag = _from_ecb(base, quote, start)
    if s is not None and not s.empty:
        _save_cache(base, quote, s)
        return s, tag

    return None, f"FX_FAIL {base}{quote}"
