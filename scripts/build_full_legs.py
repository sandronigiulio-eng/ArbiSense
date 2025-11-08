import argparse, pandas as pd
from pathlib import Path
import yfinance as yf
from datetime import date

def _to_utc_naive_index(df: pd.DataFrame) -> pd.DataFrame:
    idx = df.index
    try:
        if getattr(idx, "tz", None) is not None:
            df.index = idx.tz_convert("UTC").tz_localize(None)
    except Exception:
        try:
            df.index = pd.DatetimeIndex(idx).tz_localize(None)
        except Exception:
            pass
    return df

def _daily_last(df: pd.DataFrame, col: str) -> pd.DataFrame:
    # porta a indice per "data" (YYYY-MM-DD) prendendo l'ultimo valore del giorno
    s = df[col].copy()
    s.index = pd.to_datetime(s.index).date
    s = s.groupby(s.index).last()      # ultimo close del giorno
    s.index = pd.Index([d.isoformat() for d in s.index], name="date")
    return s.to_frame()

def get_hist(ticker: str, start: str):
    df = yf.Ticker(ticker).history(period="max", auto_adjust=False)
    if df.empty:
        return None, None
    df = _to_utc_naive_index(df)
    df = df.loc[df.index >= pd.to_datetime(start)]
    cur = yf.Ticker(ticker).fast_info.get("currency", None)
    df = _daily_last(df, "Close")
    df.columns = [ticker]
    return df, cur

def fx_series(fc: str, tc: str, start: str):
    if fc is None or tc is None:
        return None, "FX_FAIL missing cur"
    fc, tc = str(fc).upper(), str(tc).upper()
    scale = 1.0
    notes = []
    if fc in {"GBX", "GBPP", "GBPp", "GBp", "GBx"}:
        fc = "GBP"
        scale *= 0.01
        notes.append("GBX->GBP 0.01")
    if fc == tc:
        return None, "1.0"  # stessa valuta: non serve serie FX

    sym = f"{fc}{tc}=X"
    df = yf.Ticker(sym).history(period="max")
    if df.empty:
        inv = f"{tc}{fc}=X"
        d2 = yf.Ticker(inv).history(period="max")
        if d2.empty:
            return None, f"FX_FAIL {sym} & {inv}"
        fx = (1.0 / d2["Close"]).to_frame("FX")
        notes.append(f"1/({inv})")
    else:
        fx = df[["Close"]].rename(columns={"Close": "FX"})
        notes.append(f"FX {sym}")

    fx = _to_utc_naive_index(fx)
    fx = fx.loc[fx.index >= pd.to_datetime(start)].dropna()
    fx["FX"] = fx["FX"] * scale
    fx = _daily_last(fx, "FX")
    return fx["FX"], "; ".join(notes)

def build(pair: str, a: str, b: str, start: str, denom: str):
    ah, cura = get_hist(a, start)
    bh, curb = get_hist(b, start)
    if ah is None or bh is None:
        raise SystemExit(f"[ERR] history missing ({a} or {b})")

    # join per "data" (inner) così abbiamo solo i giorni comuni
    df = ah.join(bh, how="inner")
    fx_note = "1.0"

    if denom.upper() == "B":
        if cura != curb:
            fx, fx_note = fx_series(cura, curb, start)
            if fx is None and fx_note.startswith("FX_FAIL"):
                raise SystemExit(f"[ERR] FX missing {cura}->{curb} ({fx_note})")
            if fx is not None:
                df = df.join(fx, how="left")
                df[a] = df[a] * df["FX"]
    else:  # denom = A
        if curb != cura:
            fx, fx_note = fx_series(curb, cura, start)
            if fx is None and fx_note.startswith("FX_FAIL"):
                raise SystemExit(f"[ERR] FX missing {curb}->{cura} ({fx_note})")
            if fx is not None:
                df = df.join(fx, how="left")
                df[b] = df[b] * df["FX"]

    out = df.rename(columns={a: "A_price", b: "B_price"}).copy()
    out = out.dropna(subset=["A_price", "B_price"])

    out["date"] = out.index
    out["pair"] = pair
    out["A_ticker"] = a
    out["B_ticker"] = b
    out["fx_used"] = fx_note
    out["curA"] = cura
    out["curB"] = curb
    out["denom"] = denom.upper()

    out = out[["date","pair","A_ticker","A_price","B_ticker","B_price","fx_used","curA","curB","denom"]]

    Path("data_sample").mkdir(parents=True, exist_ok=True)
    fp = Path(f"data_sample/legs_{pair}.full.csv")
    out.to_csv(fp, index=False)
    print(f"[WROTE] {fp} rows={len(out)} fx_note={fx_note}")

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--pair", required=True)
    p.add_argument("--a_yh", required=True)
    p.add_argument("--b_yh", required=True)
    p.add_argument("--start", default="2014-01-01")
    p.add_argument("--denom", default="B", choices=["A", "B"])
    args = p.parse_args()
    build(args.pair, args.a_yh, args.b_yh, args.start, args.denom)

# --- FX override (fallback Yahoo -> ECB + cache) ---
try:
    from scripts.utils_fx import fx_series
except Exception:
    from utils_fx import fx_series
# --- end override ---
try:
    from scripts.utils_fx import fx_series
except Exception:
    from utils_fx import fx_series
