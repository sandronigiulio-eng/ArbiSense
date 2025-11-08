import argparse, pandas as pd, numpy as np, yfinance as yf, math
from pathlib import Path

# --- set iniziale di cross-listing/provideri noti e liquidi (tutti UCITS) ---
DEFAULT_PAIRS = [
    # S&P 500 (no CSP1)
    ("VUSA.L","IUSA.DE"),
    ("VUAA.L","IUSA.DE"),
    # MSCI World (provider diversi)
    ("SWRD.L","XDWD.DE"),
    ("IWDA.AS","XDWD.DE"),
    ("SWRD.L","SXR8.DE"),
    ("IWDA.AS","SWRD.L"),
    # Emerging Markets
    ("EIMI.L","IS3N.DE"),
    ("VFEM.L","IS3N.DE"),
    # Europe (Core MSCI Europe)
    ("IMEU.L","EUN2.DE"),
    # ACWI
    ("SSAC.L","IUSQ.DE"),
]

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--win", type=int, default=40, help="rolling window per z-score")
    ap.add_argument("--z_enter", type=float, default=2.2, help="soglia ENTER")
    ap.add_argument("--period", default="5y", help="storico Yahoo (es. 3y, 5y, 10y, max)")
    ap.add_argument("--zvol_max", type=float, default=1.0, help="max zvol p75 per scan");
    ap.add_argument("--topk", type=int, default=12)
    ap.add_argument("--outfile", default="reports/candidate_scan_live.csv")
    ap.add_argument("--pairs_file", default="", help="CSV opzionale con colonne A_yh,B_yh (sovrascrive DEFAULT)")
    return ap.parse_args()

def norm_ccy(c):
    if not c: return None
    u = c.upper()
    return "GBP" if u=="GBX" else u

def gbx_factor(c):
    return 0.01 if c and c.upper()=="GBX" else 1.0

def last_hist(ticker, period):
    try:
        t = yf.Ticker(ticker)
        h = t.history(period=period)["Close"].dropna()
        if h.empty: return None, None
        cur = t.fast_info.get("currency", None)
        return h, cur
    except Exception:
        return None, None

def to_pair_name(a,b):
    base = lambda x: x.split(".")[0]
    suf  = lambda x: x.split(".")[1] if "." in x else ""
    return f"{base(a)}_{suf(a)}_{base(b)}_{suf(b)}"

def load_pairs(pairs_file):
    if not pairs_file: return DEFAULT_PAIRS
    df = pd.read_csv(pairs_file)
    assert {"A_yh","B_yh"} <= set(df.columns)
    return [(r.A_yh, r.B_yh) for _, r in df.iterrows()]

def main():
    args = parse_args()
    pairs = load_pairs(args.pairs_file)

    rows=[]
    for A,B in pairs:
        hA,cA = last_hist(A, args.period)
        hB,cB = last_hist(B, args.period)
        if hA is None or hB is None: 
            print(f"[SKIP] no data: {A} / {B}"); 
            continue

        cA, cB = norm_ccy(cA), norm_ccy(cB)
        if not cA or not cB:
            print(f"[SKIP] no currency: {A}={cA} / {B}={cB}")
            continue

        # GBp -> GBP
        hA = hA * gbx_factor(cA)
        hB = hB * gbx_factor(cB)

        # FX A->B se diverso
        if cA != cB:
            try:
                fx = yf.Ticker(f"{cA}{cB}=X").next((yf.Ticker(tk).history(period=per) for per in FALLBACK_PERIODS for _ in [0] if not yf.Ticker(tk).history(period=per).empty), yf.Ticker(tk).history(period=args.period))["Close"].dropna()
                if fx.empty: 
                    print(f"[SKIP] no FX {cA}{cB}=X"); 
                    continue
                fx = fx.reindex(hA.index).ffill()
                hA = hA * fx
            except Exception:
                print(f"[SKIP] FX fail {cA}{cB}=X")
                continue

        idx = hA.index.intersection(hB.index)
        if len(idx) < max(260, args.win+10): 
            print(f"[SKIP] short history: {A}/{B}")
            continue
        a = hA.reindex(idx)
        b = hB.reindex(idx)

        # spread: log-ratio
        s = np.log(a/b).dropna()
        mu = s.rolling(args.win, min_periods=args.win//2).mean()
        sd = s.rolling(args.win, min_periods=args.win//2).std(ddof=0)
        z  = ((s - mu) / sd).dropna()
        if z.empty: 
            print(f"[SKIP] no z: {A}/{B}")
            continue

        # z-vol (p75 su 20)
        zvol = z.rolling(20, min_periods=10).std(ddof=0).dropna()
        zvol_p75 = float(np.nanpercentile(zvol, 75)) if len(zvol) else math.inf

        # crossing ENTER (debounce 3g)
        cross = (z.shift(1) < args.z_enter) & (z >= args.z_enter)
        idxs = list(np.where(cross)[0])
        enters, last = 0, -999
        for i in idxs:
            if i-last > 3:
                enters += 1
                last = i

        pair_name = to_pair_name(A,B)
        rows.append({"pair": pair_name, "A_yh": A, "B_yh": B,
                     "enters_oos": int(enters), "zvol_p75": zvol_p75})

    out = pd.DataFrame(rows)
    Path("reports").mkdir(exist_ok=True)
    if out.empty:
        print("[INFO] Nessuna coppia con storico sufficiente."); 
        out.to_csv(args.outfile, index=False); 
        return

    # Filtri coerenti con il tuo regime filter
    out = out[(out["enters_oos"] >= 3) & (out["zvol_p75"] <= args.zvol_max)].copy()
    if out.empty:
        print("[INFO] Ci sono coppie, ma non passano (enters>=3 & zvol<=1.0). Prova --period 10y o --win 30.")
        out = pd.DataFrame(rows)  # salva comunque tutte per ispezione
    out["onboard_cmd"] = out.apply(lambda r: f"bin/pair_onboard.sh {r.pair} {r.A_yh} {r.B_yh}", axis=1)
    out = out.sort_values(["enters_oos","zvol_p75"], ascending=[False, True])
    out.to_csv(args.outfile, index=False)
    print(out.to_string(index=False))

if __name__ == "__main__":
    main()

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
try:
    from scripts.utils_fx import fx_series
except Exception:
    from utils_fx import fx_series
