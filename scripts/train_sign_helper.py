import numpy as np
import pandas as pd

def zscore(x: pd.Series, win: int) -> pd.Series:
    m = x.rolling(win, min_periods=max(5, win//4)).mean()
    v = x.rolling(win, min_periods=max(5, win//4)).std(ddof=0)
    return (x - m) / v

def simulate_pnl(spread: pd.Series, *, side: str, z_enter: float, z_exit: float,
                 z_stop: float, max_hold: int, latency_days: int,
                 is_pct: bool, spread_scale: float, notional: float) -> float:
    """Mini backtest su una sola serie (TRAIN). Restituisce PnL totale."""
    z = zscore(spread.astype(float), 60)  # usa stesso z-window del WF, se vuoi param
    z_lag = z.shift(latency_days) if latency_days > 0 else z
    s_lag = spread.shift(latency_days) if latency_days > 0 else spread

    in_pos = False; direction=None; entry_i=None; pnl=0.0
    for i in range(len(spread)):
        zi = z_lag.iat[i] if not np.isnan(z_lag.iat[i]) else np.nan
        if not in_pos:
            if side in ("short","both") and zi >= z_enter: in_pos, direction, entry_i = True, "SHORT", i
            elif side in ("long","both")  and zi <= -z_enter: in_pos, direction, entry_i = True, "LONG", i
        if in_pos:
            exit_now=False
            if (direction=="SHORT" and zi <= z_exit) or (direction=="LONG" and zi >= -z_exit): exit_now=True
            if (direction=="SHORT" and zi >= z_stop) or (direction=="LONG" and zi <= -z_stop): exit_now=True
            if entry_i is not None and (i - entry_i) >= max_hold: exit_now=True
            if exit_now:
                entry = float(s_lag.iat[entry_i] if not np.isnan(s_lag.iat[entry_i]) else spread.iat[entry_i])
                ex = float(s_lag.iat[i] if not np.isnan(s_lag.iat[i]) else spread.iat[i])
                delta = ex - entry
                dir_sign = -1.0 if direction=="SHORT" else +1.0
                gross = dir_sign * delta * (notional if is_pct else (spread_scale*notional))
                pnl += gross
                in_pos=False; direction=None; entry_i=None
    return float(pnl)

def choose_sign_on_train(spread_train: pd.Series, **kwargs) -> int:
    """Ritorna +1 o -1 in base al PnL su TRAIN."""
    pnl_pos = simulate_pnl(spread_train, **kwargs)
    pnl_neg = simulate_pnl(-spread_train, **kwargs)
    return 1 if pnl_pos >= pnl_neg else -1
