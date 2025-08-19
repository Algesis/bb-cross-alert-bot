import os
from datetime import datetime, timezone
import yfinance as yf
import pandas as pd
import numpy as np
import requests

# ========= CONFIG =========
TICKERS = [
    "AAPL", "MSFT", "TSLA", "SPY", "QQQ", "NVDA",
    "MES=F", "MNQ=F", "MGC=F", "MCL=F", "MHG=F", "SIL=F",
    "EURUSD=X", "GBPUSD=X", "JPY=X", "USDJPY=X", "USDCAD=X", "AUDUSD=X"
]

# Prefer a BB-specific webhook, otherwise reuse the RSI one
WEBHOOK_URL = os.getenv("BB_DISCORD_WEBHOOK") or os.getenv("RSI_DISCORD_WEBHOOK")

LOG_FILE = "bb_alert_log.txt"         # de-dup store (bar-timestamp keyed)
INTERVAL = "5m"
LOOKBACK_PERIOD = "7d"                # enough 5m bars for BB len=107
BB_LEN = 107
BB_MULT = 1.7

# Map some common symbols to their TradingView exchange
TRADINGVIEW_EXCHANGES = {
    "AAPL": "NASDAQ", "MSFT": "NASDAQ", "TSLA": "NASDAQ",
    "AMZN": "NASDAQ", "SPY": "AMEX", "QQQ": "NASDAQ", "NVDA": "NASDAQ",
    "MES=F": "CME_MINI", "MNQ=F": "CME_MINI", "MGC=F": "COMEX", "MCL=F": "NYMEX",
    "MHG=F": "COMEX", "SIL=F": "COMEX"
}

# ========= HELPERS =========
def get_tradingview_link(ticker: str) -> str:
    # FX (Yahoo: EURUSD=X) -> TradingView: FX:EURUSD
    if ticker.endswith("=X"):
        return f"https://www.tradingview.com/chart/?symbol=FX:{ticker.replace('=X','')}"
    # Futures (Yahoo: MES=F) -> TradingView continuous: MES1! (or exchange-mapped)
    if ticker.endswith("=F"):
        exch = TRADINGVIEW_EXCHANGES.get(ticker, "CME_MINI")
        tv_ticker = ticker.replace("=F", "1!")
        return f"https://www.tradingview.com/chart/?symbol={exch}:{tv_ticker}"
    # Stocks/ETFs
    exch = TRADINGVIEW_EXCHANGES.get(ticker, "NASDAQ")
    return f"https://www.tradingview.com/chart/?symbol={exch}:{ticker}"

def load_alerted_log(path: str = LOG_FILE) -> set:
    try:
        with open(path, "r") as f:
            return set(line.strip() for line in f if line.strip())
    except FileNotFoundError:
        return set()

def append_alert_log(keys: list, path: str = LOG_FILE):
    with open(path, "a") as f:
        for k in keys:
            f.write(k + "\n")

def send_discord_alert(items: list, bar_time_iso: str):
    if not WEBHOOK_URL:
        print("❌ Discord webhook not configured.")
        return
    lines = []
    for t, sig in items:
        label = "CROSS ABOVE UPPER" if sig == "UP" else "CROSS BELOW LOWER"
        lines.append(f"• **{t}** — {label} → [Chart]({get_tradingview_link(t)})")
    msg = (
        f"🎯 **Bollinger Cross** on **{INTERVAL}** | len={BB_LEN}, mult={BB_MULT}\n"
        + "\n".join(lines)
        + (f"\n🕒 Bar time: `{bar_time_iso}`" if bar_time_iso else "")
    )
    try:
        r = requests.post(WEBHOOK_URL, json={"content": msg})
        print(f"✅ Discord alert sent (status {r.status_code}).")
    except Exception as e:
        print(f"❌ Discord send error: {e}")

def compute_bbands(close: pd.Series, length: int, mult: float):
    basis = close.rolling(length, min_periods=length).mean()
    stdev = close.rolling(length, min_periods=length).std(ddof=0)   # Wilder/Pine-like stdev
    upper = basis + mult * stdev
    lower = basis - mult * stdev
    return basis, upper, lower

def check_bb_cross_5m(ticker: str):
    """
    Return (triggered(bool), signal('UP'|'DOWN'|None), prev_close, prev_upper, prev_lower,
            last_close, last_upper, last_lower, bar_time)
    Signal 'UP'  = close crossed ABOVE upper band on the last closed bar.
    Signal 'DOWN'= close crossed BELOW lower band on the last closed bar.
    """
    df = yf.download(ticker, period=LOOKBACK_PERIOD, interval=INTERVAL, progress=False, auto_adjust=False)
    if df.empty or "Close" not in df.columns:
        return (False, None, None, None, None, None, None, None, None)

    # compute bands
    basis, upper, lower = compute_bbands(df["Close"], BB_LEN, BB_MULT)
    df = df.assign(Basis=basis, Upper=upper, Lower=lower).dropna(subset=["Basis","Upper","Lower"])

    if len(df) < 2:
        # Not enough fully-formed bars for a cross check
        return (False, None, None, None, None, None, None, None, None)

    prev, last = df.iloc[-2], df.iloc[-1]
    prev_close, last_close = float(prev["Close"]), float(last["Close"])
    prev_upper, prev_lower = float(prev["Upper"]), float(prev["Lower"])
    last_upper, last_lower = float(last["Upper"]), float(last["Lower"])
    last_time = df.index[-1]

    cross_above = (prev_close <= prev_upper) and (last_close > last_upper)
    cross_below = (prev_close >= prev_lower) and (last_close < last_lower)

    if cross_above:
        return (True, "UP", prev_close, prev_upper, prev_lower, last_close, last_upper, last_lower, last_time)
    if cross_below:
        return (True, "DOWN", prev_close, prev_upper, prev_lower, last_close, last_upper, last_lower, last_time)
    return (False, None, prev_close, prev_upper, prev_lower, last_close, last_upper, last_lower, last_time)

# ========= MAIN =========
if __name__ == "__main__":
    print(f"🔍 Run at {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S %Z')} | interval={INTERVAL} | BB(len={BB_LEN}, mult={BB_MULT})")
    already = load_alerted_log()
    to_alert = []          # list of (ticker, 'UP'|'DOWN')
    alert_keys = []
    bar_time_for_message = None

    for t in TICKERS:
        try:
            (triggered, sig,
             pC, pU, pL, cC, cU, cL, bar_time) = check_bb_cross_5m(t)

            if bar_time is None:
                print(f"… {t}: no BB yet (insufficient data)")
                continue

            # Diagnostics
            bar_iso = bar_time.tz_convert("UTC").strftime("%Y-%m-%d %H:%M:%S %Z") if hasattr(bar_time, "tz_convert") else str(bar_time)
            if pC is not None:
                print(f"… {t}: prevC={pC:.6f} prevU={pU:.6f} prevL={pL:.6f} | curC={cC:.6f} curU={cU:.6f} curL={cL:.6f} @ {bar_iso}")

            if triggered:
                # de-dup key uses ticker + the specific bar timestamp + signal
                key = f"{t}|{sig}|{bar_iso}"
                if key not in already:
                    to_alert.append((t, sig))
                    alert_keys.append(key)
                    bar_time_for_message = bar_iso
        except Exception as e:
            print(f"❌ {t}: error {e}")

    if to_alert:
        send_discord_alert(to_alert, bar_time_for_message or "")
        append_alert_log(alert_keys)
    else:
        print("📉 No BB crosses this run.")
