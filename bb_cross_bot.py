#!/usr/bin/env python3
"""
Bollinger Band crossing alerts (5m) -> Discord webhook
- Length: 107
- Std Dev: 1.7

Env:
  DISCORD_WEBHOOK      (required)
  TICKERS              (optional CSV; defaults to list below)
  BB_LENGTH            (default 107)
  BB_MULT              (default 1.7)
  DRY_RUN              ("1" to print only)
  DEBUG                ("1" to verbose log)
  BACKFILL_LOOKBACK    (int, default 0; e.g. 200 scans recent bars for last crossing)
  LOOSE_MODE           ("1" to alert if latest bar is outside bands even w/o strict cross)
"""
import os, sys, time, json, sqlite3
from pathlib import Path
from typing import Tuple, Optional

import requests
import pandas as pd
import yfinance as yf

# ---- Default tickers
DEFAULT_TICKERS = [
    "AAPL", "MSFT", "TSLA", "SPY", "QQQ", "NVDA",
    "MES=F", "MNQ=F", "MGC=F", "MCL=F", "MHG=F", "SIL=F",
    "EURUSD=X", "GBPUSD=X", "JPY=X", "USDJPY=X", "USDCAD=X", "AUDUSD=X"
]

# ---- TradingView exchange mapping (used to format clickable symbols like NASDAQ:AAPL)
TRADINGVIEW_EXCHANGES = {
    "AAPL": "NASDAQ", "MSFT": "NASDAQ", "TSLA": "NASDAQ",
    "AMZN": "NASDAQ", "SPY": "AMEX", "QQQ": "NASDAQ", "NVDA": "NASDAQ",
    "MES=F": "CME_MINI", "MNQ=F": "CME_MINI",
    "MGC=F": "COMEX", "MCL=F": "NYMEX", "MHG=F": "COMEX", "SIL=F": "COMEX"
}
def tv_symbol(symbol: str) -> str:
    exch = TRADINGVIEW_EXCHANGES.get(symbol)
    return f"{exch}:{symbol}" if exch else symbol

# ---- Config
WEBHOOK = os.environ.get("DISCORD_WEBHOOK", "").strip()
if not WEBHOOK:
    print("ERROR: DISCORD_WEBHOOK not set", file=sys.stderr)
    sys.exit(2)

env_tickers = os.environ.get("TICKERS", "").strip()
TICKERS = [t.strip() for t in env_tickers.split(",") if t.strip()] if env_tickers else DEFAULT_TICKERS
BB_LENGTH = int(os.environ.get("BB_LENGTH", "107"))
BB_MULT   = float(os.environ.get("BB_MULT", "1.7"))
DRY_RUN   = os.environ.get("DRY_RUN", "") == "1"
DEBUG     = os.environ.get("DEBUG", "") == "1"
BACKFILL_LOOKBACK = int(os.environ.get("BACKFILL_LOOKBACK", "0"))
LOOSE_MODE = os.environ.get("LOOSE_MODE", "") == "1"

STATE_DIR = Path(".state"); STATE_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = STATE_DIR / "bb_alerts.sqlite3"

# ---- DB helpers
def db_init():
    conn = sqlite3.connect(DB_PATH)
    with conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS alerts(
                symbol TEXT NOT NULL,
                bar_ts TEXT NOT NULL,
                signal TEXT NOT NULL,
                PRIMARY KEY(symbol, bar_ts, signal)
            )
        """)
    return conn

def already_sent(conn, symbol: str, bar_ts: str, signal: str) -> bool:
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM alerts WHERE symbol=? AND bar_ts=? AND signal=? LIMIT 1",
                (symbol, bar_ts, signal))
    return cur.fetchone() is not None

def mark_sent(conn, symbol: str, bar_ts: str, signal: str):
    with conn:
        conn.execute("INSERT OR IGNORE INTO alerts(symbol, bar_ts, signal) VALUES(?,?,?)",
                     (symbol, bar_ts, signal))

# ---- TA
def compute_bb(close: pd.Series, length: int, mult: float) -> Tuple[pd.Series, pd.Series, pd.Series]:
    basis = close.rolling(length, min_periods=length).mean()
    stdev = close.rolling(length, min_periods=length).std(ddof=0)  # Pine-like
    upper = basis + mult * stdev
    lower = basis - mult * stdev
    return basis, upper, lower

def detect_cross(prev_close, prev_upper, prev_lower, cur_close, cur_upper, cur_lower) -> Optional[str]:
    cross_above = (pd.notna(prev_upper) and pd.notna(cur_upper)
                   and prev_close <= prev_upper and cur_close > cur_upper)
    cross_below = (pd.notna(prev_lower) and pd.notna(cur_lower)
                   and prev_close >= prev_lower and cur_close < cur_lower)
    if cross_above: return "CROSS_ABOVE"
    if cross_below: return "CROSS_BELOW"
    return None

# ---- Utils
def fmt_ts(ts: pd.Timestamp) -> str:
    if ts.tz is None: ts = ts.tz_localize("UTC")
    return ts.tz_convert("UTC").strftime("%Y-%m-%d %H:%M:%S %Z")

def send_discord(content: str):
    payload = {"content": content, "username": "BBand 5m Alerts"}
    if DRY_RUN:
        print("[DRY_RUN] Would POST:", json.dumps(payload))
        return True
    r = requests.post(WEBHOOK, json=payload, timeout=20)
    if r.status_code in (200, 204):
        return True
    print(f"Discord webhook error {r.status_code}: {r.text}", file=sys.stderr)
    return False

def fetch(symbol: str) -> pd.DataFrame:
    df = yf.download(symbol, period="7d", interval="5m", auto_adjust=False, progress=False)
    if df is None or df.empty or "Close" not in df.columns:
        return pd.DataFrame()
    if df.index.tz is None:
        df.index = df.index.tz_localize("UTC")
    else:
        df.index = df.index.tz_convert("UTC")
    return df

def log_debug(msg: str):
    if DEBUG: print(msg)

# ---- Core
def process_symbol(conn, symbol: str):
    df = fetch(symbol)
    if df.empty:
        print(f"WARN: no 5m data for {symbol}")
        return

    close = df["Close"].copy()
    basis, upper, lower = compute_bb(close, BB_LENGTH, BB_MULT)
    df = df.assign(Basis=basis, Upper=upper, Lower=lower).dropna(subset=["Basis","Upper","Lower"])

    if len(df) < 2:
        print(f"INFO: {symbol} insufficient bars after BB calc")
        return

    # Backfill diagnostics: scan recent bars for latest crossing
    if BACKFILL_LOOKBACK > 0:
        sub = df.tail(max(2, BACKFILL_LOOKBACK))
        last_sig, last_ts = None, None
        for i in range(1, len(sub)):
            prev, cur = sub.iloc[i-1], sub.iloc[i]
            sig = detect_cross(prev.Close, prev.Upper, prev.Lower, cur.Close, cur.Upper, cur.Lower)
            if sig:
                last_sig, last_ts = sig, sub.index[i]
        if last_sig:
            log_debug(f"🔎 Backfill: {tv_symbol(symbol)} last {last_sig.replace('_',' ')} at {fmt_ts(last_ts)}")
        else:
            log_debug(f"🔎 Backfill: {tv_symbol(symbol)} no crossing in last {len(sub)} bars")

    # Live check: latest completed bar
    prev, cur = df.iloc[-2], df.iloc[-1]
    prev_ts, cur_ts = df.index[-2], df.index[-1]
    sig = detect_cross(prev.Close, prev.Upper, prev.Lower, cur.Close, cur.Upper, cur.Lower)

    # Optional: outside-without-cross (testing helper)
    if not sig and LOOSE_MODE:
        if cur.Close > cur.Upper: sig = "OUTSIDE_ABOVE"
        elif cur.Close < cur.Lower: sig = "OUTSIDE_BELOW"

    if not sig:
        log_debug(
            f"{tv_symbol(symbol)}: no signal on {fmt_ts(cur_ts)} "
            f"(prevC={prev.Close:.6f}, prevU={prev.Upper:.6f}, prevL={prev.Lower:.6f} | "
            f"curC={cur.Close:.6f}, curU={cur.Upper:.6f}, curL={cur.Lower:.6f})"
        )
        return

    sig_key = sig
    cur_ts_iso = cur_ts.isoformat()
    if already_sent(conn, symbol, cur_ts_iso, sig_key):
        log_debug(f"{tv_symbol(symbol)}: duplicate suppressed for {sig} at {cur_ts_iso}")
        return

    arrow = {"CROSS_ABOVE":"✅↑", "CROSS_BELOW":"❌↓",
             "OUTSIDE_ABOVE":"ℹ️↑", "OUTSIDE_BELOW":"ℹ️↓"}[sig]
    content = (
        f"{arrow} **{tv_symbol(symbol)}** 5m **{sig.replace('_',' ')}**\n"
        f"Close: {cur.Close:.6f}\n"
        f"Upper: {cur.Upper:.6f} | Lower: {cur.Lower:.6f}\n"
        f"Bar: {fmt_ts(cur_ts)}"
    )

    if send_discord(content):
        mark_sent(conn, symbol, cur_ts_iso, sig_key)

def main():
    conn = db_init()
    print(f"Tickers: {', '.join(TICKERS)}")
    print(f"Params: len={BB_LENGTH}, mult={BB_MULT}, backfill={BACKFILL_LOOKBACK}, loose={LOOSE_MODE}, debug={DEBUG}, dry_run={DRY_RUN}")
    for sym in TICKERS:
        try:
            process_symbol(conn, sym)
            time.sleep(0.25)
        except Exception as e:
            print(f"ERROR {sym}: {e}", file=sys.stderr)

if __name__ == "__main__":
    main()
