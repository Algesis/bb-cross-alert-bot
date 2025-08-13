#!/usr/bin/env python3
"""
Bollinger Band crossing alerts (5m) -> Discord webhook
- Length: 107
- Std Dev: 1.7
- Duplicate suppression via SQLite
- Env:
  - DISCORD_WEBHOOK (required)
  - TICKERS (optional; comma-separated). If not set, uses the default list below.
  - BB_LENGTH (default 107)
  - BB_MULT (default 1.7)
  - DRY_RUN (optional: "1" to print only)
"""
import os
import sys
import time
import json
import sqlite3
from pathlib import Path
from typing import List, Tuple

import requests
import pandas as pd
import yfinance as yf

DEFAULT_TICKERS = [
    "AAPL", "MSFT", "TSLA", "SPY", "QQQ", "NVDA",
    "MES=F", "MNQ=F", "MGC=F", "MCL=F", "MHG=F", "SIL=F",
    "EURUSD=X", "GBPUSD=X", "JPY=X", "USDJPY=X", "USDCAD=X", "AUDUSD=X"
]

# --- config from env
WEBHOOK = os.environ.get("DISCORD_WEBHOOK", "").strip()
if not WEBHOOK:
    print("ERROR: DISCORD_WEBHOOK not set", file=sys.stderr)
    sys.exit(2)

env_tickers = os.environ.get("TICKERS", "").strip()
TICKERS = [t.strip() for t in env_tickers.split(",") if t.strip()] if env_tickers else DEFAULT_TICKERS

BB_LENGTH = int(os.environ.get("BB_LENGTH", "107"))
BB_MULT = float(os.environ.get("BB_MULT", "1.7"))
DRY_RUN = os.environ.get("DRY_RUN", "").strip() == "1"

STATE_DIR = Path(".state")
STATE_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = STATE_DIR / "bb_alerts.sqlite3"

# --- database helpers
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

# --- TA: Bollinger bands + crossing
def compute_bb(close: pd.Series, length: int, mult: float) -> Tuple[pd.Series, pd.Series, pd.Series]:
    basis = close.rolling(length, min_periods=length).mean()
    stdev = close.rolling(length, min_periods=length).std(ddof=0)  # Pine-like
    upper = basis + mult * stdev
    lower = basis - mult * stdev
    return basis, upper, lower

def detect_cross(prev_close, prev_upper, prev_lower, cur_close, cur_upper, cur_lower):
    cross_above = (pd.notna(prev_upper) and pd.notna(cur_upper)
                   and prev_close <= prev_upper and cur_close > cur_upper)
    cross_below = (pd.notna(prev_lower) and pd.notna(cur_lower)
                   and prev_close >= prev_lower and cur_close < cur_lower)
    if cross_above:
        return "CROSS_ABOVE"
    if cross_below:
        return "CROSS_BELOW"
    return None

# --- discord
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

def fmt_ts(ts: pd.Timestamp) -> str:
    if ts.tz is None:
        ts = ts.tz_localize("UTC")
    return ts.tz_convert("UTC").strftime("%Y-%m-%d %H:%M:%S %Z")

def process_symbol(conn, symbol: str):
    # 5m data; 7d window gives enough for 107-length calc
    df = yf.download(symbol, period="7d", interval="5m", auto_adjust=False, progress=False)
    if df is None or df.empty or "Close" not in df.columns:
        print(f"WARN: no data for {symbol}")
        return

    # ensure tz-aware in UTC
    if df.index.tz is None:
        df.index = df.index.tz_localize("UTC")
    else:
        df.index = df.index.tz_convert("UTC")

    close = df["Close"].copy()
    basis, upper, lower = compute_bb(close, BB_LENGTH, BB_MULT)
    df = df.assign(Basis=basis, Upper=upper, Lower=lower)

    # need last two completed bars with bands computed
    valid = df.dropna(subset=["Basis", "Upper", "Lower"])
    if len(valid) < 2:
        print(f"INFO: not enough bars after BB calc for {symbol}")
        return

    prev, cur = valid.iloc[-2], valid.iloc[-1]
    prev_ts, cur_ts = valid.index[-2], valid.index[-1]

    signal = detect_cross(prev.Close, prev.Upper, prev.Lower, cur.Close, cur.Upper, cur.Lower)
    if not signal:
        return

    sig_key = signal
    cur_ts_str = cur_ts.isoformat()
    if already_sent(conn, symbol, cur_ts_str, sig_key):
        return

    arrow = "✅↑" if signal == "CROSS_ABOVE" else "❌↓"
    content = (
        f"{arrow} **{symbol}** 5m **{signal.replace('_', ' ')}**\n"
        f"Close: {cur.Close:.6f}\n"
        f"Upper: {cur.Upper:.6f} | Lower: {cur.Lower:.6f}\n"
        f"Bar: {fmt_ts(cur_ts)}"
    )

    if send_discord(content):
        mark_sent(conn, symbol, cur_ts_str, sig_key)

def main():
    conn = db_init()
    for sym in TICKERS:
        try:
            process_symbol(conn, sym)
            time.sleep(0.3)  # gentle spacing for API
        except Exception as e:
            print(f"ERROR processing {sym}: {e}", file=sys.stderr)

if __name__ == "__main__":
    main()
