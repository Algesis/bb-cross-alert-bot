# BBand 5m Crossing Alerts -> Discord

Detects crossings **above the upper** or **below the lower** Bollinger Band with:
- Length = **107**
- Std Dev = **1.7**
- Timeframe = **5 minutes** (pulled via yfinance; only *closed* candles considered)

## Setup

1. **Create Discord Webhook**
   - Discord → Channel → Settings → Integrations → Webhooks → New → Copy URL.

2. **Repo Secrets / Variables**
   - Settings → Secrets and variables:
     - Secrets → **New repository secret**: `DISCORD_WEBHOOK` = your webhook URL.
     - Variables → **New repository variable** (optional): `TICKERS` = `AAPL,MSFT,ES=F,CL=F`.

3. **Enable GitHub Actions**
   - Commit this repo. The included workflow runs **every 5 minutes**.

## Local Run
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
export DISCORD_WEBHOOK="https://discord.com/api/webhooks/...."
export TICKERS="AAPL,MSFT"
python bb_cross_bot.py
