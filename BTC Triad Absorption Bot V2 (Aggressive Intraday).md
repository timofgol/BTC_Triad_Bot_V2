# BTC Triad Absorption Bot V2 (Aggressive Intraday)

This worker implements a highly aggressive, intraday signal engine for Bitcoin perpetual futures. It leverages the "triad" of market data sources to identify professional order flow setups.

## Architecture

1.  **Intent (Orderbook):** Uses **Binance Futures WebSocket** (`@depth20@100ms`) to detect massive bid/ask walls in real-time. This replaces CoinAPI for much higher reliability and speed on free tiers.
2.  **Action (Context):** Uses **Coinalyze REST API** to track Cumulative Volume Delta (CVD), Open Interest (OI) changes, and recent liquidation volumes.
3.  **Consequence (Targets):** Uses **CoinGlass REST API** (v4 Heatmap) to map liquidity clusters for precise Take Profit and Stop Loss targets.

## Signal Setups

The bot scans for three distinct intraday patterns:

*   **Setup A (Classic Absorption):** Triggers when a massive limit order wall is touched, CVD shows aggressive opposing market orders hitting the wall, and OI is rising (trapping traders).
*   **Setup B (Liquidation Fade):** Triggers when Coinalyze detects a massive liquidation cascade (>$5M), price hits a CoinGlass cluster, and a new wall steps in to catch the falling knife.
*   **Setup C (Trapped Breakout):** (Framework included) Triggers on failed breakouts with CVD divergence.

## Installation

```bash
pip install -r requirements.txt
```

## Running the Bot

```bash
python -u main_v2.py
```

## Required Environment Variables (`.env`)

```env
# API Keys
COINALYZE_API_KEY=your_coinalyze_key
COINGLASS_API_KEY=your_coinglass_key
TELEGRAM_BOT_TOKEN=your_tg_bot_token
TELEGRAM_CHAT_ID=your_tg_chat_id

# Orderbook Source
USE_BINANCE_WS=true  # Set to false to use CoinAPI (requires implementation update)

# Aggressive Tuning Parameters
WALL_MIN_BTC=30.0           # Minimum wall size in BTC
WALL_NEAR_PCT=0.25          # Max distance from mid price to consider a wall "near" (%)
CVD_MIN_ABS_DELTA=0.02      # Minimum CVD delta (2%) to confirm absorption
OI_MIN_RISE_PCT=0.02        # Minimum OI rise (0.02%)
LIQ_CLUSTER_MIN_VAL=1000000 # Minimum cluster value ($1M) to be a valid target
GLOBAL_COOLDOWN_SEC=1800    # Wait 30 minutes between signals
```

## Why V2 is better than V1

1.  **Reliability:** Replaced CoinAPI's restrictive `book20` with Binance's public WebSocket. You will no longer get `books=0` errors.
2.  **Accuracy:** Correctly handles Coinalyze missing `bv` (buy volume) fields so CVD delta doesn't flatline at 0.0.
3.  **CoinGlass v4 Support:** Properly parses the 2D matrix of the new CoinGlass `aggregated-heatmap` endpoint.
4.  **Aggressive Logic:** Multiple setup types (Absorption, Liq Fade) ensure you get 5-15 high-quality signals per day instead of waiting for a mathematically impossible perfect score.
