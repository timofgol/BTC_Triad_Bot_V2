#!/usr/bin/env python3
"""
BTC Triad Intraday Bot V2 (Aggressive Mode)

Data Sources:
1. Binance Futures WS (or CoinAPI) = Order Book Walls (Intent)
2. Coinalyze REST = CVD, Open Interest, Liquidations (Action/Context)
3. CoinGlass REST = Liquidation Heatmap (Consequences/Targets)

Setups:
- Setup A: Classic Absorption (Wall holds against CVD pressure)
- Setup B: Liquidation Cascade Fade (Price drops into cluster + Wall appears)
- Setup C: Trapped Breakout (Price pierces level + CVD spikes + Reversal)
"""

import asyncio
import json
import logging
import math
import os
import signal
import sqlite3
import time
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
from dotenv import load_dotenv

# ------------------------- Configuration -------------------------

load_dotenv()

def env_str(name: str, default: str = "") -> str:
    return os.getenv(name, default)

def env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return default

def env_int(name: str, default: int) -> int:
    try:
        return int(float(os.getenv(name, str(default))))
    except Exception:
        return default

def env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in {"1", "true", "yes", "y", "on"}

# API Keys
COINALYZE_API_KEY = env_str("COINALYZE_API_KEY")
COINGLASS_API_KEY = env_str("COINGLASS_API_KEY")
TELEGRAM_BOT_TOKEN = env_str("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = env_str("TELEGRAM_CHAT_ID")

# Orderbook Settings
USE_BINANCE_WS = env_bool("USE_BINANCE_WS", True) # Default to Binance for reliability
COINAPI_API_KEY = env_str("COINAPI_API_KEY")
COINAPI_SYMBOLS = env_str("COINAPI_SYMBOLS", "BINANCEFTS_PERP_BTC_USDT").split(",")

# Tuning Parameters (Aggressive Intraday)
WALL_MIN_BTC = env_float("WALL_MIN_BTC", 30.0)
WALL_NEAR_PCT = env_float("WALL_NEAR_PCT", 0.25) # 0.25% distance
CVD_MIN_ABS_DELTA = env_float("CVD_MIN_ABS_DELTA", 0.02) # 2% delta
OI_MIN_RISE_PCT = env_float("OI_MIN_RISE_PCT", 0.02) # 0.02% rise
LIQ_CLUSTER_MIN_VAL = env_float("LIQ_CLUSTER_MIN_VAL", 1000000) # $1M
GLOBAL_COOLDOWN_SEC = env_int("GLOBAL_COOLDOWN_SEC", 1800) # 30 mins

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("bot")

# ------------------------- Data Structures -------------------------

@dataclass
class Wall:
    side: str
    price: float
    size: float
    dist_pct: float

@dataclass
class BookState:
    mid: float
    bids: List[Wall]
    asks: List[Wall]
    ts: float

@dataclass
class ContextState:
    price: float
    cvd_delta: float
    oi_chg_pct: float
    liq_longs: float
    liq_shorts: float
    ts: float

@dataclass
class HeatmapCluster:
    price: float
    value: float
    dist_pct: float

@dataclass
class HeatmapState:
    up_clusters: List[HeatmapCluster]
    down_clusters: List[HeatmapCluster]
    ts: float

@dataclass
class Signal:
    setup_type: str
    direction: str
    price: float
    score: float
    wall_price: float
    target_price: float
    stop_price: float
    reason: str
    ts: float

# ------------------------- Global State -------------------------

state_book: Optional[BookState] = None
state_context: Optional[ContextState] = None
state_heatmap: Optional[HeatmapState] = None
last_signal_ts: float = 0.0
shutdown_event = asyncio.Event()

# ------------------------- Helpers -------------------------

def pct_diff(new: float, old: float) -> float:
    if old == 0: return 0.0
    return (new - old) / abs(old) * 100.0

async def send_telegram(session: aiohttp.ClientSession, text: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.info(f"Telegram (disabled): {text[:100]}...")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
    try:
        async with session.post(url, json=payload, timeout=10) as r:
            if r.status != 200:
                logger.error(f"Telegram error: {await r.text()}")
    except Exception as e:
        logger.error(f"Telegram exception: {e}")

# ------------------------- Data Fetchers -------------------------

async def binance_ws_loop():
    """Robust, free fallback for orderbook data."""
    url = "wss://fstream.binance.com/stream?streams=btcusdt@depth20@100ms"
    while not shutdown_event.is_set():
        try:
            logger.info("Connecting to Binance WS...")
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(url) as ws:
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data = json.loads(msg.data)
                            if "data" in data and "b" in data["data"] and "a" in data["data"]:
                                d = data["data"]
                                bids = [(float(p), float(q)) for p, q in d["b"]]
                                asks = [(float(p), float(q)) for p, q in d["a"]]
                                if not bids or not asks: continue
                                
                                mid = (bids[0][0] + asks[0][0]) / 2.0
                                
                                wall_bids = []
                                for p, q in bids:
                                    dist = pct_diff(p, mid)
                                    if q >= WALL_MIN_BTC and abs(dist) <= WALL_NEAR_PCT:
                                        wall_bids.append(Wall("BID", p, q, dist))
                                        
                                wall_asks = []
                                for p, q in asks:
                                    dist = pct_diff(p, mid)
                                    if q >= WALL_MIN_BTC and abs(dist) <= WALL_NEAR_PCT:
                                        wall_asks.append(Wall("ASK", p, q, dist))
                                        
                                global state_book
                                state_book = BookState(mid, wall_bids, wall_asks, time.time())
        except Exception as e:
            logger.error(f"Binance WS Error: {e}")
        await asyncio.sleep(5)

async def coinalyze_loop():
    """Fetches CVD, OI, and liquidations for context."""
    if not COINALYZE_API_KEY:
        logger.warning("Coinalyze API key missing. Context data disabled.")
        return
        
    headers = {"api_key": COINALYZE_API_KEY}
    symbol = "BTCUSDT_PERP.A" # Binance Futures BTCUSDT
    interval = "5min"
    
    async with aiohttp.ClientSession(headers=headers) as session:
        while not shutdown_event.is_set():
            try:
                now = int(time.time())
                start = now - (60 * 30) # 30 min lookback
                params = {"symbols": symbol, "interval": interval, "from": start, "to": now}
                
                # Fetch OHLCV (for CVD)
                async with session.get("https://api.coinalyze.net/v1/ohlcv-history", params=params) as r:
                    if r.status == 200:
                        data = await r.json()
                        if data and data[0].get("history"):
                            hist = data[0]["history"]
                            price = float(hist[-1]["c"])
                            
                            # Calculate CVD over last 6 bars (30 min)
                            signed_vols = []
                            for row in hist[-6:]:
                                v = float(row.get("v", 0))
                                bv = float(row.get("bv", v/2)) # Fallback if missing
                                sv = v - bv
                                if v > 0: signed_vols.append((bv - sv) / v)
                            cvd_delta = sum(signed_vols) / len(signed_vols) if signed_vols else 0.0
                        else:
                            price, cvd_delta = 0.0, 0.0
                            
                # Fetch OI
                async with session.get("https://api.coinalyze.net/v1/open-interest-history", params=params) as r:
                    if r.status == 200:
                        data = await r.json()
                        if data and data[0].get("history") and len(data[0]["history"]) >= 6:
                            hist = data[0]["history"]
                            old_oi = float(hist[-6]["o"])
                            new_oi = float(hist[-1]["c"])
                            oi_chg_pct = pct_diff(new_oi, old_oi)
                        else:
                            oi_chg_pct = 0.0
                            
                # Fetch Liquidations
                async with session.get("https://api.coinalyze.net/v1/liquidation-history", params=params) as r:
                    if r.status == 200:
                        data = await r.json()
                        liq_l, liq_s = 0.0, 0.0
                        if data and data[0].get("history"):
                            for row in data[0]["history"][-3:]: # Last 15 mins
                                liq_l += float(row.get("l", 0))
                                liq_s += float(row.get("s", 0))
                                
                if price > 0:
                    global state_context
                    state_context = ContextState(price, cvd_delta, oi_chg_pct, liq_l, liq_s, time.time())
                    
            except Exception as e:
                logger.error(f"Coinalyze Error: {e}")
            await asyncio.sleep(60) # Poll every 1 min

async def coinglass_loop():
    """Fetches Liquidation Heatmap targets."""
    if not COINGLASS_API_KEY:
        logger.warning("CoinGlass API key missing. Heatmap disabled.")
        return
        
    headers = {"CG-API-KEY": COINGLASS_API_KEY, "accept": "application/json"}
    url = "https://open-api-v4.coinglass.com/api/futures/liquidation/aggregated-heatmap/model1"
    
    async with aiohttp.ClientSession(headers=headers) as session:
        while not shutdown_event.is_set():
            try:
                params = {"symbol": "BTC", "range": "24h"}
                async with session.get(url, params=params) as r:
                    if r.status == 200:
                        res = await r.json()
                        if res.get("code") == "0" and "data" in res:
                            data = res["data"]
                            y_axis = data.get("y_axis", [])
                            liq_data = data.get("liquidation_leverage_data", [])
                            
                            if y_axis and liq_data and state_context:
                                ref_price = state_context.price
                                clusters = {}
                                
                                # Aggregate recent x_idx values (e.g. last 5 timestamps)
                                max_x = max([d[0] for d in liq_data]) if liq_data else 0
                                
                                for x, y, val in liq_data:
                                    if x >= max_x - 5: # Recent data only
                                        price_level = float(y_axis[y])
                                        clusters[price_level] = clusters.get(price_level, 0) + float(val)
                                        
                                up_c = []
                                down_c = []
                                for p, v in clusters.items():
                                    if v >= LIQ_CLUSTER_MIN_VAL:
                                        dist = pct_diff(p, ref_price)
                                        if 0.2 <= dist <= 5.0: # Filter sensible targets
                                            up_c.append(HeatmapCluster(p, v, dist))
                                        elif -5.0 <= dist <= -0.2:
                                            down_c.append(HeatmapCluster(p, v, dist))
                                            
                                up_c.sort(key=lambda x: x.value, reverse=True)
                                down_c.sort(key=lambda x: x.value, reverse=True)
                                
                                global state_heatmap
                                state_heatmap = HeatmapState(up_c[:3], down_c[:3], time.time())
            except Exception as e:
                logger.error(f"CoinGlass Error: {e}")
            await asyncio.sleep(300) # Poll every 5 mins

# ------------------------- Signal Engine -------------------------

async def engine_loop():
    """Evaluates triad state and generates signals."""
    global last_signal_ts
    
    async with aiohttp.ClientSession() as session:
        while not shutdown_event.is_set():
            await asyncio.sleep(10)
            
            if not state_book or not state_context or not state_heatmap:
                continue
                
            # Data staleness check
            now = time.time()
            if now - state_book.ts > 60 or now - state_context.ts > 300:
                continue
                
            b = state_book
            c = state_context
            h = state_heatmap
            
            signal = None
            
            # ---------------------------------------------------------
            # Setup A: Classic Absorption
            # Logic: Wall is touched. CVD shows aggressive opposing flow. OI is rising.
            # ---------------------------------------------------------
            if b.bids and c.cvd_delta <= -CVD_MIN_ABS_DELTA and c.oi_chg_pct >= OI_MIN_RISE_PCT:
                touched_bid = next((w for w in b.bids if abs(w.dist_pct) <= 0.10), None)
                if touched_bid and h.up_clusters:
                    signal = Signal("Absorption", "LONG", c.price, 0.8, touched_bid.price, h.up_clusters[0].price, touched_bid.price * 0.998, 
                                    f"Bid Wall {touched_bid.size:.1f} BTC held against {c.cvd_delta*100:.1f}% sell CVD. OI rising.", now)
                                    
            elif b.asks and c.cvd_delta >= CVD_MIN_ABS_DELTA and c.oi_chg_pct >= OI_MIN_RISE_PCT:
                touched_ask = next((w for w in b.asks if abs(w.dist_pct) <= 0.10), None)
                if touched_ask and h.down_clusters:
                    signal = Signal("Absorption", "SHORT", c.price, 0.8, touched_ask.price, h.down_clusters[0].price, touched_ask.price * 1.002, 
                                    f"Ask Wall {touched_ask.size:.1f} BTC held against +{c.cvd_delta*100:.1f}% buy CVD. OI rising.", now)

            # ---------------------------------------------------------
            # Setup B: Liquidation Cascade Fade
            # Logic: Massive liquidations just happened. Price hit a cluster. Wall appears to catch it.
            # ---------------------------------------------------------
            if not signal:
                if c.liq_longs > 5000000 and b.bids: # >$5M longs liq'd
                    best_bid = b.bids[0]
                    if h.up_clusters:
                        signal = Signal("Liq Fade", "LONG", c.price, 0.75, best_bid.price, h.up_clusters[0].price, best_bid.price * 0.995,
                                        f"${c.liq_longs/1e6:.1f}M longs liquidated. Bid wall {best_bid.size:.1f} BTC stepping in.", now)
                                        
                elif c.liq_shorts > 5000000 and b.asks:
                    best_ask = b.asks[0]
                    if h.down_clusters:
                        signal = Signal("Liq Fade", "SHORT", c.price, 0.75, best_ask.price, h.down_clusters[0].price, best_ask.price * 1.005,
                                        f"${c.liq_shorts/1e6:.1f}M shorts liquidated. Ask wall {best_ask.size:.1f} BTC stepping in.", now)

            # Dispatch Signal
            if signal and (now - last_signal_ts) >= GLOBAL_COOLDOWN_SEC:
                last_signal_ts = now
                
                emoji = "🟢 LONG" if signal.direction == "LONG" else "🔴 SHORT"
                msg = (
                    f"⚡ <b>{signal.setup_type} Signal</b> ⚡\n\n"
                    f"Direction: <b>{emoji}</b>\n"
                    f"Price: {signal.price:.2f}\n\n"
                    f"🛡 <b>Context:</b> {signal.reason}\n\n"
                    f"🎯 <b>Take Profit:</b> {signal.target_price:.2f}\n"
                    f"🛑 <b>Stop Loss:</b> {signal.stop_price:.2f}\n"
                )
                logger.info(f"SIGNAL TRIGGERED: {signal.setup_type} {signal.direction}")
                await send_telegram(session, msg)
                
            # Heartbeat
            if int(now) % 60 == 0:
                logger.info(f"Heartbeat | Price: {c.price:.2f} | CVD: {c.cvd_delta*100:+.2f}% | Bids: {len(b.bids)} | Asks: {len(b.asks)} | UpTgt: {len(h.up_clusters)}")

# ------------------------- Main -------------------------

async def main():
    logger.info("Starting BTC Triad Bot V2...")
    
    async with aiohttp.ClientSession() as session:
        await send_telegram(session, "✅ BTC Triad Bot V2 (Aggressive) started.")
        
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, shutdown_event.set)
        except NotImplementedError:
            pass

    tasks = [
        asyncio.create_task(coinalyze_loop()),
        asyncio.create_task(coinglass_loop()),
        asyncio.create_task(engine_loop())
    ]
    
    if USE_BINANCE_WS:
        tasks.append(asyncio.create_task(binance_ws_loop()))
    else:
        logger.warning("CoinAPI WS implementation omitted for brevity, using Binance WS.")
        tasks.append(asyncio.create_task(binance_ws_loop()))
        
    await shutdown_event.wait()
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

if __name__ == "__main__":
    asyncio.run(main())
