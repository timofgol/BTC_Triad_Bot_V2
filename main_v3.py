#!/usr/bin/env python3
"""
BTC Triad Bot V3 (Proactive Market State Evaluator)

Evaluates the market continuously based on Path of Least Resistance (POLR).
Alerts on Intraday Phase Shifts instead of rigid buy/sell triggers.
"""

import asyncio
import json
import logging
import os
import signal
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import aiohttp
from dotenv import load_dotenv

load_dotenv()

def env_float(name: str, default: float) -> float:
    try: return float(os.getenv(name, str(default)))
    except: return default

COINALYZE_API_KEY = os.getenv("COINALYZE_API_KEY", "")
COINGLASS_API_KEY = os.getenv("COINGLASS_API_KEY", "")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# Tuning
WALL_MIN_BTC = env_float("WALL_MIN_BTC", 20.0)
LIQ_CLUSTER_MIN_VAL = env_float("LIQ_CLUSTER_MIN_VAL", 1000000)
POLR_THRESHOLD = env_float("POLR_THRESHOLD", 30.0) # Threshold for phase shift
CLUSTER_DISSOLVE_PCT = env_float("CLUSTER_DISSOLVE_PCT", 0.3) # 30% drop in cluster value

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("bot_v3")

@dataclass
class MarketState:
    price: float = 0.0
    cvd_delta: float = 0.0
    oi_chg_pct: float = 0.0
    bids_vol: float = 0.0
    asks_vol: float = 0.0
    nearest_bid_wall: float = 0.0
    nearest_ask_wall: float = 0.0
    up_clusters: Dict[float, float] = field(default_factory=dict)
    down_clusters: Dict[float, float] = field(default_factory=dict)
    ts: float = 0.0

state = MarketState()
prev_clusters: Dict[float, float] = {}
current_phase = "NEUTRAL"
last_alert_ts = 0.0
shutdown_event = asyncio.Event()

def pct_diff(new: float, old: float) -> float:
    if old == 0: return 0.0
    return (new - old) / abs(old) * 100.0

async def send_telegram(session: aiohttp.ClientSession, text: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.info(f"TG (disabled): {text[:50]}...")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"}
    try:
        async with session.post(url, json=payload, timeout=10) as r:
            pass
    except Exception as e:
        logger.error(f"TG error: {e}")

async def binance_ws_loop():
    url = "wss://fstream.binance.com/stream?streams=btcusdt@depth20@100ms"
    while not shutdown_event.is_set():
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(url) as ws:
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            d = json.loads(msg.data).get("data", {})
                            if "b" in d and "a" in d:
                                bids = [(float(p), float(q)) for p, q in d["b"]]
                                asks = [(float(p), float(q)) for p, q in d["a"]]
                                if not bids or not asks: continue
                                
                                state.price = (bids[0][0] + asks[0][0]) / 2.0
                                
                                bid_walls = [w for w in bids if w[1] >= WALL_MIN_BTC]
                                ask_walls = [w for w in asks if w[1] >= WALL_MIN_BTC]
                                
                                state.bids_vol = sum(w[1] for w in bid_walls)
                                state.asks_vol = sum(w[1] for w in ask_walls)
                                state.nearest_bid_wall = bid_walls[0][0] if bid_walls else 0.0
                                state.nearest_ask_wall = ask_walls[0][0] if ask_walls else 0.0
                                state.ts = time.time()
        except Exception as e:
            logger.error(f"Binance WS Error: {e}")
        await asyncio.sleep(5)

async def coinalyze_loop():
    headers = {"api_key": COINALYZE_API_KEY} if COINALYZE_API_KEY else {}
    if not COINALYZE_API_KEY: return
    
    async with aiohttp.ClientSession(headers=headers) as session:
        while not shutdown_event.is_set():
            try:
                now = int(time.time())
                params = {"symbols": "BTCUSDT_PERP.A", "interval": "5min", "from": now - 1800, "to": now}
                
                async with session.get("https://api.coinalyze.net/v1/ohlcv-history", params=params) as r:
                    if r.status == 200:
                        data = await r.json()
                        if data and data[0].get("history"):
                            hist = data[0]["history"]
                            signed_vols = []
                            for row in hist[-6:]:
                                v = float(row.get("v", 0))
                                bv = float(row.get("bv", v/2))
                                if v > 0: signed_vols.append((bv - (v-bv)) / v)
                            state.cvd_delta = sum(signed_vols) / len(signed_vols) if signed_vols else 0.0
                            
                async with session.get("https://api.coinalyze.net/v1/open-interest-history", params=params) as r:
                    if r.status == 200:
                        data = await r.json()
                        if data and data[0].get("history") and len(data[0]["history"]) >= 6:
                            hist = data[0]["history"]
                            state.oi_chg_pct = pct_diff(float(hist[-1]["c"]), float(hist[-6]["o"]))
            except Exception as e:
                logger.error(f"Coinalyze Error: {e}")
            await asyncio.sleep(60)

async def coinglass_loop():
    global prev_clusters
    headers = {"CG-API-KEY": COINGLASS_API_KEY, "accept": "application/json"}
    if not COINGLASS_API_KEY: return
    
    url = "https://open-api-v4.coinglass.com/api/futures/liquidation/aggregated-heatmap/model1"
    async with aiohttp.ClientSession(headers=headers) as session:
        while not shutdown_event.is_set():
            try:
                async with session.get(url, params={"symbol": "BTC", "range": "24h"}) as r:
                    if r.status == 200:
                        res = await r.json()
                        if res.get("code") == "0" and "data" in res:
                            data = res["data"]
                            y_axis = data.get("y_axis", [])
                            liq_data = data.get("liquidation_leverage_data", [])
                            
                            if y_axis and liq_data and state.price > 0:
                                max_x = max([d[0] for d in liq_data])
                                clusters = {}
                                for x, y, val in liq_data:
                                    if x >= max_x - 5:
                                        p = float(y_axis[y])
                                        clusters[p] = clusters.get(p, 0) + float(val)
                                        
                                up_c, down_c = {}, {}
                                for p, v in clusters.items():
                                    if v >= LIQ_CLUSTER_MIN_VAL:
                                        if p > state.price * 1.002: up_c[p] = v
                                        elif p < state.price * 0.998: down_c[p] = v
                                        
                                # Store current for diffing next time
                                prev_clusters = {**state.up_clusters, **state.down_clusters}
                                state.up_clusters = up_c
                                state.down_clusters = down_c
            except Exception as e:
                logger.error(f"CoinGlass Error: {e}")
            await asyncio.sleep(300)

async def evaluator_loop():
    global current_phase, last_alert_ts
    async with aiohttp.ClientSession() as session:
        while not shutdown_event.is_set():
            await asyncio.sleep(15)
            
            if state.price == 0.0: continue
            
            # 1. Cluster Dissolution Check
            dissolved_cluster_msg = ""
            for p, old_v in prev_clusters.items():
                new_v = state.up_clusters.get(p, state.down_clusters.get(p, 0))
                if new_v < old_v * (1 - CLUSTER_DISSOLVE_PCT) and old_v > LIQ_CLUSTER_MIN_VAL * 2:
                    dist = pct_diff(p, state.price)
                    if abs(dist) < 1.0: # Only care if it's near price
                        dir_str = "Upside" if p > state.price else "Downside"
                        dissolved_cluster_msg = f"⚠️ Major {dir_str} cluster at {p:.0f} dissolved! (-{(old_v-new_v)/1e6:.1f}M)"
                        break

            # 2. Calculate POLR (Path of Least Resistance) Score
            # Magnetism (-100 to 100)
            sum_up = sum(state.up_clusters.values())
            sum_down = sum(state.down_clusters.values())
            tot_mag = sum_up + sum_down
            mag_score = ((sum_up - sum_down) / tot_mag * 100) if tot_mag > 0 else 0
            
            # Friction (-100 to 100)
            tot_fric = state.bids_vol + state.asks_vol
            fric_score = ((state.bids_vol - state.asks_vol) / tot_fric * 100) if tot_fric > 0 else 0
            
            # Fuel (-100 to 100)
            fuel_score = max(-100, min(100, state.cvd_delta * 1000)) # Scale delta to roughly -100/100
            
            # Weighted POLR
            polr = (0.4 * mag_score) + (0.4 * fric_score) + (0.2 * fuel_score)
            
            # 3. Phase Determination
            new_phase = "NEUTRAL"
            if polr >= POLR_THRESHOLD: new_phase = "BULLISH"
            elif polr <= -POLR_THRESHOLD: new_phase = "BEARISH"
            
            now = time.time()
            if new_phase != current_phase and (now - last_alert_ts) > 600: # 10 min debounce
                current_phase = new_phase
                last_alert_ts = now
                
                emoji = "🟢" if new_phase == "BULLISH" else "🔴" if new_phase == "BEARISH" else "⚪"
                
                nearest_up = min(state.up_clusters.keys()) if state.up_clusters else 0
                nearest_down = max(state.down_clusters.keys()) if state.down_clusters else 0
                
                msg = (
                    f"{emoji} <b>INTRADAY PHASE SHIFT: {new_phase}</b>\n\n"
                    f"Price: {state.price:.2f}\n"
                    f"POLR Score: {polr:+.1f} (Bias: {emoji})\n\n"
                    f"<b>Path Analysis:</b>\n"
                    f"• Nearest Upside Target: {nearest_up:.0f}\n"
                    f"• Nearest Downside Target: {nearest_down:.0f}\n"
                    f"• Bid Support: {state.bids_vol:.1f} BTC (Nearest: {state.nearest_bid_wall:.0f})\n"
                    f"• Ask Resistance: {state.asks_vol:.1f} BTC (Nearest: {state.nearest_ask_wall:.0f})\n"
                    f"• CVD Fuel: {state.cvd_delta*100:+.2f}%\n"
                )
                if dissolved_cluster_msg:
                    msg += f"\n{dissolved_cluster_msg}"
                    
                logger.info(f"PHASE SHIFT -> {new_phase} (POLR: {polr:.1f})")
                await send_telegram(session, msg)

            if int(now) % 60 == 0:
                logger.info(f"Phase: {current_phase} | POLR: {polr:+.1f} | Price: {state.price:.2f} | Bids: {state.bids_vol:.0f} | Asks: {state.asks_vol:.0f}")

async def main():
    logger.info("Starting BTC Triad Bot V3 (Proactive State)...")
    async with aiohttp.ClientSession() as session:
        await send_telegram(session, "✅ BTC Triad Bot V3 (Proactive State) started.")
        
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try: loop.add_signal_handler(sig, shutdown_event.set)
        except NotImplementedError: pass

    tasks = [
        asyncio.create_task(binance_ws_loop()),
        asyncio.create_task(coinalyze_loop()),
        asyncio.create_task(coinglass_loop()),
        asyncio.create_task(evaluator_loop())
    ]
    await shutdown_event.wait()
    for t in tasks: t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

if __name__ == "__main__":
    asyncio.run(main())
