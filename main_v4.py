#!/usr/bin/env python3
"""
BTC Triad Bot V3.3 — STARTUP-tier compatible.

What changed vs V3.2:
- All CoinGlass heatmap/liquidation-map endpoints require Professional+ tier.
  On STARTUP they return 401 "Upgrade plan", which kept Magnetism = 0 forever.
- V3.3 builds a *synthetic liquidity map* using ONLY endpoints available on
  STARTUP and below:
    1) /api/futures/orderbook/ask-bids-history    (depth at ±0.25%, ±0.5%, ±1.0%)
    2) /api/futures/liquidation/aggregated-history  (30m bars, long/short USD)
    3) /api/futures/liquidation/coin-list          (live 1h/4h/12h/24h totals)
    4) /api/futures/top-long-short-position-ratio/history (sentiment overlay)
- Magnetism component of POLR now comes from the synthetic map.
- Friction still derived from local Binance Futures WS (top20 imbalance + walls).
- Fuel still comes from Coinalyze CVD + OI direction.
- Same proactive POLR phase detector and Telegram pipeline as V3.2.
"""

import asyncio
import json
import logging
import math
import os
import signal
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import aiohttp
from dotenv import load_dotenv

load_dotenv()

# ------------------------- Config -------------------------

def env_str(name: str, default: str = "") -> str:
    return os.getenv(name, default)

def env_float(name: str, default: float) -> float:
    try: return float(os.getenv(name, str(default)))
    except: return default

def env_int(name: str, default: int) -> int:
    try: return int(float(os.getenv(name, str(default))))
    except: return default

def env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None: return default
    return v.strip().lower() in {"1", "true", "yes", "y", "on"}

COINALYZE_API_KEY = env_str("COINALYZE_API_KEY")
COINGLASS_API_KEY = env_str("COINGLASS_API_KEY")
COINAPI_KEY = env_str("COINAPI_KEY")
TELEGRAM_BOT_TOKEN = env_str("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = env_str("TELEGRAM_CHAT_ID")

WALL_MIN_BTC = env_float("WALL_MIN_BTC", 10.0)
POLR_STAGE1_THRESHOLD = env_float("POLR_STAGE1_THRESHOLD", 25.0)
STAGE2_CONFIRM_THRESHOLD = env_float("STAGE2_CONFIRM_THRESHOLD", 50.0)
VERBOSE = env_bool("VERBOSE", True)
HEARTBEAT_SEC = env_int("HEARTBEAT_SEC", 30)
EVAL_INTERVAL = env_int("EVAL_INTERVAL", 15)
DEBOUNCE_SEC = env_int("DEBOUNCE_SEC", 600)
TG_TEST_ON_START = env_bool("TG_TEST_ON_START", True)

CG_BASE = "https://open-api-v4.coinglass.com"
CG_EXCHANGE = env_str("CG_EXCHANGE", "Binance")
CG_PAIR = env_str("CG_PAIR", "BTCUSDT")
CG_COIN = env_str("CG_COIN", "BTC")

LEVEL = logging.DEBUG if VERBOSE else logging.INFO
logging.basicConfig(
    level=LEVEL,
    format="%(asctime)s [%(levelname)s] %(name)s :: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("bot")
log_ws = logging.getLogger("bot.ws")
log_cz = logging.getLogger("bot.coinalyze")
log_cg = logging.getLogger("bot.coinglass")
log_ca = logging.getLogger("bot.coinapi")
log_ev = logging.getLogger("bot.eval")
log_tg = logging.getLogger("bot.telegram")

# ------------------------- State -------------------------

@dataclass
class MarketState:
    price: float = 0.0
    cvd_delta: float = 0.0          # mean buy-ratio over last 6 bars in [-1,+1]
    oi_chg_pct: float = 0.0
    liq_longs_30m: float = 0.0      # from Coinalyze
    liq_shorts_30m: float = 0.0

    # Local Binance WS (Friction)
    book_imbalance: float = 0.0
    bids_total_top20: float = 0.0
    asks_total_top20: float = 0.0
    bids_walls_btc: float = 0.0
    asks_walls_btc: float = 0.0
    nearest_bid_wall: float = 0.0
    nearest_ask_wall: float = 0.0

    # Synthetic CoinGlass map (Magnetism)
    cg_book_bids_usd: Dict[str, float] = field(default_factory=dict)  # depth->usd
    cg_book_asks_usd: Dict[str, float] = field(default_factory=dict)
    cg_liq_long_1h: float = 0.0
    cg_liq_short_1h: float = 0.0
    cg_liq_long_4h: float = 0.0
    cg_liq_short_4h: float = 0.0
    cg_liq_cascade_score: float = 0.0  # +100..-100 (recent 30m vs avg)
    cg_top_long_pct: float = 0.0       # 0..100
    cg_endpoints_ok: List[str] = field(default_factory=list)

    # Stage 2 CoinAPI metrics
    ca_cvd_usd: float = 0.0
    ca_vol_multiplier: float = 1.0
    ca_premium_pct: float = 0.0
    ca_ts: float = 0.0

    book_ts: float = 0.0
    ctx_ts: float = 0.0
    map_ts: float = 0.0
    book_msg_count: int = 0

state = MarketState()
current_phase = "NEUTRAL"
last_alert_ts = 0.0
shutdown_event = asyncio.Event()

# ------------------------- Helpers -------------------------

def pct_diff(new: float, old: float) -> float:
    if old == 0: return 0.0
    return (new - old) / abs(old) * 100.0

def soft_scale(x: float, scale: float = 4.0) -> float:
    return math.tanh(x * scale) * 100

def mask(v: str) -> str:
    if not v: return "<MISSING>"
    return f"{v[:4]}…{v[-4:]} (len={len(v)})"

async def send_telegram(session: aiohttp.ClientSession, text: str) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log_tg.warning("Telegram not configured.")
        log_tg.debug(f"Would send: {text}")
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML",
               "disable_web_page_preview": True}
    try:
        async with session.post(url, json=payload, timeout=10) as r:
            body = await r.text()
            if r.status == 200:
                log_tg.info(f"Telegram OK ({len(text)} chars).")
                return True
            log_tg.error(f"Telegram FAIL {r.status}: {body[:200]}")
    except Exception as e:
        log_tg.error(f"Telegram exception: {type(e).__name__}: {e}")
    return False

# ------------------------- Binance WS (Friction) -------------------------

async def binance_ws_loop():
    url = "wss://fstream.binance.com/stream?streams=btcusdt@depth20@100ms"
    backoff = 1
    while not shutdown_event.is_set():
        try:
            log_ws.info(f"Connecting to Binance Futures WS")
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(url, heartbeat=20, timeout=15) as ws:
                    log_ws.info("Connected.")
                    backoff = 1
                    last_summary = 0.0
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            d = json.loads(msg.data).get("data", {})
                            if "b" in d and "a" in d:
                                bids = [(float(p), float(q)) for p, q in d["b"]]
                                asks = [(float(p), float(q)) for p, q in d["a"]]
                                if not bids or not asks: continue

                                state.price = (bids[0][0] + asks[0][0]) / 2.0
                                bids_total = sum(q for _, q in bids)
                                asks_total = sum(q for _, q in asks)
                                state.bids_total_top20 = bids_total
                                state.asks_total_top20 = asks_total
                                tot = bids_total + asks_total
                                state.book_imbalance = (bids_total - asks_total)/tot if tot else 0.0

                                bid_walls = [(p, q) for p, q in bids if q >= WALL_MIN_BTC]
                                ask_walls = [(p, q) for p, q in asks if q >= WALL_MIN_BTC]
                                state.bids_walls_btc = sum(q for _, q in bid_walls)
                                state.asks_walls_btc = sum(q for _, q in ask_walls)
                                state.nearest_bid_wall = bid_walls[0][0] if bid_walls else 0.0
                                state.nearest_ask_wall = ask_walls[0][0] if ask_walls else 0.0
                                state.book_ts = time.time()
                                state.book_msg_count += 1

                                now = time.time()
                                if VERBOSE and (now - last_summary) >= 30:
                                    last_summary = now
                                    log_ws.debug(
                                        f"Book #{state.book_msg_count} | mid={state.price:.2f} | "
                                        f"depth20: bids={bids_total:.1f} asks={asks_total:.1f} "
                                        f"imb={state.book_imbalance:+.3f} | "
                                        f"walls>={WALL_MIN_BTC} bids={len(bid_walls)} asks={len(ask_walls)}"
                                    )
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            log_ws.warning(f"WS closed/err: {msg.type}")
                            break
        except Exception as e:
            log_ws.error(f"WS error: {type(e).__name__}: {e}")
        log_ws.info(f"Reconnecting in {backoff}s")
        await asyncio.sleep(backoff)
        backoff = min(backoff*2, 30)

# ------------------------- Coinalyze (Fuel) -------------------------

async def coinalyze_loop():
    if not COINALYZE_API_KEY:
        log_cz.error("COINALYZE_API_KEY missing. Context disabled.")
        return
    headers = {"api_key": COINALYZE_API_KEY}
    async with aiohttp.ClientSession(headers=headers) as session:
        while not shutdown_event.is_set():
            try:
                now_ts = int(time.time())
                params = {"symbols": "BTCUSDT_PERP.A", "interval": "5min",
                          "from": now_ts - 1800, "to": now_ts}

                async with session.get("https://api.coinalyze.net/v1/ohlcv-history",
                                       params=params, timeout=15) as r:
                    body = await r.text()
                    if r.status != 200:
                        log_cz.error(f"OHLCV {r.status}: {body[:200]}")
                    else:
                        data = json.loads(body) if body else []
                        if data and data[0].get("history"):
                            hist = data[0]["history"]
                            ratios = []
                            for row in hist[-6:]:
                                v = float(row.get("v", 0))
                                bv = float(row.get("bv", v/2))
                                if v > 0:
                                    ratios.append((bv - (v - bv)) / v)
                            state.cvd_delta = sum(ratios)/len(ratios) if ratios else 0.0
                            log_cz.debug(f"CVD ratio (last 6 bars avg) = {state.cvd_delta:+.3f}")
                        else:
                            log_cz.warning("OHLCV empty.")

                async with session.get("https://api.coinalyze.net/v1/open-interest-history",
                                       params=params, timeout=15) as r:
                    body = await r.text()
                    if r.status == 200:
                        data = json.loads(body) if body else []
                        if data and data[0].get("history") and len(data[0]["history"]) >= 6:
                            hist = data[0]["history"]
                            state.oi_chg_pct = pct_diff(float(hist[-1]["c"]), float(hist[-6]["o"]))

                async with session.get("https://api.coinalyze.net/v1/liquidation-history",
                                       params=params, timeout=15) as r:
                    body = await r.text()
                    if r.status == 200:
                        data = json.loads(body) if body else []
                        liq_l = liq_s = 0.0
                        if data and data[0].get("history"):
                            for row in data[0]["history"][-3:]:
                                liq_l += float(row.get("l", 0))
                                liq_s += float(row.get("s", 0))
                        state.liq_longs_30m = liq_l
                        state.liq_shorts_30m = liq_s

                state.ctx_ts = time.time()
                log_cz.info(
                    f"Context: CVD={state.cvd_delta*100:+.2f}% "
                    f"OI={state.oi_chg_pct:+.3f}% LiqL=${state.liq_longs_30m/1e6:.2f}M "
                    f"LiqS=${state.liq_shorts_30m/1e6:.2f}M"
                )
            except Exception as e:
                log_cz.error(f"Coinalyze exception: {type(e).__name__}: {e}")
            await asyncio.sleep(60)

# ------------------------- CoinGlass (Magnetism via STARTUP-friendly endpoints) -------------------------

async def cg_get(session: aiohttp.ClientSession, path: str, params: dict) -> Optional[dict]:
    url = f"{CG_BASE}{path}"
    try:
        async with session.get(url, params=params, timeout=20) as r:
            body = await r.text()
            if r.status != 200:
                log_cg.warning(f"{path} HTTP {r.status}: {body[:140]}")
                return None
            try:
                res = json.loads(body)
            except Exception:
                log_cg.warning(f"{path} not JSON: {body[:120]}")
                return None
            code = str(res.get("code", "?"))
            if code != "0":
                log_cg.warning(f"{path} code={code} msg={res.get('msg','')}")
                return None
            return res.get("data")
    except Exception as e:
        log_cg.warning(f"{path} exception: {type(e).__name__}: {e}")
        return None

async def cg_fetch_orderbook_depth(session: aiohttp.ClientSession):
    """Pull depth bid/ask totals at ±0.25%, ±0.5%, ±1.0% — STARTUP allows 30m interval."""
    bids: Dict[str, float] = {}
    asks: Dict[str, float] = {}
    for rng in ("0.25", "0.5", "1"):
        params = {"exchange": CG_EXCHANGE, "symbol": CG_PAIR,
                  "interval": "30m", "limit": "1", "range": rng}
        data = await cg_get(session, "/api/futures/orderbook/ask-bids-history", params)
        if data and isinstance(data, list) and data:
            row = data[-1]
            bids[rng] = float(row.get("bids_usd", 0))
            asks[rng] = float(row.get("asks_usd", 0))
    if bids and asks:
        state.cg_book_bids_usd = bids
        state.cg_book_asks_usd = asks
        state.cg_endpoints_ok.append("orderbook_depth")
        log_cg.info(
            f"Depth: ±0.25% bid=${bids.get('0.25',0)/1e6:.1f}M ask=${asks.get('0.25',0)/1e6:.1f}M | "
            f"±1% bid=${bids.get('1',0)/1e6:.1f}M ask=${asks.get('1',0)/1e6:.1f}M"
        )
        return True
    return False

async def cg_fetch_liq_history_cascade(session: aiohttp.ClientSession):
    """Detect liquidation cascade: last 30m vs trailing average (last 8 bars)."""
    params = {"exchange_list": "Binance,OKX,Bybit", "symbol": CG_COIN,
              "interval": "30m", "limit": "8"}
    data = await cg_get(session, "/api/futures/liquidation/aggregated-history", params)
    if not data or not isinstance(data, list) or len(data) < 4:
        return False
    longs = [float(r.get("aggregated_long_liquidation_usd", 0)) for r in data]
    shorts = [float(r.get("aggregated_short_liquidation_usd", 0)) for r in data]
    if not longs or not shorts: return False
    last_long, last_short = longs[-1], shorts[-1]
    avg_long = sum(longs[:-1]) / max(1, len(longs)-1)
    avg_short = sum(shorts[:-1]) / max(1, len(shorts)-1)
    # Spike score: if last_long >> avg, market just liquidated longs (bearish move) →
    # mean-revert is bullish bias, so cascade_score should be POSITIVE for long-cascade.
    # If short_liq spike → squeeze just happened, cascade_score NEGATIVE.
    long_spike = (last_long / avg_long) if avg_long > 0 else 1.0
    short_spike = (last_short / avg_short) if avg_short > 0 else 1.0
    score = 0.0
    if long_spike > 2.0 and last_long > 1_000_000:
        # bullish mean-revert pressure
        score = min(100.0, 30 * math.log(long_spike))
    elif short_spike > 2.0 and last_short > 1_000_000:
        score = -min(100.0, 30 * math.log(short_spike))
    state.cg_liq_cascade_score = score
    state.cg_endpoints_ok.append("liq_history")
    log_cg.info(
        f"LiqHist: long_last=${last_long/1e6:.2f}M (avg=${avg_long/1e6:.2f}M, x{long_spike:.1f}) | "
        f"short_last=${last_short/1e6:.2f}M (avg=${avg_short/1e6:.2f}M, x{short_spike:.1f}) → cascade={score:+.1f}"
    )
    return True

async def cg_fetch_liq_coin_list(session: aiohttp.ClientSession):
    """Live 1h/4h totals — no interval limit."""
    params = {"exchange_list": "Binance,OKX,Bybit"}
    data = await cg_get(session, "/api/futures/liquidation/coin-list", params)
    if not data or not isinstance(data, list): return False
    for row in data:
        if str(row.get("symbol", "")).upper() == CG_COIN.upper():
            state.cg_liq_long_1h = float(row.get("long_liquidation_usd_1h", 0))
            state.cg_liq_short_1h = float(row.get("short_liquidation_usd_1h", 0))
            state.cg_liq_long_4h = float(row.get("long_liquidation_usd_4h", 0))
            state.cg_liq_short_4h = float(row.get("short_liquidation_usd_4h", 0))
            state.cg_endpoints_ok.append("liq_coin_list")
            log_cg.info(
                f"LiqCoin BTC 1h: long=${state.cg_liq_long_1h/1e6:.2f}M "
                f"short=${state.cg_liq_short_1h/1e6:.2f}M | "
                f"4h: long=${state.cg_liq_long_4h/1e6:.2f}M short=${state.cg_liq_short_4h/1e6:.2f}M"
            )
            return True
    return False

async def cg_fetch_top_long_short(session: aiohttp.ClientSession):
    params = {"exchange": CG_EXCHANGE, "symbol": CG_PAIR, "interval": "30m", "limit": "1"}
    data = await cg_get(session, "/api/futures/top-long-short-position-ratio/history", params)
    if not data or not isinstance(data, list) or not data: return False
    row = data[-1]
    state.cg_top_long_pct = float(row.get("top_position_long_percent", 50))
    state.cg_endpoints_ok.append("top_ls")
    log_cg.info(f"TopTrader long%={state.cg_top_long_pct:.1f}")
    return True

async def coinglass_loop():
    if not COINGLASS_API_KEY:
        log_cg.error("COINGLASS_API_KEY missing. Synthetic map disabled.")
        return
    headers = {"CG-API-KEY": COINGLASS_API_KEY, "accept": "application/json"}
    async with aiohttp.ClientSession(headers=headers) as session:
        while not shutdown_event.is_set():
            state.cg_endpoints_ok = []
            ok_depth = await cg_fetch_orderbook_depth(session)
            ok_hist = await cg_fetch_liq_history_cascade(session)
            ok_list = await cg_fetch_liq_coin_list(session)
            ok_top = await cg_fetch_top_long_short(session)
            n_ok = sum([ok_depth, ok_hist, ok_list, ok_top])
            if n_ok > 0:
                state.map_ts = time.time()
                log_cg.info(f"Synthetic map updated. Sources OK: {n_ok}/4 = {state.cg_endpoints_ok}")
            else:
                log_cg.error("All STARTUP endpoints failed. Magnetism disabled this cycle.")
            await asyncio.sleep(120)  # every 2 min — STARTUP rate is generous enough

# ------------------------- CoinAPI (Stage 2 Impulse Confirmation) -------------------------

async def ca_get(session: aiohttp.ClientSession, path: str, params: dict = None) -> Optional[dict]:
    if not COINAPI_KEY: return None
    url = f"https://rest.coinapi.io{path}"
    headers = {"X-CoinAPI-Key": COINAPI_KEY}
    try:
        async with session.get(url, headers=headers, params=params, timeout=10) as r:
            if r.status != 200:
                body = await r.text()
                log_ca.warning(f"{path} HTTP {r.status}: {body[:100]}")
                return None
            return await r.json()
    except Exception as e:
        log_ca.error(f"CoinAPI exception {path}: {e}")
        return None

async def execute_stage2_confirmation(session: aiohttp.ClientSession, proposed_phase: str) -> Tuple[bool, float, str]:
    """
    Burst-fetches CoinAPI to confirm impulse.
    Returns: (is_confirmed, score, reason_msg)
    """
    if not COINAPI_KEY:
        log_ca.warning("COINAPI_KEY missing. Auto-confirming Stage 1 without Stage 2.")
        return True, 100.0, "Auto-confirmed (no key)"

    # Venues for aggregated CVD
    venues = ["BINANCE_FTS_PERP_BTC_USDT", "COINBASE_SPOT_BTC_USD", "KRAKEN_SPOT_BTC_USD"]
    
    cvd_tasks = [ca_get(session, f"/v1/trades/{v}/latest", {"limit": "100"}) for v in venues]
    ohlcv_task = ca_get(session, "/v1/ohlcv/BINANCE_FTS_PERP_BTC_USDT/latest", {"period_id": "1MIN", "limit": "6"})
    rate_task = ca_get(session, "/v1/exchangerate/BTC/USD")
    
    results = await asyncio.gather(*cvd_tasks, ohlcv_task, rate_task, return_exceptions=True)
    
    # 1. Aggregated CVD
    total_buy = 0.0
    total_sell = 0.0
    for i, v in enumerate(venues):
        res = results[i]
        if isinstance(res, list):
            for t in res:
                vol = float(t.get("price", 0)) * float(t.get("size", 0))
                if t.get("taker_side") == "BUY": total_buy += vol
                elif t.get("taker_side") == "SELL": total_sell += vol
    
    net_cvd = total_buy - total_sell
    state.ca_cvd_usd = net_cvd
    
    cvd_score = 0.0
    if total_buy + total_sell > 0:
        cvd_ratio = net_cvd / (total_buy + total_sell)
        # We want CVD to match proposed phase
        if proposed_phase == "BULLISH" and cvd_ratio > 0:
            cvd_score = min(100, cvd_ratio * 300) # +33% imbalance = 100 score
        elif proposed_phase == "BEARISH" and cvd_ratio < 0:
            cvd_score = min(100, abs(cvd_ratio) * 300)

    # 2. Volume Burst
    vol_score = 0.0
    ohlcv_res = results[-2]
    if isinstance(ohlcv_res, list) and len(ohlcv_res) >= 2:
        last_vol = float(ohlcv_res[0].get("volume_traded", 0))
        prev_vols = [float(c.get("volume_traded", 0)) for c in ohlcv_res[1:]]
        avg_vol = sum(prev_vols) / len(prev_vols) if prev_vols else 1.0
        multiplier = last_vol / avg_vol if avg_vol > 0 else 1.0
        state.ca_vol_multiplier = multiplier
        if multiplier > 1.2:
            vol_score = min(100, (multiplier - 1.0) * 100) # 2.0x = 100 score
    
    # 3. Premium vs Spot
    prem_score = 0.0
    rate_res = results[-1]
    if isinstance(rate_res, dict) and "rate" in rate_res and state.price > 0:
        spot_price = float(rate_res["rate"])
        premium_pct = (state.price - spot_price) / spot_price * 100
        state.ca_premium_pct = premium_pct
        if proposed_phase == "BULLISH" and premium_pct > 0.02:
            prem_score = min(100, premium_pct * 1000)
        elif proposed_phase == "BEARISH" and premium_pct < -0.02:
            prem_score = min(100, abs(premium_pct) * 1000)

    # Total Score
    final_score = (cvd_score * 0.5) + (vol_score * 0.3) + (prem_score * 0.2)
    state.ca_ts = time.time()
    
    msg = f"CVD=${net_cvd/1e6:+.2f}M Vol={state.ca_vol_multiplier:.1f}x Prem={state.ca_premium_pct:+.3f}%"
    log_ca.info(f"Stage 2 evaluated: Score={final_score:.1f}/100. {msg}")
    
    is_confirmed = final_score >= STAGE2_CONFIRM_THRESHOLD
    return is_confirmed, final_score, msg

# ------------------------- Evaluator (POLR with synthetic Magnetism) -------------------------

def compute_magnetism() -> Tuple[float, dict]:
    """
    Magnetism = combination of:
      A) Orderbook depth tilt at ±0.25% / ±1% (resting liquidity skew)   weight 0.35
      B) Liquidation cascade score (mean-revert pressure)                weight 0.30
      C) Live 1h liquidation imbalance (where pain just happened)        weight 0.20
      D) Top trader long% (smart money sentiment, 50% = neutral)         weight 0.15
    Returns score in [-100..+100] and breakdown.
    """
    parts: List[Tuple[str, float, float]] = []
    breakdown = {}

    # A) Depth tilt — average across depths
    if state.cg_book_bids_usd and state.cg_book_asks_usd:
        tilts = []
        for k in state.cg_book_bids_usd:
            b = state.cg_book_bids_usd.get(k, 0)
            a = state.cg_book_asks_usd.get(k, 0)
            if b + a > 0:
                tilts.append((b - a) / (b + a))
        if tilts:
            depth_score = soft_scale(sum(tilts) / len(tilts), scale=2.0)
            parts.append(("depth", depth_score, 0.35))
            breakdown["depth"] = depth_score

    # B) Liquidation cascade
    if state.cg_liq_cascade_score != 0.0 or "liq_history" in state.cg_endpoints_ok:
        parts.append(("cascade", state.cg_liq_cascade_score, 0.30))
        breakdown["cascade"] = state.cg_liq_cascade_score

    # C) Live 1h liq imbalance: more longs liquidated → bullish mean-revert
    if state.cg_liq_long_1h + state.cg_liq_short_1h > 100_000:
        tot = state.cg_liq_long_1h + state.cg_liq_short_1h
        # IF longs liquidated more, that creates downside that often mean-reverts up.
        liq_score = soft_scale((state.cg_liq_long_1h - state.cg_liq_short_1h) / tot, scale=2.0)
        parts.append(("liq1h", liq_score, 0.20))
        breakdown["liq1h"] = liq_score

    # D) Top trader long% (50% neutral, scale ±50%)
    if state.cg_top_long_pct > 0:
        # If 70% top traders are long, they often get stopped → contrarian bearish bias
        # But for "Magnetism" interpretation we want where price is *being pulled*.
        # Use as a soft sentiment overlay: 50% center, scale.
        smart_dev = (state.cg_top_long_pct - 50.0) / 50.0
        smart_score = -soft_scale(smart_dev, scale=2.0) * 0.5  # contrarian, half-weight
        parts.append(("smart", smart_score, 0.15))
        breakdown["smart"] = smart_score

    if not parts:
        return 0.0, breakdown
    total_w = sum(w for _, _, w in parts)
    mag = sum(s * (w / total_w) for _, s, w in parts) if total_w > 0 else 0.0
    return mag, breakdown

def compute_polr() -> dict:
    components: List[Tuple[str, float, float]] = []

    # Magnetism (synthetic)
    mag, mag_breakdown = compute_magnetism()
    if mag_breakdown:
        components.append(("mag", mag, 0.40))

    # Friction = top20 imbalance + walls (walls weighted by their share of top20 depth)
    imb = state.book_imbalance
    fric_imb = soft_scale(imb, scale=3.0)
    walls_total = state.bids_walls_btc + state.asks_walls_btc
    top20_total = state.bids_total_top20 + state.asks_total_top20
    if walls_total > 0:
        raw_wall_bias = (state.bids_walls_btc - state.asks_walls_btc) / walls_total * 100
        # Soften: weight wall bias by how dominant walls are vs full top20
        wall_share = min(1.0, walls_total / max(top20_total, 1.0))
        wall_bias = raw_wall_bias * wall_share
    else:
        wall_bias = 0.0
    fric = 0.7 * fric_imb + 0.3 * wall_bias
    if state.book_msg_count > 0:
        components.append(("fric", fric, 0.40))

    # Fuel = CVD + OI
    fuel_cvd = soft_scale(state.cvd_delta, scale=4.0)
    fuel_oi = soft_scale(state.oi_chg_pct / 100.0, scale=8.0)
    fuel = 0.7 * fuel_cvd + 0.3 * fuel_oi
    if state.ctx_ts > 0:
        components.append(("fuel", fuel, 0.20))

    total_w = sum(w for _, _, w in components)
    polr = sum(s * (w / total_w) for _, s, w in components) if total_w > 0 else 0.0

    return {
        "polr": polr,
        "mag": mag, "mag_breakdown": mag_breakdown,
        "fric_imb": fric_imb, "wall_bias": wall_bias, "fric": fric,
        "fuel_cvd": fuel_cvd, "fuel_oi": fuel_oi, "fuel": fuel,
        "imbalance": imb,
        "components_used": [c[0] for c in components],
    }

def readiness() -> str:
    missing = []
    if state.price <= 0: missing.append("orderbook")
    if state.ctx_ts == 0: missing.append("coinalyze-context")
    if state.map_ts == 0: missing.append("coinglass-map")
    return ",".join(missing) if missing else "ALL_OK"

async def evaluator_loop():
    global current_phase, last_alert_ts
    next_heartbeat = 0.0
    async with aiohttp.ClientSession() as session:
        while not shutdown_event.is_set():
            await asyncio.sleep(EVAL_INTERVAL)
            now = time.time()
            ready = readiness()
            if state.price <= 0:
                log_ev.info(f"Waiting for orderbook. Missing: {ready}.")
                continue

            r = compute_polr()
            polr = r["polr"]
            new_phase = "NEUTRAL"
            if polr >= POLR_THRESHOLD: new_phase = "BULLISH"
            elif polr <= -POLR_THRESHOLD: new_phase = "BEARISH"

            if VERBOSE or now >= next_heartbeat:
                next_heartbeat = now + HEARTBEAT_SEC
                mb = r["mag_breakdown"]
                mb_str = " ".join(f"{k}={v:+.0f}" for k, v in mb.items()) if mb else "none"
                log_ev.info(
                    f"TICK ready={ready} cg_ok={state.cg_endpoints_ok} "
                    f"price={state.price:.2f} | POLR={polr:+.1f} "
                    f"[mag={r['mag']:+.1f} ({mb_str}) "
                    f"fric={r['fric']:+.1f} (imb={r['fric_imb']:+.1f},wall={r['wall_bias']:+.1f}) "
                    f"fuel={r['fuel']:+.1f} (cvd={r['fuel_cvd']:+.1f},oi={r['fuel_oi']:+.1f})] "
                    f"phase={current_phase}->{new_phase} used={r['components_used']}"
                )

            if new_phase == current_phase:
                log_ev.debug(f"No phase change. |POLR|={abs(polr):.1f} vs {POLR_STAGE1_THRESHOLD}")
                continue

            since_last = now - last_alert_ts
            if since_last < DEBOUNCE_SEC and last_alert_ts > 0:
                log_ev.info(f"Would shift to {new_phase} (POLR={polr:+.1f}) but debounced "
                            f"({int(since_last)}s/{DEBOUNCE_SEC}s).")
                continue

            # Stage 2 Escalation
            log_ev.info(f"Stage 1 trigger: {new_phase} (POLR={polr:+.1f}). Escalating to Stage 2 (CoinAPI)...")
            stage2_ok, s2_score, s2_msg = await execute_stage2_confirmation(session, new_phase)
            if not stage2_ok:
                log_ev.warning(f"Stage 2 REJECTED impulse: score={s2_score:.1f} < {STAGE2_CONFIRM_THRESHOLD}. Reason: {s2_msg}")
                # We do not change phase, wait for next tick.
                # To avoid spamming CoinAPI, apply a short penalty debounce (e.g. 3 mins)
                last_alert_ts = now - DEBOUNCE_SEC + 180 
                continue
            
            log_ev.info(f"Stage 2 CONFIRMED impulse! Score={s2_score:.1f}")

            current_phase = new_phase
            last_alert_ts = now
            emoji = "🟢" if new_phase == "BULLISH" else "🔴" if new_phase == "BEARISH" else "⚪"

            # Build map summary
            depth_line = ""
            if state.cg_book_bids_usd:
                b25 = state.cg_book_bids_usd.get("0.25", 0) / 1e6
                a25 = state.cg_book_asks_usd.get("0.25", 0) / 1e6
                b1 = state.cg_book_bids_usd.get("1", 0) / 1e6
                a1 = state.cg_book_asks_usd.get("1", 0) / 1e6
                depth_line = (f"<b>CG Depth:</b> ±0.25% bid ${b25:.1f}M / ask ${a25:.1f}M | "
                              f"±1% bid ${b1:.1f}M / ask ${a1:.1f}M\n")
            liq_line = ""
            if state.cg_liq_long_1h or state.cg_liq_short_1h:
                liq_line = (f"<b>CG Liq 1h:</b> long ${state.cg_liq_long_1h/1e6:.2f}M | "
                            f"short ${state.cg_liq_short_1h/1e6:.2f}M\n")

            msg = (
                f"{emoji} <b>INTRADAY PHASE SHIFT: {new_phase}</b>\n\n"
                f"Price: {state.price:.2f}\n"
                f"POLR: <b>{polr:+.1f}</b>\n"
                f"  • Magnetism : {r['mag']:+.1f}  ({mb_str if (mb_str:=' '.join(f'{k} {v:+.0f}' for k,v in r['mag_breakdown'].items())) else 'n/a'})\n"
                f"  • Friction  : {r['fric']:+.1f}  (imb {r['fric_imb']:+.1f} / wall {r['wall_bias']:+.1f})\n"
                f"  • Fuel      : {r['fuel']:+.1f}  (cvd {r['fuel_cvd']:+.1f} / oi {r['fuel_oi']:+.1f})\n\n"
                f"{depth_line}{liq_line}"
                f"<b>Local Book (top20):</b> bids {state.bids_total_top20:.1f} | asks {state.asks_total_top20:.1f} "
                f"(imb {r['imbalance']*100:+.1f}%)\n"
                f"Walls: bid {state.bids_walls_btc:.1f} BTC | ask {state.asks_walls_btc:.1f} BTC\n"
                f"<b>Context:</b> CVD {state.cvd_delta*100:+.2f}% | OI {state.oi_chg_pct:+.2f}%\n"
                f"<b>TopTrader long:</b> {state.cg_top_long_pct:.1f}%\n\n"
                f"<b>Stage 2 Confirmation:</b>\n"
                f"Score: {s2_score:.1f} / 100\n"
                f"Cross-CVD: ${state.ca_cvd_usd/1e6:+.2f}M\n"
                f"Vol Burst: {state.ca_vol_multiplier:.1f}x\n"
                f"Futures Prem: {state.ca_premium_pct:+.3f}%\n"
            )

            log_ev.info(f"PHASE -> {new_phase} (POLR={polr:+.1f}). Sending Telegram…")
            await send_telegram(session, msg)

# ------------------------- Main -------------------------

async def main():
    logger.info("Starting BTC Triad Bot V4 (Two-Stage Impulse Detector)…")
    logger.info(
        "Config:\n"
        f"  COINALYZE_API_KEY   = {mask(COINALYZE_API_KEY)}\n"
        f"  COINGLASS_API_KEY   = {mask(COINGLASS_API_KEY)}\n"
        f"  TELEGRAM_BOT_TOKEN  = {mask(TELEGRAM_BOT_TOKEN)}\n"
        f"  TELEGRAM_CHAT_ID    = {TELEGRAM_CHAT_ID or '<MISSING>'}\n"
        f"  WALL_MIN_BTC        = {WALL_MIN_BTC}\n"
        f"  POLR_STAGE1_THRESHOLD= {POLR_STAGE1_THRESHOLD}\n"
        f"  STAGE2_CONFIRM_SCORE= {STAGE2_CONFIRM_THRESHOLD}\n"
        f"  CG_EXCHANGE/PAIR    = {CG_EXCHANGE}/{CG_PAIR}/{CG_COIN}\n"
        f"  EVAL_INTERVAL       = {EVAL_INTERVAL}s\n"
        f"  DEBOUNCE_SEC        = {DEBOUNCE_SEC}s"
    )

    async with aiohttp.ClientSession() as session:
        if TG_TEST_ON_START:
            await send_telegram(session, "✅ <b>BTC Triad Bot V4</b> started (Two-Stage Impulse Detector).")

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try: loop.add_signal_handler(sig, shutdown_event.set)
        except NotImplementedError: pass

    tasks = [
        asyncio.create_task(binance_ws_loop(), name="binance_ws"),
        asyncio.create_task(coinalyze_loop(), name="coinalyze"),
        asyncio.create_task(coinglass_loop(), name="coinglass"),
        asyncio.create_task(evaluator_loop(), name="evaluator"),
    ]
    await shutdown_event.wait()
    for t in tasks: t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

if __name__ == "__main__":
    asyncio.run(main())
