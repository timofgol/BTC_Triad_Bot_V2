#!/usr/bin/env python3
"""
BTC Triad Bot V3.1 (Proactive Market State Evaluator + Verbose Diagnostics)

Verbose mode: when LOG_LEVEL=DEBUG (or VERBOSE=true in .env), the bot logs
every step of the pipeline so you can immediately see WHY no signals are
being produced (no orderbook data, missing API key, low POLR, debounce, etc.).
"""

import asyncio
import json
import logging
import os
import signal
import time
from dataclasses import dataclass, field
from typing import Dict, Optional

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
    return v.strip().lower() in {"1","true","yes","y","on"}

COINALYZE_API_KEY = env_str("COINALYZE_API_KEY")
COINGLASS_API_KEY = env_str("COINGLASS_API_KEY")
TELEGRAM_BOT_TOKEN = env_str("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = env_str("TELEGRAM_CHAT_ID")

WALL_MIN_BTC = env_float("WALL_MIN_BTC", 20.0)
LIQ_CLUSTER_MIN_VAL = env_float("LIQ_CLUSTER_MIN_VAL", 1_000_000)
POLR_THRESHOLD = env_float("POLR_THRESHOLD", 30.0)
CLUSTER_DISSOLVE_PCT = env_float("CLUSTER_DISSOLVE_PCT", 0.3)

VERBOSE = env_bool("VERBOSE", True)             # detailed logging on by default
HEARTBEAT_SEC = env_int("HEARTBEAT_SEC", 30)    # how often to print state snapshot
EVAL_INTERVAL = env_int("EVAL_INTERVAL", 15)    # evaluator tick
DEBOUNCE_SEC = env_int("DEBOUNCE_SEC", 600)     # min seconds between alerts
TG_TEST_ON_START = env_bool("TG_TEST_ON_START", True)

LEVEL = logging.DEBUG if VERBOSE else logging.INFO
logging.basicConfig(
    level=LEVEL,
    format="%(asctime)s [%(levelname)s] %(name)s :: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("bot")
log_ws  = logging.getLogger("bot.ws")
log_cz  = logging.getLogger("bot.coinalyze")
log_cg  = logging.getLogger("bot.coinglass")
log_ev  = logging.getLogger("bot.eval")
log_tg  = logging.getLogger("bot.telegram")

# ------------------------- State -------------------------

@dataclass
class MarketState:
    price: float = 0.0
    cvd_delta: float = 0.0
    oi_chg_pct: float = 0.0
    liq_longs: float = 0.0
    liq_shorts: float = 0.0
    bids_vol: float = 0.0
    asks_vol: float = 0.0
    nearest_bid_wall: float = 0.0
    nearest_ask_wall: float = 0.0
    up_clusters: Dict[float, float] = field(default_factory=dict)
    down_clusters: Dict[float, float] = field(default_factory=dict)
    book_ts: float = 0.0
    ctx_ts: float = 0.0
    map_ts: float = 0.0
    book_msg_count: int = 0

state = MarketState()
prev_clusters: Dict[float, float] = {}
current_phase = "NEUTRAL"
last_alert_ts = 0.0
last_polr_logged = 0.0
shutdown_event = asyncio.Event()

# ------------------------- Helpers -------------------------

def pct_diff(new: float, old: float) -> float:
    if old == 0: return 0.0
    return (new - old) / abs(old) * 100.0

async def send_telegram(session: aiohttp.ClientSession, text: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log_tg.warning("Telegram not configured (token or chat_id missing). Message NOT sent.")
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
            else:
                log_tg.error(f"Telegram FAIL status={r.status} body={body[:200]}")
                return False
    except Exception as e:
        log_tg.error(f"Telegram exception: {type(e).__name__}: {e}")
        return False

def env_summary() -> str:
    def mask(v: str) -> str:
        if not v: return "<MISSING>"
        return f"{v[:4]}…{v[-4:]} (len={len(v)})"
    return (
        f"\n  COINALYZE_API_KEY  = {mask(COINALYZE_API_KEY)}"
        f"\n  COINGLASS_API_KEY  = {mask(COINGLASS_API_KEY)}"
        f"\n  TELEGRAM_BOT_TOKEN = {mask(TELEGRAM_BOT_TOKEN)}"
        f"\n  TELEGRAM_CHAT_ID   = {TELEGRAM_CHAT_ID or '<MISSING>'}"
        f"\n  WALL_MIN_BTC       = {WALL_MIN_BTC}"
        f"\n  LIQ_CLUSTER_MIN_VAL= {LIQ_CLUSTER_MIN_VAL}"
        f"\n  POLR_THRESHOLD     = {POLR_THRESHOLD}"
        f"\n  CLUSTER_DISSOLVE   = {CLUSTER_DISSOLVE_PCT}"
        f"\n  VERBOSE            = {VERBOSE}"
        f"\n  EVAL_INTERVAL      = {EVAL_INTERVAL}s"
        f"\n  DEBOUNCE_SEC       = {DEBOUNCE_SEC}s"
    )

# ------------------------- Binance WS (Orderbook) -------------------------

async def binance_ws_loop():
    url = "wss://fstream.binance.com/stream?streams=btcusdt@depth20@100ms"
    backoff = 1
    while not shutdown_event.is_set():
        try:
            log_ws.info(f"Connecting to Binance Futures WS: {url}")
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(url, heartbeat=20, timeout=15) as ws:
                    log_ws.info("Connected. Receiving depth20@100ms…")
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
                                bid_walls = [w for w in bids if w[1] >= WALL_MIN_BTC]
                                ask_walls = [w for w in asks if w[1] >= WALL_MIN_BTC]

                                state.bids_vol = sum(w[1] for w in bid_walls)
                                state.asks_vol = sum(w[1] for w in ask_walls)
                                state.nearest_bid_wall = bid_walls[0][0] if bid_walls else 0.0
                                state.nearest_ask_wall = ask_walls[0][0] if ask_walls else 0.0
                                state.book_ts = time.time()
                                state.book_msg_count += 1

                                # Periodic verbose snapshot every 30s
                                now = time.time()
                                if VERBOSE and (now - last_summary) >= 30:
                                    last_summary = now
                                    top_bid_qty = max((q for _, q in bids), default=0)
                                    top_ask_qty = max((q for _, q in asks), default=0)
                                    log_ws.debug(
                                        f"Book #{state.book_msg_count} | mid={state.price:.2f} | "
                                        f"max bid qty={top_bid_qty:.2f} BTC | max ask qty={top_ask_qty:.2f} BTC | "
                                        f"walls(>{WALL_MIN_BTC}): bids={len(bid_walls)} asks={len(ask_walls)}"
                                    )
                                    if not bid_walls and not ask_walls:
                                        log_ws.debug(
                                            f"No walls >= {WALL_MIN_BTC} BTC in top 20. "
                                            f"Largest top-20 sizes: bid={top_bid_qty:.2f}, ask={top_ask_qty:.2f}. "
                                            f"Consider lowering WALL_MIN_BTC."
                                        )
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            log_ws.warning(f"WS closed/err: type={msg.type}")
                            break
        except Exception as e:
            log_ws.error(f"Binance WS error: {type(e).__name__}: {e}")

        log_ws.info(f"Reconnecting in {backoff}s…")
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 30)

# ------------------------- Coinalyze (Context) -------------------------

async def coinalyze_loop():
    if not COINALYZE_API_KEY:
        log_cz.error("COINALYZE_API_KEY missing. Context data DISABLED. POLR will only use Map+Friction.")
        return

    headers = {"api_key": COINALYZE_API_KEY}
    async with aiohttp.ClientSession(headers=headers) as session:
        while not shutdown_event.is_set():
            try:
                now_ts = int(time.time())
                params = {"symbols": "BTCUSDT_PERP.A", "interval": "5min",
                          "from": now_ts - 1800, "to": now_ts}
                log_cz.debug(f"Fetching OHLCV/OI/Liq with params={params}")

                # OHLCV (CVD)
                async with session.get("https://api.coinalyze.net/v1/ohlcv-history",
                                       params=params, timeout=15) as r:
                    body = await r.text()
                    if r.status != 200:
                        log_cz.error(f"OHLCV HTTP {r.status}: {body[:200]}")
                    else:
                        try:
                            data = json.loads(body)
                        except Exception as e:
                            log_cz.error(f"OHLCV JSON parse error: {e}; body[:200]={body[:200]}")
                            data = []
                        if not data or not data[0].get("history"):
                            log_cz.warning(f"OHLCV empty. data_len={len(data)}")
                        else:
                            hist = data[0]["history"]
                            log_cz.debug(f"OHLCV bars received: {len(hist)} (showing last 1: {hist[-1]})")
                            signed = []
                            missing_bv = 0
                            for row in hist[-6:]:
                                v = float(row.get("v", 0))
                                if "bv" not in row:
                                    missing_bv += 1
                                bv = float(row.get("bv", v / 2))
                                if v > 0:
                                    signed.append((bv - (v - bv)) / v)
                            state.cvd_delta = sum(signed) / len(signed) if signed else 0.0
                            if missing_bv:
                                log_cz.warning(f"{missing_bv}/6 OHLCV bars had no `bv` field (CVD will be muted).")
                            log_cz.debug(f"CVD delta (avg over last 6 bars) = {state.cvd_delta:+.4f}")

                # OI
                async with session.get("https://api.coinalyze.net/v1/open-interest-history",
                                       params=params, timeout=15) as r:
                    body = await r.text()
                    if r.status != 200:
                        log_cz.error(f"OI HTTP {r.status}: {body[:200]}")
                    else:
                        try:
                            data = json.loads(body)
                        except Exception as e:
                            log_cz.error(f"OI JSON parse error: {e}")
                            data = []
                        if data and data[0].get("history") and len(data[0]["history"]) >= 6:
                            hist = data[0]["history"]
                            old_oi = float(hist[-6]["o"])
                            new_oi = float(hist[-1]["c"])
                            state.oi_chg_pct = pct_diff(new_oi, old_oi)
                            log_cz.debug(f"OI change last 30min: {state.oi_chg_pct:+.3f}% "
                                         f"({old_oi:.0f} -> {new_oi:.0f})")
                        else:
                            log_cz.warning("OI history insufficient (<6 bars).")

                # Liquidations
                async with session.get("https://api.coinalyze.net/v1/liquidation-history",
                                       params=params, timeout=15) as r:
                    body = await r.text()
                    if r.status == 200:
                        try:
                            data = json.loads(body)
                        except Exception:
                            data = []
                        liq_l = liq_s = 0.0
                        if data and data[0].get("history"):
                            for row in data[0]["history"][-3:]:
                                liq_l += float(row.get("l", 0))
                                liq_s += float(row.get("s", 0))
                        state.liq_longs = liq_l
                        state.liq_shorts = liq_s
                        log_cz.debug(f"Liq last 15min: longs=${liq_l:,.0f} shorts=${liq_s:,.0f}")
                    else:
                        log_cz.error(f"Liq HTTP {r.status}: {body[:200]}")

                state.ctx_ts = time.time()
                log_cz.info(
                    f"Context updated: price~{state.price:.0f} | "
                    f"CVD={state.cvd_delta*100:+.2f}% | OI={state.oi_chg_pct:+.3f}% | "
                    f"LiqL=${state.liq_longs/1e6:.2f}M LiqS=${state.liq_shorts/1e6:.2f}M"
                )
            except Exception as e:
                log_cz.error(f"Coinalyze exception: {type(e).__name__}: {e}")
            await asyncio.sleep(60)

# ------------------------- CoinGlass (Map) -------------------------

async def coinglass_loop():
    global prev_clusters
    if not COINGLASS_API_KEY:
        log_cg.error("COINGLASS_API_KEY missing. Heatmap DISABLED. POLR will only use Friction+Fuel.")
        return

    headers = {"CG-API-KEY": COINGLASS_API_KEY, "accept": "application/json"}
    url = "https://open-api-v4.coinglass.com/api/futures/liquidation/aggregated-heatmap/model1"
    async with aiohttp.ClientSession(headers=headers) as session:
        while not shutdown_event.is_set():
            try:
                params = {"symbol": "BTC", "range": "24h"}
                log_cg.debug(f"GET {url} params={params}")
                async with session.get(url, params=params, timeout=20) as r:
                    body = await r.text()
                    if r.status != 200:
                        log_cg.error(f"CoinGlass HTTP {r.status}: {body[:200]}")
                        await asyncio.sleep(60); continue
                    try:
                        res = json.loads(body)
                    except Exception as e:
                        log_cg.error(f"CoinGlass JSON parse error: {e}; body[:200]={body[:200]}")
                        await asyncio.sleep(60); continue

                    code = str(res.get("code", "?"))
                    if code != "0":
                        log_cg.error(f"CoinGlass API error code={code} msg={res.get('msg')}")
                        await asyncio.sleep(60); continue

                    data = res.get("data") or {}
                    y_axis = data.get("y_axis", [])
                    liq_data = data.get("liquidation_leverage_data", [])
                    log_cg.debug(f"Heatmap received: y_axis={len(y_axis)} levels, "
                                 f"liq_data={len(liq_data)} cells")

                    if not y_axis or not liq_data:
                        log_cg.warning("Heatmap returned empty arrays. Skipping.")
                        await asyncio.sleep(60); continue
                    if state.price <= 0:
                        log_cg.warning("Skipping heatmap parse: orderbook price not yet available.")
                        await asyncio.sleep(60); continue

                    max_x = max(d[0] for d in liq_data)
                    clusters: Dict[float, float] = {}
                    for x, y, val in liq_data:
                        if x >= max_x - 5:
                            try:
                                p = float(y_axis[y])
                            except (IndexError, TypeError):
                                continue
                            clusters[p] = clusters.get(p, 0.0) + float(val)

                    up_c, down_c = {}, {}
                    for p, v in clusters.items():
                        if v < LIQ_CLUSTER_MIN_VAL: continue
                        if p > state.price * 1.002: up_c[p] = v
                        elif p < state.price * 0.998: down_c[p] = v

                    prev_clusters = {**state.up_clusters, **state.down_clusters}
                    state.up_clusters = up_c
                    state.down_clusters = down_c
                    state.map_ts = time.time()

                    top_up = sorted(up_c.items(), key=lambda kv: -kv[1])[:3]
                    top_dn = sorted(down_c.items(), key=lambda kv: -kv[1])[:3]
                    log_cg.info(
                        f"Map updated: ref={state.price:.0f} | "
                        f"up_clusters={len(up_c)} (top: {[(int(p),int(v)) for p,v in top_up]}) | "
                        f"down_clusters={len(down_c)} (top: {[(int(p),int(v)) for p,v in top_dn]})"
                    )
                    if not up_c and not down_c:
                        log_cg.warning(
                            f"No clusters >= ${LIQ_CLUSTER_MIN_VAL:,.0f} near price. "
                            f"Total raw clusters parsed: {len(clusters)}. "
                            f"Consider lowering LIQ_CLUSTER_MIN_VAL."
                        )
            except Exception as e:
                log_cg.error(f"CoinGlass exception: {type(e).__name__}: {e}")
            await asyncio.sleep(300)

# ------------------------- Evaluator (POLR + Phase) -------------------------

def compute_polr() -> dict:
    sum_up = sum(state.up_clusters.values())
    sum_down = sum(state.down_clusters.values())
    tot_mag = sum_up + sum_down
    mag_score = ((sum_up - sum_down) / tot_mag * 100) if tot_mag > 0 else 0.0

    tot_fric = state.bids_vol + state.asks_vol
    fric_score = ((state.bids_vol - state.asks_vol) / tot_fric * 100) if tot_fric > 0 else 0.0

    fuel_score = max(-100, min(100, state.cvd_delta * 1000))

    polr = (0.4 * mag_score) + (0.4 * fric_score) + (0.2 * fuel_score)
    return {
        "polr": polr,
        "mag": mag_score, "fric": fric_score, "fuel": fuel_score,
        "sum_up": sum_up, "sum_down": sum_down,
        "bids": state.bids_vol, "asks": state.asks_vol,
        "cvd": state.cvd_delta,
    }

def readiness() -> str:
    """Return human-readable readiness string explaining what data is missing."""
    missing = []
    if state.price <= 0: missing.append("orderbook")
    if state.ctx_ts == 0: missing.append("coinalyze-context")
    if state.map_ts == 0: missing.append("coinglass-map")
    return ",".join(missing) if missing else "ALL_OK"

async def evaluator_loop():
    global current_phase, last_alert_ts, last_polr_logged
    next_heartbeat = 0.0
    async with aiohttp.ClientSession() as session:
        while not shutdown_event.is_set():
            await asyncio.sleep(EVAL_INTERVAL)
            now = time.time()

            # Readiness check
            ready = readiness()
            if state.price <= 0:
                log_ev.info(f"Waiting for orderbook (price=0). Missing: {ready}.")
                continue

            # Cluster Dissolution Check
            dissolved = ""
            for p, old_v in prev_clusters.items():
                new_v = state.up_clusters.get(p, state.down_clusters.get(p, 0.0))
                if new_v < old_v * (1 - CLUSTER_DISSOLVE_PCT) and old_v > LIQ_CLUSTER_MIN_VAL * 2:
                    dist = pct_diff(p, state.price)
                    if abs(dist) < 1.0:
                        side = "Upside" if p > state.price else "Downside"
                        dissolved = f"⚠️ {side} cluster at {p:.0f} dissolved! (-{(old_v-new_v)/1e6:.1f}M)"
                        log_ev.info(f"Dissolution detected: {dissolved}")
                        break

            # Compute POLR
            r = compute_polr()
            polr = r["polr"]

            # New phase
            new_phase = "NEUTRAL"
            if polr >= POLR_THRESHOLD: new_phase = "BULLISH"
            elif polr <= -POLR_THRESHOLD: new_phase = "BEARISH"

            # Detailed reasoning every tick (verbose) or every HEARTBEAT_SEC
            if VERBOSE or now >= next_heartbeat:
                next_heartbeat = now + HEARTBEAT_SEC
                log_ev.info(
                    f"TICK ready={ready} price={state.price:.2f} | "
                    f"POLR={polr:+.1f} (mag={r['mag']:+.1f}, fric={r['fric']:+.1f}, fuel={r['fuel']:+.1f}) | "
                    f"phase={current_phase}->{new_phase} | "
                    f"bids={r['bids']:.0f} asks={r['asks']:.0f} | "
                    f"sumUp=${r['sum_up']/1e6:.1f}M sumDn=${r['sum_down']/1e6:.1f}M | "
                    f"CVD={r['cvd']*100:+.2f}%"
                )

            # Gating reasons
            if new_phase == current_phase:
                log_ev.debug(f"No phase change (still {current_phase}). |POLR|={abs(polr):.1f} vs threshold={POLR_THRESHOLD}")
                continue

            since_last = now - last_alert_ts
            if since_last < DEBOUNCE_SEC and last_alert_ts > 0:
                log_ev.info(
                    f"Phase WOULD change to {new_phase} (POLR={polr:+.1f}), "
                    f"but debounced ({int(since_last)}s/{DEBOUNCE_SEC}s)."
                )
                continue

            current_phase = new_phase
            last_alert_ts = now
            emoji = "🟢" if new_phase == "BULLISH" else "🔴" if new_phase == "BEARISH" else "⚪"
            nearest_up = min(state.up_clusters.keys()) if state.up_clusters else 0
            nearest_dn = max(state.down_clusters.keys()) if state.down_clusters else 0

            msg = (
                f"{emoji} <b>INTRADAY PHASE SHIFT: {new_phase}</b>\n\n"
                f"Price: {state.price:.2f}\n"
                f"POLR Score: {polr:+.1f}\n"
                f"  • Magnetism : {r['mag']:+.1f}\n"
                f"  • Friction  : {r['fric']:+.1f}\n"
                f"  • Fuel (CVD): {r['fuel']:+.1f}\n\n"
                f"<b>Path Analysis:</b>\n"
                f"• Up target  : {nearest_up:.0f}\n"
                f"• Down target: {nearest_dn:.0f}\n"
                f"• Bid support: {state.bids_vol:.1f} BTC (nearest {state.nearest_bid_wall:.0f})\n"
                f"• Ask resist.: {state.asks_vol:.1f} BTC (nearest {state.nearest_ask_wall:.0f})\n"
                f"• CVD: {state.cvd_delta*100:+.2f}% | OI: {state.oi_chg_pct:+.2f}%\n"
            )
            if dissolved: msg += f"\n{dissolved}"

            log_ev.info(f"PHASE SHIFT -> {new_phase} (POLR={polr:+.1f}). Sending Telegram…")
            ok = await send_telegram(session, msg)
            if not ok:
                log_ev.error("Phase shift NOT delivered to Telegram (see telegram logs above).")

# ------------------------- Main -------------------------

async def main():
    logger.info("Starting BTC Triad Bot V3.1 (Verbose Diagnostics)…")
    logger.info(f"Configuration:{env_summary()}")

    async with aiohttp.ClientSession() as session:
        if TG_TEST_ON_START:
            logger.info("Sending Telegram start-up test message…")
            await send_telegram(session, "✅ <b>BTC Triad Bot V3.1</b> started (Verbose Diagnostics).")

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try: loop.add_signal_handler(sig, shutdown_event.set)
        except NotImplementedError: pass

    tasks = [
        asyncio.create_task(binance_ws_loop(),  name="binance_ws"),
        asyncio.create_task(coinalyze_loop(),   name="coinalyze"),
        asyncio.create_task(coinglass_loop(),   name="coinglass"),
        asyncio.create_task(evaluator_loop(),   name="evaluator"),
    ]
    logger.info(f"Tasks launched: {[t.get_name() for t in tasks]}")
    await shutdown_event.wait()
    for t in tasks: t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    logger.info("Bot shut down cleanly.")

if __name__ == "__main__":
    asyncio.run(main())
