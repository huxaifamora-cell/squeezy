"""
squeezy_bridge.py
=================
Run this on your Windows VPS alongside MT5.
It connects to your Render.com backend via WebSocket and acts as the
two-way bridge between MT5 (via MetaTrader5 Python API) and the web dashboard.

INSTALL (on VPS):
  pip install MetaTrader5 websocket-client requests

USAGE:
  python bridge.py
"""

import json
import time
import threading
import logging
import sys
import os

import MetaTrader5 as mt5
import websocket

# ─────────────────────────────────────────
#  CONFIG  —  edit these before running
# ─────────────────────────────────────────
RENDER_WS_URL  = "wss://YOUR-APP.onrender.com/bridge"   # your Render.com WS URL
BRIDGE_SECRET  = "CHANGE_THIS_SECRET"                   # must match server .env
POLL_INTERVAL  = 2          # seconds between MT5 position polls
RECONNECT_WAIT = 5          # seconds before reconnecting on drop

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [BRIDGE] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────
#  MT5 HELPERS
# ─────────────────────────────────────────

def mt5_connect():
    if not mt5.initialize():
        log.error("MT5 initialize() failed: %s", mt5.last_error())
        return False
    info = mt5.account_info()
    if info is None:
        log.error("Cannot get account info: %s", mt5.last_error())
        return False
    log.info("MT5 connected — account %s | balance $%.2f", info.login, info.balance)
    return True


def get_account_snapshot():
    info = mt5.account_info()
    if info is None:
        return {}
    return {
        "balance":  round(info.balance, 2),
        "equity":   round(info.equity, 2),
        "floating": round(info.profit, 2),
        "margin":   round(info.margin, 2),
        "currency": info.currency,
        "leverage": info.leverage,
        "server":   info.server,
        "login":    info.login,
    }


def get_positions():
    positions = mt5.positions_get()
    if positions is None:
        return []
    result = []
    for p in positions:
        result.append({
            "ticket":  p.ticket,
            "symbol":  p.symbol,
            "dir":     "BUY" if p.type == mt5.ORDER_TYPE_BUY else "SELL",
            "lot":     p.volume,
            "open_price": round(p.price_open, 5),
            "current_price": round(p.price_current, 5),
            "pnl":     round(p.profit, 2),
            "sl":      round(p.sl, 5),
            "tp":      round(p.tp, 5),
            "open_time": p.time,
        })
    return result


def fire_trade(symbol, direction, lot, sl_usd, tp_usd):
    """Open a market order. SL/TP are in USD profit terms (converted to price)."""
    tick = mt5.symbol_info_tick(symbol)
    if tick is None:
        return {"ok": False, "error": f"No tick for {symbol}"}

    order_type = mt5.ORDER_TYPE_BUY if direction == "BUY" else mt5.ORDER_TYPE_SELL
    price      = tick.ask if direction == "BUY" else tick.bid

    # Convert USD SL/TP to price distance (approximate via contract value)
    sym_info = mt5.symbol_info(symbol)
    if sym_info is None:
        return {"ok": False, "error": f"Symbol info unavailable for {symbol}"}

    # Price per pip in account currency
    pip_value = sym_info.trade_tick_value * lot
    sl_distance = (sl_usd / pip_value) * sym_info.trade_tick_size if pip_value > 0 else 0
    tp_distance = (tp_usd / pip_value) * sym_info.trade_tick_size if pip_value > 0 else 0

    sl_price = (price - sl_distance) if direction == "BUY" else (price + sl_distance)
    tp_price = (price + tp_distance) if direction == "BUY" else (price - tp_distance)

    request = {
        "action":       mt5.TRADE_ACTION_DEAL,
        "symbol":       symbol,
        "volume":       lot,
        "type":         order_type,
        "price":        price,
        "sl":           round(sl_price, sym_info.digits),
        "tp":           round(tp_price, sym_info.digits),
        "deviation":    20,
        "magic":        202,
        "comment":      "SqueezyWeb",
        "type_time":    mt5.ORDER_TIME_GTC,
        "type_filling": mt5.ORDER_FILLING_IOC,
    }
    result = mt5.order_send(request)
    if result.retcode == mt5.TRADE_RETCODE_DONE:
        log.info("Trade opened: %s %s %.2f lot @ %.5f", direction, symbol, lot, price)
        return {"ok": True, "ticket": result.order}
    else:
        log.error("Trade failed: %s", result.comment)
        return {"ok": False, "error": result.comment, "retcode": result.retcode}


def close_position(ticket):
    positions = mt5.positions_get(ticket=ticket)
    if not positions:
        return {"ok": False, "error": "Position not found"}
    pos = positions[0]
    tick = mt5.symbol_info_tick(pos.symbol)
    close_type  = mt5.ORDER_TYPE_SELL if pos.type == mt5.ORDER_TYPE_BUY else mt5.ORDER_TYPE_BUY
    close_price = tick.bid if pos.type == mt5.ORDER_TYPE_BUY else tick.ask
    request = {
        "action":       mt5.TRADE_ACTION_DEAL,
        "symbol":       pos.symbol,
        "volume":       pos.volume,
        "type":         close_type,
        "position":     ticket,
        "price":        close_price,
        "deviation":    20,
        "magic":        202,
        "comment":      "SqueezyWeb-Close",
        "type_time":    mt5.ORDER_TIME_GTC,
        "type_filling": mt5.ORDER_FILLING_IOC,
    }
    result = mt5.order_send(request)
    if result.retcode == mt5.TRADE_RETCODE_DONE:
        log.info("Position %d closed", ticket)
        return {"ok": True}
    else:
        return {"ok": False, "error": result.comment}


def close_all_positions():
    positions = mt5.positions_get()
    if not positions:
        return {"ok": True, "closed": 0}
    results = [close_position(p.ticket) for p in positions]
    failed  = [r for r in results if not r["ok"]]
    return {"ok": len(failed) == 0, "closed": len(results) - len(failed), "failed": len(failed)}


# ─────────────────────────────────────────
#  EA SETTINGS  (stored in memory on VPS)
# ─────────────────────────────────────────
ea_settings = {
    "lot_size":          0.05,
    "squeeze_percentile": 50.0,
    "atr_percentile":    40.0,
    "expansion_pct":     20.0,
    "min_squeeze_score": 60.0,
    "sl_usd":            2.00,
    "tp_usd":            2.00,
    "alert_watch":       True,
    "alert_ready":       True,
    "alert_signal":      False,
    "cooldown_signal":   300,
    "cooldown_ready":    120,
    "cooldown_watch":    300,
    "ea_running":        True,
}

# ─────────────────────────────────────────
#  WEBSOCKET CLIENT
# ─────────────────────────────────────────
ws_app = None
ws_connected = False


def send(ws, msg_type, payload):
    ws.send(json.dumps({"type": msg_type, "payload": payload, "secret": BRIDGE_SECRET}))


def on_message(ws, raw):
    try:
        msg = json.loads(raw)
    except Exception:
        return
    t = msg.get("type")
    p = msg.get("payload", {})
    log.info("← %s", t)

    if t == "FIRE_TRADE":
        result = fire_trade(p["symbol"], p["direction"], p["lot"], p.get("sl_usd", 2), p.get("tp_usd", 2))
        send(ws, "TRADE_RESULT", result)

    elif t == "CLOSE_POSITION":
        result = close_position(p["ticket"])
        send(ws, "CLOSE_RESULT", result)

    elif t == "CLOSE_ALL":
        result = close_all_positions()
        send(ws, "CLOSE_ALL_RESULT", result)

    elif t == "UPDATE_SETTINGS":
        ea_settings.update(p)
        log.info("Settings updated: %s", p)
        send(ws, "SETTINGS_ACK", ea_settings)

    elif t == "GET_SETTINGS":
        send(ws, "SETTINGS", ea_settings)

    elif t == "PING":
        send(ws, "PONG", {})


def on_open(ws):
    global ws_connected
    ws_connected = True
    log.info("✓ Connected to Render backend")
    send(ws, "HELLO", {"role": "bridge", "version": "2.02"})


def on_close(ws, code, msg):
    global ws_connected
    ws_connected = False
    log.warning("WebSocket closed (%s %s) — reconnecting in %ds", code, msg, RECONNECT_WAIT)


def on_error(ws, err):
    log.error("WebSocket error: %s", err)


# ─────────────────────────────────────────
#  POLLING THREAD — pushes state every N sec
# ─────────────────────────────────────────
def poll_loop():
    while True:
        time.sleep(POLL_INTERVAL)
        if ws_app and ws_connected:
            try:
                snapshot = {
                    "account":   get_account_snapshot(),
                    "positions": get_positions(),
                    "settings":  ea_settings,
                }
                ws_app.send(json.dumps({"type": "STATE_UPDATE", "payload": snapshot, "secret": BRIDGE_SECRET}))
            except Exception as e:
                log.error("Poll error: %s", e)


# ─────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────
def run():
    global ws_app
    if not mt5_connect():
        log.error("Cannot connect to MT5 — make sure MT5 is open and logged in")
        sys.exit(1)

    threading.Thread(target=poll_loop, daemon=True).start()

    while True:
        ws_app = websocket.WebSocketApp(
            RENDER_WS_URL,
            on_open    = on_open,
            on_message = on_message,
            on_close   = on_close,
            on_error   = on_error,
        )
        ws_app.run_forever(ping_interval=20, ping_timeout=10)
        log.info("Reconnecting in %ds...", RECONNECT_WAIT)
        time.sleep(RECONNECT_WAIT)


if __name__ == "__main__":
    run()
