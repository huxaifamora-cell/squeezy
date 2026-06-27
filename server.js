/**
 * server.js — Squeezy EA  (Option B — Deriv API, no VPS/MT5 needed)
 *
 * What this does:
 *  1. Connects to Deriv WebSocket API
 *  2. Subscribes to live M1 candles for all Volatility symbols
 *  3. Runs squeeze detection + breakout logic (ported from your MQ5 EA)
 *  4. Pushes WATCH/READY/SIGNAL alerts to the dashboard in real time
 *  5. Fires real trades on your Deriv account when you click Fire Burst
 *  6. Streams live positions and P&L back to the dashboard
 *
 * SETUP:
 *  1. Add these env vars on Render.com:
 *     DERIV_TOKEN   = your Deriv API token (app.deriv.com/account/api-token)
 *     DERIV_APP_ID  = 1089  (Deriv's default app id — leave as-is)
 *  2. Deploy — no VPS, no bridge, no MT5 needed
 */

require("dotenv").config();
const express = require("express");
const http    = require("http");
const { WebSocketServer, WebSocket } = require("ws");
const cors    = require("cors");
const path    = require("path");

const PORT         = process.env.PORT          || 3000;
const DERIV_TOKEN  = process.env.DERIV_TOKEN   || "";
const DERIV_APP_ID = process.env.DERIV_APP_ID  || "1089";
const DERIV_WS_URL = `wss://ws.binaryws.com/websockets/v3?app_id=${DERIV_APP_ID}`;

// ─────────────────────────────────────────
//  SYMBOLS
// ─────────────────────────────────────────
const SYMBOLS = [
  "1HZ10V",   // Volatility 10 Index
  "1HZ25V",   // Volatility 25 Index
  "1HZ50V",   // Volatility 50 Index
  "1HZ75V",   // Volatility 75 Index
  "1HZ100V",  // Volatility 100 Index
  "1HZ150V",  // Volatility 150 Index
  "1HZ250V",  // Volatility 250 Index
  "R_10",     // Volatility 10 (1s) Index
  "R_25",     // Volatility 25 (1s) Index
  "R_50",     // Volatility 50 (1s) Index
  "R_75",     // Volatility 75 (1s) Index
  "R_100",    // Volatility 100 (1s) Index
];

const SYM_NAMES = {
  "1HZ10V":  "Volatility 10 Index",
  "1HZ25V":  "Volatility 25 Index",
  "1HZ50V":  "Volatility 50 Index",
  "1HZ75V":  "Volatility 75 Index",
  "1HZ100V": "Volatility 100 Index",
  "1HZ150V": "Volatility 150 Index",
  "1HZ250V": "Volatility 250 Index",
  "R_10":    "Volatility 10 (1s) Index",
  "R_25":    "Volatility 25 (1s) Index",
  "R_50":    "Volatility 50 (1s) Index",
  "R_75":    "Volatility 75 (1s) Index",
  "R_100":   "Volatility 100 (1s) Index",
};

// ─────────────────────────────────────────
//  EA PARAMETERS  (mirrors your MQ5 inputs)
// ─────────────────────────────────────────
let EA = {
  bb_period:           10,
  bb_dev:              2.0,
  squeeze_lookback:    100,
  squeeze_percentile:  50.0,
  contraction_bars:    1,
  atr_lookback:        100,
  atr_percentile:      40.0,
  expansion_pct:       20.0,
  breakout_bars:       1,
  band_proximity:      0.5,
  mbb_curve_bars:      1,
  min_squeeze_score:   60.0,
  sl_usd:              2.00,
  tp_usd:              2.00,
  alert_watch:         true,
  alert_ready:         true,
  alert_signal:        false,
  cooldown_watch:      300,
  cooldown_ready:      120,
  cooldown_signal:     300,
  ea_running:          true,
};

// ─────────────────────────────────────────
//  CANDLE STORE  — 200 bars per symbol
// ─────────────────────────────────────────
const BARS_NEEDED = 200;
const candles = {};   // sym -> [{open,high,low,close,epoch}]  newest first
SYMBOLS.forEach(s => candles[s] = []);

// ─────────────────────────────────────────
//  INDICATOR MATH  (ported from MQ5)
// ─────────────────────────────────────────

function bbWidth(closes, period, dev) {
  // returns array same length as closes, newest = index 0
  const out = new Array(closes.length).fill(0);
  for (let i = 0; i <= closes.length - period; i++) {
    const sl = closes.slice(i, i + period);
    const mean = sl.reduce((a, b) => a + b, 0) / period;
    const variance = sl.reduce((a, b) => a + (b - mean) ** 2, 0) / period;
    out[i] = 2 * dev * Math.sqrt(variance);
  }
  return out;
}

function bbBands(closes, period, dev) {
  const upper = new Array(closes.length).fill(0);
  const lower = new Array(closes.length).fill(0);
  const mid   = new Array(closes.length).fill(0);
  for (let i = 0; i <= closes.length - period; i++) {
    const sl = closes.slice(i, i + period);
    const mean = sl.reduce((a, b) => a + b, 0) / period;
    const std  = Math.sqrt(sl.reduce((a, b) => a + (b - mean) ** 2, 0) / period);
    mid[i]   = mean;
    upper[i] = mean + dev * std;
    lower[i] = mean - dev * std;
  }
  return { upper, lower, mid };
}

function calcATR(bars, period) {
  const n = bars.length;
  const tr = new Array(n).fill(0);
  tr[n - 1] = bars[n - 1].high - bars[n - 1].low;
  for (let i = n - 2; i >= 0; i--) {
    const hl = bars[i].high - bars[i].low;
    const hc = Math.abs(bars[i].high - bars[i + 1].close);
    const lc = Math.abs(bars[i].low  - bars[i + 1].close);
    tr[i] = Math.max(hl, hc, lc);
  }
  const out = new Array(n).fill(0);
  for (let i = 0; i <= n - period; i++) {
    out[i] = tr.slice(i, i + period).reduce((a, b) => a + b, 0) / period;
  }
  return out;
}

function percentileRank(arr, start, window, value) {
  const sl = arr.slice(start, start + window).filter(v => v > 0);
  if (!sl.length) return 50;
  return 100 * sl.filter(v => v < value).length / sl.length;
}

function curveDir(arr, nBars) {
  // arr[0] = newest; rising means arr[i] > arr[i+1]
  let rising = true, falling = true;
  for (let i = 1; i <= nBars; i++) {
    if (i + 1 >= arr.length) return 0;
    if (arr[i] <= arr[i + 1]) rising  = false;
    if (arr[i] >= arr[i + 1]) falling = false;
  }
  if (rising)  return  1;
  if (falling) return -1;
  return 0;
}

// ─────────────────────────────────────────
//  SQUEEZE DETECTION
// ─────────────────────────────────────────
function isTightSqueeze(sym) {
  const bars = candles[sym];
  if (bars.length < BARS_NEEDED) return { squeeze: false, score: 0 };

  const closes = bars.map(b => b.close);
  const bbw    = bbWidth(closes, EA.bb_period, EA.bb_dev);
  const atr    = calcATR(bars, EA.atr_lookback);

  const currentBBW = bbw[1];
  if (currentBBW <= 0) return { squeeze: false, score: 0 };

  // Gate 1: BB Width in bottom percentile
  const bbwPct = percentileRank(bbw, 1, EA.squeeze_lookback, currentBBW);
  if (bbwPct > EA.squeeze_percentile) return { squeeze: false, score: 0 };

  // Gate 2: Contracting
  let contracting = true;
  for (let i = 1; i < EA.contraction_bars; i++) {
    if (i + 1 >= bbw.length || bbw[i] >= bbw[i + 1]) { contracting = false; break; }
  }
  if (!contracting) return { squeeze: false, score: 0 };

  // Gate 3: ATR compressed
  const currentATR = atr[1];
  if (currentATR > 0) {
    const atrPct = percentileRank(atr, 1, EA.atr_lookback, currentATR);
    if (atrPct > EA.atr_percentile) return { squeeze: false, score: 0 };
  }

  // Score
  const tightness = 100 - bbwPct;
  let consec = 0;
  for (let i = 1; i + 1 < bbw.length; i++) {
    if (bbw[i] < bbw[i + 1]) consec++;
    else break;
  }
  const score = Math.min(100, tightness * 0.7 + Math.min(30, 5 * consec));
  return { squeeze: true, score: Math.round(score * 10) / 10 };
}

// ─────────────────────────────────────────
//  BREAKOUT DETECTION
// ─────────────────────────────────────────
function detectBreakout(sym) {
  const bars = candles[sym];
  if (bars.length < BARS_NEEDED) return 0;

  const closes = bars.map(b => b.close);
  const bbw    = bbWidth(closes, EA.bb_period, EA.bb_dev);
  const { upper, lower, mid } = bbBands(closes, EA.bb_period, EA.bb_dev);

  // Squeeze floor
  let squeezeFloor = bbw[1];
  for (let i = 2; i <= EA.squeeze_lookback && i < bbw.length; i++) {
    if (bbw[i] > 0 && bbw[i] < squeezeFloor) squeezeFloor = bbw[i];
  }
  if (squeezeFloor <= 0) return 0;

  // Gate 1: Expansion from floor
  const expansion = 100 * (bbw[1] - squeezeFloor) / squeezeFloor;
  if (expansion < EA.expansion_pct) return 0;

  // Gate 2: Consecutive same-direction bars
  let bull = 0, bear = 0;
  for (let i = 1; i <= EA.breakout_bars + 5 && i < bars.length; i++) {
    if (bars[i].close > bars[i].open) bull++; else break;
  }
  for (let i = 1; i <= EA.breakout_bars + 5 && i < bars.length; i++) {
    if (bars[i].close < bars[i].open) bear++; else break;
  }
  let direction = 0;
  if (bull >= EA.breakout_bars)      direction =  1;
  else if (bear >= EA.breakout_bars) direction = -1;
  if (direction === 0) return 0;

  // Gate 3: Price near outer band
  const bandWidth = upper[1] - lower[1];
  const close     = closes[1];
  const dist      = direction === 1 ? (upper[1] - close) : (close - lower[1]);
  const proximity = bandWidth > 0 ? dist / bandWidth : 1;
  if (proximity > EA.band_proximity) return 0;

  // Gate 4: MBB curving in breakout direction
  if (curveDir(mid, EA.mbb_curve_bars) !== direction) return 0;

  return direction;
}

// ─────────────────────────────────────────
//  SCANNER
// ─────────────────────────────────────────
const lastAlertTime = {};  // sym -> { watch, ready, signal }
SYMBOLS.forEach(s => lastAlertTime[s] = { watch: 0, ready: 0, signal: 0 });

function scanSymbol(sym) {
  if (!EA.ea_running) return null;
  if (candles[sym].length < BARS_NEEDED) return null;

  const { squeeze, score } = isTightSqueeze(sym);
  if (!squeeze || score < EA.min_squeeze_score) return { sym, score: 0, squeeze: false, direction: 0 };

  const direction = detectBreakout(sym);
  const now = Date.now() / 1000;
  const times = lastAlertTime[sym];

  let level = null;
  if (direction === 0 && EA.alert_watch && (now - times.watch) >= EA.cooldown_watch) {
    level = "WATCH";
    lastAlertTime[sym].watch = now;
  } else if (direction !== 0 && EA.alert_ready && (now - times.ready) >= EA.cooldown_ready) {
    level = "READY";
    lastAlertTime[sym].ready = now;
  }

  return { sym, score, squeeze: true, direction, level };
}

function scanAll() {
  const symStates = [];
  const alerts    = [];

  for (const sym of SYMBOLS) {
    const result = scanSymbol(sym);
    if (!result) continue;
    symStates.push({
      symbol:    SYM_NAMES[sym] || sym,
      score:     result.score,
      squeeze:   result.squeeze,
      direction: result.direction,
    });
    if (result.level) {
      const alert = {
        symbol:    SYM_NAMES[sym] || sym,
        level:     result.level,
        score:     result.score,
        direction: result.direction,
        time:      Date.now(),
      };
      alerts.push(alert);
      broadcastDash({ type: "SQUEEZE_ALERT", payload: alert });
      console.log(`[SCAN] ${result.level} — ${sym} score=${result.score}`);
    }
  }

  broadcastDash({
    type: "STATE_UPDATE",
    payload: {
      sym_states: symStates,
      account:    accountState,
      positions:  positionsState,
      settings:   EA,
    }
  });
}

// ─────────────────────────────────────────
//  STATE
// ─────────────────────────────────────────
let accountState   = null;
let positionsState = [];

// ─────────────────────────────────────────
//  DERIV WEBSOCKET
// ─────────────────────────────────────────
let derivWs        = null;
let derivReady     = false;
let reqId          = 1;
const pendingReqs  = {};  // reqId -> resolve

function derivConnect() {
  console.log("[DERIV] Connecting…");
  derivWs = new WebSocket(DERIV_WS_URL);

  derivWs.on("open", () => {
    console.log("[DERIV] Connected — authorizing…");
    derivSend({ authorize: DERIV_TOKEN });
  });

  derivWs.on("message", raw => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }
    handleDeriv(msg);
  });

  derivWs.on("close", () => {
    console.log("[DERIV] Disconnected — reconnecting in 5s…");
    derivReady = false;
    broadcastDash({ type: "BRIDGE_STATUS", payload: { connected: false } });
    setTimeout(derivConnect, 5000);
  });

  derivWs.on("error", err => console.error("[DERIV] Error:", err.message));
}

function derivSend(obj) {
  if (!derivWs || derivWs.readyState !== WebSocket.OPEN) return null;
  const id = reqId++;
  obj.req_id = id;
  derivWs.send(JSON.stringify(obj));
  return id;
}

function derivRequest(obj) {
  return new Promise(resolve => {
    const id = derivSend(obj);
    if (!id) return resolve(null);
    pendingReqs[id] = resolve;
    setTimeout(() => { delete pendingReqs[id]; resolve(null); }, 10000);
  });
}

function handleDeriv(msg) {
  // Resolve pending requests
  if (msg.req_id && pendingReqs[msg.req_id]) {
    pendingReqs[msg.req_id](msg);
    delete pendingReqs[msg.req_id];
  }

  const type = msg.msg_type;

  if (type === "authorize") {
    if (msg.error) {
      console.error("[DERIV] Auth failed:", msg.error.message);
      broadcastDash({ type: "ERROR", payload: { message: "Deriv auth failed: " + msg.error.message } });
      return;
    }
    console.log("[DERIV] Authorized — account:", msg.authorize.loginid);
    derivReady = true;
    broadcastDash({ type: "BRIDGE_STATUS", payload: { connected: true } });
    updateAccount(msg.authorize);
    subscribeCandles();
    subscribeBalance();
    pollPositions();
  }

  else if (type === "candles") {
    // Historical candles loaded
    const sym = msg.echo_req?.ticks_history;
    if (sym && candles[sym] !== undefined) {
      const bars = (msg.candles || []).map(c => ({
        open:  c.open,  high:  c.high,
        low:   c.low,   close: c.close,
        epoch: c.epoch,
      })).reverse(); // newest first
      candles[sym] = bars.slice(0, BARS_NEEDED);
      console.log(`[DERIV] Loaded ${bars.length} candles for ${sym}`);
    }
  }

  else if (type === "ohlc") {
    // Live candle tick
    const o = msg.ohlc;
    const sym = o?.symbol;
    if (sym && candles[sym] !== undefined) {
      const bar = {
        open:  parseFloat(o.open),
        high:  parseFloat(o.high),
        low:   parseFloat(o.low),
        close: parseFloat(o.close),
        epoch: o.epoch,
      };
      if (candles[sym].length && candles[sym][0].epoch === bar.epoch) {
        candles[sym][0] = bar; // update current bar
      } else {
        candles[sym].unshift(bar); // new bar
        if (candles[sym].length > BARS_NEEDED) candles[sym].pop();
      }
    }
  }

  else if (type === "balance") {
    accountState = {
      ...accountState,
      balance:  msg.balance?.balance  || accountState?.balance || 0,
      currency: msg.balance?.currency || accountState?.currency || "USD",
    };
    broadcastDash({ type: "POSITIONS_UPDATE", payload: { account: accountState, positions: positionsState } });
  }

  else if (type === "profit_table") {
    // Used for open positions approximation
  }

  else if (type === "buy") {
    if (msg.error) {
      broadcastDash({ type: "TRADE_RESULT", payload: { ok: false, error: msg.error.message } });
    } else {
      broadcastDash({ type: "TRADE_RESULT", payload: { ok: true, contract_id: msg.buy?.contract_id, price: msg.buy?.buy_price } });
      pollPositions();
    }
  }

  else if (type === "sell") {
    if (msg.error) {
      broadcastDash({ type: "CLOSE_RESULT", payload: { ok: false, error: msg.error.message } });
    } else {
      broadcastDash({ type: "CLOSE_RESULT", payload: { ok: true } });
      pollPositions();
    }
  }

  else if (type === "portfolio") {
    positionsState = (msg.portfolio?.contracts || []).map(c => ({
      ticket:      c.contract_id,
      symbol:      c.symbol,
      dir:         c.contract_type?.includes("CALL") ? "BUY" : "SELL",
      lot:         1,
      open_price:  c.buy_price,
      current_price: c.bid_price || c.buy_price,
      pnl:         parseFloat(((c.bid_price || c.buy_price) - c.buy_price).toFixed(2)),
      sl:          0,
      tp:          0,
    }));
    broadcastDash({ type: "POSITIONS_UPDATE", payload: { account: accountState, positions: positionsState } });
  }
}

function updateAccount(auth) {
  accountState = {
    balance:  auth.balance      || 0,
    equity:   auth.balance      || 0,
    floating: 0,
    currency: auth.currency     || "USD",
    login:    auth.loginid      || "",
    server:   "Deriv",
  };
}

function subscribeCandles() {
  for (const sym of SYMBOLS) {
    // Load 200 historical M1 candles
    derivSend({
      ticks_history: sym,
      granularity:   60,
      count:         BARS_NEEDED,
      end:           "latest",
      style:         "candles",
      subscribe:     1,
    });
  }
  console.log("[DERIV] Subscribed to candles for", SYMBOLS.length, "symbols");
}

function subscribeBalance() {
  derivSend({ balance: 1, subscribe: 1 });
}

function pollPositions() {
  derivSend({ portfolio: 1 });
}

// ─────────────────────────────────────────
//  TRADE EXECUTION
// ─────────────────────────────────────────
async function fireTrade({ symbol, direction, lot, tp_usd, sl_usd }) {
  // Find Deriv symbol code from display name
  const sym = Object.keys(SYM_NAMES).find(k => SYM_NAMES[k] === symbol) || symbol;

  // Deriv uses CALL/PUT for synthetic indices
  const contract_type = direction === "BUY" ? "CALL" : "PUT";

  // Use duration-based contract (no traditional SL/TP on synthetics)
  // TP in USD = profit amount
  const id = derivSend({
    buy: 1,
    price: tp_usd || 2,
    parameters: {
      contract_type,
      symbol:     sym,
      duration:   5,
      duration_unit: "m",
      basis:      "payout",
      amount:     tp_usd || 2,
      currency:   accountState?.currency || "USD",
    }
  });
  console.log(`[TRADE] ${direction} ${symbol} payout=$${tp_usd}`);
}

async function closePosition(contractId) {
  derivSend({ sell: contractId, price: 0 });
}

async function closeAll() {
  for (const pos of positionsState) {
    derivSend({ sell: pos.ticket, price: 0 });
  }
  broadcastDash({ type: "CLOSE_ALL_RESULT", payload: { ok: true, closed: positionsState.length } });
}

// ─────────────────────────────────────────
//  EXPRESS + DASHBOARD WEBSOCKET
// ─────────────────────────────────────────
const app    = express();
const server = http.createServer(app);
const dashWss = new WebSocketServer({ noServer: true });

app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

server.on("upgrade", (req, socket, head) => {
  if (req.url === "/ws") {
    dashWss.handleUpgrade(req, socket, head, ws => dashWss.emit("connection", ws));
  } else {
    socket.destroy();
  }
});

function broadcastDash(msg) {
  const raw = JSON.stringify(msg);
  dashWss.clients.forEach(c => {
    if (c.readyState === WebSocket.OPEN) c.send(raw);
  });
}

dashWss.on("connection", ws => {
  console.log("[DASH] Browser connected");

  // Send current state immediately
  ws.send(JSON.stringify({ type: "BRIDGE_STATUS", payload: { connected: derivReady } }));
  if (accountState) {
    ws.send(JSON.stringify({
      type: "STATE_UPDATE",
      payload: { account: accountState, positions: positionsState, settings: EA, sym_states: [] }
    }));
  }

  ws.on("message", raw => {
    let msg; try { msg = JSON.parse(raw); } catch { return; }
    const { type, payload } = msg;

    switch(type) {
      case "FIRE_TRADE":
        if (!derivReady) { ws.send(JSON.stringify({ type: "ERROR", payload: { message: "Not connected to Deriv" } })); return; }
        for (let i = 0; i < (payload.count || 1); i++) fireTrade(payload);
        break;
      case "CLOSE_POSITION":
        closePosition(payload.ticket);
        break;
      case "CLOSE_ALL":
        closeAll();
        break;
      case "UPDATE_SETTINGS":
        Object.assign(EA, payload);
        broadcastDash({ type: "SETTINGS_ACK", payload: EA });
        break;
      case "GET_SETTINGS":
        ws.send(JSON.stringify({ type: "SETTINGS", payload: EA }));
        break;
    }
  });

  ws.on("close", () => console.log("[DASH] Browser disconnected"));
});

app.get("/api/state", (req, res) => res.json({ connected: derivReady, account: accountState }));

app.get("*", (req, res) => {
  res.sendFile(path.join(__dirname, "public/index.html"));
});

// ─────────────────────────────────────────
//  SCAN LOOP  — runs every 3 seconds
// ─────────────────────────────────────────
setInterval(() => {
  if (derivReady) scanAll();
}, 3000);

// Poll positions every 5 seconds
setInterval(() => {
  if (derivReady) pollPositions();
}, 5000);

// Keepalive ping to Deriv (prevents timeout)
setInterval(() => {
  if (derivWs && derivWs.readyState === WebSocket.OPEN) {
    derivWs.send(JSON.stringify({ ping: 1 }));
  }
}, 25000);

// ─────────────────────────────────────────
//  START
// ─────────────────────────────────────────
server.listen(PORT, () => {
  console.log(`Squeezy server running on port ${PORT}`);
  if (!DERIV_TOKEN) {
    console.warn("WARNING: DERIV_TOKEN not set — add it to Render environment variables");
  } else {
    derivConnect();
  }
});
