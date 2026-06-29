/**
 * server.js — Squeezy EA  (Deriv NEW API — 2025)
 *
 * Contract type: MULTIPLIERS (MULTUP / MULTDOWN) on Volatility (Synthetic) Indices
 * NOT binary options — multiplier contracts have no expiry, no payout field,
 * profit = stake × multiplier × price_change_pct, max loss = stake.
 *
 * New API breaking changes applied:
 *  - proposal:               symbol → underlying_symbol, no duration/duration_unit for mults,
 *                            contract_type = MULTUP|MULTDOWN, multiplier param required,
 *                            ask_price/payout now string|number
 *  - portfolio:              symbol → underlying_symbol
 *  - proposal_open_contract: bid_price/buy_price/current_spot/profit now string|number,
 *                            sell_spot → exit_spot, display_value removed, loginid removed
 *  - buy:                    loginid removed, buy object always present on success
 *
 * Flow:
 *  1. REST GET /accounts            — find account ID
 *  2. REST POST /accounts/{id}/otp  — get authenticated WebSocket URL (one-time)
 *  3. Public WS                     — M1 candle history + live OHLC for all symbols
 *  4. Trading WS (authed)           — balance, portfolio, trade execution, live P/L
 *  5. Scanner loop every 3s         — squeeze detection → alerts → dashboard
 *
 * ENV VARS (Render.com):
 *   DERIV_TOKEN     = PAT token from developers.deriv.com
 *   DERIV_APP_ID    = 33FPKmmaz5Yxy6DuhhyVt
 *   DERIV_CLIENT_ID = 019eb390-b034-7ab0-860c-526190c7c3e6
 */

require("dotenv").config();
const express  = require("express");
const http     = require("http");
const { WebSocketServer, WebSocket } = require("ws");
const cors     = require("cors");
const path     = require("path");

const PORT            = process.env.PORT            || 3000;
let   DERIV_TOKEN     = process.env.DERIV_TOKEN     || "";
const DERIV_APP_ID    = process.env.DERIV_APP_ID    || "33FPKmmaz5Yxy6DuhhyVt";
const DERIV_CLIENT_ID = process.env.DERIV_CLIENT_ID || "019eb390-b034-7ab0-860c-526190c7c3e6";
const DERIV_REST_BASE = "https://api.derivws.com";
const PUBLIC_WS_URL   = "wss://api.derivws.com/trading/v1/options/ws/public";

// ─────────────────────────────────────────
//  SYMBOLS
// ─────────────────────────────────────────
const SYMBOLS = [
  "1HZ10V","1HZ25V","1HZ50V","1HZ75V","1HZ100V","1HZ150V","1HZ250V",
  "R_10","R_25","R_50","R_75","R_100",
];

const SYM_NAMES = {
  "1HZ10V" :"Volatility 10 Index",
  "1HZ25V" :"Volatility 25 Index",
  "1HZ50V" :"Volatility 50 Index",
  "1HZ75V" :"Volatility 75 Index",
  "1HZ100V":"Volatility 100 Index",
  "1HZ150V":"Volatility 150 Index",
  "1HZ250V":"Volatility 250 Index",
  "R_10"   :"Volatility 10 (1s) Index",
  "R_25"   :"Volatility 25 (1s) Index",
  "R_50"   :"Volatility 50 (1s) Index",
  "R_75"   :"Volatility 75 (1s) Index",
  "R_100"  :"Volatility 100 (1s) Index",
};

// Reverse: display name → API code
const NAME_TO_SYM = Object.fromEntries(Object.entries(SYM_NAMES).map(([k,v])=>[v,k]));

// ─────────────────────────────────────────
//  EA PARAMETERS
// ─────────────────────────────────────────
let EA = {
  // Squeeze detection
  bb_period:10, bb_dev:2.0, squeeze_lookback:100, squeeze_percentile:50.0,
  contraction_bars:1, atr_lookback:100, atr_percentile:40.0,
  expansion_pct:20.0, breakout_bars:1, band_proximity:0.5, mbb_curve_bars:1,
  min_squeeze_score:60.0,

  // Multiplier trade defaults
  stake:2.00,          // USD amount staked per trade
  multiplier:50,       // accepted: 50, 100, 200, 300, 500
  take_profit:0,       // 0 = disabled; set USD amount to enable TP
  stop_loss:2.00,      // USD amount; set to stake to cap loss at stake

  // Alerts
  alert_watch:true, alert_ready:true, alert_signal:false,
  cooldown_watch:300, cooldown_ready:120, cooldown_signal:300,
  ea_running:true,
};

// ─────────────────────────────────────────
//  PER-SYMBOL MULTIPLIER STORE
//  Populated at runtime via contracts_for calls on the trading WS.
//  Each entry: { min, available: [50, 100, 200, ...] }
//  Falls back to [] until Deriv responds — trades are blocked until loaded.
// ─────────────────────────────────────────
const SYM_MULTIPLIERS = {};
SYMBOLS.forEach(s => SYM_MULTIPLIERS[s] = { min: 50, available: [] });

let contractsFetched = false;

function fetchContractsFor(ws) {
  if (!ws || ws.readyState !== WebSocket.OPEN) return;
  console.log("[CONTRACTS] Fetching multiplier ranges for all symbols…");
  for (const sym of SYMBOLS) {
    const id = reqId++;
    pendingCallbacks[id] = (msg) => handleContractsFor(msg, sym);
    ws.send(JSON.stringify({
      contracts_for   : sym,
      currency        : "USD",
      landing_company : "svg",
      product_type    : "basic",
      req_id          : id,
    }));
  }
}

function handleContractsFor(msg, sym) {
  if (msg.error) {
    console.warn(`[CONTRACTS] ${sym} error: ${msg.error.message}`);
    return;
  }

  const available = msg.contracts_for?.available || [];

  // Log all contract types returned so we can see what Deriv actually supports
  const allTypes = [...new Set(available.map(c => c.contract_type))];
  console.log(`[CONTRACTS] ${sym} raw contract types: ${allTypes.join(", ") || "(none)"}`);

  const multContracts = available.filter(c =>
    c.contract_type === "MULTUP" || c.contract_type === "MULTDOWN"
  );
  const allMults = [...new Set(
    multContracts.flatMap(c => c.multiplier_range || [])
  )].map(Number).filter(Boolean).sort((a, b) => a - b);

  if (allMults.length) {
    SYM_MULTIPLIERS[sym] = { min: allMults[0], available: allMults };
    console.log(`[CONTRACTS] ${sym} → multipliers: [${allMults.join(", ")}]`);
  } else {
    // Symbol returned no multiplier contracts — keep empty so trades are blocked
    console.warn(`[CONTRACTS] ${sym} → no MULTUP/MULTDOWN contracts found`);
  }

  // Check if all symbols are resolved (have data, even if empty)
  const done = SYMBOLS.every(s => {
    const d = SYM_MULTIPLIERS[s];
    // Consider resolved if we got a response (available may be empty for unsupported syms)
    return d._fetched || d.available.length > 0;
  });

  // Mark this symbol as fetched regardless of result
  SYM_MULTIPLIERS[sym]._fetched = true;

  const allFetched = SYMBOLS.every(s => SYM_MULTIPLIERS[s]._fetched);
  if (allFetched && !contractsFetched) {
    contractsFetched = true;
    console.log("[CONTRACTS] All symbol multipliers loaded ✓");
    console.log("[CONTRACTS] Summary:", SYMBOLS.map(s =>
      `${s}:[${SYM_MULTIPLIERS[s].available.join(",")||"none"}]`
    ).join(" "));
    broadcastDash({ type: "SYM_MULTIPLIERS", payload: SYM_MULTIPLIERS });
  }
}

// Resolve the best multiplier for a symbol:
//   - If caller passed a value and it's valid for this symbol → use it
//   - Otherwise → use the symbol's minimum (lowest accepted value)
//   - Returns null if symbol has no known multipliers (caller should block trade)
function resolveMultiplier(sym, requested) {
  const data = SYM_MULTIPLIERS[sym];
  const req  = parseInt(requested) || 0;

  if (data?.available?.length) {
    // Use requested if valid for this symbol, else fall back to symbol minimum
    const resolved = data.available.includes(req) ? req : data.min;
    return resolved;
  }

  // Data not yet fetched or symbol has no multiplier contracts
  return null;
}

// ─────────────────────────────────────────
//  CANDLE STORE
// ─────────────────────────────────────────
const BARS_NEEDED = 200;
const candles = {};
SYMBOLS.forEach(s => candles[s] = []);

// ─────────────────────────────────────────
//  SAFE NUMBER HELPER
//  New API returns many numeric fields as string|number — always parse safely
// ─────────────────────────────────────────
function n(v, fallback=0) {
  const f = parseFloat(v);
  return isNaN(f) ? fallback : f;
}

// ─────────────────────────────────────────
//  INDICATOR MATH
// ─────────────────────────────────────────
function bbWidth(closes, period, dev) {
  const out = new Array(closes.length).fill(0);
  for (let i = 0; i <= closes.length - period; i++) {
    const sl = closes.slice(i, i + period);
    const mean = sl.reduce((a,b)=>a+b,0)/period;
    const variance = sl.reduce((a,b)=>a+(b-mean)**2,0)/period;
    out[i] = 2*dev*Math.sqrt(variance);
  }
  return out;
}

function bbBands(closes, period, dev) {
  const upper=new Array(closes.length).fill(0);
  const lower=new Array(closes.length).fill(0);
  const mid  =new Array(closes.length).fill(0);
  for (let i=0; i<=closes.length-period; i++) {
    const sl=closes.slice(i,i+period);
    const mean=sl.reduce((a,b)=>a+b,0)/period;
    const std=Math.sqrt(sl.reduce((a,b)=>a+(b-mean)**2,0)/period);
    mid[i]=mean; upper[i]=mean+dev*std; lower[i]=mean-dev*std;
  }
  return {upper,lower,mid};
}

function calcATR(bars, period) {
  const n=bars.length, tr=new Array(n).fill(0);
  tr[n-1]=bars[n-1].high-bars[n-1].low;
  for (let i=n-2;i>=0;i--) {
    tr[i]=Math.max(
      bars[i].high-bars[i].low,
      Math.abs(bars[i].high-bars[i+1].close),
      Math.abs(bars[i].low -bars[i+1].close)
    );
  }
  const out=new Array(n).fill(0);
  for (let i=0;i<=n-period;i++)
    out[i]=tr.slice(i,i+period).reduce((a,b)=>a+b,0)/period;
  return out;
}

function percentileRank(arr, start, window, value) {
  const sl=arr.slice(start,start+window).filter(v=>v>0);
  if (!sl.length) return 50;
  return 100*sl.filter(v=>v<value).length/sl.length;
}

function curveDir(arr, nBars) {
  let rising=true, falling=true;
  for (let i=1;i<=nBars;i++) {
    if (i+1>=arr.length) return 0;
    if (arr[i]<=arr[i+1]) rising=false;
    if (arr[i]>=arr[i+1]) falling=false;
  }
  return rising?1:falling?-1:0;
}

// ─────────────────────────────────────────
//  SQUEEZE & BREAKOUT
// ─────────────────────────────────────────
function isTightSqueeze(sym) {
  const bars=candles[sym];
  if (bars.length<BARS_NEEDED) return {squeeze:false,score:0};
  const closes=bars.map(b=>b.close);
  const bbw=bbWidth(closes,EA.bb_period,EA.bb_dev);
  const atr=calcATR(bars,EA.atr_lookback);
  const currentBBW=bbw[1];
  if (currentBBW<=0) return {squeeze:false,score:0};
  const bbwPct=percentileRank(bbw,1,EA.squeeze_lookback,currentBBW);
  if (bbwPct>EA.squeeze_percentile) return {squeeze:false,score:0};
  let contracting=true;
  for (let i=1;i<EA.contraction_bars;i++) {
    if (i+1>=bbw.length||bbw[i]>=bbw[i+1]) {contracting=false;break;}
  }
  if (!contracting) return {squeeze:false,score:0};
  const currentATR=atr[1];
  if (currentATR>0) {
    if (percentileRank(atr,1,EA.atr_lookback,currentATR)>EA.atr_percentile)
      return {squeeze:false,score:0};
  }
  let consec=0;
  for (let i=1;i+1<bbw.length;i++) { if (bbw[i]<bbw[i+1]) consec++; else break; }
  const score=Math.min(100,(100-bbwPct)*0.7+Math.min(30,5*consec));
  return {squeeze:true, score:Math.round(score*10)/10};
}

function detectBreakout(sym) {
  const bars=candles[sym];
  if (bars.length<BARS_NEEDED) return 0;
  const closes=bars.map(b=>b.close);
  const bbw=bbWidth(closes,EA.bb_period,EA.bb_dev);
  const {upper,lower,mid}=bbBands(closes,EA.bb_period,EA.bb_dev);
  let squeezeFloor=bbw[1];
  for (let i=2;i<=EA.squeeze_lookback&&i<bbw.length;i++)
    if (bbw[i]>0&&bbw[i]<squeezeFloor) squeezeFloor=bbw[i];
  if (squeezeFloor<=0) return 0;
  if (100*(bbw[1]-squeezeFloor)/squeezeFloor<EA.expansion_pct) return 0;
  let bull=0,bear=0;
  for (let i=1;i<=EA.breakout_bars+5&&i<bars.length;i++) {
    if (bars[i].close>bars[i].open) bull++; else break;
  }
  for (let i=1;i<=EA.breakout_bars+5&&i<bars.length;i++) {
    if (bars[i].close<bars[i].open) bear++; else break;
  }
  let direction=bull>=EA.breakout_bars?1:bear>=EA.breakout_bars?-1:0;
  if (direction===0) return 0;
  const bw=upper[1]-lower[1];
  const dist=direction===1?(upper[1]-closes[1]):(closes[1]-lower[1]);
  if (bw>0&&dist/bw>EA.band_proximity) return 0;
  if (curveDir(mid,EA.mbb_curve_bars)!==direction) return 0;
  return direction;
}

// ─────────────────────────────────────────
//  SCANNER
// ─────────────────────────────────────────
const lastAlert={};
SYMBOLS.forEach(s=>lastAlert[s]={watch:0,ready:0,signal:0});

function scanAll() {
  if (!EA.ea_running) return;
  const symStates=[];
  for (const sym of SYMBOLS) {
    if (candles[sym].length<BARS_NEEDED) continue;
    const {squeeze,score}=isTightSqueeze(sym);
    const direction=squeeze&&score>=EA.min_squeeze_score?detectBreakout(sym):0;
    symStates.push({symbol:SYM_NAMES[sym]||sym,score:squeeze?score:0,squeeze,direction});
    if (!squeeze||score<EA.min_squeeze_score) continue;
    const now=Date.now()/1000;
    let level=null;
    if (direction===0&&EA.alert_watch&&(now-lastAlert[sym].watch)>=EA.cooldown_watch) {
      level="WATCH"; lastAlert[sym].watch=now;
    } else if (direction!==0&&EA.alert_ready&&(now-lastAlert[sym].ready)>=EA.cooldown_ready) {
      level="READY"; lastAlert[sym].ready=now;
    }
    if (level) {
      const alert={symbol:SYM_NAMES[sym]||sym,level,score,direction,time:Date.now()};
      broadcastDash({type:"SQUEEZE_ALERT",payload:alert});
      console.log(`[SCAN] ${level} — ${sym} score=${score}`);
    }
  }
  broadcastDash({type:"STATE_UPDATE",payload:{
    sym_states:symStates, account:accountState,
    positions:positionsState, settings:EA,
  }});
}

// ─────────────────────────────────────────
//  SHARED STATE
// ─────────────────────────────────────────
let accountState   = null;
let positionsState = [];
let derivAccountId = null;
let allAccounts    = [];
let tradingWs      = null;
let tradingWsReady = false;
let reqId          = 1;
const pendingCallbacks = {};
const pendingProposals = {};   // reqId → {sym, stake, multiplier, direction}

// ─────────────────────────────────────────
//  REST HELPERS
// ─────────────────────────────────────────
async function derivRest(method, urlPath, body=null) {
  const opts={
    method,
    headers:{
      "Authorization":"Bearer "+DERIV_TOKEN,
      "Deriv-App-ID" :DERIV_APP_ID,
      "Content-Type" :"application/json",
    },
  };
  if (body) opts.body=JSON.stringify(body);
  const res=await fetch(DERIV_REST_BASE+urlPath,opts);
  return res.json();
}

async function getAccounts() {
  console.log("[DERIV] Fetching accounts…");
  const data=await derivRest("GET","/trading/v1/options/accounts");
  if (data.errors) { console.error("[DERIV] Accounts error:",JSON.stringify(data.errors)); return null; }
  const accounts=data.data||[];
  console.log("[DERIV] Accounts:",accounts.map(a=>a.account_id||a.id).join(", "));
  allAccounts=accounts.map(a=>({
    id      : a.account_id||a.id,
    type    : (a.account_id||a.id||"").startsWith("DOT")
              ||(a.account_type||"").toLowerCase().includes("demo") ? "demo" : "real",
    currency: a.currency||"USD",
    label   : a.account_id||a.id,
  }));
  broadcastDash({type:"ACCOUNTS_LIST",payload:{accounts:allAccounts,current:derivAccountId}});
  const demo=accounts.find(a=>{
    const id=(a.account_id||a.id||"");
    const type=(a.account_type||a.type||"").toLowerCase();
    return id.startsWith("DOT")||id.startsWith("VR")||type.includes("demo")||type.includes("virtual");
  });
  const chosen=demo||accounts[0];
  const chosenId=chosen?.account_id||chosen?.id||null;
  console.log("[DERIV] Using:",chosenId,demo?"(demo)":"(first)");
  return chosenId;
}

async function getOTP(accountId) {
  console.log("[DERIV] Getting OTP for:",accountId);
  const data=await derivRest("POST",`/trading/v1/options/accounts/${accountId}/otp`);
  if (data.errors) { console.error("[DERIV] OTP error:",JSON.stringify(data.errors)); return null; }
  return data.data?.url||null;
}

// ─────────────────────────────────────────
//  PUBLIC WS  (market data — no auth needed)
// ─────────────────────────────────────────
let publicWs=null;

function connectPublicWs() {
  console.log("[DERIV] Connecting public WS…");
  publicWs=new WebSocket(PUBLIC_WS_URL);

  publicWs.on("open",()=>{
    console.log("[DERIV] Public WS open — subscribing candles…");
    for (const sym of SYMBOLS) {
      publicWs.send(JSON.stringify({
        ticks_history:sym, granularity:60, count:BARS_NEEDED,
        end:"latest", style:"candles", subscribe:1, req_id:reqId++,
      }));
    }
  });

  publicWs.on("message",raw=>{
    let msg; try{msg=JSON.parse(raw);}catch{return;}
    handleMarketData(msg);
  });

  publicWs.on("close",()=>{
    console.log("[DERIV] Public WS closed — reconnecting in 5s…");
    setTimeout(connectPublicWs,5000);
  });

  publicWs.on("error",err=>console.error("[DERIV] Public WS error:",err.message));

  setInterval(()=>{
    if (publicWs?.readyState===WebSocket.OPEN)
      publicWs.send(JSON.stringify({ping:1}));
  },25000);
}

function handleMarketData(msg) {
  const type=msg.msg_type;
  if (type==="candles") {
    const sym=msg.echo_req?.ticks_history;
    if (sym&&candles[sym]!==undefined) {
      candles[sym]=(msg.candles||[]).map(c=>({
        open:n(c.open), high:n(c.high), low:n(c.low), close:n(c.close), epoch:c.epoch,
      })).reverse().slice(0,BARS_NEEDED);
      console.log(`[CANDLE] ${sym}: ${candles[sym].length} bars`);
    }
  } else if (type==="ohlc") {
    const o=msg.ohlc, sym=o?.symbol;
    if (sym&&candles[sym]!==undefined) {
      const bar={open:n(o.open),high:n(o.high),low:n(o.low),close:n(o.close),epoch:o.epoch};
      if (candles[sym].length&&candles[sym][0].epoch===bar.epoch)
        candles[sym][0]=bar;
      else { candles[sym].unshift(bar); if(candles[sym].length>BARS_NEEDED) candles[sym].pop(); }
    }
  }
}

// ─────────────────────────────────────────
//  TRADING WS  (authenticated)
// ─────────────────────────────────────────
async function connectTradingWs() {
  if (!DERIV_TOKEN) { console.warn("[DERIV] No token — market data only"); return; }
  try {
    if (!derivAccountId) derivAccountId=await getAccounts();
    if (!derivAccountId) { console.error("[DERIV] No account — retry 30s"); setTimeout(connectTradingWs,30000); return; }
    const wsUrl=await getOTP(derivAccountId);
    if (!wsUrl) { console.error("[DERIV] No OTP — retry 30s"); setTimeout(connectTradingWs,30000); return; }

    console.log("[DERIV] Connecting trading WS…");
    tradingWs=new WebSocket(wsUrl);

    tradingWs.on("open",()=>{
      console.log("[DERIV] Trading WS open — account:",derivAccountId);
      tradingWsReady=true;
      broadcastDash({type:"BRIDGE_STATUS",payload:{connected:true,account:derivAccountId}});
      tradingWs.send(JSON.stringify({balance:1, subscribe:1, req_id:reqId++}));
      tradingWs.send(JSON.stringify({portfolio:1, req_id:reqId++}));
      // Global subscription to all open contract updates (new API: no loginid)
      tradingWs.send(JSON.stringify({proposal_open_contract:1, subscribe:1, req_id:reqId++}));
      // Fetch valid multiplier ranges for every symbol
      fetchContractsFor(tradingWs);
    });

    tradingWs.on("message",raw=>{
      let msg; try{msg=JSON.parse(raw);}catch{return;}
      if (msg.req_id&&pendingCallbacks[msg.req_id]) {
        pendingCallbacks[msg.req_id](msg); delete pendingCallbacks[msg.req_id];
      }
      handleTradingMsg(msg);
    });

    tradingWs.on("close",()=>{
      console.log("[DERIV] Trading WS closed — new OTP in 10s…");
      tradingWsReady=false;
      broadcastDash({type:"BRIDGE_STATUS",payload:{connected:false}});
      setTimeout(connectTradingWs,10000);
    });

    tradingWs.on("error",err=>console.error("[DERIV] Trading WS error:",err.message));

    const ka=setInterval(()=>{
      if (tradingWs?.readyState===WebSocket.OPEN) tradingWs.send(JSON.stringify({ping:1}));
      else clearInterval(ka);
    },25000);

  } catch(err) {
    console.error("[DERIV] connectTradingWs error:",err.message);
    setTimeout(connectTradingWs,15000);
  }
}

// ─────────────────────────────────────────
//  BUILD POSITION OBJECT FROM CONTRACT DATA
//  Works for both portfolio snapshot and proposal_open_contract stream
// ─────────────────────────────────────────
function buildPosition(c) {
  // underlying_symbol is the new API field (replaces symbol/underlying)
  const symCode = c.underlying_symbol || c.underlying || "";
  const symName = SYM_NAMES[symCode] || symCode || "Unknown";

  // direction: MULTUP = BUY (Long), MULTDOWN = SELL (Short)
  const dir = (c.contract_type === "MULTUP") ? "BUY" : "SELL";

  // ── STAKE ──────────────────────────────────────────────────────────────────
  // Priority order for the real staked amount on multiplier contracts:
  //   1. contract_parameters.amount  — most reliable (raw stake sent in proposal)
  //   2. contract_parameters.stake   — alternate field name used by some API versions
  //   3. c.stake                     — top-level field, present on some responses
  //   4. c.buy_price                 — last resort fallback (includes commission)
  const stake = n(
    c.contract_parameters?.amount ??
    c.contract_parameters?.stake  ??
    c.stake                        ??
    c.buy_price
  );

  const mult        = n(c.multiplier, 0);
  const entrySpot   = n(c.entry_spot || c.entry_tick || c.buy_price);
  const currentSpot = n(c.current_spot || c.entry_spot || c.buy_price);
  const pnl         = parseFloat(n(c.profit).toFixed(2));

  return {
    ticket       : c.contract_id,
    symbol       : symName,
    sym_code     : symCode,
    dir,
    stake        : stake.toFixed(2),
    multiplier   : mult,
    exposure     : (stake * mult).toFixed(2),
    open_price   : entrySpot.toFixed(5),
    current_price: currentSpot.toFixed(5),
    pnl,
    contract_type: c.contract_type || "",
    expiry       : c.date_expiry || null,
  };
}

// ─────────────────────────────────────────
//  TRADING MESSAGE HANDLER
// ─────────────────────────────────────────
function handleTradingMsg(msg) {
  const type=msg.msg_type;

  // ── BALANCE ────────────────────────────────────────────────────────────────
  if (type==="balance") {
    accountState={
      ...accountState,
      balance : n(msg.balance?.balance),
      currency: msg.balance?.currency||"USD",
      equity  : n(msg.balance?.balance),
      floating: positionsState.reduce((s,p)=>s+p.pnl,0),
      login   : derivAccountId,
      server  : "Deriv",
    };
    broadcastDash({type:"POSITIONS_UPDATE",payload:{account:accountState,positions:positionsState}});

  // ── PORTFOLIO SNAPSHOT ──────────────────────────────────────────────────────
  } else if (type==="portfolio") {
    const contracts=msg.portfolio?.contracts||[];
    positionsState=contracts.map(buildPosition);
    console.log(`[PORTFOLIO] ${positionsState.length} open position(s)`);
    broadcastDash({type:"POSITIONS_UPDATE",payload:{account:accountState,positions:positionsState}});

  // ── LIVE CONTRACT UPDATES ───────────────────────────────────────────────────
  } else if (type==="proposal_open_contract") {
    const c=msg.proposal_open_contract;
    if (!c||!c.contract_id) return;

    if (c.is_sold) {
      const removed=positionsState.find(p=>p.ticket===c.contract_id);
      positionsState=positionsState.filter(p=>p.ticket!==c.contract_id);
      console.log(`[POS] Contract ${c.contract_id} closed — P/L: $${n(c.profit).toFixed(2)}`);
      if (removed) broadcastDash({type:"POSITION_CLOSED",payload:{
        ticket:c.contract_id, symbol:removed.symbol, pnl:n(c.profit),
      }});
    } else {
      const pos=buildPosition(c);
      const idx=positionsState.findIndex(p=>p.ticket===c.contract_id);
      if (idx!==-1) {
        positionsState[idx]=pos;
      } else {
        positionsState.push(pos);
        console.log(`[POS] New: ${pos.symbol} ${pos.dir} stake=$${pos.stake} x${pos.multiplier} (exposure=$${pos.exposure})`);
      }
    }

    const floating=positionsState.reduce((s,p)=>s+(p.pnl||0),0);
    if (accountState) accountState={...accountState,floating};
    broadcastDash({type:"POSITIONS_UPDATE",payload:{account:accountState,positions:positionsState}});

  // ── PROPOSAL RESPONSE ───────────────────────────────────────────────────────
  } else if (type==="proposal") {
    if (msg.error) {
      console.error("[TRADE] Proposal error:",msg.error.message,"| code:",msg.error.code);
      broadcastDash({type:"TRADE_RESULT",payload:{ok:false,error:msg.error.message}});
      delete pendingProposals[msg.req_id];
      return;
    }

    const proposal_id = msg.proposal?.id;
    const ask_price   = n(msg.proposal?.ask_price);
    const pendingInfo = pendingProposals[msg.req_id];

    if (!proposal_id) {
      console.warn("[TRADE] Proposal response missing id — msg:", JSON.stringify(msg));
      return;
    }
    if (!pendingInfo) {
      // This proposal_id isn't one we sent (e.g. a late duplicate response) — ignore
      return;
    }

    delete pendingProposals[msg.req_id];
    console.log(`[TRADE] Proposal ${proposal_id} ask=$${ask_price} — buying…`);

    tradingWs.send(JSON.stringify({
      buy    : proposal_id,
      price  : ask_price,
      req_id : reqId++,
    }));

  // ── BUY RESPONSE ────────────────────────────────────────────────────────────
  } else if (type==="buy") {
    if (msg.error) {
      console.error("[TRADE] Buy error:",msg.error.message,"| code:",msg.error.code);
      broadcastDash({type:"TRADE_RESULT",payload:{ok:false,error:msg.error.message}});
      return;
    }
    const b          = msg.buy;
    const contractId = b?.contract_id;
    const buyPrice   = n(b?.buy_price);

    broadcastDash({type:"TRADE_RESULT",payload:{ok:true,contract_id:contractId,price:buyPrice}});
    console.log(`[TRADE] Bought — contract ${contractId} stake=$${buyPrice}`);

    if (contractId&&tradingWs?.readyState===WebSocket.OPEN) {
      tradingWs.send(JSON.stringify({
        proposal_open_contract:1, contract_id:contractId, subscribe:1, req_id:reqId++,
      }));
    }

    setTimeout(()=>{
      if (tradingWs?.readyState===WebSocket.OPEN)
        tradingWs.send(JSON.stringify({portfolio:1,req_id:reqId++}));
    },2000);

  // ── SELL RESPONSE ───────────────────────────────────────────────────────────
  } else if (type==="sell") {
    if (msg.error) {
      console.error("[TRADE] Sell error:",msg.error.message,"| code:",msg.error.code);
      broadcastDash({type:"CLOSE_RESULT",payload:{ok:false,error:msg.error.message}});
    } else {
      const sold_for=n(msg.sell?.sold_for);
      broadcastDash({type:"CLOSE_RESULT",payload:{ok:true,sold_for}});
      console.log(`[TRADE] Sold — received $${sold_for}`);
      setTimeout(()=>{
        if (tradingWs?.readyState===WebSocket.OPEN)
          tradingWs.send(JSON.stringify({portfolio:1,req_id:reqId++}));
      },1000);
    }
  }
}

function tradingSend(obj) {
  if (!tradingWs||tradingWs.readyState!==WebSocket.OPEN) return false;
  obj.req_id=reqId++;
  tradingWs.send(JSON.stringify(obj));
  return true;
}

// ─────────────────────────────────────────
//  TRADE EXECUTION  — MULTIPLIER CONTRACTS
// ─────────────────────────────────────────
function fireTrade({symbol, direction, stake, multiplier, take_profit, stop_loss}) {
  if (!tradingWsReady || !tradingWs) {
    console.warn("[TRADE] Blocked — trading WS not ready");
    broadcastDash({type:"TRADE_RESULT",payload:{ok:false,error:"Trading not connected. Check DERIV_TOKEN."}});
    return;
  }

  // Resolve symbol: accept display name or raw code
  const sym    = NAME_TO_SYM[symbol] || symbol;
  const amount = n(stake) || EA.stake;

  // ── MULTIPLIER GUARD ────────────────────────────────────────────────────────
  // Block the trade entirely if we don't yet have valid multiplier data for
  // this symbol. Sending an invalid multiplier causes a silent proposal error.
  if (!SYM_MULTIPLIERS[sym]?.available?.length) {
    const reason = SYM_MULTIPLIERS[sym]?._fetched
      ? `${sym} does not support multiplier contracts`
      : `Multiplier data for ${sym} not yet loaded — try again in a moment`;
    console.warn(`[TRADE] Blocked — ${reason}`);
    broadcastDash({type:"TRADE_RESULT",payload:{ok:false,error:reason}});
    // Trigger a re-fetch in case the first attempt failed
    if (!SYM_MULTIPLIERS[sym]?._fetched) fetchContractsFor(tradingWs);
    return;
  }

  const mult = resolveMultiplier(sym, multiplier || EA.multiplier);
  if (mult === null) {
    console.warn(`[TRADE] Blocked — could not resolve multiplier for ${sym}`);
    broadcastDash({type:"TRADE_RESULT",payload:{ok:false,error:`No valid multiplier for ${sym}`}});
    return;
  }

  const currency     = accountState?.currency || "USD";
  const contract_type = direction === "BUY" ? "MULTUP" : "MULTDOWN";

  console.log(
    `[TRADE] Resolved multiplier for ${sym}:`,
    `requested=${multiplier || EA.multiplier},`,
    `using=${mult},`,
    `available=[${SYM_MULTIPLIERS[sym].available.join(",")}]`
  );

  // Build optional limit_order for TP/SL
  const limit_order = {};
  const tp = n(take_profit);
  const sl = n(stop_loss);
  if (tp > 0) limit_order.take_profit = tp;
  if (sl > 0) limit_order.stop_loss   = sl;

  const id = reqId++;
  pendingProposals[id] = {sym, amount, mult, direction, contract_type};

  // Multiplier proposal — no duration/duration_unit (multipliers have no fixed expiry)
  const proposal = {
    proposal          : 1,
    req_id            : id,
    contract_type,
    underlying_symbol : sym,
    amount,
    basis             : "stake",
    currency,
    multiplier        : mult,
  };

  if (Object.keys(limit_order).length) proposal.limit_order = limit_order;

  tradingWs.send(JSON.stringify(proposal));
  console.log(
    `[TRADE] MULT proposal — ${direction} ${sym}`,
    `stake=$${amount} x${mult}`,
    tp ? `TP=$${tp}` : "",
    sl ? `SL=$${sl}` : ""
  );
}

function closePosition(contractId) {
  tradingSend({sell:contractId, price:0});
}

function closeAll() {
  const count=positionsState.length;
  for (const pos of positionsState) tradingSend({sell:pos.ticket, price:0});
  broadcastDash({type:"CLOSE_ALL_RESULT",payload:{ok:true,closed:count}});
}

// ─────────────────────────────────────────
//  EXPRESS + DASHBOARD WS
// ─────────────────────────────────────────
const app    =express();
const server =http.createServer(app);
const dashWss=new WebSocketServer({noServer:true});

app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname,"public")));

server.on("upgrade",(req,socket,head)=>{
  if (req.url==="/ws")
    dashWss.handleUpgrade(req,socket,head,ws=>dashWss.emit("connection",ws));
  else socket.destroy();
});

function broadcastDash(msg) {
  const raw=JSON.stringify(msg);
  dashWss.clients.forEach(c=>{if(c.readyState===WebSocket.OPEN)c.send(raw);});
}

dashWss.on("connection",ws=>{
  console.log("[DASH] Browser connected");
  ws.send(JSON.stringify({type:"BRIDGE_STATUS",payload:{connected:tradingWsReady}}));
  if (accountState)
    ws.send(JSON.stringify({type:"STATE_UPDATE",payload:{
      account:accountState, positions:positionsState, settings:EA, sym_states:[],
    }}));
  if (allAccounts.length)
    ws.send(JSON.stringify({type:"ACCOUNTS_LIST",payload:{accounts:allAccounts,current:derivAccountId}}));

  ws.on("message",raw=>{
    let msg; try{msg=JSON.parse(raw);}catch{return;}
    const {type,payload}=msg;
    switch(type) {
      case "FIRE_TRADE":
        if (!tradingWsReady) {
          ws.send(JSON.stringify({type:"ERROR",payload:{message:"Trading not connected. Check DERIV_TOKEN."}}));
          return;
        }
        for (let i=0;i<(payload.count||1);i++) fireTrade(payload);
        break;
      case "CLOSE_POSITION": closePosition(payload.ticket); break;
      case "CLOSE_ALL":      closeAll(); break;
      case "UPDATE_SETTINGS":
        Object.assign(EA,payload);
        broadcastDash({type:"SETTINGS_ACK",payload:EA});
        break;
      case "GET_SETTINGS":
        ws.send(JSON.stringify({type:"SETTINGS",payload:EA}));
        break;
      case "GET_ACCOUNTS":
        ws.send(JSON.stringify({type:"ACCOUNTS_LIST",payload:{accounts:allAccounts,current:derivAccountId}}));
        break;
      case "SWITCH_ACCOUNT":
        if (!payload.account_id) return;
        console.log("[DASH] Switch to:",payload.account_id);
        derivAccountId=payload.account_id;
        accountState=null; positionsState=[];
        // Reset multiplier data so it re-fetches for the new account context
        SYMBOLS.forEach(s => SYM_MULTIPLIERS[s] = { min: 50, available: [] });
        contractsFetched = false;
        if (tradingWs) tradingWs.close();
        broadcastDash({type:"BRIDGE_STATUS",payload:{connected:false}});
        broadcastDash({type:"ACCOUNT_SWITCHED",payload:{account_id:payload.account_id}});
        setTimeout(connectTradingWs,1000);
        break;
      case "LOGIN_TOKEN":
        if (!payload.token) return;
        DERIV_TOKEN=payload.token;
        derivAccountId=null; allAccounts=[]; accountState=null; positionsState=[];
        SYMBOLS.forEach(s => SYM_MULTIPLIERS[s] = { min: 50, available: [] });
        contractsFetched = false;
        if (tradingWs) tradingWs.close();
        broadcastDash({type:"BRIDGE_STATUS",payload:{connected:false}});
        broadcastDash({type:"LOGGED_OUT",payload:{}});
        setTimeout(connectTradingWs,1000);
        break;
      case "LOGOUT":
        DERIV_TOKEN="";
        derivAccountId=null; allAccounts=[]; accountState=null; positionsState=[];
        SYMBOLS.forEach(s => SYM_MULTIPLIERS[s] = { min: 50, available: [] });
        contractsFetched = false;
        if (tradingWs) { tradingWs.close(); tradingWs=null; }
        tradingWsReady=false;
        broadcastDash({type:"BRIDGE_STATUS",payload:{connected:false}});
        broadcastDash({type:"LOGGED_OUT",payload:{}});
        break;
    }
  });
  ws.on("close",()=>console.log("[DASH] Browser disconnected"));
});

app.get("/api/state",(req,res)=>res.json({connected:tradingWsReady,account:accountState}));
app.get("*",(req,res)=>res.sendFile(path.join(__dirname,"public/index.html")));

// ─────────────────────────────────────────
//  LOOPS
// ─────────────────────────────────────────
setInterval(scanAll,3000);
setInterval(()=>{ if(tradingWsReady) tradingSend({portfolio:1}); },10000);

// ─────────────────────────────────────────
//  START
// ─────────────────────────────────────────
server.listen(PORT,()=>{
  console.log(`[START] Squeezy EA on port ${PORT}`);
  connectPublicWs();
  if (DERIV_TOKEN) connectTradingWs();
  else console.warn("[START] No DERIV_TOKEN — add to Render env vars");
});
