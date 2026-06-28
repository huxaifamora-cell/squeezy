/**
 * server.js — Squeezy EA  (Deriv NEW API — 2025)
 *
 * New API breaking changes applied throughout:
 *  - proposal:  symbol → underlying_symbol, loginid removed, ask_price/payout now string|number
 *  - portfolio: symbol → underlying_symbol, loginid removed
 *  - proposal_open_contract: loginid removed, bid_price/buy_price/current_spot/profit now
 *                            string|number, sell_spot → exit_spot, display_value removed
 *  - buy: loginid removed, buy object always present in success response
 *  - active_symbols: symbol → underlying_symbol, display_name → underlying_symbol_name
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

// Maps API code → human display name
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

// Reverse map: display name → API code  (for fireTrade lookup)
const NAME_TO_SYM = Object.fromEntries(Object.entries(SYM_NAMES).map(([k,v])=>[v,k]));

// ─────────────────────────────────────────
//  EA PARAMETERS
// ─────────────────────────────────────────
let EA = {
  bb_period:10, bb_dev:2.0, squeeze_lookback:100, squeeze_percentile:50.0,
  contraction_bars:1, atr_lookback:100, atr_percentile:40.0,
  expansion_pct:20.0, breakout_bars:1, band_proximity:0.5, mbb_curve_bars:1,
  min_squeeze_score:60.0, sl_usd:2.00, tp_usd:2.00,
  alert_watch:true, alert_ready:true, alert_signal:false,
  cooldown_watch:300, cooldown_ready:120, cooldown_signal:300, ea_running:true,
};

// ─────────────────────────────────────────
//  CANDLE STORE
// ─────────────────────────────────────────
const BARS_NEEDED = 200;
const candles = {};
SYMBOLS.forEach(s => candles[s] = []);

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
const pendingCallbacks = {};   // reqId → callback fn  (renamed from 'pending' to avoid confusion)

// ─────────────────────────────────────────
//  SAFE NUMBER HELPER
//  New API returns many numeric fields as string|number — always parse safely
// ─────────────────────────────────────────
function n(v, fallback=0) {
  const f = parseFloat(v);
  return isNaN(f) ? fallback : f;
}

// ─────────────────────────────────────────
//  REST HELPERS
// ─────────────────────────────────────────
async function derivRest(method, path, body=null) {
  const opts={
    method,
    headers:{
      "Authorization" :`Bearer ${DERIV_TOKEN}`,
      "Deriv-App-ID"  : DERIV_APP_ID,
      "Content-Type"  :"application/json",
    },
  };
  if (body) opts.body=JSON.stringify(body);
  const res=await fetch(`${DERIV_REST_BASE}${path}`,opts);
  return res.json();
}

async function getAccounts() {
  console.log("[DERIV] Fetching accounts…");
  const data=await derivRest("GET","/trading/v1/options/accounts");
  if (data.errors) {
    console.error("[DERIV] Get accounts error:",JSON.stringify(data.errors));
    return null;
  }
  const accounts=data.data||[];
  console.log("[DERIV] Accounts found:",accounts.map(a=>a.account_id||a.id).join(", "));
  allAccounts=accounts.map(a=>({
    id      : a.account_id||a.id,
    type    : (a.account_id||a.id||"").startsWith("DOT")
              ||(a.account_type||"").toLowerCase().includes("demo")
              ? "demo" : "real",
    currency: a.currency||"USD",
    label   : a.account_id||a.id,
  }));
  broadcastDash({type:"ACCOUNTS_LIST",payload:{accounts:allAccounts,current:derivAccountId}});
  const demo=accounts.find(a=>{
    const id  =(a.account_id||a.id||"");
    const type=(a.account_type||a.type||"").toLowerCase();
    return id.startsWith("DOT")||id.startsWith("VR")
           ||type.includes("demo")||type.includes("virtual");
  });
  const chosen  =demo||accounts[0];
  const chosenId=chosen?.account_id||chosen?.id||null;
  console.log("[DERIV] Using account:",chosenId,(demo?"(demo)":"(first available)"));
  return chosenId;
}

async function getOTP(accountId) {
  console.log("[DERIV] Getting OTP for account:",accountId);
  const data=await derivRest("POST",`/trading/v1/options/accounts/${accountId}/otp`);
  if (data.errors) {
    console.error("[DERIV] OTP error:",JSON.stringify(data.errors));
    return null;
  }
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
        ticks_history: sym,
        granularity  : 60,
        count        : BARS_NEEDED,
        end          : "latest",
        style        : "candles",
        subscribe    : 1,
        req_id       : reqId++,
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

  // keepalive
  setInterval(()=>{
    if (publicWs?.readyState===WebSocket.OPEN)
      publicWs.send(JSON.stringify({ping:1}));
  },25000);
}

function handleMarketData(msg) {
  const type=msg.msg_type;
  if (type==="candles") {
    // New API: echo_req still contains ticks_history for candle history responses
    const sym=msg.echo_req?.ticks_history;
    if (sym&&candles[sym]!==undefined) {
      candles[sym]=(msg.candles||[]).map(c=>({
        open :n(c.open),
        high :n(c.high),
        low  :n(c.low),
        close:n(c.close),
        epoch:c.epoch,
      })).reverse().slice(0,BARS_NEEDED);
      console.log(`[CANDLE] ${sym}: ${candles[sym].length} bars loaded`);
    }
  } else if (type==="ohlc") {
    // Live OHLC stream — symbol field unchanged in new API
    const o=msg.ohlc;
    const sym=o?.symbol;
    if (sym&&candles[sym]!==undefined) {
      const bar={
        open :n(o.open),
        high :n(o.high),
        low  :n(o.low),
        close:n(o.close),
        epoch:o.epoch,
      };
      if (candles[sym].length&&candles[sym][0].epoch===bar.epoch)
        candles[sym][0]=bar;
      else {
        candles[sym].unshift(bar);
        if (candles[sym].length>BARS_NEEDED) candles[sym].pop();
      }
    }
  }
}

// ─────────────────────────────────────────
//  TRADING WS  (authenticated)
// ─────────────────────────────────────────
async function connectTradingWs() {
  if (!DERIV_TOKEN) {
    console.warn("[DERIV] No DERIV_TOKEN — market data only");
    return;
  }
  try {
    // Step 1: get account ID via REST
    if (!derivAccountId) derivAccountId=await getAccounts();
    if (!derivAccountId) {
      console.error("[DERIV] No account ID — retry in 30s");
      setTimeout(connectTradingWs,30000); return;
    }

    // Step 2: get one-time authenticated WS URL via REST
    const wsUrl=await getOTP(derivAccountId);
    if (!wsUrl) {
      console.error("[DERIV] No OTP URL — retry in 30s");
      setTimeout(connectTradingWs,30000); return;
    }

    console.log("[DERIV] Connecting trading WS…");
    tradingWs=new WebSocket(wsUrl);

    tradingWs.on("open",()=>{
      console.log("[DERIV] Trading WS open — account:",derivAccountId);
      tradingWsReady=true;
      broadcastDash({type:"BRIDGE_STATUS",payload:{connected:true,account:derivAccountId}});

      // Balance stream
      tradingWs.send(JSON.stringify({balance:1, subscribe:1, req_id:reqId++}));

      // Initial portfolio snapshot
      tradingWs.send(JSON.stringify({portfolio:1, req_id:reqId++}));

      // Subscribe to ALL open contract updates — drives live position display
      // New API: no loginid field
      tradingWs.send(JSON.stringify({
        proposal_open_contract: 1,
        subscribe              : 1,
        req_id                 : reqId++,
      }));
    });

    tradingWs.on("message",raw=>{
      let msg; try{msg=JSON.parse(raw);}catch{return;}
      // Resolve any one-shot callbacks
      if (msg.req_id && pendingCallbacks[msg.req_id]) {
        pendingCallbacks[msg.req_id](msg);
        delete pendingCallbacks[msg.req_id];
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
      if (tradingWs?.readyState===WebSocket.OPEN)
        tradingWs.send(JSON.stringify({ping:1}));
      else clearInterval(ka);
    },25000);

  } catch(err) {
    console.error("[DERIV] connectTradingWs error:",err.message);
    setTimeout(connectTradingWs,15000);
  }
}

// ─────────────────────────────────────────
//  TRADING MESSAGE HANDLER
// ─────────────────────────────────────────
function handleTradingMsg(msg) {
  const type=msg.msg_type;

  // ── BALANCE ──────────────────────────────────────────────────────────────
  if (type==="balance") {
    // New API: balance/currency fields unchanged, still numeric
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

  // ── PORTFOLIO SNAPSHOT ───────────────────────────────────────────────────
  } else if (type==="portfolio") {
    // New API: contract.symbol → contract.underlying_symbol
    const contracts=msg.portfolio?.contracts||[];
    positionsState=contracts.map(c=>({
      ticket       : c.contract_id,
      // New API field: underlying_symbol (replaces symbol)
      symbol       : SYM_NAMES[c.underlying_symbol] || c.underlying_symbol || "Unknown",
      dir          : c.contract_type?.includes("CALL")?"BUY":"SELL",
      // stake = actual amount staked (buy_price in portfolio is the purchase cost)
      lot          : n(c.buy_price).toFixed(2),
      open_price   : n(c.buy_price).toFixed(5),
      current_price: n(c.bid_price||c.buy_price).toFixed(5),
      // bid_price - buy_price = current P/L for portfolio snapshot
      pnl          : parseFloat((n(c.bid_price||c.buy_price) - n(c.buy_price)).toFixed(2)),
    }));
    broadcastDash({type:"POSITIONS_UPDATE",payload:{account:accountState,positions:positionsState}});

  // ── LIVE CONTRACT UPDATES ────────────────────────────────────────────────
  } else if (type==="proposal_open_contract") {
    const c=msg.proposal_open_contract;
    if (!c||!c.contract_id) return;

    if (c.is_sold) {
      // Contract expired or manually closed
      positionsState=positionsState.filter(p=>p.ticket!==c.contract_id);
      console.log(`[POS] Contract ${c.contract_id} sold/expired — removed`);
    } else {
      const idx=positionsState.findIndex(p=>p.ticket===c.contract_id);

      // New API field mapping:
      //   underlying_symbol  — replaces symbol/underlying
      //   buy_price          — string|number  (stake / purchase cost)
      //   current_spot       — string|number  (live market price)
      //   profit             — string|number  (current P/L)
      //   exit_spot          — replaces sell_spot (only set when sold)
      //   payout             — string (always string in new API)
      //   display_value      — REMOVED in new API (do not reference)
      const symCode = c.underlying_symbol || "";
      const pos={
        ticket       : c.contract_id,
        symbol       : SYM_NAMES[symCode] || symCode || "Unknown",
        dir          : c.contract_type?.includes("CALL")?"BUY":"SELL",
        lot          : n(c.buy_price).toFixed(2),           // stake amount
        open_price   : n(c.entry_spot||c.buy_price).toFixed(5),
        current_price: n(c.current_spot||c.entry_spot||c.buy_price).toFixed(5),
        pnl          : parseFloat(n(c.profit).toFixed(2)),  // n() handles string|number
        payout       : n(c.payout),                         // string in new API → parse
        contract_type: c.contract_type||"",
        expiry       : c.date_expiry||null,
      };

      if (idx !== -1) {
        positionsState[idx]=pos;
      } else {
        positionsState.push(pos);
        console.log(`[POS] New position: ${pos.symbol} ${pos.dir} stake=$${pos.lot} contract=${c.contract_id}`);
      }
    }

    // Keep floating P/L in sync on account strip
    const floating=positionsState.reduce((s,p)=>s+(p.pnl||0),0);
    if (accountState) accountState={...accountState,floating};
    broadcastDash({type:"POSITIONS_UPDATE",payload:{account:accountState,positions:positionsState}});

  // ── PROPOSAL RESPONSE ─────────────────────────────────────────────────────
  } else if (type==="proposal") {
    if (msg.error) {
      console.error("[TRADE] Proposal error:",msg.error.message);
      broadcastDash({type:"TRADE_RESULT",payload:{ok:false,error:msg.error.message}});
      delete pendingProposals[msg.req_id];
      return;
    }

    // New API: proposal.id is the only required field; ask_price is string|number
    const proposal_id = msg.proposal?.id;
    // n() safely parses string|number
    const ask_price   = n(msg.proposal?.ask_price);

    const pendingInfo = pendingProposals[msg.req_id];
    if (!proposal_id || !pendingInfo) return;
    delete pendingProposals[msg.req_id];

    console.log(`[TRADE] Got proposal ${proposal_id} ask=$${ask_price} — buying…`);

    // New API: buy request — no loginid field
    tradingWs.send(JSON.stringify({
      buy    : proposal_id,
      price  : ask_price,
      req_id : reqId++,
    }));

  // ── BUY RESPONSE ──────────────────────────────────────────────────────────
  } else if (type==="buy") {
    if (msg.error) {
      broadcastDash({type:"TRADE_RESULT",payload:{ok:false,error:msg.error.message}});
      return;
    }
    // New API: buy object is always present on success
    const b=msg.buy;
    const contractId = b?.contract_id;
    // buy_price, payout are numeric (same as legacy)
    const buyPrice   = n(b?.buy_price);
    const payout     = n(b?.payout);

    broadcastDash({type:"TRADE_RESULT",payload:{
      ok:true, contract_id:contractId, price:buyPrice, payout,
    }});
    console.log(`[TRADE] Buy confirmed — contract ${contractId} stake=$${buyPrice} payout=$${payout}`);

    // Subscribe to this specific contract for immediate position display
    // (the global subscription may take a moment to pick it up)
    if (contractId && tradingWs?.readyState===WebSocket.OPEN) {
      tradingWs.send(JSON.stringify({
        proposal_open_contract: 1,
        contract_id           : contractId,
        subscribe             : 1,
        req_id                : reqId++,
      }));
    }

    // Also refresh portfolio after short delay as belt-and-suspenders
    setTimeout(()=>{
      if (tradingWs?.readyState===WebSocket.OPEN)
        tradingWs.send(JSON.stringify({portfolio:1,req_id:reqId++}));
    }, 2000);

  // ── SELL RESPONSE ─────────────────────────────────────────────────────────
  } else if (type==="sell") {
    if (msg.error) {
      broadcastDash({type:"CLOSE_RESULT",payload:{ok:false,error:msg.error.message}});
    } else {
      const sold_for=n(msg.sell?.sold_for);
      broadcastDash({type:"CLOSE_RESULT",payload:{ok:true,sold_for}});
      console.log(`[TRADE] Sell confirmed — sold for $${sold_for}`);
      // Refresh portfolio
      setTimeout(()=>{
        if (tradingWs?.readyState===WebSocket.OPEN)
          tradingWs.send(JSON.stringify({portfolio:1,req_id:reqId++}));
      }, 1000);
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
//  TRADE EXECUTION
// ─────────────────────────────────────────
// Maps proposal reqId → metadata while waiting for proposal → buy flow
const pendingProposals={};

function fireTrade({symbol, direction, lot, tp_usd}) {
  // Resolve symbol code: accept either display name or raw API code
  const sym    = NAME_TO_SYM[symbol] || symbol;
  const amount = n(tp_usd) || 2;
  // New API: CALL/PUT for binary options (Rise/Fall)
  const contract_type = direction==="BUY" ? "CALL" : "PUT";
  const currency      = accountState?.currency || "USD";

  const id=reqId++;
  pendingProposals[id]={contract_type, amount, currency, sym};

  // New API proposal request: underlying_symbol (not symbol), no loginid
  tradingWs.send(JSON.stringify({
    proposal          : 1,
    req_id            : id,
    contract_type,
    underlying_symbol : sym,          // ← new API field name
    duration          : 5,
    duration_unit     : "m",
    basis             : "stake",
    amount,
    currency,
  }));
  console.log(`[TRADE] Proposal requested — ${direction} ${sym} stake=$${amount}`);
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
  else
    socket.destroy();
});

function broadcastDash(msg) {
  const raw=JSON.stringify(msg);
  dashWss.clients.forEach(c=>{
    if (c.readyState===WebSocket.OPEN) c.send(raw);
  });
}

dashWss.on("connection",ws=>{
  console.log("[DASH] Browser connected");

  // Send current state immediately on connect
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

      case "CLOSE_POSITION":
        closePosition(payload.ticket);
        break;

      case "CLOSE_ALL":
        closeAll();
        break;

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
        console.log("[DASH] Switching account to:",payload.account_id);
        derivAccountId = payload.account_id;
        accountState   = null;
        positionsState = [];
        if (tradingWs) tradingWs.close();  // triggers reconnect with new account
        broadcastDash({type:"BRIDGE_STATUS",payload:{connected:false}});
        broadcastDash({type:"ACCOUNT_SWITCHED",payload:{account_id:payload.account_id}});
        setTimeout(connectTradingWs,1000);
        break;

      case "LOGIN_TOKEN":
        if (!payload.token) return;
        console.log("[DASH] New token login");
        DERIV_TOKEN    = payload.token;
        derivAccountId = null;
        allAccounts    = [];
        accountState   = null;
        positionsState = [];
        if (tradingWs) tradingWs.close();
        broadcastDash({type:"BRIDGE_STATUS",payload:{connected:false}});
        broadcastDash({type:"LOGGED_OUT",payload:{}});
        setTimeout(connectTradingWs,1000);
        break;

      case "LOGOUT":
        console.log("[DASH] Logout");
        DERIV_TOKEN    = "";
        derivAccountId = null;
        allAccounts    = [];
        accountState   = null;
        positionsState = [];
        if (tradingWs) { tradingWs.close(); tradingWs=null; }
        tradingWsReady = false;
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
setInterval(scanAll, 3000);

// Periodic portfolio refresh to catch anything missed by subscriptions
setInterval(()=>{
  if (tradingWsReady) tradingSend({portfolio:1});
}, 10000);

// ─────────────────────────────────────────
//  START
// ─────────────────────────────────────────
server.listen(PORT,()=>{
  console.log(`[START] Squeezy EA on port ${PORT}`);
  connectPublicWs();
  if (DERIV_TOKEN) connectTradingWs();
  else console.warn("[START] No DERIV_TOKEN — add to Render env vars to enable trading");
});
