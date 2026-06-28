/**
 * server.js — Squeezy EA  (New Deriv API — 2025)
 *
 * Flow:
 *  1. On startup, call REST GET /accounts to find your account ID
 *  2. Call REST POST /accounts/{id}/otp to get an authenticated WebSocket URL
 *  3. Connect to that WebSocket URL for live trading + market data
 *  4. Subscribe to M1 candles for all Volatility symbols
 *  5. Run squeeze detection every 3s, push alerts to dashboard
 *  6. Execute trades when dashboard fires them
 *
 * ENV VARS on Render.com:
 *   DERIV_TOKEN    = your PAT token from developers.deriv.com/dashboard/tokens/create
 *   DERIV_APP_ID   = 33FPKmmaz5Yxy6DuhhyVt
 *   DERIV_CLIENT_ID = 019eb390-b034-7ab0-860c-526190c7c3e6
 */

require("dotenv").config();
const express  = require("express");
const http     = require("http");
const { WebSocketServer, WebSocket } = require("ws");
const cors     = require("cors");
const path     = require("path");

const PORT             = process.env.PORT             || 3000;
let   DERIV_TOKEN      = process.env.DERIV_TOKEN      || "";
const DERIV_APP_ID     = process.env.DERIV_APP_ID     || "33FPKmmaz5Yxy6DuhhyVt";
const DERIV_CLIENT_ID  = process.env.DERIV_CLIENT_ID  || "019eb390-b034-7ab0-860c-526190c7c3e6";
const DERIV_REST_BASE  = "https://api.derivws.com";
const PUBLIC_WS_URL    = "wss://api.derivws.com/trading/v1/options/ws/public";

// ─────────────────────────────────────────
//  SYMBOLS  (Deriv API codes)
// ─────────────────────────────────────────
const SYMBOLS = [
  "1HZ10V","1HZ25V","1HZ50V","1HZ75V","1HZ100V","1HZ150V","1HZ250V",
  "R_10","R_25","R_50","R_75","R_100",
];
const SYM_NAMES = {
  "1HZ10V":"Volatility 10 Index","1HZ25V":"Volatility 25 Index",
  "1HZ50V":"Volatility 50 Index","1HZ75V":"Volatility 75 Index",
  "1HZ100V":"Volatility 100 Index","1HZ150V":"Volatility 150 Index",
  "1HZ250V":"Volatility 250 Index","R_10":"Volatility 10 (1s) Index",
  "R_25":"Volatility 25 (1s) Index","R_50":"Volatility 50 (1s) Index",
  "R_75":"Volatility 75 (1s) Index","R_100":"Volatility 100 (1s) Index",
};

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
//  INDICATOR MATH  (ported from MQ5)
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
  const mid=new Array(closes.length).fill(0);
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
    tr[i]=Math.max(bars[i].high-bars[i].low,
      Math.abs(bars[i].high-bars[i+1].close),
      Math.abs(bars[i].low-bars[i+1].close));
  }
  const out=new Array(n).fill(0);
  for (let i=0;i<=n-period;i++) out[i]=tr.slice(i,i+period).reduce((a,b)=>a+b,0)/period;
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
  const symStates=[], alerts=[];
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
      alerts.push(alert);
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
//  STATE
// ─────────────────────────────────────────
let accountState=null, positionsState=[], derivAccountId=null, allAccounts=[];
let tradingWs=null, tradingWsReady=false;
let reqId=1;
const pending={};

// ─────────────────────────────────────────
//  REST HELPERS  (new Deriv API)
// ─────────────────────────────────────────
async function derivRest(method, path, body=null) {
  const opts={
    method,
    headers:{
      "Authorization":`Bearer ${DERIV_TOKEN}`,
      "Deriv-App-ID": DERIV_APP_ID,
      "Content-Type":"application/json",
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
  // store all accounts for switcher
  allAccounts=accounts.map(a=>({
    id:    a.account_id||a.id,
    type:  (a.account_id||a.id||"").startsWith("DOT")||(a.account_type||"").toLowerCase().includes("demo")?"demo":"real",
    currency: a.currency||"USD",
    label: a.account_id||a.id,
  }));
  broadcastDash({type:"ACCOUNTS_LIST",payload:{accounts:allAccounts, current:derivAccountId}});
  // prefer demo — DOT=demo, ROT=real
  const demo=accounts.find(a=>{
    const id=(a.account_id||a.id||"");
    const type=(a.account_type||a.type||"").toLowerCase();
    return id.startsWith("DOT")||id.startsWith("VR")||type.includes("demo")||type.includes("virtual");
  });
  const chosen=demo||accounts[0];
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
  console.log("[DERIV] Connecting to public WebSocket for market data…");
  publicWs=new WebSocket(PUBLIC_WS_URL);

  publicWs.on("open",()=>{
    console.log("[DERIV] Public WS connected — loading candles…");
    for (const sym of SYMBOLS) {
      publicWs.send(JSON.stringify({
        ticks_history:sym, granularity:60, count:BARS_NEEDED,
        end:"latest", style:"candles", subscribe:1,
        req_id:reqId++,
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
    const sym=msg.echo_req?.ticks_history;
    if (sym&&candles[sym]!==undefined) {
      candles[sym]=(msg.candles||[]).map(c=>({
        open:parseFloat(c.open), high:parseFloat(c.high),
        low:parseFloat(c.low),  close:parseFloat(c.close), epoch:c.epoch,
      })).reverse().slice(0,BARS_NEEDED);
      console.log(`[DERIV] Loaded ${candles[sym].length} candles for ${sym}`);
    }
  } else if (type==="ohlc") {
    const o=msg.ohlc; const sym=o?.symbol;
    if (sym&&candles[sym]!==undefined) {
      const bar={open:parseFloat(o.open),high:parseFloat(o.high),
                 low:parseFloat(o.low),close:parseFloat(o.close),epoch:o.epoch};
      if (candles[sym].length&&candles[sym][0].epoch===bar.epoch)
        candles[sym][0]=bar;
      else { candles[sym].unshift(bar); if(candles[sym].length>BARS_NEEDED) candles[sym].pop(); }
    }
  }
}

// ─────────────────────────────────────────
//  TRADING WS  (authenticated — for trades + balance)
// ─────────────────────────────────────────
async function connectTradingWs() {
  if (!DERIV_TOKEN) {
    console.warn("[DERIV] No DERIV_TOKEN set — trading disabled, market data only");
    return;
  }
  try {
    // Step 1: get account ID
    if (!derivAccountId) derivAccountId=await getAccounts();
    if (!derivAccountId) {
      console.error("[DERIV] Could not get account ID — retrying in 30s");
      setTimeout(connectTradingWs,30000); return;
    }

    // Step 2: get OTP → authenticated WS URL
    const wsUrl=await getOTP(derivAccountId);
    if (!wsUrl) {
      console.error("[DERIV] Could not get OTP — retrying in 30s");
      setTimeout(connectTradingWs,30000); return;
    }

    console.log("[DERIV] Connecting to trading WebSocket…");
    tradingWs=new WebSocket(wsUrl);

    tradingWs.on("open",()=>{
      console.log("[DERIV] Trading WS connected — account:",derivAccountId);
      tradingWsReady=true;
      broadcastDash({type:"BRIDGE_STATUS",payload:{connected:true,account:derivAccountId}});
      // subscribe to balance
      tradingWs.send(JSON.stringify({balance:1,subscribe:1,req_id:reqId++}));
      // get portfolio
      tradingWs.send(JSON.stringify({portfolio:1,req_id:reqId++}));
    });

    tradingWs.on("message",raw=>{
      let msg; try{msg=JSON.parse(raw);}catch{return;}
      // resolve pending
      if (msg.req_id&&pending[msg.req_id]) {
        pending[msg.req_id](msg); delete pending[msg.req_id];
      }
      handleTradingMsg(msg);
    });

    tradingWs.on("close",()=>{
      console.log("[DERIV] Trading WS closed — getting new OTP in 10s…");
      tradingWsReady=false;
      broadcastDash({type:"BRIDGE_STATUS",payload:{connected:false}});
      // OTP is one-time — need to fetch a new one
      setTimeout(connectTradingWs,10000);
    });

    tradingWs.on("error",err=>console.error("[DERIV] Trading WS error:",err.message));

    // keepalive
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

function handleTradingMsg(msg) {
  const type=msg.msg_type;
  if (type==="balance") {
    accountState={
      ...accountState,
      balance:msg.balance?.balance||0,
      currency:msg.balance?.currency||"USD",
      equity:msg.balance?.balance||0,
      floating:0,
      login:derivAccountId,
      server:"Deriv",
    };
    broadcastDash({type:"POSITIONS_UPDATE",payload:{account:accountState,positions:positionsState}});
  } else if (type==="portfolio") {
    positionsState=(msg.portfolio?.contracts||[]).map(c=>({
      ticket:c.contract_id, symbol:c.symbol,
      dir:c.contract_type?.includes("CALL")?"BUY":"SELL",
      lot:1, open_price:c.buy_price,
      current_price:c.bid_price||c.buy_price,
      pnl:parseFloat(((c.bid_price||c.buy_price)-c.buy_price).toFixed(2)),
    }));
    broadcastDash({type:"POSITIONS_UPDATE",payload:{account:accountState,positions:positionsState}});
  } else if (type==="buy") {
    if (msg.error) broadcastDash({type:"TRADE_RESULT",payload:{ok:false,error:msg.error.message}});
    else {
      broadcastDash({type:"TRADE_RESULT",payload:{ok:true,contract_id:msg.buy?.contract_id,price:msg.buy?.buy_price}});
      tradingWs?.send(JSON.stringify({portfolio:1,req_id:reqId++}));
    }
  } else if (type==="sell") {
    if (msg.error) broadcastDash({type:"CLOSE_RESULT",payload:{ok:false,error:msg.error.message}});
    else {
      broadcastDash({type:"CLOSE_RESULT",payload:{ok:true}});
      tradingWs?.send(JSON.stringify({portfolio:1,req_id:reqId++}));
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
function fireTrade({symbol,direction,lot,tp_usd}) {
  const sym=Object.keys(SYM_NAMES).find(k=>SYM_NAMES[k]===symbol)||symbol;
  tradingSend({
    buy:1, price:tp_usd||2,
    parameters:{
      contract_type:direction==="BUY"?"CALL":"PUT",
      symbol:sym, duration:5, duration_unit:"m",
      basis:"payout", amount:tp_usd||2,
      currency:accountState?.currency||"USD",
    }
  });
}

function closePosition(contractId) { tradingSend({sell:contractId,price:0}); }

function closeAll() {
  for (const pos of positionsState) tradingSend({sell:pos.ticket,price:0});
  broadcastDash({type:"CLOSE_ALL_RESULT",payload:{ok:true,closed:positionsState.length}});
}

// ─────────────────────────────────────────
//  EXPRESS + DASHBOARD WS
// ─────────────────────────────────────────
const app=express();
const server=http.createServer(app);
const dashWss=new WebSocketServer({noServer:true});

app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname,"public")));

server.on("upgrade",(req,socket,head)=>{
  if (req.url==="/ws") dashWss.handleUpgrade(req,socket,head,ws=>dashWss.emit("connection",ws));
  else socket.destroy();
});

function broadcastDash(msg) {
  const raw=JSON.stringify(msg);
  dashWss.clients.forEach(c=>{if(c.readyState===WebSocket.OPEN)c.send(raw);});
}

dashWss.on("connection",ws=>{
  console.log("[DASH] Browser connected");
  ws.send(JSON.stringify({type:"BRIDGE_STATUS",payload:{connected:tradingWsReady}}));
  if (accountState) ws.send(JSON.stringify({type:"STATE_UPDATE",payload:{
    account:accountState, positions:positionsState, settings:EA, sym_states:[],
  }}));
  if (allAccounts.length) ws.send(JSON.stringify({type:"ACCOUNTS_LIST",payload:{accounts:allAccounts,current:derivAccountId}}));

  ws.on("message",raw=>{
    let msg; try{msg=JSON.parse(raw);}catch{return;}
    const {type,payload}=msg;
    switch(type) {
      case "FIRE_TRADE":
        if (!tradingWsReady) { ws.send(JSON.stringify({type:"ERROR",payload:{message:"Trading not connected. Check DERIV_TOKEN on Render."}})); return; }
        for (let i=0;i<(payload.count||1);i++) fireTrade(payload);
        break;
      case "CLOSE_POSITION": closePosition(payload.ticket); break;
      case "CLOSE_ALL":      closeAll(); break;
      case "UPDATE_SETTINGS": Object.assign(EA,payload); broadcastDash({type:"SETTINGS_ACK",payload:EA}); break;
      case "GET_SETTINGS":
        ws.send(JSON.stringify({type:"SETTINGS",payload:EA}));
        break;

      case "GET_ACCOUNTS":
        ws.send(JSON.stringify({type:"ACCOUNTS_LIST",payload:{accounts:allAccounts,current:derivAccountId}}));
        break;

      case "SWITCH_ACCOUNT":
        // Switch to a different account (same token)
        if (!payload.account_id) return;
        console.log("[DASH] Switch account to:",payload.account_id);
        derivAccountId = payload.account_id;
        accountState   = null;
        positionsState = [];
        if (tradingWs) tradingWs.close(); // triggers reconnect with new account
        broadcastDash({type:"BRIDGE_STATUS",payload:{connected:false}});
        broadcastDash({type:"ACCOUNT_SWITCHED",payload:{account_id:payload.account_id}});
        setTimeout(connectTradingWs, 1000);
        break;

      case "LOGIN_TOKEN":
        // Log in with a new PAT token
        if (!payload.token) return;
        console.log("[DASH] New token login requested");
        DERIV_TOKEN    = payload.token;
        derivAccountId = null;
        allAccounts    = [];
        accountState   = null;
        positionsState = [];
        if (tradingWs) tradingWs.close();
        broadcastDash({type:"BRIDGE_STATUS",payload:{connected:false}});
        broadcastDash({type:"LOGGED_OUT",payload:{}});
        setTimeout(connectTradingWs, 1000);
        break;

      case "LOGOUT":
        console.log("[DASH] Logout requested");
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
//  SCAN LOOP
// ─────────────────────────────────────────
setInterval(scanAll, 3000);
setInterval(()=>{ if(tradingWsReady) tradingSend({portfolio:1}); }, 5000);

// ─────────────────────────────────────────
//  START
// ─────────────────────────────────────────
server.listen(PORT,()=>{
  console.log(`Squeezy server on port ${PORT}`);
  connectPublicWs();   // market data — no token needed
  if (DERIV_TOKEN) connectTradingWs();  // trading — needs token
  else console.warn("[DERIV] No DERIV_TOKEN — add it to Render env vars to enable trading");
});
