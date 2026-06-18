/**
 * server.js — Squeezy EA Web Backend
 * Deployed on Render.com (free tier works fine)
 *
 * Two WebSocket routes:
 *   /bridge  — the VPS bridge connects here (authenticated)
 *   /ws      — browser dashboard connects here
 *
 * The server acts as a relay:
 *   Browser → command → Bridge → MT5
 *   MT5 → state update → Bridge → Browser
 */

require("dotenv").config();
const express    = require("express");
const http       = require("http");
const { WebSocketServer, WebSocket } = require("ws");
const cors       = require("cors");
const path       = require("path");
const { v4: uuidv4 } = require("uuid");

const PORT          = process.env.PORT          || 3000;
const BRIDGE_SECRET = process.env.BRIDGE_SECRET || "CHANGE_THIS_SECRET";

const app    = express();
const server = http.createServer(app);

app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

// ─────────────────────────────────────────
//  WEBSOCKET SERVERS
// ─────────────────────────────────────────

// Bridge WS (VPS connects here)
const bridgeWss = new WebSocketServer({ noServer: true });
// Dashboard WS (browsers connect here)
const dashWss   = new WebSocketServer({ noServer: true });

// Route upgrade requests by path
server.on("upgrade", (req, socket, head) => {
  const url = req.url;
  if (url === "/bridge") {
    bridgeWss.handleUpgrade(req, socket, head, ws => bridgeWss.emit("connection", ws, req));
  } else if (url === "/ws") {
    dashWss.handleUpgrade(req, socket, head, ws => dashWss.emit("connection", ws, req));
  } else {
    socket.destroy();
  }
});

// ─────────────────────────────────────────
//  STATE
// ─────────────────────────────────────────
let bridgeSocket  = null;   // single VPS bridge connection
let latestState   = null;   // last STATE_UPDATE from bridge
const pendingCmds = {};     // id → resolve for request-reply pattern

// ─────────────────────────────────────────
//  HELPERS
// ─────────────────────────────────────────
function broadcastDash(msg) {
  const raw = JSON.stringify(msg);
  dashWss.clients.forEach(c => {
    if (c.readyState === WebSocket.OPEN) c.send(raw);
  });
}

function sendBridge(type, payload, id) {
  if (!bridgeSocket || bridgeSocket.readyState !== WebSocket.OPEN) {
    return false;
  }
  bridgeSocket.send(JSON.stringify({ type, payload, id: id || uuidv4(), secret: BRIDGE_SECRET }));
  return true;
}

// ─────────────────────────────────────────
//  BRIDGE CONNECTION
// ─────────────────────────────────────────
bridgeWss.on("connection", (ws, req) => {
  console.log("[BRIDGE] VPS connected from", req.socket.remoteAddress);

  ws.on("message", raw => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    // Authenticate every message from bridge
    if (msg.secret !== BRIDGE_SECRET) {
      console.warn("[BRIDGE] Bad secret — dropping message");
      return;
    }

    const { type, payload, id } = msg;
    console.log(`[BRIDGE] ← ${type}`);

    switch (type) {
      case "HELLO":
        bridgeSocket = ws;
        broadcastDash({ type: "BRIDGE_STATUS", payload: { connected: true } });
        break;

      case "STATE_UPDATE":
        latestState = payload;
        broadcastDash({ type: "STATE_UPDATE", payload });
        break;

      case "TRADE_RESULT":
      case "CLOSE_RESULT":
      case "CLOSE_ALL_RESULT":
      case "SETTINGS_ACK":
      case "SETTINGS":
      case "TRADE_RESULT":
        // Forward result back to dashboards
        broadcastDash({ type, payload });
        // Resolve pending promise if any
        if (id && pendingCmds[id]) {
          pendingCmds[id](payload);
          delete pendingCmds[id];
        }
        break;

      case "PONG":
        break;

      default:
        broadcastDash({ type, payload });
    }
  });

  ws.on("close", () => {
    console.log("[BRIDGE] VPS disconnected");
    if (bridgeSocket === ws) bridgeSocket = null;
    broadcastDash({ type: "BRIDGE_STATUS", payload: { connected: false } });
  });

  ws.on("error", err => console.error("[BRIDGE] error:", err.message));
});

// ─────────────────────────────────────────
//  DASHBOARD CONNECTIONS
// ─────────────────────────────────────────
dashWss.on("connection", ws => {
  console.log("[DASH] Browser connected");

  // Send current state immediately
  if (latestState) ws.send(JSON.stringify({ type: "STATE_UPDATE", payload: latestState }));
  ws.send(JSON.stringify({ type: "BRIDGE_STATUS", payload: { connected: !!bridgeSocket } }));

  ws.on("message", raw => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }
    const { type, payload } = msg;
    console.log(`[DASH] → ${type}`);

    // Commands from browser → forward to bridge
    const bridgeCommands = [
      "FIRE_TRADE", "CLOSE_POSITION", "CLOSE_ALL",
      "UPDATE_SETTINGS", "GET_SETTINGS", "PING"
    ];
    if (bridgeCommands.includes(type)) {
      const ok = sendBridge(type, payload);
      if (!ok) ws.send(JSON.stringify({ type: "ERROR", payload: { message: "Bridge not connected — is the VPS running?" } }));
    }
  });

  ws.on("close", () => console.log("[DASH] Browser disconnected"));
  ws.on("error", err => console.error("[DASH] error:", err.message));
});

// ─────────────────────────────────────────
//  REST API  (fallback for non-WS clients)
// ─────────────────────────────────────────
app.get("/api/state", (req, res) => {
  res.json({ ok: true, state: latestState, bridgeConnected: !!bridgeSocket });
});

app.get("/api/bridge-status", (req, res) => {
  res.json({ connected: !!bridgeSocket });
});

// Serve dashboard for all other routes (SPA)
app.get("*", (req, res) => {
 res.sendFile(path.join(__dirname, "public/index.html"));
});

// ─────────────────────────────────────────
//  KEEPALIVE  (prevents Render free tier sleep)
// ─────────────────────────────────────────
setInterval(() => {
  if (bridgeSocket && bridgeSocket.readyState === WebSocket.OPEN) {
    bridgeSocket.send(JSON.stringify({ type: "PING", payload: {}, secret: BRIDGE_SECRET }));
  }
}, 25000);

// ─────────────────────────────────────────
//  START
// ─────────────────────────────────────────
server.listen(PORT, () => {
  console.log(`Squeezy backend running on port ${PORT}`);
  console.log(`Bridge WS:    ws://localhost:${PORT}/bridge`);
  console.log(`Dashboard WS: ws://localhost:${PORT}/ws`);
});
