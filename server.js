// server/server.js — username-routed signaling server for P2P Voice, with
// optional Firebase Cloud Messaging push so calls ring even when the app
// is closed on the callee's phone.
//
// Run locally:    node server.js
// Requires:       npm install      (installs ws, plus firebase-admin if you
//                                   want push). FCM is optional.
//
// Environment variables:
//   PORT                       — port to listen on (default 8080)
//   FCM_SERVICE_ACCOUNT_JSON   — Firebase Admin service account JSON, AS A
//                                 STRING. If set, the server will send push
//                                 messages on `call`. If unset, FCM is
//                                 skipped silently and the server behaves
//                                 exactly like before.
//   FCM_SERVICE_ACCOUNT_FILE   — alternative: path to a JSON file on disk.

const http = require('http');
const WebSocket = require('ws');

const PORT = process.env.PORT || 8080;

// ----- Optional Firebase Admin setup -----
let fcm = null;
try {
  const accountStr  = process.env.FCM_SERVICE_ACCOUNT_JSON;
  const accountFile = process.env.FCM_SERVICE_ACCOUNT_FILE;
  let serviceAccount = null;
  if (accountStr) {
    serviceAccount = JSON.parse(accountStr);
  } else if (accountFile) {
    serviceAccount = JSON.parse(require('fs').readFileSync(accountFile, 'utf8'));
  }
  if (serviceAccount) {
    const admin = require('firebase-admin');
    admin.initializeApp({ credential: admin.credential.cert(serviceAccount) });
    fcm = admin.messaging();
    console.log('FCM: enabled');
  } else {
    console.log('FCM: disabled (no service account configured)');
  }
} catch (err) {
  console.log('FCM: disabled (' + err.message + ')');
}

// ----- HTTP server (for healthchecks + WebSocket upgrade) -----
const httpServer = http.createServer((req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      ok: true,
      online: users.size,
      fcm: !!fcm,
    }));
    return;
  }
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('P2P Voice signaling server\n');
});

const wss = new WebSocket.Server({ server: httpServer });

// ----- State -----
// username -> ws
const users = new Map();
// username -> { token, updated }  (FCM device token for push wakeup)
const fcmTokens = new Map();
// callId -> { caller, callee, peers:Set }
const calls = new Map();

function send(ws, obj) {
  if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(obj));
}

function uid() {
  return Math.random().toString(36).slice(2) + Date.now().toString(36);
}

// Clean up any calls a socket is part of, notifying the other peer.
function endCallsFor(ws) {
  for (const [callId, call] of calls) {
    if (call.peers.has(ws)) {
      for (const peer of call.peers) {
        if (peer !== ws) send(peer, { type: 'call_ended', callId });
      }
      calls.delete(callId);
    }
  }
}

// Send an FCM data message to wake the callee's app for an incoming call.
async function pushIncomingCall(toUsername, fromUsername, callId) {
  if (!fcm) return;
  const entry = fcmTokens.get(toUsername);
  if (!entry || !entry.token) return;
  const msg = {
    token: entry.token,
    // Data-only message (no `notification` field) so the Android service
    // handles it directly. Use high priority for immediate delivery.
    data: {
      type: 'incoming_call',
      from: fromUsername,
      callId: callId,
    },
    android: {
      priority: 'high',
      ttl: 30 * 1000,            // 30 seconds — calls don't matter once stale
    },
  };
  try {
    const id = await fcm.send(msg);
    console.log(`fcm sent to ${toUsername} (${id})`);
  } catch (err) {
    console.log(`fcm failed to ${toUsername}: ${err.message}`);
    // If the token is now invalid, drop it so we don't keep retrying.
    if (err.code === 'messaging/registration-token-not-registered'
        || err.code === 'messaging/invalid-registration-token') {
      fcmTokens.delete(toUsername);
    }
  }
}

wss.on('connection', (ws) => {
  ws.username = null;
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    switch (msg.type) {

      // --- Registration: client announces its username ---
      case 'register': {
        const name = (msg.username || '').trim().toLowerCase();
        if (!name) { send(ws, { type: 'register_error', reason: 'empty' }); return; }

        const existing = users.get(name);
        if (existing && existing !== ws) {
          endCallsFor(existing);
          send(existing, { type: 'force_logout', reason: 'logged_in_elsewhere' });
          try { existing.username = null; existing.terminate(); } catch (e) {}
          users.delete(name);
        }

        ws.username = name;
        users.set(name, ws);
        send(ws, { type: 'registered', username: name });
        console.log(`registered: ${name} (online=${users.size})`);
        break;
      }

      // --- FCM device token from a client; stored against its username ---
      case 'fcm_token': {
        if (!ws.username) return;
        const token = (msg.token || '').trim();
        if (!token) return;
        fcmTokens.set(ws.username, { token, updated: Date.now() });
        console.log(`fcm token registered for ${ws.username}`);
        break;
      }

      // --- Presence: is a username online? ---
      case 'presence': {
        const target = (msg.username || '').trim().toLowerCase();
        send(ws, { type: 'presence', username: target, online: users.has(target) });
        break;
      }

      // --- Caller initiates a call to a username ---
      case 'call': {
        const callee = (msg.to || '').trim().toLowerCase();
        const calleeWs = users.get(callee);
        // Even if not WebSocket-connected, we can still try to wake the
        // callee via FCM. Only mark "offline" if we have NO way to reach.
        if (!calleeWs && !fcmTokens.has(callee)) {
          send(ws, { type: 'call_failed', to: callee, reason: 'offline' });
          return;
        }
        if (calleeWs === ws) {
          send(ws, { type: 'call_failed', to: callee, reason: 'self' });
          return;
        }
        const callId = uid();
        const peers = new Set([ws]);
        if (calleeWs) peers.add(calleeWs);
        calls.set(callId, { caller: ws.username, callee, peers });
        ws.callId = callId;
        if (calleeWs) {
          send(calleeWs, { type: 'incoming_call', from: ws.username, callId });
        }
        // Push wakeup is independent of WebSocket presence: if the callee's
        // app is in background, the WS message above won't render UI fast
        // enough; the FCM push wakes it up and the WS message arrives once
        // the app reconnects.
        pushIncomingCall(callee, ws.username, callId);
        send(ws, { type: 'calling', to: callee, callId });
        console.log(`call ${ws.username} -> ${callee} (${callId})`);
        break;
      }

      // --- Callee accepts ---
      case 'accept': {
        const call = calls.get(msg.callId);
        if (!call) return;
        ws.callId = msg.callId;
        // If callee was woken by FCM, its WebSocket may have just reconnected
        // and not yet in the peers set. Add it now.
        if (ws.username === call.callee) call.peers.add(ws);
        const callerWs = users.get(call.caller);
        send(callerWs, { type: 'call_accepted', callId: msg.callId, initiator: true });
        send(ws, { type: 'call_accepted', callId: msg.callId, initiator: false });
        console.log(`accepted ${msg.callId}`);
        break;
      }

      // --- Callee rejects ---
      case 'reject': {
        const call = calls.get(msg.callId);
        if (!call) return;
        const callerWs = users.get(call.caller);
        send(callerWs, { type: 'call_rejected', callId: msg.callId });
        calls.delete(msg.callId);
        console.log(`rejected ${msg.callId}`);
        break;
      }

      // --- Hang up / cancel ---
      case 'hangup': {
        const call = calls.get(msg.callId);
        if (!call) return;
        for (const peer of call.peers) {
          if (peer !== ws) send(peer, { type: 'call_ended', callId: msg.callId });
        }
        calls.delete(msg.callId);
        console.log(`hangup ${msg.callId}`);
        break;
      }

      // --- WebRTC signaling relay (offer/answer/candidate) ---
      case 'offer':
      case 'answer':
      case 'candidate': {
        const call = calls.get(msg.callId);
        if (!call) return;
        // Make sure the sender is in the peer set (FCM-woken callees may
        // reconnect during the offer/answer dance).
        call.peers.add(ws);
        for (const peer of call.peers) {
          if (peer !== ws && peer.readyState === WebSocket.OPEN) {
            peer.send(raw.toString());
          }
        }
        break;
      }

      default:
        break;
    }
  });

  ws.on('close', () => {
    if (ws.username && users.get(ws.username) === ws) {
      users.delete(ws.username);
      console.log(`disconnected: ${ws.username} (online=${users.size})`);
    }
    endCallsFor(ws);
  });

  ws.on('error', () => {});
});

// --- Heartbeat: drop dead sockets so usernames get freed promptly ---
const heartbeat = setInterval(() => {
  for (const ws of wss.clients) {
    if (ws.isAlive === false) {
      try { ws.terminate(); } catch (e) {}
      continue;
    }
    ws.isAlive = false;
    try { ws.ping(); } catch (e) {}
  }
}, 30000);

wss.on('close', () => clearInterval(heartbeat));

httpServer.listen(PORT, () => {
  console.log(`Signaling server listening on :${PORT} (ws + http)`);
});
