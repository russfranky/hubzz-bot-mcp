#!/usr/bin/env node
/**
 * Hubzz Bot MCP Server — Enhanced
 *
 * Exposes tools for spawning, controlling, and testing batches of bots in Hubzz worlds.
 * Connects to game servers via WebSocket using the Hubzz game protocol.
 *
 * Protocol: MCP (stdio newline-delimited JSON-RPC 2.0)
 * Game protocol: JSON + U+F8FF delimiter over WebSocket
 */

import WebSocket from 'ws';
import { EventEmitter } from 'events';
import https from 'https';
import net from 'net';

// True when run directly (node bot-mcp.mjs), false when imported for unit
// tests. Server-only side effects (token check, stdin loop) are gated on this.
const isMainModule = (() => {
  try {
    const mainPath = process.argv[1] || '';
    return mainPath !== '' && (mainPath.endsWith('/bot-mcp.mjs') || mainPath.endsWith('\\bot-mcp.mjs'));
  } catch { return true; }
})();
import tls from 'tls';
import wrtcPkg from '@roamhq/wrtc';
import { Device } from 'mediasoup-client';
import { io } from 'socket.io-client';

// Polyfill WebRTC globals for mediasoup-client (Node.js)
const { RTCPeerConnection, RTCSessionDescription, RTCIceCandidate, MediaStream, MediaStreamTrack, nonstandard } = wrtcPkg;
const { RTCAudioSource } = nonstandard;
globalThis.RTCPeerConnection = RTCPeerConnection;
globalThis.RTCSessionDescription = RTCSessionDescription;
globalThis.RTCIceCandidate = RTCIceCandidate;
globalThis.MediaStream = MediaStream;
globalThis.MediaStreamTrack = MediaStreamTrack;

// --- Configuration ---

const DEFAULT_WS_URL = process.env.HUBZZ_WS_URL || 'wss://hubzz.app/socket/';
// Cherry-picked from archived russfranky/hubzz-alpha (packages/bot-mcp): the
// server_* tools query the Hubzz HTTP API. Default points at production.
const DEFAULT_API_URL = process.env.HUBZZ_API_URL || 'https://hubzz.app';
// S-001: wsUrl allowlist — the real HUBZZ_BOT_TOKEN is sent in the login frame,
// so never connect to an arbitrary host. Returns error string or null.
const WS_URL_ALLOWLIST = ['hubzz.xyz', 'hubzz.app', 'localhost', '127.0.0.1'];
function validateWsUrl(raw) {
  const wsUrl = raw || DEFAULT_WS_URL;
  let host;
  try { host = new URL(wsUrl).hostname.toLowerCase(); }
  catch (_) { return { error: 'wsUrl must be a valid WebSocket URL' }; }
  const ok = WS_URL_ALLOWLIST.some(h => host === h || host.endsWith('.' + h));
  if (!ok) return { error: `wsUrl host not allowed: ${host}` };
  return { wsUrl };
}
const BOT_TOKEN = process.env.HUBZZ_BOT_TOKEN;
// The token is only required when running as the MCP server, not when
// imported for unit tests.
if (!BOT_TOKEN && isMainModule) { console.error('[bot-mcp] HUBZZ_BOT_TOKEN env var is required'); process.exit(1); }
const DELIMITER = '\uF8FF';
const MAX_CHAT_BUFFER = 50;
const MAX_EVENT_BUFFER = 200;
const MAX_ERRORS = 50;
const MAX_NOTICES = 20;
const MAX_LATENCIES = 20;
const MAX_BATCH_SIZE = 20;
const MAX_STRESS_DURATION = 60;

const AVAILABLE_EMOTES = [
  'idle', 'wave', 'clap', 'thumbs_up', 'spawn',
  'dance', 'dance1', 'dance2', 'dance3', 'dance4', 'dance5',
  'dance6', 'dance7', 'dance8', 'dance9', 'dance_flair',
  'sad', 'giddy', 'ugh', 'beg', 'yay', 'waiting',
];

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

// HTTPS Agent that tunnels through the sandbox egress proxy via CONNECT.
// Used by fetchJson when https_proxy/HTTPS_PROXY is set (no direct egress).
// NOTE: Node's https module aborts responses on manually-tunneled TLS sockets
// (see D-001 investigation). We shell out to curl for the proxy case instead,
// which handles CONNECT tunneling reliably.
import { execFile } from 'child_process';

function fetchJsonViaCurl(url, timeoutMs) {
  return new Promise((resolve, reject) => {
    // S-002: validate URL before it reaches curl — blocks option injection
    // (e.g. mapUrl:'-o /tmp/pwned') and non-http(s) SSRF vectors.
    let parsed;
    try { parsed = new URL(url); }
    catch (_) { reject(new Error('fetchJson: invalid URL')); return; }
    if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') {
      reject(new Error('fetchJson: only http(s) URLs allowed')); return;
    }
    const args = [
      '-sS', '-f',           // silent, fail on HTTP error
      '--proxy', process.env.https_proxy || process.env.HTTPS_PROXY,
      '--max-time', String(Math.ceil(timeoutMs / 1000)),
      '-L', '--max-redirs', '5',
      '--',                  // S-002: end of options — url cannot be parsed as flags
      url,
    ];
    execFile('curl', args, { maxBuffer: 10 * 1024 * 1024 }, (err, stdout, stderr) => {
      if (err) {
        reject(new Error(`fetchJson curl failed: ${err.message} ${stderr.slice(0, 200)}`));
        return;
      }
      try {
        resolve(JSON.parse(stdout));
      } catch (e) {
        reject(new Error(`fetchJson curl JSON parse failed: ${e.message}`));
      }
    });
  });
}

function fetchJson(url, timeoutMs = 15000, _redirects = 0) {
  return new Promise((resolve, reject) => {
    let settled = false;
    const fail = (err) => { if (!settled) { settled = true; reject(err); } };
    const ok = (val) => { if (!settled) { settled = true; resolve(val); } };
    const timeoutErr = () => new Error(`fetchJson timeout after ${timeoutMs}ms: ${url}`);
    const timer = setTimeout(() => fail(timeoutErr()), timeoutMs);
    const onBody = (res) => {
      // Follow redirects (e.g. hubzz.xyz -> hubzz.app)
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        clearTimeout(timer);
        if (_redirects >= 5) { fail(new Error(`fetchJson: too many redirects: ${url}`)); return; }
        res.resume();
        fetchJson(new URL(res.headers.location, url).toString(), timeoutMs, _redirects + 1).then(ok, fail);
        return;
      }
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => { clearTimeout(timer); try { ok(JSON.parse(data)); } catch (e) { fail(e); } });
      res.on('error', (e) => { clearTimeout(timer); fail(e); });
    };
    const wireReq = (req) => {
      req.on('timeout', () => { req.destroy(); fail(timeoutErr()); });
      req.on('error', (e) => { clearTimeout(timer); fail(e); });
    };

    const proxyEnv = process.env.https_proxy || process.env.HTTPS_PROXY || '';
    if (proxyEnv) {
      // No direct egress: use curl for CONNECT tunneling (Node's https aborts
      // manually-tunneled TLS sockets; see D-001). Curl handles proxy, redirects,
      // and timeouts reliably.
      clearTimeout(timer);
      fetchJsonViaCurl(url, timeoutMs).then(ok, fail);
    } else {
      wireReq(https.get(url, { timeout: timeoutMs }, onBody));
    }
  });
}

// HTTP GET returning { status, body } without throwing on non-2xx — used by the
// server_* tools cherry-picked from archived russfranky/hubzz-alpha. Same S-002
// URL validation as fetchJsonViaCurl: http(s) only, '--' end-of-options.
function httpGetJson(url, timeoutMs = 30000, retries = 2) {
  return new Promise((resolve, reject) => {
    let parsed;
    try { parsed = new URL(url); }
    catch (_) { reject(new Error('httpGetJson: invalid URL')); return; }
    if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') {
      reject(new Error('httpGetJson: only http(s) URLs allowed')); return;
    }
    const attempt = (n) => {
      const args = [
        '-sS',
        '--proxy', process.env.https_proxy || process.env.HTTPS_PROXY,
        '--max-time', String(Math.ceil(timeoutMs / 1000)),
        '-L', '--max-redirs', '5',
        '-w', '\n%{http_code}',
        '--',
        url,
      ];
      execFile('curl', args, { maxBuffer: 10 * 1024 * 1024 }, (err, stdout, stderr) => {
        if (err) {
          if (n < retries) { setTimeout(() => attempt(n + 1), 1500 * n); return; }
          reject(new Error(`httpGetJson curl failed: ${err.message} ${String(stderr).slice(0, 200)}`));
          return;
        }
        const idx = stdout.lastIndexOf('\n');
        const status = Number(stdout.slice(idx + 1).trim());
        const rawBody = stdout.slice(0, idx);
        let body = null;
        try { body = rawBody ? JSON.parse(rawBody) : null; }
        catch (_) { body = { _raw: rawBody.slice(0, 2000) }; }
        resolve({ status: Number.isFinite(status) ? status : 0, body });
      });
    };
    attempt(1);
  });
}

// Exponential falloff gain: (refDistance / max(refDistance, d))^rolloffFactor
function spatialGain(d, refDistance = 1, rolloffFactor = 0.75) {
  if (d <= 0) return 1;
  return Math.pow(refDistance / Math.max(refDistance, d), rolloffFactor);
}

// --- Sonar / navigation helpers (bot_sonar, bot_navigate) ---

// In-memory world-map cache keyed by mapUrl.
const mapCache = new Map();
async function getCachedMap(mapUrl, retries = 3) {
  if (mapCache.has(mapUrl)) return mapCache.get(mapUrl);
  // The egress proxy is slow and flaky: retry with backoff, and fall back to
  // the canonical hubzz.app host if the hubzz.xyz URL keeps failing.
  const urls = [mapUrl];
  if (mapUrl.includes('hubzz.xyz')) urls.push(mapUrl.replace('hubzz.xyz', 'hubzz.app'));
  let lastErr = null;
  for (const u of urls) {
    for (let attempt = 1; attempt <= retries; attempt++) {
      try {
        const data = await fetchJson(u, 45000);
        const tiles = Object.values(data.tiles || {});
        const walkable = tiles.filter(t => t.walkable);
        const byId = new Map();
        for (const t of tiles) byId.set(Number(t.id), t);
        // Seat objects (chairs etc.) live in data.objects with a tile id.
        const seats = Array.isArray(data.objects)
          ? data.objects.filter(o => o && o.tile != null).map(o => ({
              name: o.name || o.type || 'seat',
              type: o.type || 'seat',
              tile: Number(o.tile),
            }))
          : [];
        const entry = { tiles, walkable, byId, seats };
        mapCache.set(mapUrl, entry);
        return entry;
      } catch (e) {
        lastErr = e;
        await sleep(1000 * attempt);
      }
    }
  }
  throw lastErr;
}

// Compass convention matches bot_find_tiles: 0 deg = East (+X), 90 = North (-Z).
function bearingFromDelta(dx, dz) {
  return (Math.atan2(-dz, dx) * 180 / Math.PI + 360) % 360;
}
function compassFromDelta(dx, dz) {
  const dirs = ['E', 'NE', 'N', 'NW', 'W', 'SW', 'S', 'SE'];
  return dirs[Math.round(bearingFromDelta(dx, dz) / 45) % 8];
}
// Server rotation: {x: pitch, y: yaw, z: 0} where yaw = atan2(dir.x, dir.z).
// Facing vector is (sin(yaw), cos(yaw)) in (x, z). Returns a compass label
// like "SE" or null when the rotation is unknown.
export function facingFromRotation(rot) {
  if (!rot || !Number.isFinite(rot.y)) return null;
  return compassFromDelta(Math.sin(rot.y), Math.cos(rot.y));
}
// Mirrors server moveTiming.ts: per-step duration scales with segment length
// so diagonal steps take √2× the base. baseStepMs 500 (250 when boosted).
export function stepDurationMs(from, to, baseStepMs, tileSize = 2) {
  const dist = Math.hypot(to.x - from.x, to.z - from.z);
  const size = tileSize || 2;
  return Math.max(1, Math.round(baseStepMs * (dist / size)));
}
export { bearingFromDelta, compassFromDelta, dist2 };
function dist2(ax, az, bx, bz) {
  return Math.hypot(ax - bx, az - bz);
}
function nearestWalkableTile(walkable, x, z) {
  let best = null, bd = Infinity;
  for (const t of walkable) {
    const d = dist2(t.x, t.z, x, z);
    if (d < bd) { bd = d; best = t; }
  }
  return best;
}

// A* over walkable tiles, 8-connected (neighbor dist <= 3.0 catches diagonals on
// the 2-unit tile grid). Extra nodes (start/goal) may be non-walkable.
function astarPath(walkable, byId, startTile, goalTile) {
  const nodes = new Map();
  const nodeFor = (t) => {
    const id = Number(t.id);
    if (!nodes.has(id)) nodes.set(id, { tile: t, neighbors: [] });
    return nodes.get(id);
  };
  for (const t of walkable) nodeFor(t);
  nodeFor(startTile); nodeFor(goalTile);
  const all = [...nodes.values()];
  for (let i = 0; i < all.length; i++) {
    for (let j = i + 1; j < all.length; j++) {
      const a = all[i].tile, b = all[j].tile;
      if (dist2(a.x, a.z, b.x, b.z) <= 3.0) {
        all[i].neighbors.push(all[j]);
        all[j].neighbors.push(all[i]);
      }
    }
  }
  const startId = Number(startTile.id), goalId = Number(goalTile.id);
  const h = (n) => dist2(n.tile.x, n.tile.z, goalTile.x, goalTile.z);
  const open = [nodes.get(startId)];
  const gScore = new Map([[startId, 0]]);
  const cameFrom = new Map();
  const closed = new Set();
  while (open.length > 0) {
    let bi = 0;
    for (let i = 1; i < open.length; i++) {
      const f = (gScore.get(Number(open[i].tile.id)) ?? Infinity) + h(open[i]);
      const bf = (gScore.get(Number(open[bi].tile.id)) ?? Infinity) + h(open[bi]);
      if (f < bf) bi = i;
    }
    const cur = open.splice(bi, 1)[0];
    const curId = Number(cur.tile.id);
    if (curId === goalId) {
      const path = [cur.tile];
      let c = curId;
      while (cameFrom.has(c)) { c = cameFrom.get(c); path.unshift(nodes.get(c).tile); }
      return path;
    }
    if (closed.has(curId)) continue;
    closed.add(curId);
    for (const nb of cur.neighbors) {
      const nbId = Number(nb.tile.id);
      if (closed.has(nbId)) continue;
      const tentative = (gScore.get(curId) ?? Infinity) + dist2(cur.tile.x, cur.tile.z, nb.tile.x, nb.tile.z);
      if (tentative < (gScore.get(nbId) ?? Infinity)) {
        cameFrom.set(nbId, curId);
        gScore.set(nbId, tentative);
        if (!open.includes(nb)) open.push(nb);
      }
    }
  }
  return null;
}

// --- Bot Connection ---

class BotConnection extends EventEmitter {
  constructor(wsUrl, username, vrmUrl = '', opts = {}) {
    super();
    this.wsUrl = wsUrl;
    this.username = username;
    this.vrmUrl = vrmUrl;
    this.isGuest = opts.isGuest || false;
    this.token = opts.token || BOT_TOKEN;
    this.ws = null;
    this.connected = false;
    this.intentionallyClosed = false;
    this.knownUsers = new Map();
    this.chatBuffer = [];
    this.connectionTimeout = null;

    // Position tracking
    this.ownTile = null;
    this.ownPosition = null;
    this.ownRotation = null;
    // ownUserId: the bot's server-side user id, learned from the arrival echo
    // of the first w:move we send (the server broadcasts our steps back),
    // or from the serverUsernameHint at spawn.
    this.ownUserId = null;
    this.serverUsernameHint = null;
    // _pendingMove: {tile, sentAt, resolve} while we wait for an arrival echo.
    this._pendingMove = null;
    // _pendingAvatar: {vrmUrl, sentAt, timer, resolve} while waiting for
    // setAvatar:ok / setAvatar:failed after a setAvatar send.
    this._pendingAvatar = null;

    // Timing / health
    this.connectedAt = null;
    this.lastPingReceived = null;
    this.pingLatencies = [];
    this.messageCount = { sent: 0, received: 0 };

    // Error tracking
    this.errors = [];
    this.disconnectCount = 0;

    // Event subscription system
    this.eventBuffer = [];
    this.eventSubscriptions = new Set();

    // System notices
    this.notices = [];

    // Entity tracking
    this.entities = new Map();

    // Patrol state
    this.patrolRoute = null;

    // Auto-reconnect
    this.autoReconnect = opts.autoReconnect || false;
    this.reconnectAttempts = 0;
    this.maxReconnectAttempts = 5;
    this.reconnectTimeout = null;

    // Keepalive ping (nginx default timeout is 60s)
    this.keepaliveTimer = null;
  }

  connect() {
    return new Promise((resolve, reject) => {
      this.intentionallyClosed = false;
      // L-5: close any existing socket before creating a new one — otherwise
      // the old ws orphans with its listeners attached.
      if (this.ws) { try { this.ws.terminate(); } catch (_) {} this.ws = null; }
      // Bug-5: track pending reject so close() can settle a connect in flight
      this._connectReject = reject;

      try {
        // Allow self-signed certs for localhost/127.0.0.1 relay (dev/QA only)
        const wsOpts = {};
        try {
          const u = new URL(this.wsUrl);
          if (u.hostname === '127.0.0.1' || u.hostname === 'localhost') {
            wsOpts.rejectUnauthorized = false;
          }
        } catch (_) {}
        this.ws = new WebSocket(this.wsUrl, wsOpts);
      } catch (err) {
        this._trackError('ws_create', err.message);
        reject(new Error(`Failed to create WebSocket: ${err.message}`));
        return;
      }

      this.connectionTimeout = setTimeout(() => {
        if (this.ws) this.ws.terminate();
        this._trackError('timeout', 'Connection timed out after 10s');
        reject(new Error('Connection timed out after 10s'));
      }, 10000);

      this.ws.on('open', () => {
        if (this.isGuest) {
          this._send({ h: 'login_guest', a: [] });
        } else {
          this._send({ h: 'login', a: [this.token, this.username, this.vrmUrl] });
        }
      });

      this.ws.on('message', (data) => {
        const raw = data.toString();
        const parts = raw.split(DELIMITER).filter(m => m.length > 0);
        for (const part of parts) {
          let msg;
          try { msg = JSON.parse(part); }
          catch (_) { continue; } // skip unparseable frames only
          this.messageCount.received++;
          this._handleMessage(msg, resolve, reject);
        }
      });

      this.ws.on('close', () => {
        clearTimeout(this.connectionTimeout);
        this._stopKeepalive();
        const wasConnected = this.connected;
        this.connected = false;
        // Clear stale world state (re-populates via w:add on reconnect)
        this.knownUsers.clear();
        this.entities.clear();
        if (wasConnected) this.disconnectCount++;
        this._bufferEvent('disconnect', { wasConnected, intentional: this.intentionallyClosed });
        this.emit('disconnected');

        // Auto-reconnect
        if (!this.intentionallyClosed && this.autoReconnect && this.reconnectAttempts < this.maxReconnectAttempts) {
          this.reconnectAttempts++;
          const delay = Math.min(1000 * Math.pow(2, this.reconnectAttempts), 30000);
          this.reconnectTimeout = setTimeout(() => this.connect().catch((err) => this._trackError('reconnect_failed', err.message)), delay);
        }
      });

      this.ws.on('error', (err) => {
        clearTimeout(this.connectionTimeout);
        this._trackError('ws_error', err.message);
        if (!this.connected) reject(new Error(`WebSocket error: ${err.message}`));
      });
    });
  }

  _handleMessage(msg, resolve, reject) {
    switch (msg.h) {
      case 'ping': {
        const now = Date.now();
        if (this.lastPingReceived) {
          this.pingLatencies.push(now - this.lastPingReceived);
          if (this.pingLatencies.length > MAX_LATENCIES) this.pingLatencies.shift();
        }
        this.lastPingReceived = now;
        this._send({ h: 'pong', a: [] });
        break;
      }

      case 'acc:ok':
        clearTimeout(this.connectionTimeout);
        this._send({ h: 'ready', a: [] });
        this.connected = true;
        this.connectedAt = Date.now();
        this.reconnectAttempts = 0;
        this._startKeepalive();
        this.emit('ready');
        this._connectReject = null;
        if (resolve) resolve();
        break;

      case 'acc:fail':
        clearTimeout(this.connectionTimeout);
        this._trackError('auth', JSON.stringify(msg.a));
        this._connectReject = null;
        if (reject) reject(new Error(`Login failed: ${JSON.stringify(msg.a)}`));
        break;

      case 'w:add': {
        const data = msg.a?.[0];
        if (!data) break;
        // Server sends either {id, username, type, position, ...} object or [id, username] pair
        const userId = data.id ?? data;
        const userName = data.username ?? msg.a?.[1] ?? 'unknown';
        const type = data.type ?? 'avatar';
        if (type !== 'avatar') {
          // Entity (screen, drone, text, etc.) — track separately
          this.entities.set(String(userId), { id: String(userId), type, position: data.position });
          this._bufferEvent('w:add', { userId, type, entity: true });
          break;
        }
        this.knownUsers.set(String(userId), {
          id: String(userId),
          username: String(userName),
          tile: 0,
          position: data.position || null,
          rotation: data.rotation || null,
          animation: data.animation || null,
          avatarPath: data.path || null,
          avatarCollection: data.collection || null,
          avatarThumb: data.thumb || null,
          isBot: data.isBot === true,
          afkState: data.afkState || null,
          lastSeen: Date.now(),
        });
        // Self-identification: if the spawner told us our server-side username
        // (the token account's name, e.g. "cado"), this entry is us. That gives
        // us our true starting position on a fresh connection.
        if (this.serverUsernameHint && String(userName) === this.serverUsernameHint) {
          this.ownUserId = String(userId);
        }
        this._bufferEvent('w:add', { userId, userName, type });
        break;
      }

      case 'w:rem': {
        const data = msg.a?.[0];
        const userId = String(typeof data === 'object' ? data.id ?? data : data);
        this.knownUsers.delete(userId);
        this.entities.delete(userId);
        this._bufferEvent('w:rem', { userId });
        break;
      }

      case 'w:move': {
        const data = msg.a?.[0];
        if (data == null) break;
        // Real server broadcast format: {id, g, t, st, s, r} where t = step tile id,
        // st = true on the final step (arrival), s = step duration ms (500 orth / 707 diag).
        // (Older guess was [userId, tileId]; keep a fallback for it.)
        let userId, tileId, arrived, stepMs;
        if (typeof data === 'object' && data.t != null) {
          userId = String(data.id);
          tileId = Number(data.t);
          arrived = data.st === true;
          stepMs = Number(data.s) || null;
        } else {
          userId = String(data);
          tileId = Number(msg.a?.[1]);
          arrived = false;
          stepMs = null;
        }
        const user = this.knownUsers.get(userId);
        if (user) {
          user.tile = tileId;
          user.lastSeen = Date.now();
          // data.r is the server rotation {x:pitch, y:yaw, z:0} — the facing
          // direction. Track it so sonar can report who faces whom.
          if (data && typeof data === 'object' && data.r) user.rotation = data.r;
        }
        // Self tracking: the server echoes our own steps. Learn our user id
        // from the arrival echo of a move we sent, then track every own step.
        // ownTile stays accurate without any time estimates.
        const pending = this._pendingMove;
        const isSelf = (this.ownUserId && userId === this.ownUserId) ||
          (pending && arrived && tileId === pending.tile);
        if (isSelf) {
          if (!this.ownUserId) this.ownUserId = userId;
          this.ownTile = tileId;
          if (pending) pending.seenEcho = true;
          if (arrived && pending && pending.tile === tileId) {
            this._pendingMove = null;
            if (pending.resolve) pending.resolve({ tileId, userId, moved: true });
          } else if (arrived && pending && tileId !== pending.tile) {
            // Server "no path" signal: st:true for the CURRENT tile (no `s`
            // field) when findPath fails. Fail fast instead of timing out.
            this._pendingMove = null;
            if (pending.resolve) pending.resolve({ tileId, userId, moved: false, reason: 'no_path' });
          }
        }
        this._bufferEvent('w:move', { userId, tileId, arrived, stepMs });
        break;
      }

      case 'chat': {
        const [userId, message] = msg.a || [];
        const user = this.knownUsers.get(String(userId));
        const entry = {
          userId: String(userId),
          username: user?.username ?? 'unknown',
          message: String(message),
          timestamp: Date.now(),
        };
        this.chatBuffer.push(entry);
        if (this.chatBuffer.length > MAX_CHAT_BUFFER) this.chatBuffer.shift();
        this._bufferEvent('chat', entry);
        break;
      }

      case 'w:ready':
        this._bufferEvent('w:ready', {});
        this.emit('worldReady');
        break;

      case 'w:call': {
        // Server sends: { h: 'w:call', a: [{ id, f, g, a }] }
        // where id = session id, f = function name, a = args array
        const callMsg = msg.a?.[0];
        const func = callMsg?.f;
        const target = callMsg?.id;
        const callArgs = callMsg?.a || [];

        // Chat messages arrive as w:call with f='chat' — populate chatBuffer
        if (func === 'chat') {
          const text = callArgs[0];
          const userId = String(target);
          const user = this.knownUsers.get(userId);
          const entry = {
            userId,
            username: user?.username ?? 'unknown',
            message: String(text ?? ''),
            timestamp: Date.now(),
          };
          this.chatBuffer.push(entry);
          if (this.chatBuffer.length > MAX_CHAT_BUFFER) this.chatBuffer.shift();
          this._bufferEvent('chat', entry);
        }

        this._bufferEvent('w:call', { target, func, args: callArgs });
        break;
      }

      case 'w:o':
        this._bufferEvent('w:o', { overrides: msg.a });
        break;

      case 'w:loadSpace':
        this._bufferEvent('w:loadSpace', { data: msg.a });
        break;

      case 'notice':
      case 'snotice': {
        const notice = { type: msg.h, text: msg.a?.[0], timestamp: Date.now() };
        this.notices.push(notice);
        if (this.notices.length > MAX_NOTICES) this.notices.shift();
        this._bufferEvent('notice', notice);
        break;
      }

      case 'setAvatar:ok': {
        const p = this._pendingAvatar;
        if (p) {
          this._pendingAvatar = null;
          clearTimeout(p.timer);
          // The server rebroadcasts w:rem + w:add with the new path; give it
          // a beat to arrive so we can report the confirmed avatar.
          setTimeout(() => {
            const av = this.getOwnAvatar();
            p.resolve({ ok: true, vrmUrl: p.vrmUrl, ms: Date.now() - p.sentAt,
              confirmedPath: av?.path || null, confirmedCollection: av?.collection || null });
          }, 800);
        }
        this._bufferEvent('setAvatar:ok', {});
        break;
      }

      case 'setAvatar:failed': {
        const reason = msg.a?.[0]?.reason || msg.a?.[0] || 'unknown';
        const p = this._pendingAvatar;
        if (p) {
          this._pendingAvatar = null;
          clearTimeout(p.timer);
          p.resolve({ ok: false, vrmUrl: p.vrmUrl, reason: String(reason), ms: Date.now() - p.sentAt });
        }
        this._bufferEvent('setAvatar:failed', { reason: String(reason) });
        break;
      }

      case 'redirect':
        this._bufferEvent('redirect', { target: msg.a?.[0] });
        break;

      case 'disconnect':
        this._bufferEvent('disconnect', { reason: msg.a?.[0] });
        break;

      case 'kbs': {
        // Position sync from another user: [userId, position, rotation, animation]
        const [userId, pos, rot, anim] = msg.a || [];
        const user = this.knownUsers.get(String(userId));
        if (user) {
          if (pos) user.position = pos;
          if (rot) user.rotation = rot;
          if (anim) user.animation = anim;
          user.lastSeen = Date.now();
        }
        break;
      }

      case 'voiceState': {
        const vsData = msg.a?.[0];
        if (vsData && vsData.id != null) {
          const user = this.knownUsers.get(String(vsData.id));
          if (user) user.voiceState = vsData.state ?? false;
        }
        this._bufferEvent('voiceState', vsData);
        break;
      }

      case 'emote': {
        const [userId, animation] = msg.a || [];
        this._bufferEvent('emote', { userId, animation });
        break;
      }

      case 'ts': {
        const [userId, typing] = msg.a || [];
        this._bufferEvent('ts', { userId, typing });
        break;
      }
    }
  }

  _bufferEvent(type, data) {
    // Always emit for waitForEvent listeners
    this.emit('_event', { type, data });
    // 'error' is special on EventEmitters: emitting it with no listener throws
    // ERR_UNHANDLED_ERROR and kills the server. Errors are already tracked in
    // this.errors and buffered for subscribers, so skip the raw emit.
    if (type !== 'error') this.emit(type, data);
    // Only store in buffer if subscribed
    if (this.eventSubscriptions.has('*') || this.eventSubscriptions.has(type)) {
      this.eventBuffer.push({ type, data, timestamp: Date.now() });
      if (this.eventBuffer.length > MAX_EVENT_BUFFER) this.eventBuffer.shift();
    }
  }

  waitForEvent(type, matchFn = null, timeoutMs = 5000) {
    return new Promise((resolve, reject) => {
      let off;
      const timer = setTimeout(() => {
        off?.();
        reject(new Error(`Timeout: no "${type}" event within ${timeoutMs}ms`));
      }, timeoutMs);

      const handler = (data) => {
        let ok = false;
        try { ok = !matchFn || matchFn(data); }
        catch (e) { clearTimeout(timer); off?.(); reject(e); return; }
        if (ok) {
          clearTimeout(timer);
          off?.();
          resolve({ type, data, timestamp: Date.now() });
        }
      };

      if (type === '*') {
        const anyHandler = (ev) => handler(ev.data);
        this.on('_event', anyHandler);
        off = () => this.removeListener('_event', anyHandler);
      } else {
        this.on(type, handler);
        off = () => this.removeListener(type, handler);
      }
    });
  }

  _trackError(type, detail) {
    this.errors.push({ type, detail, timestamp: Date.now() });
    if (this.errors.length > MAX_ERRORS) this.errors.shift();
    this._bufferEvent('error', { type, detail });
  }

  _send(data) {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(data) + DELIMITER);
      this.messageCount.sent++;
      return true;
    }
    return false;
  }

  // --- Keepalive ---

  _startKeepalive() {
    this._stopKeepalive();
    this.keepaliveTimer = setInterval(() => {
      if (this.connected) this._send({ h: 'ping', a: [] });
    }, 30000);
  }

  _stopKeepalive() {
    if (this.keepaliveTimer) {
      clearInterval(this.keepaliveTimer);
      this.keepaliveTimer = null;
    }
  }

  // --- Actions ---

  // The bot's true current (x, z) from the server snapshot/broadcasts, or null
  // if we haven't identified our own user entry yet.
  getOwnPosition() {
    if (!this.ownUserId) return null;
    const me = this.knownUsers.get(String(this.ownUserId));
    return me && me.position ? { x: me.position.x, z: me.position.z } : null;
  }
  // The bot's own avatar as the server sees it (from our w:add entry).
  getOwnAvatar() {
    if (!this.ownUserId) return null;
    const me = this.knownUsers.get(String(this.ownUserId));
    if (!me) return null;
    return { path: me.avatarPath || null, collection: me.avatarCollection || null, thumb: me.avatarThumb || null };
  }
  moveToTile(tileId, boost = false) {
    const target = Number(tileId);
    // Light tracking so the arrival echo can correct ownTile/learn ownUserId
    // even when the caller doesn't wait (see w:move handler).
    if (this._pendingMove?.reject) { try { this._pendingMove.reject(new Error('superseded by a newer move')); } catch {} }
    this._pendingMove = { tile: target, sentAt: Date.now(), resolve: null, reject: null };
    // Server MoveMessage.handle(tile, boost): boost = 250ms steps (2x speed).
    this._send({ h: 'w:move', a: boost ? [target, true] : [target] });
    this.ownTile = target; // optimistic; corrected by step echoes
  }
  // Send a move and wait for the server's arrival echo (w:move broadcast with
  // st:true for our own user id). The server pathfinds and streams step
  // echoes; there is no other arrival signal. Resolves {tileId, userId, ms,
  // moved:true} on arrival, or {moved:false} if no step echoes arrive within
  // noEchoMs (bot already there, or the move was rejected). Rejects on timeout.
  moveToTileAndWait(tileId, timeoutMs = 90000, noEchoMs = 5000, boost = false) {
    return new Promise((resolve, reject) => {
      const target = Number(tileId);
      if (!Number.isFinite(target)) { reject(new Error('moveToTileAndWait: invalid tileId')); return; }
      const sentAt = Date.now();
      const timer = setTimeout(() => {
        if (this._pendingMove?.tile === target) this._pendingMove = null;
        reject(new Error(`move to tile ${target} timed out after ${timeoutMs}ms (no arrival echo)`));
      }, timeoutMs);
      const noEchoTimer = setTimeout(() => {
        const p = this._pendingMove;
        if (p && p.tile === target && !p.seenEcho) {
          this._pendingMove = null;
          clearTimeout(timer);
          resolve({ tileId: target, userId: this.ownUserId, ms: Date.now() - sentAt, moved: false });
        }
      }, noEchoMs);
      this.moveToTile(target, boost); // sets _pendingMove (light)
      // Upgrade the pending entry to a waiter.
      this._pendingMove.seenEcho = false;
      this._pendingMove.resolve = (info) => {
        clearTimeout(timer); clearTimeout(noEchoTimer);
        resolve({ tileId: info.tileId, userId: info.userId, ms: Date.now() - sentAt, moved: info.moved !== false, reason: info.reason || null });
      };
      this._pendingMove.reject = (err) => { clearTimeout(timer); clearTimeout(noEchoTimer); reject(err); };
    });
  }
  sendChat(message) { return this._send({ h: 'chat', a: [message] }); }
  sendEmote(animation) { this._send({ h: 'emote', a: [animation, false] }); }
  sendRotation(x, y, z) { this._send({ h: 'w:rot', a: [{ x, y, z }] }); }
  sendLookAt(x, y, z) { this._send({ h: 'w:lookAt', a: [{ x, y, z }] }); }
  sendTypingStatus(isTyping) { this._send({ h: 'ts', a: [isTyping] }); }
  sendSetAvatar(asset) { this._send({ h: 'setAvatar', a: [asset] }); }
  // Send setAvatar and wait for the server's verdict (setAvatar:ok or
  // setAvatar:failed with a reason like not_optimized/gated/invalid_url).
  // Resolves {ok, vrmUrl, ms[, reason]}; never rejects on server refusal.
  setAvatarAndWait(vrmUrl, timeoutMs = 15000) {
    return new Promise((resolve, reject) => {
      if (!vrmUrl || typeof vrmUrl !== 'string') { reject(new Error('vrmUrl is required')); return; }
      if (this._pendingAvatar) { clearTimeout(this._pendingAvatar.timer); this._pendingAvatar.resolve({ ok: false, vrmUrl: this._pendingAvatar.vrmUrl, reason: 'superseded', ms: Date.now() - this._pendingAvatar.sentAt }); }
      const sentAt = Date.now();
      const timer = setTimeout(() => {
        if (this._pendingAvatar?.vrmUrl === vrmUrl) this._pendingAvatar = null;
        resolve({ ok: false, vrmUrl, reason: 'timeout', ms: Date.now() - sentAt });
      }, timeoutMs);
      this._pendingAvatar = { vrmUrl, sentAt, timer, resolve };
      this.sendSetAvatar(vrmUrl);
    });
  }

  // --- Patrol ---

  startPatrol(tiles, intervalMs = 2000, loop = true) {
    this.stopPatrol();
    if (!tiles || tiles.length === 0) return;
    this.patrolRoute = { tiles, index: 0, intervalMs, loop, timer: null };
    const step = () => {
      if (!this.connected || !this.patrolRoute) return;
      const tile = this.patrolRoute.tiles[this.patrolRoute.index];
      this.moveToTile(tile);
      this.ownTile = tile;
      this.patrolRoute.index++;
      if (this.patrolRoute.index >= this.patrolRoute.tiles.length) {
        if (this.patrolRoute.loop) {
          this.patrolRoute.index = 0;
        } else {
          this.stopPatrol();
        }
      }
    };
    step(); // first move immediately
    this.patrolRoute.timer = setInterval(step, intervalMs);
  }

  stopPatrol() {
    if (this.patrolRoute?.timer) {
      clearInterval(this.patrolRoute.timer);
      this.patrolRoute.timer = null;
    }
    this.patrolRoute = null;
  }

  // --- State / Reporting ---

  getHealthStats() {
    const uptime = this.connectedAt ? Date.now() - this.connectedAt : 0;
    const avgLatency = this.pingLatencies.length > 0
      ? Math.round(this.pingLatencies.reduce((a, b) => a + b, 0) / this.pingLatencies.length)
      : null;
    return {
      uptime,
      uptimeFormatted: `${Math.floor(uptime / 60000)}m ${Math.floor((uptime % 60000) / 1000)}s`,
      avgLatencyMs: avgLatency,
      messagesSent: this.messageCount.sent,
      messagesReceived: this.messageCount.received,
      errorCount: this.errors.length,
      disconnects: this.disconnectCount,
      reconnectAttempts: this.reconnectAttempts,
    };
  }

  getState() {
    return {
      connected: this.connected,
      username: this.username,
      wsUrl: this.wsUrl,
      users: Array.from(this.knownUsers.values()),
      recentChat: this.chatBuffer.slice(-10),
    };
  }

  getFullState() {
    return {
      connected: this.connected,
      username: this.username,
      wsUrl: this.wsUrl,
      ownTile: this.ownTile,
      users: Array.from(this.knownUsers.values()),
      recentChat: this.chatBuffer.slice(-10),
      notices: this.notices.slice(-5),
      health: this.getHealthStats(),
      entityCount: this.entities.size,
      eventBufferSize: this.eventBuffer.length,
      patrolling: !!(this.patrolRoute?.timer),
      subscriptions: Array.from(this.eventSubscriptions),
    };
  }

  close() {
    this.intentionallyClosed = true;
    // Bug-5: settle a pending connect() so awaiters don't hang forever
    if (this._connectReject) {
      const r = this._connectReject;
      this._connectReject = null;
      r(new Error('Connection closed'));
    }
    this.stopPatrol();
    this._stopKeepalive();
    clearTimeout(this.connectionTimeout);
    clearTimeout(this.reconnectTimeout);
    if (this.toneTimer) { clearInterval(this.toneTimer); this.toneTimer = null; }
    if (this.typingClearTimer) { clearTimeout(this.typingClearTimer); this.typingClearTimer = null; }
    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }
    this.connected = false;
    this.knownUsers.clear();
    this.chatBuffer = [];
    this.eventBuffer = [];
    this.eventSubscriptions.clear();
  }
}

// --- Bot Manager ---

const bots = new Map();
const rtcSessions = new Map();

// Conductor state
let conductorBot = null;
let conductorInterval = null;
let conductorGen = 0; // Bug-3: generation counter — concurrent starts supersede older ones
let conductorConfig = {
  firstPerson: { refDistance: 1, rolloffFactor: 0.75, distanceModel: 'exponential', volume: 1.0 },
  isometric: { volume: 1.0 },
};
let conductorChatCursor = 0;

// --- Bot RTC Session (mediasoup audio production) ---

const RTC_SERVER = 'https://demo.hubzz.com/';
const AUDIO_SAMPLE_RATE = 48000;
const AUDIO_FRAME_SIZE = 480; // 10ms at 48kHz

class BotRTCSession {
  constructor(botName, roomId) {
    this.botName = botName;
    this.roomId = roomId;
    this.socket = null;
    this.device = null;
    this.sendTransport = null;
    this.producer = null;
    this.audioSource = null;
    this.toneTimer = null;
    this.phase = 0;
    this.frequency = 440;
    this.gain = 0.5;
    this.status = 'idle';
    this.error = null;
  }

  socketRequest(type, data = {}) {
    return new Promise((resolve, reject) => {
      this.socket.emit(type, data, (res) => {
        if (res?.error) reject(new Error(typeof res.error === 'string' ? res.error : JSON.stringify(res.error)));
        else resolve(res);
      });
    });
  }

  async start(frequency = 440, gain = 0.5) {
    this.frequency = frequency;
    this.gain = gain;
    this.status = 'connecting';

    this.socket = io(RTC_SERVER, { transports: ['websocket'] });
    await new Promise((resolve, reject) => {
      const t = setTimeout(() => reject(new Error('Socket.IO connection timed out')), 8000);
      this.socket.on('connect', () => { clearTimeout(t); resolve(); });
      this.socket.on('connect_error', e => { clearTimeout(t); reject(new Error(`Socket.IO connect error: ${e.message}`)); });
    });

    await this.socketRequest('join', {
      name: this.botName,
      room_id: this.roomId,
      token: crypto.randomUUID(),
      user_id: -1,
    });

    const routerRtpCapabilities = await this.socketRequest('getRouterRtpCapabilities');
    this.device = await Device.factory({ handlerName: 'Chrome111' });
    await this.device.load({ routerRtpCapabilities });

    if (!this.device.canProduce('audio')) throw new Error('Router does not support audio production');

    const transportData = await this.socketRequest('createWebRtcTransport', {
      forceTcp: false,
      rtpCapabilities: this.device.rtpCapabilities,
    });

    this.sendTransport = this.device.createSendTransport({
      id: transportData.id,
      iceParameters: transportData.iceParameters,
      iceCandidates: transportData.iceCandidates,
      dtlsParameters: transportData.dtlsParameters,
      sctpParameters: transportData.sctpParameters,
    });

    this.sendTransport.on('connect', async ({ dtlsParameters }, callback, errback) => {
      try {
        await this.socketRequest('connectTransport', { dtlsParameters, transport_id: this.sendTransport.id });
        callback();
      } catch (e) { errback(e); }
    });

    this.sendTransport.on('produce', async ({ kind, rtpParameters }, callback, errback) => {
      try {
        const { producer_id } = await this.socketRequest('produce', {
          producerTransportId: this.sendTransport.id,
          kind,
          rtpParameters,
        });
        callback({ id: producer_id });
      } catch (e) { errback(e); }
    });

    // Create audio source and start tone
    this.audioSource = new RTCAudioSource();
    this.track = this.audioSource.createTrack();
    this.producer = await this.sendTransport.produce({ track: this.track });
    this._startTone();

    this.status = 'producing';
    return { producerId: this.producer.id, transportId: this.sendTransport.id };
  }

  _startTone() {
    const samplesPerFrame = AUDIO_FRAME_SIZE;
    this.toneTimer = setInterval(() => {
      const samples = new Int16Array(samplesPerFrame);
      for (let i = 0; i < samplesPerFrame; i++) {
        samples[i] = Math.round(Math.sin(this.phase) * 32767 * this.gain);
        this.phase += (2 * Math.PI * this.frequency) / AUDIO_SAMPLE_RATE;
        if (this.phase > 2 * Math.PI) this.phase -= 2 * Math.PI;
      }
      this.audioSource.onData({
        samples,
        sampleRate: AUDIO_SAMPLE_RATE,
        bitsPerSample: 16,
        channelCount: 1,
        numberOfFrames: samplesPerFrame,
      });
    }, 10);
  }

  setTone(frequency, gain) {
    if (frequency != null) this.frequency = frequency;
    if (gain != null) this.gain = Math.max(0, Math.min(1, gain));
  }

  stop() {
    if (this.toneTimer) { clearInterval(this.toneTimer); this.toneTimer = null; }
    if (this.producer) { try { this.producer.close(); } catch (_) {} this.producer = null; }
    if (this.sendTransport) { try { this.sendTransport.close(); } catch (_) {} this.sendTransport = null; }
    // L-3: stop the native audio track — otherwise it holds resources per cycle
    if (this.track) { try { this.track.stop(); } catch (_) {} this.track = null; }
    if (this.socket) { try { this.socket.disconnect(); } catch (_) {} this.socket = null; }
    this.status = 'stopped';
  }

  getState() {
    return {
      botName: this.botName,
      roomId: this.roomId,
      status: this.status,
      frequency: this.frequency,
      gain: this.gain,
      producerId: this.producer?.id || null,
    };
  }
}

// --- MCP Protocol (stdio JSON-RPC) ---

function sendResponse(id, result) {
  const msg = JSON.stringify({ jsonrpc: '2.0', id, result });
  process.stdout.write(msg + '\n');
}

function sendError(id, code, message) {
  const msg = JSON.stringify({ jsonrpc: '2.0', id, error: { code, message } });
  process.stdout.write(msg + '\n');
}

// --- Helper: get bot or return error ---

function getBot(name, requireConnected = true) {
  const bot = bots.get(name);
  if (!bot) return { error: `Bot "${name}" not found` };
  if (requireConnected && !bot.connected) return { error: `Bot "${name}" is not connected` };
  return bot;
}

// --- Tool Definitions ---

const TOOLS = [
  // === Original tools ===
  {
    name: 'bot_spawn',
    description: 'Spawn a bot that connects to a Hubzz world. Returns when connected and ready.',
    inputSchema: {
      type: 'object',
      properties: {
        name: { type: 'string', description: 'Bot username (unique identifier)' },
        wsUrl: { type: 'string', description: `WebSocket URL (default: ${DEFAULT_WS_URL})` },
        vrmUrl: { type: 'string', description: 'VRM avatar URL (optional)' },
        token: { type: 'string', description: 'hbz_ access token override (default: HUBZZ_BOT_TOKEN env)' },
        serverUsername: { type: 'string', description: 'The token account\'s server-side username (e.g. "cado"). Lets the bot identify its own user entry for accurate self position.' },
        autoReconnect: { type: 'boolean', description: 'Enable auto-reconnect on disconnect (default: false)' },
      },
      required: ['name'],
    },
  },
  {
    name: 'bot_move',
    description: 'Move a bot to a specific tile in the world. The server pathfinds and walks; step echoes correct the tracked position.',
    inputSchema: {
      type: 'object',
      properties: {
        name: { type: 'string', description: 'Bot name' },
        tileId: { type: 'number', description: 'Tile ID to move to' },
        boost: { type: 'boolean', description: 'Double-speed walk (250ms steps instead of 500ms). Default false.' },
      },
      required: ['name', 'tileId'],
    },
  },
  {
    name: 'bot_chat',
    description: 'Make a bot send a chat message visible to all nearby users.',
    inputSchema: {
      type: 'object',
      properties: {
        name: { type: 'string', description: 'Bot name' },
        message: { type: 'string', description: 'Chat message to send' },
      },
      required: ['name', 'message'],
    },
  },
  {
    name: 'bot_emote',
    description: 'Play an animation (legacy — prefer bot_dance for full emote list).',
    inputSchema: {
      type: 'object',
      properties: {
        name: { type: 'string', description: 'Bot name' },
        animation: { type: 'string', description: 'Animation name', enum: ['idle', 'wave', 'dance_flair', 'clap', 'thumbs_up', 'spawn'] },
      },
      required: ['name', 'animation'],
    },
  },
  {
    name: 'bot_look',
    description: 'Get comprehensive world state from a bot — users, chat, health, position, notices.',
    inputSchema: {
      type: 'object',
      properties: { name: { type: 'string', description: 'Bot name' } },
      required: ['name'],
    },
  },
  {
    name: 'bot_close',
    description: 'Disconnect and remove a bot.',
    inputSchema: {
      type: 'object',
      properties: { name: { type: 'string', description: 'Bot name' } },
      required: ['name'],
    },
  },
  {
    name: 'bot_voice',
    description: 'Toggle voice state for a bot (shows mic indicator to other users).',
    inputSchema: {
      type: 'object',
      properties: {
        name: { type: 'string', description: 'Bot name' },
        state: { type: 'boolean', description: 'true = mic on, false = mic off' },
      },
      required: ['name', 'state'],
    },
  },
  {
    name: 'bot_list',
    description: 'List all active bots and their connection status.',
    inputSchema: { type: 'object', properties: {} },
  },
  {
    name: 'bot_close_all',
    description: 'Disconnect and remove all bots.',
    inputSchema: { type: 'object', properties: {} },
  },

  // === New tools ===
  {
    name: 'bot_batch_spawn',
    description: 'Spawn multiple bots at once with staggered connections. Returns status of all spawns.',
    inputSchema: {
      type: 'object',
      properties: {
        prefix: { type: 'string', description: 'Name prefix (bots named prefix-0, prefix-1, ...)' },
        count: { type: 'number', description: 'Number of bots to spawn (1-20)' },
        wsUrl: { type: 'string', description: `WebSocket URL (default: ${DEFAULT_WS_URL})` },
        vrmUrl: { type: 'string', description: 'VRM avatar URL (optional, same for all)' },
        staggerMs: { type: 'number', description: 'Delay between each spawn in ms (default: 500)' },
        autoReconnect: { type: 'boolean', description: 'Enable auto-reconnect (default: false)' },
      },
      required: ['prefix', 'count'],
    },
  },
  {
    name: 'bot_observe',
    description: 'Get comprehensive world state from a bot: all users with positions, full chat log, events, latency, health.',
    inputSchema: {
      type: 'object',
      properties: {
        name: { type: 'string', description: 'Bot name' },
        includeChat: { type: 'boolean', description: 'Include full chat buffer (default: true)' },
        includeEvents: { type: 'boolean', description: 'Include event buffer (default: false)' },
        includeNotices: { type: 'boolean', description: 'Include system notices (default: true)' },
      },
      required: ['name'],
    },
  },
  {
    name: 'bot_patrol',
    description: 'Make a bot walk through a sequence of tiles on a timer. Useful for movement testing.',
    inputSchema: {
      type: 'object',
      properties: {
        name: { type: 'string', description: 'Bot name' },
        tiles: { type: 'array', items: { type: 'number' }, description: 'Array of tile IDs to visit in order' },
        intervalMs: { type: 'number', description: 'Milliseconds between each move (default: 2000)' },
        loop: { type: 'boolean', description: 'Loop back to start after finishing (default: true)' },
        action: { type: 'string', enum: ['start', 'stop', 'status'], description: 'Action (default: start)' },
      },
      required: ['name'],
    },
  },
  {
    name: 'bot_stress_test',
    description: 'Run a stress test: spawn N bots, have them act simultaneously, auto-cleanup, return report.',
    inputSchema: {
      type: 'object',
      properties: {
        prefix: { type: 'string', description: 'Bot name prefix' },
        count: { type: 'number', description: 'Number of bots (1-20)' },
        wsUrl: { type: 'string', description: 'WebSocket URL' },
        test: { type: 'string', enum: ['connect', 'chat_flood', 'move_flood', 'mixed'], description: 'Test type' },
        durationSec: { type: 'number', description: 'Test duration in seconds (default: 10, max: 60)' },
        messagesPerSec: { type: 'number', description: 'Messages per second per bot (default: 1, max: 5)' },
      },
      required: ['prefix', 'count', 'test'],
    },
  },
  {
    name: 'bot_set_avatar',
    description: 'Change a bot\'s avatar via setAvatar protocol or !shuffle chat command. For vrm, waits for the server verdict (ok / failed with reason like not_optimized, gated).',
    inputSchema: {
      type: 'object',
      properties: {
        name: { type: 'string', description: 'Bot name' },
        method: { type: 'string', enum: ['vrm', 'shuffle'], description: 'vrm = set VRM URL, shuffle = random avatar' },
        vrmUrl: { type: 'string', description: 'VRM URL (required if method is vrm). Must be altii.co-hosted (worker URLs are canonicalized); .mml must be hubzz.app.' },
        collection: { type: 'string', description: 'Collection slug for shuffle (optional)' },
        timeoutMs: { type: 'number', description: 'Wait for server verdict this long (default 15000)' },
      },
      required: ['name', 'method'],
    },
  },
  {
    name: 'bot_test_avatars',
    description: 'Batch-test avatar VRM URLs for issues. Cycles a bot through each URL, waits for the server verdict per avatar, and reports ok/failed with the failure reason (not_optimized, gated, invalid_url, timeout). Use to find broken avatars.',
    inputSchema: {
      type: 'object',
      properties: {
        name: { type: 'string', description: 'Bot name' },
        vrmUrls: { type: 'array', items: { type: 'string' }, description: 'VRM URLs to test, in order' },
        timeoutMs: { type: 'number', description: 'Wait per avatar for the server verdict (default 15000)' },
        pauseMs: { type: 'number', description: 'Pause between avatars in ms (default 1000)' },
      },
      required: ['name', 'vrmUrls'],
    },
  },
  {
    name: 'bot_avatar_info',
    description: 'Get avatar metadata (height_m, triangles, file sizes, thumbnail, collection, license) from the avatars-api for a VRM URL, or for a bot\'s current avatar. Uses sonar-tracked w:add data when no URL is given.',
    inputSchema: {
      type: 'object',
      properties: {
        name: { type: 'string', description: 'Bot name (used for current-avatar lookup when vrmUrl is omitted)' },
        vrmUrl: { type: 'string', description: 'VRM URL to look up (defaults to the bot\'s current avatar)' },
        apiBase: { type: 'string', description: 'Avatars API base (default https://avatars.hubzz.app/avatar-api)' },
      },
      required: [],
    },
  },
  {
    name: 'bot_test_all_avatars',
    description: 'Test EVERY avatar in the avatars-api for issues. Pages the full avatar catalog, HEAD-checks each VRM URL for reachability, and optionally spot-checks a sample through the bot\'s setAvatar for server acceptance. Returns counts plus the list of broken avatars.',
    inputSchema: {
      type: 'object',
      properties: {
        name: { type: 'string', description: 'Bot name (only required when spotCheck > 0)' },
        limit: { type: 'number', description: 'Max avatars to test (default: all)' },
        offset: { type: 'number', description: 'Start at this catalog offset (default 0)' },
        concurrency: { type: 'number', description: 'Parallel HEAD checks (default 20, max 50)' },
        spotCheck: { type: 'number', description: 'Also run this many avatars through the bot setAvatar for server acceptance (default 0 = reachability only)' },
        apiBase: { type: 'string', description: 'Avatars API base (default https://avatars.hubzz.app/avatar-api)' },
      },
      required: [],
    },
  },
  {
    name: 'bot_nick',
    description: 'Change a bot\'s display name via the !nick chat command.',
    inputSchema: {
      type: 'object',
      properties: {
        name: { type: 'string', description: 'Bot name (manager key)' },
        newNick: { type: 'string', description: 'New display name' },
      },
      required: ['name', 'newNick'],
    },
  },
  {
    name: 'bot_dance',
    description: `Make a bot play any animation. Available: ${AVAILABLE_EMOTES.join(', ')} — or any custom animation name.`,
    inputSchema: {
      type: 'object',
      properties: {
        name: { type: 'string', description: 'Bot name' },
        animation: { type: 'string', description: 'Animation name' },
        useChat: { type: 'boolean', description: 'Send as !anim chat command instead of protocol message (default: false)' },
      },
      required: ['name', 'animation'],
    },
  },
  {
    name: 'bot_subscribe',
    description: 'Subscribe a bot to collect specific event types in a buffer for later retrieval.',
    inputSchema: {
      type: 'object',
      properties: {
        name: { type: 'string', description: 'Bot name' },
        events: { type: 'array', items: { type: 'string' }, description: 'Event types: chat, w:add, w:rem, w:move, notice, w:call, emote, ts, error, disconnect, or * for all' },
        action: { type: 'string', enum: ['subscribe', 'unsubscribe', 'clear', 'read'], description: 'Action (default: subscribe)' },
      },
      required: ['name'],
    },
  },
  {
    name: 'bot_report',
    description: 'Generate a summary report from one or all bots: uptime, users, chat volume, latency, errors.',
    inputSchema: {
      type: 'object',
      properties: {
        name: { type: 'string', description: 'Bot name (omit for all bots)' },
        format: { type: 'string', enum: ['summary', 'detailed'], description: 'Report detail level (default: summary)' },
      },
    },
  },
  {
    name: 'bot_find_tiles',
    description: 'Fetch the world map and find walkable tiles at specified distances from a reference tile (or world origin). Returns a tiles array ready to pass to bot_spatial_grid.',
    inputSchema: {
      type: 'object',
      properties: {
        distances: {
          type: 'array',
          items: { type: 'number' },
          description: 'Target distances in world units (e.g. [5, 10, 20, 30, 50])',
        },
        centerTileId: { type: 'number', description: 'Reference tile ID to measure from (default: nearest walkable tile to world origin)' },
        mapUrl: { type: 'string', description: 'Map JSON URL (default: https://hubzz.xyz/data/maps/world_2.json)' },
        showFalloff: { type: 'boolean', description: 'Include predicted volume falloff based on current spatial audio config (default: true)' },
        direction: { description: 'Constrain results to a compass sector: N, S, E, W, NE, NW, SE, SW — or a number in degrees (0=East, 90=North). Leave unset for nearest-tile regardless of direction.' },
      },
      required: ['distances'],
    },
  },
  {
    name: 'bot_spatial_grid',
    description: 'Spatial audio test tool. Spawns bots at specified tiles, activates their voice indicators, and reports positions. Use this to create a grid of "speakers" at known distances so you can walk through the world and tune spatial audio falloff.',
    inputSchema: {
      type: 'object',
      properties: {
        tiles: {
          type: 'array',
          description: 'Array of tile placements. Each entry has a tileId and optional label.',
          items: {
            type: 'object',
            properties: {
              tileId: { type: 'number', description: 'Tile ID to place the bot at' },
              label: { type: 'string', description: 'Human-readable label (e.g. "near", "mid", "far")' },
            },
            required: ['tileId'],
          },
        },
        prefix: { type: 'string', description: 'Bot name prefix (default: "audio")' },
        wsUrl: { type: 'string', description: `WebSocket URL (default: ${DEFAULT_WS_URL})` },
        voiceOn: { type: 'boolean', description: 'Activate voice indicator on all bots (default: true)' },
        staggerMs: { type: 'number', description: 'Delay between spawns in ms (default: 400)' },
      },
      required: ['tiles'],
    },
  },
  {
    name: 'bot_voice_all',
    description: 'Toggle voice state on all currently active bots at once.',
    inputSchema: {
      type: 'object',
      properties: {
        state: { type: 'boolean', description: 'true = mic on, false = mic off' },
      },
      required: ['state'],
    },
  },
  {
    name: 'bot_audio_start',
    description: 'Connect a bot to the mediasoup RTC server and start producing a sine wave tone. Other users in the world will hear real spatial audio from the bot\'s position. Use this to physically test spatial audio falloff.',
    inputSchema: {
      type: 'object',
      properties: {
        name: { type: 'string', description: 'Bot name (must already be spawned)' },
        frequency: { type: 'number', description: 'Tone frequency in Hz (default: 440). Use different freqs per bot to distinguish them.' },
        gain: { type: 'number', description: 'Volume gain 0.0–1.0 (default: 0.5)' },
        wsUrl: { type: 'string', description: `WebSocket URL to derive room ID from (default: ${DEFAULT_WS_URL})` },
      },
      required: ['name'],
    },
  },
  {
    name: 'bot_audio_stop',
    description: 'Stop a bot\'s audio production and disconnect from the RTC server.',
    inputSchema: {
      type: 'object',
      properties: {
        name: { type: 'string', description: 'Bot name' },
      },
      required: ['name'],
    },
  },
  {
    name: 'bot_audio_tune',
    description: 'Change a bot\'s tone frequency or gain while it\'s producing audio. Use this to hot-swap tones during a spatial audio test.',
    inputSchema: {
      type: 'object',
      properties: {
        name: { type: 'string', description: 'Bot name' },
        frequency: { type: 'number', description: 'New frequency in Hz' },
        gain: { type: 'number', description: 'New gain 0.0–1.0' },
      },
      required: ['name'],
    },
  },
  {
    name: 'bot_audio_status',
    description: 'Get the RTC audio status of one or all bots.',
    inputSchema: {
      type: 'object',
      properties: {
        name: { type: 'string', description: 'Bot name (omit for all)' },
      },
    },
  },
  {
    name: 'bot_typing',
    description: 'Make a bot send a typing indicator (shows the "..." bubble). Optionally auto-clears after a delay.',
    inputSchema: {
      type: 'object',
      properties: {
        name: { type: 'string', description: 'Bot name' },
        typing: { type: 'boolean', description: 'true = start typing, false = stop (default: true)' },
        autoClearMs: { type: 'number', description: 'Auto-send stop after N ms (optional, e.g. 1500)' },
      },
      required: ['name'],
    },
  },
  {
    name: 'bot_rotate',
    description: 'Set a bot\'s avatar rotation. Use yaw (Y-axis degrees) for simple left/right facing, or supply full {x,y,z} Euler angles.',
    inputSchema: {
      type: 'object',
      properties: {
        name: { type: 'string', description: 'Bot name' },
        yaw: { type: 'number', description: 'Horizontal rotation in degrees (0=forward, 90=left, 180=back, 270=right). Shorthand for y only.' },
        x: { type: 'number', description: 'X rotation in radians (pitch)' },
        y: { type: 'number', description: 'Y rotation in radians (yaw)' },
        z: { type: 'number', description: 'Z rotation in radians (roll)' },
      },
      required: ['name'],
    },
  },
  {
    name: 'bot_emote_loop',
    description: 'Play a looping animation on a bot (e.g. sit, idle, waiting). Unlike bot_dance, this uses the emote message with loop=true so the animation persists until explicitly stopped.',
    inputSchema: {
      type: 'object',
      properties: {
        name: { type: 'string', description: 'Bot name' },
        animation: { type: 'string', description: 'Animation name (e.g. anim_sit, anim_idle, emotion_6_waiting)' },
        loop: { type: 'boolean', description: 'true = loop, false = play once and stop (default: true)' },
      },
      required: ['name', 'animation'],
    },
  },
  {
    name: 'bot_guest',
    description: 'Spawn a guest (unauthenticated) bot using login_guest. Guests get a GuestXXX username, cannot chat, and see the spectator/guest UI. Use this to test guest-specific flows.',
    inputSchema: {
      type: 'object',
      properties: {
        name: { type: 'string', description: 'Internal manager name (not sent to server — guest gets assigned GuestXXX)' },
        wsUrl: { type: 'string', description: `WebSocket URL (default: ${DEFAULT_WS_URL})` },
      },
      required: ['name'],
    },
  },
  {
    name: 'bot_kick_test',
    description: 'Send a moderation command (!kick, !ban, !grant) from a bot that has moderator/admin permissions.',
    inputSchema: {
      type: 'object',
      properties: {
        name: { type: 'string', description: 'Bot name (must have mod/admin permissions)' },
        action: { type: 'string', enum: ['kick', 'ban', 'grant'], description: 'Moderation action' },
        target: { type: 'string', description: 'Target username' },
        duration: { type: 'number', description: 'Ban duration in minutes (0 = permanent). Only for action=ban.' },
        permission: { type: 'string', description: 'Permission to grant (e.g. command.moderate). Only for action=grant.' },
      },
      required: ['name', 'action', 'target'],
    },
  },
  {
    name: 'bot_watch_events',
    description: 'Wait for a specific event from a bot with a timeout. Returns the matched event data. Useful for asserting server responses without polling.',
    inputSchema: {
      type: 'object',
      properties: {
        name: { type: 'string', description: 'Bot name to watch' },
        eventType: { type: 'string', description: 'Event type to wait for: chat, w:add, w:rem, w:move, notice, voiceState, emote, ts, disconnect, redirect, * (any)' },
        matchField: { type: 'string', description: 'Optional dot-path field in event data to match (e.g. "username", "message")' },
        matchValue: { description: 'Value that matchField must equal' },
        timeoutMs: { type: 'number', description: 'Max wait time in ms (default: 5000)' },
      },
      required: ['name', 'eventType'],
    },
  },
  {
    name: 'bot_upload',
    description: 'Send a test file upload through a bot\'s WebSocket connection. Tests the upload message handler. Defaults to a minimal 1×1 transparent PNG.',
    inputSchema: {
      type: 'object',
      properties: {
        name: { type: 'string', description: 'Bot name' },
        filename: { type: 'string', description: 'Filename to send (default: test.png). Extension determines accepted type: png, jpg, webp, webm, mp4, gif, glb, mp3, wav, ogg.' },
        data: { type: 'string', description: 'Base64-encoded file data (optional — defaults to a minimal valid test file for the given extension)' },
        waitForResponse: { type: 'boolean', description: 'Wait for broadcast confirmation (default: true)' },
      },
      required: ['name'],
    },
  },
  {
    name: 'bot_screen',
    description: 'Control a screen entity in the world — play a URL, stop playback, or call any entity method.',
    inputSchema: {
      type: 'object',
      properties: {
        name: { type: 'string', description: 'Bot name' },
        action: { type: 'string', enum: ['play', 'stop', 'call'], description: 'play = --play url, stop = --stop, call = raw entity call' },
        url: { type: 'string', description: 'Video/media URL (required for play)' },
        entityId: { type: 'string', description: 'Entity ID (required for call)' },
        method: { type: 'string', description: 'Method name for call action' },
        args: { type: 'array', description: 'Arguments for call action' },
      },
      required: ['name', 'action'],
    },
  },
  {
    name: 'bot_kbs',
    description: 'Send a keyboard-sync (kbs) position update from a bot. This is the smooth continuous movement message used by the client during WASD movement — different from tile-based w:move.',
    inputSchema: {
      type: 'object',
      properties: {
        name: { type: 'string', description: 'Bot name' },
        position: {
          type: 'object',
          description: 'World position {x, y, z}',
          properties: { x: { type: 'number' }, y: { type: 'number' }, z: { type: 'number' } },
        },
        rotation: {
          type: 'object',
          description: 'Rotation {x, y, z} in radians',
          properties: { x: { type: 'number' }, y: { type: 'number' }, z: { type: 'number' } },
        },
        animation: { type: 'string', description: 'Animation name (e.g. anim_walk, anim_idle)' },
      },
      required: ['name', 'position'],
    },
  },
  {
    name: 'bot_world_wait',
    description: 'Wait until a world-level condition is true, polling at 100ms intervals. Higher-level than bot_watch_events — expresses conditions in plain terms.',
    inputSchema: {
      type: 'object',
      properties: {
        name: { type: 'string', description: 'Bot name to observe from' },
        condition: {
          type: 'string',
          enum: ['user_joined', 'user_left', 'chat_received', 'entity_added', 'entity_removed', 'notice_received', 'user_count_gte', 'user_count_lte'],
          description: 'Condition to wait for',
        },
        value: { description: 'Condition parameter: username for user_joined/left, substring for chat_received/notice_received, entity type for entity_added/removed, count for user_count_*' },
        timeoutMs: { type: 'number', description: 'Max wait time in ms (default: 10000)' },
      },
      required: ['name', 'condition'],
    },
  },
  {
    name: 'bot_assert',
    description: 'Assert that a bot\'s observed state matches expected values. Returns pass/fail with details. Use after actions to verify outcomes.',
    inputSchema: {
      type: 'object',
      properties: {
        name: { type: 'string', description: 'Bot name' },
        assertions: {
          type: 'array',
          description: 'List of assertions to check',
          items: {
            type: 'object',
            properties: {
              check: {
                type: 'string',
                enum: ['connected', 'user_present', 'user_absent', 'user_at_tile', 'own_tile', 'chat_contains', 'notice_contains', 'user_count', 'entity_present', 'entity_absent'],
                description: 'What to check',
              },
              value: { description: 'Expected value (username, tile ID, message substring, count, entity type, etc.)' },
              tileId: { type: 'number', description: 'For user_at_tile: expected tile ID' },
            },
            required: ['check'],
          },
        },
      },
      required: ['name', 'assertions'],
    },
  },
  {
    name: 'bot_ping_latency',
    description: 'Measure WebSocket ping latency for a bot — returns min, avg, max over recent samples, or waits to collect fresh samples.',
    inputSchema: {
      type: 'object',
      properties: {
        name: { type: 'string', description: 'Bot name' },
        samples: { type: 'number', description: 'Number of fresh ping samples to collect (0 = use existing, default: 0)' },
        timeoutMs: { type: 'number', description: 'Max wait time when collecting fresh samples (default: 15000)' },
      },
      required: ['name'],
    },
  },
  {
    name: 'bot_spawn_at',
    description: 'Spawn a bot and immediately move it to a tile — combines bot_spawn + bot_move in one call.',
    inputSchema: {
      type: 'object',
      properties: {
        name: { type: 'string', description: 'Bot username' },
        tileId: { type: 'number', description: 'Tile ID to place the bot at' },
        wsUrl: { type: 'string', description: `WebSocket URL (default: ${DEFAULT_WS_URL})` },
        vrmUrl: { type: 'string', description: 'VRM avatar URL (optional)' },
        voiceOn: { type: 'boolean', description: 'Enable voice indicator immediately (default: false)' },
      },
      required: ['name', 'tileId'],
    },
  },
  {
    name: 'bot_directional_ring',
    description: 'Place bots in a ring at equal distance from center, evenly spaced around the compass, each producing a distinct tone. Use this to test spatial audio directionality (left/right/front/behind panning) in first-person perspective.',
    inputSchema: {
      type: 'object',
      properties: {
        distance: { type: 'number', description: 'Distance from center in world units (default: 10)' },
        count: { type: 'number', description: 'Number of bots in the ring: 4 (N/E/S/W) or 8 (adds diagonals). Default: 4' },
        directions: { type: 'array', items: { type: 'string' }, description: 'Explicit directions to use, e.g. ["N","E","S","W"]. Overrides count.' },
        baseFreq: { type: 'number', description: 'Base frequency for first bot in Hz (default: 220). Each subsequent bot is 1.5x higher.' },
        gain: { type: 'number', description: 'Gain 0.0–1.0 (default: 0.7)' },
        prefix: { type: 'string', description: 'Bot name prefix (default: "dir")' },
        wsUrl: { type: 'string', description: `WebSocket URL (default: ${DEFAULT_WS_URL})` },
        mapUrl: { type: 'string', description: 'Map JSON URL (default: world_2.json)' },
        centerTileId: { type: 'number', description: 'Center tile to stand on (default: world origin)' },
        startAudio: { type: 'boolean', description: 'Start audio tones immediately (default: true)' },
      },
    },
  },
  {
    name: 'bot_scene_audio',
    description: 'All-in-one spatial audio test scene manager. Actions: "setup" (kill existing + spawn distance bots + conductor), "setup_ring" (kill existing + spawn directional ring + conductor), "status" (show all running bots + audio sessions), "teardown" (kill everything).',
    inputSchema: {
      type: 'object',
      properties: {
        action: { type: 'string', enum: ['setup', 'setup_ring', 'status', 'teardown'], description: 'What to do (default: setup)' },
        wsUrl: { type: 'string', description: `WebSocket URL (default: ${DEFAULT_WS_URL})` },
      },
    },
  },
  {
    name: 'bot_push_config',
    description: 'Push a spatial audio config update to all clients in the world via a connected bot. Use this to live-tune refDistance, rolloffFactor, volume, and distanceModel.',
    inputSchema: {
      type: 'object',
      properties: {
        botName: { type: 'string', description: 'Name of a connected bot to use as the sender' },
        firstPerson: {
          type: 'object',
          description: 'First-person spatial audio settings',
          properties: {
            refDistance: { type: 'number', description: 'Reference distance (default 1)' },
            rolloffFactor: { type: 'number', description: 'Rolloff factor (default 0.75)' },
            volume: { type: 'number', description: 'Volume multiplier (default 1.0)' },
            distanceModel: { type: 'string', description: 'Distance model: exponential, linear, inverse' },
          },
        },
        isometric: {
          type: 'object',
          description: 'Isometric (global) audio settings',
          properties: {
            volume: { type: 'number', description: 'Volume multiplier (default 1.0)' },
          },
        },
      },
      required: ['botName'],
    },
  },
  {
    name: 'bot_conductor_start',
    description: 'Start a conductor bot that listens to world chat and lets you tune spatial audio in real-time by typing commands in-world. Commands: !ref N, !rolloff N, !vol N, !isovol N, !status, !stop',
    inputSchema: {
      type: 'object',
      properties: {
        wsUrl: { type: 'string', description: 'WebSocket URL (default: wss://hubzz.xyz/socket/0,0/)' },
        username: { type: 'string', description: 'Bot username (default: conductor)' },
      },
    },
  },
  {
    name: 'bot_conductor_stop',
    description: 'Stop the conductor bot and disconnect it.',
    inputSchema: {
      type: 'object',
      properties: {},
    },
  },
  // === Cherry-picked from archived russfranky/hubzz-alpha (packages/bot-mcp) ===
  {
    name: 'bot_send_raw',
    description: 'Send a raw WebSocket message using the Hubzz protocol { h, a } format. For debugging and protocol probing.',
    inputSchema: {
      type: 'object',
      properties: {
        name: { type: 'string', description: 'Bot name' },
        h: { type: 'string', description: 'Message handler name' },
        a: { type: 'array', description: 'Args array', items: {} },
      },
      required: ['name', 'h'],
    },
  },
  {
    name: 'server_health',
    description: 'Fetch /api/health from the Hubzz server. Returns status, uptime, connections, worlds, memory.',
    inputSchema: {
      type: 'object',
      properties: {
        apiUrl: { type: 'string', description: 'API base URL (default: https://hubzz.app)' },
      },
    },
  },
  {
    name: 'server_spaces',
    description: 'List spaces/worlds from the Hubzz server via /api/spaces.',
    inputSchema: {
      type: 'object',
      properties: {
        apiUrl: { type: 'string', description: 'API base URL (default: https://hubzz.app)' },
      },
    },
  },
  {
    name: 'server_space_info',
    description: 'Get info about one space/world via /api/space/{path}.',
    inputSchema: {
      type: 'object',
      properties: {
        apiUrl: { type: 'string', description: 'API base URL (default: https://hubzz.app)' },
        path: { type: 'string', description: 'Space path, e.g. 0,0' },
      },
      required: ['path'],
    },
  },
  {
    name: 'server_emotes',
    description: 'List available emotes/animations from the Hubzz server via /api/emotes.',
    inputSchema: {
      type: 'object',
      properties: {
        apiUrl: { type: 'string', description: 'API base URL (default: https://hubzz.app)' },
      },
    },
  },
  // === Perception & navigation: the bot's eyes and feet ===
  {
    name: 'bot_sonar',
    description: 'Perception sweep for a bot: self position, nearby users with distance/direction, nearby entities, open walkable tiles, open seats (real chair objects), recent chat, an ASCII top-down map, and a plain-language scene summary. This is the bot\'s eyes.',
    inputSchema: {
      type: 'object',
      properties: {
        name: { type: 'string', description: 'Bot name' },
        radius: { type: 'number', description: 'Perception radius in world units (default: 15, max: 60)' },
        includeAscii: { type: 'boolean', description: 'Include the ASCII top-down map (default: true)' },
        maximumUsers: { type: 'number', description: 'Max nearby users to report (default: 20)' },
        selfUsername: { type: 'string', description: 'Server-side account name of the bot itself (e.g. cado) — excluded from nearby users' },
        mapUrl: { type: 'string', description: 'Map JSON URL (default: https://hubzz.xyz/data/maps/world_2.json)' },
      },
      required: ['name'],
    },
  },
  {
    name: 'bot_navigate',
    description: 'Walk a bot to a tile, a named user, or world coordinates. Sends one move; the server pathfinds and streams per-step echoes ending with an arrival signal (st:true), which this tool waits for — fully closed-loop, no time estimates.',
    inputSchema: {
      type: 'object',
      properties: {
        name: { type: 'string', description: 'Bot name' },
        tileId: { type: 'number', description: 'Destination tile id (one of tileId, username, or x+z)' },
        username: { type: 'string', description: 'Walk toward this user (resolved to nearest walkable tile at call time)' },
        x: { type: 'number', description: 'Destination world x (with z)' },
        z: { type: 'number', description: 'Destination world z (with x)' },
        arriveRadius: { type: 'number', description: 'Considered arrived within this many units of the goal (default: 3)' },
        timeoutMs: { type: 'number', description: 'Give up waiting for the arrival echo after this long (default: 90000, max 300000)' },
        boost: { type: 'boolean', description: 'Double-speed walk (250ms steps). Default false.' },
        mapUrl: { type: 'string', description: 'Map JSON URL (default: https://hubzz.app/data/maps/world_2.json)' },
      },
      required: ['name'],
    },
  },
];

// --- Tool Handlers ---

async function handleTool(name, args) {
  switch (name) {

    // === Original tools ===

    case 'bot_spawn': {
      const botName = args.name;
      if (botName == null || String(botName).trim() === '') return { error: 'name is required' };
      if (bots.has(botName)) return { error: `Bot "${botName}" already exists. Close it first or use a different name.` };
      const wsCheck = validateWsUrl(args.wsUrl);
      if (wsCheck.error) return wsCheck;
      const wsUrl = wsCheck.wsUrl;
      const bot = new BotConnection(wsUrl, botName, args.vrmUrl || '', { autoReconnect: args.autoReconnect || false, token: args.token });
      if (typeof args.serverUsername === 'string' && args.serverUsername.trim() !== '') {
        bot.serverUsernameHint = args.serverUsername.trim();
      }
      try {
        await bot.connect();
        bots.set(botName, bot);
        await sleep(1000);
        return { status: 'connected', name: botName, wsUrl, usersInWorld: bot.knownUsers.size, ownUserId: bot.ownUserId };
      } catch (err) {
        bot.close();
        return { error: `Failed to spawn bot: ${err.message}` };
      }
    }

    case 'bot_move': {
      const r = getBot(args.name); if (r.error) return r;
      const tileId = Number(args.tileId);
      if (!Number.isFinite(tileId)) return { error: `Invalid tileId: ${args.tileId}` };
      r.moveToTile(tileId, args.boost === true);
      r.ownTile = tileId;
      return { status: 'moved', name: args.name, tileId, boost: args.boost === true };
    }

    case 'bot_chat': {
      const r = getBot(args.name); if (r.error) return r;
      if (args.message == null || String(args.message).trim() === '') return { error: 'message is required' };
      const sent = r.sendChat(args.message);
      if (!sent) return { error: 'not connected: message not sent' };
      return { status: 'sent', name: args.name, message: args.message };
    }

    case 'bot_emote': {
      const r = getBot(args.name); if (r.error) return r;
      if (args.animation == null || String(args.animation).trim() === '') return { error: 'animation is required' };
      r.sendEmote(args.animation);
      return { status: 'emote_sent', name: args.name, animation: args.animation };
    }

    case 'bot_voice': {
      const r = getBot(args.name); if (r.error) return r;
      const state = args.state ?? args.voiceState ?? true;
      if (typeof state !== 'boolean') return { error: 'state (boolean) is required' };
      r._send({ h: 'voiceState', a: [state] });
      return { status: 'voice_toggled', name: args.name, state };
    }

    case 'bot_look': {
      const r = getBot(args.name, false); if (r.error) return r;
      return r.getFullState();
    }

    case 'bot_close': {
      const bot = bots.get(args.name);
      if (!bot) return { error: `Bot "${args.name}" not found` };
      // L-1: also stop the bot's RTC audio session — otherwise the socket.io
      // connection, tone timer, and mediasoup resources orphan.
      const sess = rtcSessions.get(args.name);
      if (sess) { try { sess.stop(); } catch (_) {} rtcSessions.delete(args.name); }
      bot.close();
      bots.delete(args.name);
      return { status: 'closed', name: args.name };
    }

    case 'bot_list': {
      const list = [];
      for (const [n, bot] of bots) {
        list.push({ name: n, connected: bot.connected, wsUrl: bot.wsUrl, usersInWorld: bot.knownUsers.size, uptime: bot.connectedAt ? Date.now() - bot.connectedAt : 0 });
      }
      return { bots: list, count: list.length };
    }

    case 'bot_close_all': {
      const names = Array.from(bots.keys());
      for (const [, bot] of bots) bot.close();
      // L-1: stop all RTC sessions too
      for (const [, s] of rtcSessions) { try { s.stop(); } catch (_) {} }
      rtcSessions.clear();
      bots.clear();
      return { status: 'all_closed', closed: names };
    }

    // === New tools ===

    case 'bot_batch_spawn': {
      const count = Math.min(Math.max(1, args.count || 1), MAX_BATCH_SIZE);
      const staggerMs = args.staggerMs ?? 500;
      const wsCheck = validateWsUrl(args.wsUrl);
      if (wsCheck.error) return wsCheck;
      const wsUrl = wsCheck.wsUrl;
      const results = [];
      const startTime = Date.now();
      const prefix = args.prefix ?? 'bot';

      for (let i = 0; i < count; i++) {
        const botName = `${prefix}-${i}`;
        if (bots.has(botName)) {
          results.push({ name: botName, status: 'skipped', error: 'already exists' });
          continue;
        }
        const bot = new BotConnection(wsUrl, botName, args.vrmUrl || '', { autoReconnect: args.autoReconnect || false });
        // Bug-2: reserve the name BEFORE the await gap so concurrent spawns
        // can't both pass the bots.has() check and create a ghost.
        bots.set(botName, bot);
        try {
          await bot.connect();
          results.push({ name: botName, status: 'connected' });
        } catch (err) {
          bots.delete(botName);
          bot.close();
          results.push({ name: botName, status: 'failed', error: err.message });
        }
        if (i < count - 1 && staggerMs > 0) await sleep(staggerMs);
      }

      const spawned = results.filter(r => r.status === 'connected').length;
      const failed = results.filter(r => r.status === 'failed').length;
      return { spawned, failed, skipped: results.length - spawned - failed, results, totalTimeMs: Date.now() - startTime };
    }

    case 'bot_observe': {
      const r = getBot(args.name, false); if (r.error) return r;
      const includeChat = args.includeChat !== false;
      const includeEvents = args.includeEvents || false;
      const includeNotices = args.includeNotices !== false;

      const result = {
        connection: r.getHealthStats(),
        users: Array.from(r.knownUsers.values()),
        ownTile: r.ownTile,
        summary: {
          userCount: r.knownUsers.size,
          chatCount: r.chatBuffer.length,
          connected: r.connected,
        },
      };
      if (includeChat) result.chat = r.chatBuffer;
      if (includeNotices) result.notices = r.notices;
      if (includeEvents) result.events = r.eventBuffer;
      return result;
    }

    case 'bot_patrol': {
      const action = args.action || 'start';
      const r = getBot(args.name); if (r.error) return r;

      if (action === 'stop') {
        r.stopPatrol();
        return { status: 'stopped', name: args.name };
      }
      if (action === 'status') {
        if (!r.patrolRoute) return { status: 'idle', name: args.name };
        return { status: 'patrolling', name: args.name, index: r.patrolRoute.index, total: r.patrolRoute.tiles.length, loop: r.patrolRoute.loop };
      }
      // start
      if (!Array.isArray(args.tiles) || args.tiles.length === 0 || args.tiles.some(t => !Number.isFinite(Number(t)))) return { error: 'tiles must be an array of numeric tile IDs' };
      const patrolIntervalMs = args.intervalMs == null ? 2000 : Number(args.intervalMs);
      if (!Number.isFinite(patrolIntervalMs) || patrolIntervalMs < 100) return { error: 'intervalMs must be a number >= 100' };
      r.startPatrol(args.tiles, patrolIntervalMs, args.loop !== false);
      return { status: 'patrolling', name: args.name, tiles: args.tiles.length, intervalMs: patrolIntervalMs, loop: args.loop !== false };
    }

    case 'bot_stress_test': {
      const rawCount = args.count == null ? 1 : Number(args.count);
      if (!Number.isFinite(rawCount) || rawCount < 1) return { error: 'count must be a positive number' };
      const count = Math.min(Math.max(1, rawCount), MAX_BATCH_SIZE);
      const durationSec = Math.min(Math.max(1, args.durationSec || 10), MAX_STRESS_DURATION);
      const mps = Math.min(Math.max(0.1, args.messagesPerSec || 1), 5);
      const wsCheck = validateWsUrl(args.wsUrl);
      if (wsCheck.error) return wsCheck;
      const wsUrl = wsCheck.wsUrl;
      const test = args.test;
      const testBots = [];
      const metrics = { botsSpawned: 0, botsFailed: 0, messagesAttempted: 0, messagesFailed: 0, droppedConnections: 0, errors: [] };

      // Spawn
      const spawnStart = Date.now();
      const stressPrefix = args.prefix ?? 'test';
      for (let i = 0; i < count; i++) {
        const botName = `_stress_${stressPrefix}-${i}`;
        if (bots.has(botName)) { bots.get(botName).close(); bots.delete(botName); }
        const bot = new BotConnection(wsUrl, botName, '', {});
        try {
          await bot.connect();
          bots.set(botName, bot);
          testBots.push({ bot, botName });
          metrics.botsSpawned++;
        } catch (err) {
          bot.close();
          metrics.botsFailed++;
          metrics.errors.push(`spawn ${botName}: ${err.message}`);
        }
        if (i < count - 1) await sleep(200);
      }
      const spawnTimeMs = Date.now() - spawnStart;

      if (test === 'connect') {
        // Just measure spawn, then cleanup
        for (const { bot, botName } of testBots) { bot.close(); bots.delete(botName); }
        return { test: 'connect', ...metrics, spawnTimeMs, avgSpawnMs: metrics.botsSpawned > 0 ? Math.round(spawnTimeMs / metrics.botsSpawned) : null };
      }

      // Wait for world state
      await sleep(1000);

      // Run test
      const intervalMs = Math.round(1000 / mps);
      const timers = [];
      const testStart = Date.now();

      for (const { bot } of testBots) {
        const timer = setInterval(() => {
          if (!bot.connected) { metrics.droppedConnections++; return; }
          try {
            metrics.messagesAttempted++;
            if (test === 'chat_flood') {
              bot.sendChat(`stress test ${Date.now()}`);
            } else if (test === 'move_flood') {
              bot.moveToTile(Math.floor(Math.random() * 500));
            } else { // mixed
              const r = Math.random();
              if (r < 0.4) bot.sendChat(`stress ${Date.now()}`);
              else if (r < 0.8) bot.moveToTile(Math.floor(Math.random() * 500));
              else bot.sendEmote(AVAILABLE_EMOTES[Math.floor(Math.random() * AVAILABLE_EMOTES.length)]);
            }
          } catch (err) {
            metrics.messagesFailed++;
            metrics.errors.push(err.message);
          }
        }, intervalMs);
        timers.push(timer);
      }

      // Wait for test duration
      await sleep(durationSec * 1000);

      // Cleanup
      for (const timer of timers) clearInterval(timer);
      const testTimeMs = Date.now() - testStart;

      // Collect latency stats
      const latencies = testBots.filter(({ bot }) => bot.pingLatencies.length > 0).map(({ bot }) => bot.pingLatencies.reduce((a, c) => a + c, 0) / bot.pingLatencies.length);
      const avgLatency = latencies.length > 0 ? Math.round(latencies.reduce((a, c) => a + c, 0) / latencies.length) : null;

      // Close test bots
      for (const { bot, botName } of testBots) {
        bot.close();
        bots.delete(botName);
      }

      return {
        test,
        ...metrics,
        spawnTimeMs,
        testDurationMs: testTimeMs,
        messagesPerSecPerBot: mps,
        avgLatencyMs: avgLatency,
        errors: metrics.errors.slice(-10),
      };
    }

    case 'bot_set_avatar': {
      const r = getBot(args.name); if (r.error) return r;
      if (args.method === 'shuffle') {
        const cmd = args.collection ? `!shuffle ${args.collection}` : '!shuffle';
        r.sendChat(cmd);
        return { status: 'shuffle_sent', name: args.name, collection: args.collection || 'default' };
      }
      if (!args.vrmUrl) return { error: 'vrmUrl is required when method is vrm' };
      // Wait for the server's verdict so avatar issues surface immediately.
      const res = await r.setAvatarAndWait(args.vrmUrl, args.timeoutMs || 15000);
      return { status: res.ok ? 'avatar_set' : 'avatar_failed', name: args.name, ...res };
    }

    case 'bot_test_avatars': {
      const r = getBot(args.name); if (r.error) return r;
      const urls = Array.isArray(args.vrmUrls) ? args.vrmUrls.filter(u => typeof u === 'string' && u) : [];
      if (urls.length === 0) return { error: 'vrmUrls must be a non-empty array of strings' };
      if (urls.length > 50) return { error: 'vrmUrls capped at 50 per run' };
      const perAvatarMs = args.timeoutMs || 15000;
      const pauseMs = args.pauseMs != null ? Math.max(0, Number(args.pauseMs)) : 1000;
      const results = [];
      for (const url of urls) {
        // The server only validates host/format, NOT file existence — so
        // check reachability too. A 200 with a VRM content-type/length is
        // the real "this avatar loads" signal.
        let reachable = null, httpStatus = null, contentLength = null;
        try {
          const u = new URL(url);
          const mod = u.protocol === 'https:' ? https : null;
          if (mod) {
            const info = await new Promise((resolve) => {
              const req = mod.request(url, { method: 'HEAD', timeout: 10000 }, (res) => {
                resolve({ status: res.statusCode, len: res.headers['content-length'] || null });
              });
              req.on('error', () => resolve(null));
              req.on('timeout', () => { req.destroy(); resolve(null); });
              req.end();
            });
            if (info) { reachable = info.status >= 200 && info.status < 400; httpStatus = info.status; contentLength = info.len; }
          }
        } catch {}
        const res = await r.setAvatarAndWait(url, perAvatarMs);
        results.push({ vrmUrl: url, ok: res.ok, reason: res.reason || null, ms: res.ms, reachable, httpStatus, contentLength });
        if (pauseMs > 0) await sleep(pauseMs);
      }
      const okCount = results.filter(x => x.ok).length;
      return {
        status: 'avatars_tested', name: args.name,
        total: results.length, ok: okCount, failed: results.length - okCount,
        results,
      };
    }

    case 'bot_avatar_info': {
      const apiBase = args.apiBase || 'https://avatars.hubzz.app/avatar-api';
      let vrmUrl = args.vrmUrl || null;
      let botAvatar = null;
      if (!vrmUrl) {
        if (!args.name) return { error: 'provide vrmUrl or a bot name' };
        const gr = getBot(args.name); if (gr.error) return gr;
        botAvatar = gr.getOwnAvatar();
        vrmUrl = botAvatar?.path || null;
        if (!vrmUrl) return { error: 'bot has no known avatar path yet (w:add not seen)' };
      }
      // Normalize for comparison (server canonicalizes worker URLs to altii.co).
      const norm = (u) => String(u || '').replace('https://hubzz-assets-worker.hubzzhq.workers.dev/files/', 'https://altii.co/');
      const target = norm(vrmUrl);
      // Paginated URL-match against the catalog (cap 10k).
      let found = null, offset = 0;
      while (offset < 10000 && !found) {
        let page;
        try {
          const res = await fetch(`${apiBase}/avatars?limit=500&offset=${offset}`);
          if (!res.ok) break;
          page = await res.json();
        } catch { break; }
        for (const a of (page.avatars || [])) {
          if (norm(a.optimized_vrm_url) === target || norm(a.original_vrm_url) === target) { found = a; break; }
        }
        offset += 500;
        if (!page.has_more) break;
      }
      const result = { vrmUrl, foundInCatalog: !!found };
      if (botAvatar) result.botAvatar = botAvatar;
      if (found) {
        result.name = found.name;
        result.collection = found.collection_slug || found.set_slug;
        result.height_m = found.height_m;
        result.triangles = found.triangles;
        result.fileSizeOptimized = found.file_size_optimized;
        result.fileSizeOriginal = found.file_size_original;
        result.thumbnail = found.thumbnail_url;
        result.image = found.image_url;
        result.license = found.license;
        result.bakedScale = found.baked_scale;
        result.sizingDecision = found.sizing_decision;
        result.contentRating = found.contentRating;
      } else {
        result.note = 'URL not in the published catalog (custom/unlisted avatar). Server accepts any altii.co URL; reachability is the real check.';
      }
      return result;
    }

    case 'bot_test_all_avatars': {
      // The reachability sweep is pure HTTP and does not need a live bot —
      // only the optional spotCheck goes through setAvatar. So the bot is
      // only required when spotCheck > 0.
      const spotCheckN = Math.min(50, Math.max(0, args.spotCheck || 0));
      let r = null;
      if (spotCheckN > 0) {
        const gr = getBot(args.name); if (gr.error) return gr;
        r = gr;
      }
      const apiBase = args.apiBase || 'https://avatars.hubzz.app/avatar-api';
      const concurrency = Math.min(50, Math.max(1, args.concurrency || 20));
      const startOffset = Math.max(0, args.offset || 0);
      const maxAvatars = args.limit != null ? Math.max(1, args.limit) : Infinity;

      // 1. Page the full catalog.
      const catalog = [];
      let offset = startOffset, total = Infinity;
      while (offset < total && catalog.length < maxAvatars) {
        const pageSize = Math.min(500, maxAvatars - catalog.length);
        let page;
        try {
          const res = await fetch(`${apiBase}/avatars?limit=${pageSize}&offset=${offset}`);
          if (!res.ok) return { error: `avatar-api returned HTTP ${res.status}` };
          page = await res.json();
        } catch (e) {
          return { error: `avatar-api fetch failed: ${e.message}` };
        }
        total = page.total ?? 0;
        for (const a of (page.avatars || [])) {
          const url = a.optimized_vrm_url || a.original_vrm_url || '';
          if (url) catalog.push({ id: a.id, name: a.name, url, collection: a.collection_slug || a.set_slug || '' });
        }
        offset += pageSize;
        if (!page.has_more) break;
      }

      // 2. HEAD-check every VRM URL with bounded concurrency.
      async function headCheck(url) {
        try {
          const u = new URL(url);
          if (u.protocol !== 'https:' && u.protocol !== 'http:') return { reachable: false, httpStatus: null, error: 'bad_protocol' };
          const mod = u.protocol === 'https:' ? https : http;
          return await new Promise((resolve) => {
            const req = mod.request(url, { method: 'HEAD', timeout: 15000 }, (res) => {
              resolve({ reachable: res.statusCode >= 200 && res.statusCode < 400, httpStatus: res.statusCode, contentLength: res.headers['content-length'] || null });
            });
            req.on('error', (e) => resolve({ reachable: false, httpStatus: null, error: e.message }));
            req.on('timeout', () => { req.destroy(); resolve({ reachable: false, httpStatus: null, error: 'timeout' }); });
            req.end();
          });
        } catch (e) {
          return { reachable: false, httpStatus: null, error: e.message };
        }
      }
      const results = new Array(catalog.length);
      let nextIdx = 0;
      async function worker() {
        while (nextIdx < catalog.length) {
          const i = nextIdx++;
          const c = catalog[i];
          const h = await headCheck(c.url);
          results[i] = { ...c, ...h };
        }
      }
      await Promise.all(Array.from({ length: Math.min(concurrency, catalog.length) }, worker));

      // 3. Optional: spot-check server acceptance through the bot.
      const spotResults = [];
      for (let i = 0; i < Math.min(spotCheckN, results.length); i++) {
        const sv = await r.setAvatarAndWait(results[i].url, 15000);
        spotResults.push({ url: results[i].url, serverOk: sv.ok, reason: sv.reason || null });
        await sleep(500);
      }

      const broken = results.filter(x => !x.reachable);
      return {
        status: 'all_avatars_tested',
        bot: r ? args.name : null,
        catalogTotal: total,
        tested: results.length,
        reachable: results.length - broken.length,
        broken: broken.length,
        brokenAvatars: broken.slice(0, 200).map(b => ({ id: b.id, name: b.name, url: b.url, collection: b.collection, httpStatus: b.httpStatus, error: b.error || null })),
        brokenTruncated: broken.length > 200,
        spotCheck: spotResults.length > 0 ? spotResults : undefined,
      };
    }

    case 'bot_nick': {
      const r = getBot(args.name); if (r.error) return r;
      if (args.newNick == null || String(args.newNick).trim() === '') return { error: 'newNick is required' };
      r.sendChat(`!nick ${args.newNick}`);
      return { status: 'nick_sent', name: args.name, newNick: args.newNick };
    }

    case 'bot_dance': {
      const r = getBot(args.name); if (r.error) return r;
      if (args.animation == null || String(args.animation).trim() === '') return { error: 'animation is required' };
      if (!/^[a-zA-Z0-9_]+$/.test(args.animation)) return { error: 'Animation name must be alphanumeric + underscore' };
      if (args.useChat) {
        r.sendChat(`!anim ${args.animation}`);
      } else {
        r.sendEmote(args.animation);
      }
      return { status: 'dance_sent', name: args.name, animation: args.animation, viaChat: !!args.useChat };
    }

    case 'bot_subscribe': {
      const r = getBot(args.name, false); if (r.error) return r;
      const action = args.action || 'subscribe';
      const events = args.events || ['*'];

      if (action === 'subscribe') {
        for (const e of events) r.eventSubscriptions.add(e);
        return { status: 'subscribed', name: args.name, subscriptions: Array.from(r.eventSubscriptions) };
      }
      if (action === 'unsubscribe') {
        for (const e of events) r.eventSubscriptions.delete(e);
        return { status: 'unsubscribed', name: args.name, subscriptions: Array.from(r.eventSubscriptions) };
      }
      if (action === 'clear') {
        r.eventBuffer = [];
        return { status: 'cleared', name: args.name };
      }
      if (action === 'read') {
        const events = [...r.eventBuffer];
        r.eventBuffer = [];
        return { events, count: events.length };
      }
      return { error: `Unknown action: ${action}` };
    }

    case 'bot_report': {
      const detailed = args.format === 'detailed';

      const buildBotReport = (n, bot) => {
        const report = {
          name: n,
          connected: bot.connected,
          uptime: bot.connectedAt ? Date.now() - bot.connectedAt : 0,
          usersObserved: bot.knownUsers.size,
          chatMessages: bot.chatBuffer.length,
          messagesSent: bot.messageCount.sent,
          messagesReceived: bot.messageCount.received,
          avgLatencyMs: bot.pingLatencies.length > 0 ? Math.round(bot.pingLatencies.reduce((a, b) => a + b, 0) / bot.pingLatencies.length) : null,
          errors: bot.errors.length,
          disconnects: bot.disconnectCount,
          patrolling: !!(bot.patrolRoute?.timer),
        };
        if (detailed) {
          report.recentErrors = bot.errors.slice(-5);
          report.recentNotices = bot.notices.slice(-5);
          report.recentChat = bot.chatBuffer.slice(-5);
        }
        return report;
      };

      if (args.name) {
        const bot = bots.get(args.name);
        if (!bot) return { error: `Bot "${args.name}" not found` };
        return { generatedAt: new Date().toISOString(), report: buildBotReport(args.name, bot) };
      }

      // All bots
      const reports = [];
      const allUsers = new Set();
      let totalChat = 0, totalErrors = 0, totalSent = 0, totalReceived = 0, latencySum = 0, latencyCount = 0;

      for (const [n, bot] of bots) {
        const r = buildBotReport(n, bot);
        reports.push(r);
        for (const u of bot.knownUsers.values()) allUsers.add(u.username);
        totalChat += bot.chatBuffer.length;
        totalErrors += bot.errors.length;
        totalSent += bot.messageCount.sent;
        totalReceived += bot.messageCount.received;
        if (r.avgLatencyMs !== null) { latencySum += r.avgLatencyMs; latencyCount++; }
      }

      return {
        generatedAt: new Date().toISOString(),
        botCount: bots.size,
        bots: reports,
        aggregate: {
          totalBots: bots.size,
          connectedBots: reports.filter(r => r.connected).length,
          uniqueUsersObserved: allUsers.size,
          totalChatMessages: totalChat,
          totalMessagesSent: totalSent,
          totalMessagesReceived: totalReceived,
          totalErrors,
          avgLatencyMs: latencyCount > 0 ? Math.round(latencySum / latencyCount) : null,
        },
      };
    }

    case 'bot_find_tiles': {
      const mapUrl = args.mapUrl || 'https://hubzz.xyz/data/maps/world_2.json';
      const showFalloff = args.showFalloff !== false;
      const distances = args.distances || [];

      let mapData;
      try { mapData = await fetchJson(mapUrl); }
      catch (e) { return { error: `Failed to fetch map: ${e.message}` }; }

      const allTiles = Object.values(mapData.tiles || {});
      const walkable = allTiles.filter(t => t.walkable);

      const dist2d = (t, cx, cz) => Math.sqrt((t.x - cx) ** 2 + (t.z - cz) ** 2);

      // Find center tile
      let center;
      if (args.centerTileId != null) {
        center = walkable.find(t => t.id === args.centerTileId);
        if (!center) return { error: `Tile ${args.centerTileId} not found or not walkable` };
      } else {
        if (walkable.length === 0) return { error: 'No walkable tiles in map' };
        center = walkable.reduce((best, t) => dist2d(t, 0, 0) < dist2d(best, 0, 0) ? t : best);
      }

      const cx = center.x, cz = center.z;

      // Parse optional direction into angle (degrees, 0=East/+X, 90=North/-Z, 180=West/-X, 270=South/+Z)
      const directionAngles = { E: 0, NE: 45, N: 90, NW: 135, W: 180, SW: 225, S: 270, SE: 315 };
      let angleFilter = null;
      let angleSector = 45; // ±degrees around target angle to accept
      if (args.direction != null) {
        if (typeof args.direction === 'string') {
          const key = args.direction.toUpperCase();
          if (key in directionAngles) angleFilter = directionAngles[key];
          else return { error: `Unknown direction "${args.direction}". Use N, S, E, W, NE, NW, SE, SW or a number.` };
        } else {
          angleFilter = Number(args.direction);
        }
      }

      const results = [];

      for (const targetDist of distances) {
        let best;
        if (angleFilter !== null) {
          // Filter to tiles within the angular sector, then find nearest to target distance
          const targetRad = (angleFilter * Math.PI) / 180;
          const sectorRad = (angleSector * Math.PI) / 180;
          const inSector = walkable.filter(t => {
            const dx = t.x - cx, dz = -(t.z - cz); // flip Z: -Z = north in world
            const angle = Math.atan2(dz, dx); // 0 = east
            let diff = angle - targetRad;
            while (diff > Math.PI) diff -= 2 * Math.PI;
            while (diff < -Math.PI) diff += 2 * Math.PI;
            return Math.abs(diff) <= sectorRad;
          });
          const pool = inSector.length > 0 ? inSector : walkable;
          best = pool.reduce((b, t) => {
            const da = Math.abs(dist2d(t, cx, cz) - targetDist);
            const db = Math.abs(dist2d(b, cx, cz) - targetDist);
            return da < db ? t : b;
          });
        } else {
          best = walkable.reduce((b, t) => {
            const da = Math.abs(dist2d(t, cx, cz) - targetDist);
            const db = Math.abs(dist2d(b, cx, cz) - targetDist);
            return da < db ? t : b;
          });
        }

        const actual = dist2d(best, cx, cz);
        const dx = best.x - cx, dz = -(best.z - cz);
        const actualAngleDeg = Math.round((Math.atan2(dz, dx) * 180) / Math.PI);
        const entry = {
          tileId: best.id,
          label: args.direction ? `${targetDist}u-${args.direction}` : `${targetDist}u`,
          targetDistance: targetDist,
          actualDistance: Math.round(actual * 100) / 100,
          position: { x: Math.round(best.x * 10) / 10, z: Math.round(best.z * 10) / 10 },
          angleDeg: actualAngleDeg,
        };
        if (showFalloff) {
          entry.predictedGain = Math.round(spatialGain(actual) * 1000) / 1000;
          entry.predictedDb = Math.round(20 * Math.log10(Math.max(spatialGain(actual), 0.001)) * 10) / 10;
        }
        results.push(entry);
      }

      return {
        centerTile: { id: center.id, x: Math.round(cx * 10) / 10, z: Math.round(cz * 10) / 10 },
        tiles: results,
        note: showFalloff ? 'Gain/dB based on FPP exponential model: refDistance=1, rolloffFactor=0.75. Isometric uses global audio (no falloff).' : undefined,
        spatialGridArgs: {
          tiles: results.map(r => ({ tileId: r.tileId, label: r.label })),
        },
      };
    }

    case 'bot_spatial_grid': {
      const prefix = args.prefix || 'audio';
      const wsCheck = validateWsUrl(args.wsUrl);
      if (wsCheck.error) return wsCheck;
      const wsUrl = wsCheck.wsUrl;
      const voiceOn = args.voiceOn !== false;
      const staggerMs = args.staggerMs ?? 400;
      const tiles = args.tiles || [];
      const results = [];

      for (let i = 0; i < tiles.length; i++) {
        const { tileId, label } = tiles[i];
        const botName = `${prefix}-${label || i}`;

        if (bots.has(botName)) {
          results.push({ name: botName, tileId, label: label || String(i), status: 'skipped', error: 'already exists' });
          continue;
        }

        const bot = new BotConnection(wsUrl, botName, '', {});
        try {
          await bot.connect();
          bots.set(botName, bot);
          await sleep(300);
          bot.moveToTile(tileId);
          bot.ownTile = tileId;
          await sleep(200);
          if (voiceOn) bot._send({ h: 'voiceState', a: [true] });
          results.push({
            name: botName,
            tileId,
            label: label || String(i),
            status: 'ready',
            voiceOn,
            position: bot.ownPosition || null,
          });
        } catch (err) {
          bot.close();
          bots.delete(botName);
          results.push({ name: botName, tileId, label: label || String(i), status: 'failed', error: err.message });
        }

        if (i < tiles.length - 1 && staggerMs > 0) await sleep(staggerMs);
      }

      const ready = results.filter(r => r.status === 'ready').length;
      return {
        summary: { ready, failed: results.filter(r => r.status === 'failed').length, skipped: results.filter(r => r.status === 'skipped').length },
        bots: results,
        tip: 'Walk through the world to test spatial audio falloff. Use bot_voice_all to toggle voice on/off. Use bot_close_all when done.',
      };
    }

    case 'bot_voice_all': {
      const state = args.state;
      if (typeof state !== 'boolean') return { error: 'state (boolean) is required' };
      const updated = [];
      for (const [n, bot] of bots) {
        if (bot.connected) {
          bot._send({ h: 'voiceState', a: [state] });
          updated.push(n);
        }
      }
      return { status: 'voice_set', state, updated, count: updated.length };
    }

    case 'bot_audio_start': {
      const botName = args.name;
      const r = getBot(botName); if (r.error) return r;
      if (rtcSessions.has(botName)) return { error: `Bot "${botName}" already has an active audio session. Use bot_audio_stop first.` };

      // Derive room_id from wsUrl: wss://hubzz.xyz/socket/0,0/ → hubzz.xyz@0,0
      const wsCheck = validateWsUrl(args.wsUrl);
      if (wsCheck.error) return wsCheck;
      const wsUrl = wsCheck.wsUrl;
      const urlObj = new URL(wsUrl);
      const hostname = urlObj.hostname;
      const worldPath = urlObj.pathname.replace(/^\/socket\//, '').replace(/\/$/, '') || '0,0';
      const roomId = `${hostname}@${worldPath}`;

      const session = new BotRTCSession(botName, roomId);
      rtcSessions.set(botName, session);

      try {
        const result = await session.start(args.frequency || 440, args.gain ?? 0.5);
        // Also set voiceState on the game bot if it exists
        const gameBot = bots.get(botName);
        if (gameBot?.connected) gameBot._send({ h: 'voiceState', a: [true] });
        return { status: 'producing', botName, roomId, frequency: session.frequency, gain: session.gain, ...result };
      } catch (err) {
        session.stop();
        rtcSessions.delete(botName);
        return { error: `Failed to start audio: ${err.message}` };
      }
    }

    case 'bot_audio_stop': {
      const session = rtcSessions.get(args.name);
      if (!session) return { error: `No active audio session for bot "${args.name}"` };
      session.stop();
      rtcSessions.delete(args.name);
      // Turn off voiceState on game bot
      const gameBot = bots.get(args.name);
      if (gameBot?.connected) gameBot._send({ h: 'voiceState', a: [false] });
      return { status: 'stopped', name: args.name };
    }

    case 'bot_audio_tune': {
      const session = rtcSessions.get(args.name);
      if (!session) return { error: `No active audio session for bot "${args.name}"` };
      session.setTone(args.frequency, args.gain);
      return { status: 'updated', name: args.name, frequency: session.frequency, gain: session.gain };
    }

    case 'bot_audio_status': {
      if (args.name) {
        const session = rtcSessions.get(args.name);
        if (!session) return { name: args.name, status: 'no_session' };
        return session.getState();
      }
      const all = [];
      for (const [, s] of rtcSessions) all.push(s.getState());
      return { sessions: all, count: all.length };
    }

    case 'bot_typing': {
      const r = getBot(args.name); if (r.error) return r;
      const typing = args.typing !== false;
      // L-4: cancel previous auto-clear timer so rapid re-asserts don't stack
      if (r.typingClearTimer) { clearTimeout(r.typingClearTimer); r.typingClearTimer = null; }
      r.sendTypingStatus(typing);
      if (typing && args.autoClearMs) {
        r.typingClearTimer = setTimeout(() => { r.typingClearTimer = null; if (r.connected) r.sendTypingStatus(false); }, args.autoClearMs);
      }
      return { status: 'typing_sent', name: args.name, typing, autoClearMs: args.autoClearMs || null };
    }

    case 'bot_rotate': {
      const r = getBot(args.name); if (r.error) return r;
      let x = Number(args.x ?? 0), y = Number(args.y ?? 0), z = Number(args.z ?? 0);
      if (args.yaw != null) y = (Number(args.yaw) * Math.PI) / 180;
      if (![x, y, z].every(Number.isFinite)) return { error: 'x/y/z/yaw must be numbers' };
      r.sendRotation(x, y, z);
      r.ownRotation = { x, y, z };
      return { status: 'rotated', name: args.name, rotation: { x, y, z }, yawDeg: args.yaw ?? null };
    }

    case 'bot_emote_loop': {
      const r = getBot(args.name); if (r.error) return r;
      if (args.animation == null || String(args.animation).trim() === '') return { error: 'animation is required' };
      const loop = args.loop !== false;
      r._send({ h: 'emote', a: [args.animation, loop] });
      return { status: 'emote_sent', name: args.name, animation: args.animation, loop };
    }

    case 'bot_guest': {
      if (args.name == null || String(args.name).trim() === '') return { error: 'name is required' };
      if (bots.has(args.name)) return { error: `Bot "${args.name}" already exists. Close it first or use a different name.` };
      const wsCheck = validateWsUrl(args.wsUrl);
      if (wsCheck.error) return wsCheck;
      const wsUrl = wsCheck.wsUrl;
      const bot = new BotConnection(wsUrl, args.name, '', { isGuest: true });
      try {
        await bot.connect();
        bots.set(args.name, bot);
        // Guest username is assigned by server — read it from acc:ok data if possible
        return { status: 'connected', name: args.name, isGuest: true, wsUrl };
      } catch (err) {
        bot.close();
        return { error: err.message };
      }
    }

    case 'bot_kick_test': {
      const r = getBot(args.name); if (r.error) return r;
      if (['kick', 'ban', 'grant'].includes(args.action) && (args.target == null || String(args.target).trim() === '')) return { error: 'target is required' };
      if (args.action === 'kick') {
        r.sendChat(`!kick ${args.target}`);
      } else if (args.action === 'ban') {
        const dur = args.duration ?? 0;
        r.sendChat(`!ban ${dur} ${args.target}`);
      } else if (args.action === 'grant') {
        if (!args.permission) return { error: 'permission required for grant action' };
        r.sendChat(`!grant ${args.permission} ${args.target}`);
      } else {
        return { error: `Unknown kick_test action: ${args.action}` };
      }
      return { status: 'command_sent', action: args.action, target: args.target };
    }

    case 'bot_watch_events': {
      const r = getBot(args.name); if (r.error) return r;
      const timeoutMs = Number(args.timeoutMs ?? 5000);
      if (!Number.isFinite(timeoutMs) || timeoutMs < 0) return { error: 'timeoutMs must be a non-negative number' };
      const eventType = args.eventType || '*';

      let matchFn = null;
      if (args.matchField != null && args.matchValue != null) {
        if (typeof args.matchField !== 'string') return { error: 'matchField must be a string' };
        const fieldPath = args.matchField.split('.');
        matchFn = (data) => {
          let val = data;
          for (const key of fieldPath) val = val?.[key];
          return String(val) === String(args.matchValue);
        };
      }

      // Ensure subscribed so _bufferEvent fires
      r.eventSubscriptions.add(eventType === '*' ? '*' : eventType);

      try {
        const result = await r.waitForEvent(eventType, matchFn, timeoutMs);
        return { matched: true, event: result };
      } catch (err) {
        return { matched: false, error: err.message };
      }
    }

    case 'bot_upload': {
      const r = getBot(args.name); if (r.error) return r;
      const filename = String(args.filename || 'test.png');
      const ext = filename.split('.').pop().toLowerCase();

      // Minimal valid test files as base64
      const testFiles = {
        png: 'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==',
        jpg: '/9j/4AAQSkZJRgABAQEASABIAAD/2wBDAAgGBgcGBQgHBwcJCQgKDBQNDAsLDBkSEw8UHRofHh0aHBwgJC4nICIsIxwcKDcpLDAxNDQ0Hyc5PTgyPC4zNDL/wAALCAABAAEBAREA/8QAFAABAAAAAAAAAAAAAAAAAAAACf/EABQQAQAAAAAAAAAAAAAAAAAAAAD/2gAIAQEAAD8AVIP/2Q==',
        mp3: 'SUQzBAAAAAAAI1RTU0UAAAAPAAADTGF2ZjU4LjI5LjEwMAAAAAAAAAAAAAAA//tQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAASW5mbwAAAA8AAAACAAACcABgYGBgYGBgYGBgYGBgYGBgYGBgYGBgYGBgYGBgYGBgYGBgYGBgYGBgYGBgYGBg//////////////////////////////////////////////////////////////////8AAAAATGF2YzU4LjU0AAAAAAAAAAAAAAAAJAAAAAAAAAAAAnABpMNLAAAAAAAAAAAAAAAAAAAA//tQZAAP8AAAaQAAAAgAAA0gAAABAAABpAAAACAAADSAAAAETEFNRTMuMTAwVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVV',
      };
      const data = args.data || testFiles[ext] || testFiles.png;
      const replyGuid = crypto.randomUUID();

      let responsePromise = null;
      if (args.waitForResponse !== false) {
        r.eventSubscriptions.add('w:call');
        responsePromise = r.waitForEvent('w:call', null, 5000).catch(() => null);
      }

      r._send({ h: 'upload', a: [{ name: filename, data }, replyGuid] });

      const response = responsePromise ? await responsePromise : null;
      return { status: 'sent', filename, replyGuid, response: response?.data || null };
    }

    case 'bot_screen': {
      const r = getBot(args.name); if (r.error) return r;
      if (args.action === 'play') {
        if (!args.url) return { error: 'url required for play action' };
        const sent = r.sendChat(`--play ${args.url}`);
        if (!sent) return { error: 'not connected: play command not sent' };
        return { status: 'play_sent', url: args.url };
      } else if (args.action === 'stop') {
        const sent = r.sendChat('--stop');
        if (!sent) return { error: 'not connected: stop command not sent' };
        return { status: 'stop_sent' };
      } else if (args.action === 'call') {
        if (!args.entityId || !args.method) return { error: 'entityId and method required for call action' };
        r._send({ h: 'call', a: [args.entityId, args.method, ...(args.args || [])] });
        return { status: 'call_sent', entityId: args.entityId, method: args.method };
      }
      return { error: `Unknown screen action: ${args.action}` };
    }

    case 'bot_kbs': {
      const r = getBot(args.name); if (r.error) return r;
      const pos = args.position;
      if (!pos || typeof pos.x !== 'number' || typeof pos.y !== 'number' || typeof pos.z !== 'number') return { error: 'position {x,y,z} is required' };
      const rot = args.rotation || { x: 0, y: 0, z: 0 };
      const anim = args.animation || 'anim_idle';
      r._send({ h: 'kbs', a: [pos, rot, anim] });
      r.ownPosition = pos;
      r.ownRotation = rot;
      return { status: 'kbs_sent', position: pos, rotation: rot, animation: anim };
    }

    case 'bot_world_wait': {
      const r = getBot(args.name); if (r.error) return r;
      const timeoutMs = Number(args.timeoutMs ?? 10000);
      if (!Number.isFinite(timeoutMs) || timeoutMs < 0) return { error: 'timeoutMs must be a non-negative number' };
      const condition = args.condition;
      const validConditions = ['user_joined','user_left','chat_received','notice_received','entity_added','entity_removed','user_count_gte','user_count_lte'];
      if (!validConditions.includes(condition)) return { error: `Unknown condition: ${condition}` };
      const value = args.value;
      if ((condition === 'user_count_gte' || condition === 'user_count_lte') && !Number.isFinite(Number(value))) return { error: 'value must be a number for user_count_* conditions' };
      const deadline = Date.now() + timeoutMs;

      const poll = () => new Promise((resolve, reject) => {
        const check = () => {
          let met = false;
          let detail = null;
          switch (condition) {
            case 'user_joined': {
              const found = [...r.knownUsers.values()].find(u => u.username === value || String(u.id) === String(value));
              met = !!found; detail = found || null; break;
            }
            case 'user_left':
              met = ![...r.knownUsers.values()].some(u => u.username === value || String(u.id) === String(value));
              break;
            case 'chat_received': {
              const msg = r.chatBuffer.slice().reverse().find(m => !value || m.message.includes(value));
              met = !!msg; detail = msg || null; break;
            }
            case 'notice_received': {
              const n = r.notices.slice().reverse().find(n => !value || n.text?.includes(value));
              met = !!n; detail = n || null; break;
            }
            case 'entity_added': {
              const e = [...r.entities.values()].find(e => !value || e.type === value);
              met = !!e; detail = e || null; break;
            }
            case 'entity_removed':
              met = value ? ![...r.entities.values()].some(e => e.type === value) : r.entities.size === 0;
              break;
            case 'user_count_gte':
              met = r.knownUsers.size >= Number(value); detail = { count: r.knownUsers.size }; break;
            case 'user_count_lte':
              met = r.knownUsers.size <= Number(value); detail = { count: r.knownUsers.size }; break;
          }
          if (met) { resolve({ condition, met: true, detail }); return; }
          if (Date.now() >= deadline) { reject(new Error(`Timeout: condition "${condition}" not met within ${timeoutMs}ms`)); return; }
          setTimeout(check, 100);
        };
        check();
      });

      try {
        const result = await poll();
        return result;
      } catch (err) {
        return { condition, met: false, error: err.message };
      }
    }

    case 'bot_assert': {
      const r = getBot(args.name); if (r.error) return r;
      const results = [];

      for (const assertion of (args.assertions || [])) {
        const { check, value, tileId } = assertion;
        let pass = false, actual = null, detail = null;

        switch (check) {
          case 'connected':
            pass = r.connected; actual = r.connected; break;
          case 'user_present': {
            const u = [...r.knownUsers.values()].find(u => u.username === value || String(u.id) === String(value));
            pass = !!u; actual = !!u; detail = u || null; break;
          }
          case 'user_absent':
            pass = ![...r.knownUsers.values()].some(u => u.username === value || String(u.id) === String(value));
            actual = !pass; break;
          case 'user_at_tile': {
            const u = [...r.knownUsers.values()].find(u => u.username === value || String(u.id) === String(value));
            actual = u?.tile ?? null;
            pass = u != null && Number(u.tile) === Number(tileId); break;
          }
          case 'own_tile': {
            const expected = value ?? tileId;
            actual = r.ownTile; pass = Number(r.ownTile) === Number(expected); break;
          }
          case 'chat_contains': {
            const msg = r.chatBuffer.slice().reverse().find(m => m.message.includes(value));
            pass = !!msg; actual = msg?.message || null; break;
          }
          case 'notice_contains': {
            const n = r.notices.slice().reverse().find(n => n.text?.includes(value));
            pass = !!n; actual = n?.text || null; break;
          }
          case 'user_count':
            actual = r.knownUsers.size; pass = r.knownUsers.size === Number(value); break;
          case 'entity_present': {
            const e = [...r.entities.values()].find(e => e.type === value || String(e.id) === String(value));
            pass = !!e; actual = !!e; detail = e || null; break;
          }
          case 'entity_absent':
            pass = ![...r.entities.values()].some(e => e.type === value || String(e.id) === String(value));
            actual = !pass; break;
        }
        results.push({ check, value, tileId, pass, actual, detail });
      }

      const allPassed = results.every(r => r.pass);
      return { passed: allPassed, total: results.length, passed_count: results.filter(r => r.pass).length, assertions: results };
    }

    case 'bot_ping_latency': {
      const r = getBot(args.name); if (r.error) return r;
      const targetSamples = args.samples ?? 0;

      if (targetSamples > 0) {
        // Collect fresh samples by waiting for ping events
        const initialCount = r.pingLatencies.length;
        const needed = targetSamples;
        const timeoutMs = Number(args.timeoutMs ?? 15000);
        if (!Number.isFinite(timeoutMs) || timeoutMs < 0) return { error: 'timeoutMs must be a non-negative number' };
        const deadline = Date.now() + timeoutMs;

        await new Promise((resolve) => {
          const check = () => {
            if (r.pingLatencies.length - initialCount >= needed || Date.now() >= deadline) { resolve(); return; }
            setTimeout(check, 200);
          };
          check();
        });
      }

      const samples = r.pingLatencies;
      if (samples.length === 0) return { name: args.name, error: 'No ping samples yet — bot may not have been connected long enough' };

      const sorted = [...samples].sort((a, b) => a - b);
      const avg = Math.round(samples.reduce((a, b) => a + b, 0) / samples.length);
      const p50 = sorted[Math.floor(sorted.length * 0.5)];
      const p95 = sorted[Math.floor(sorted.length * 0.95)];
      return {
        name: args.name,
        samples: samples.length,
        minMs: sorted[0],
        maxMs: sorted[sorted.length - 1],
        avgMs: avg,
        p50Ms: p50,
        p95Ms: p95,
        raw: samples,
      };
    }

    case 'bot_spawn_at': {
      if (args.name == null || String(args.name).trim() === '') return { error: 'name is required' };
      if (bots.has(args.name)) return { error: `Bot "${args.name}" already exists. Close it first or use a different name.` };
      const spawnTileId = Number(args.tileId);
      if (!Number.isFinite(spawnTileId)) return { error: `Invalid tileId: ${args.tileId}` };
      const wsCheck = validateWsUrl(args.wsUrl);
      if (wsCheck.error) return wsCheck;
      const wsUrl = wsCheck.wsUrl;
      const bot = new BotConnection(wsUrl, args.name, args.vrmUrl || '', {});
      try {
        await bot.connect();
        bots.set(args.name, bot);
        await sleep(300);
        bot.moveToTile(spawnTileId);
        bot.ownTile = spawnTileId;
        if (args.voiceOn) {
          await sleep(200);
          bot._send({ h: 'voiceState', a: [true] });
        }
        return { status: 'ready', name: args.name, tileId: spawnTileId, voiceOn: !!args.voiceOn };
      } catch (err) {
        bot.close();
        bots.delete(args.name);
        return { error: err.message };
      }
    }

    case 'bot_directional_ring': {
      const wsCheck = validateWsUrl(args.wsUrl);
      if (wsCheck.error) return wsCheck;
      const wsUrl = wsCheck.wsUrl;
      const mapUrl = args.mapUrl || 'https://hubzz.xyz/data/maps/world_2.json';
      const distance = Number(args.distance ?? 10);
      if (!Number.isFinite(distance) || distance < 0) return { error: 'distance must be a non-negative number' };
      const prefix = args.prefix || 'dir';
      const gain = args.gain ?? 0.7;
      const startAudio = args.startAudio !== false;
      const directionAngles = { E: 0, NE: 45, N: 90, NW: 135, W: 180, SW: 225, S: 270, SE: 315 };

      let dirs;
      if (args.directions?.length) {
        dirs = args.directions.map(d => d.toUpperCase());
      } else {
        const count = args.count === 8 ? 8 : 4;
        dirs = count === 8 ? ['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW'] : ['N', 'E', 'S', 'W'];
      }

      // Fetch map and find center
      let mapData;
      try { mapData = await fetchJson(mapUrl); }
      catch (e) { return { error: `Failed to fetch map: ${e.message}` }; }

      const allTiles = Object.values(mapData.tiles || {});
      const walkable = allTiles.filter(t => t.walkable);
      const dist2d = (t, cx, cz) => Math.sqrt((t.x - cx) ** 2 + (t.z - cz) ** 2);

      let center;
      if (args.centerTileId != null) {
        center = walkable.find(t => t.id === args.centerTileId);
        if (!center) return { error: `Center tile ${args.centerTileId} not found` };
      } else {
        if (walkable.length === 0) return { error: 'No walkable tiles in map' };
        center = walkable.reduce((best, t) => dist2d(t, 0, 0) < dist2d(best, 0, 0) ? t : best);
      }
      const cx = center.x, cz = center.z;

      // Assign frequencies: base, base*1.25, base*1.5, base*2, ...
      const baseFreq = args.baseFreq ?? 220;
      const freqMultipliers = [1, 1.25, 1.5, 2, 2.5, 3, 4, 5];

      const ringResults = [];

      for (let i = 0; i < dirs.length; i++) {
        const dir = dirs[i];
        const angleDeg = directionAngles[dir] ?? (parseFloat(dir) || 0);
        const angleRad = (angleDeg * Math.PI) / 180;
        const sectorRad = (45 * Math.PI) / 180;
        const freq = Math.round(baseFreq * (freqMultipliers[i] ?? (i + 1)));
        const botName = `${prefix}-${dir.toLowerCase()}`;

        // Find nearest walkable tile in this sector at target distance
        const inSector = walkable.filter(t => {
          const dx = t.x - cx, dz = -(t.z - cz);
          const angle = Math.atan2(dz, dx);
          let diff = angle - angleRad;
          while (diff > Math.PI) diff -= 2 * Math.PI;
          while (diff < -Math.PI) diff += 2 * Math.PI;
          return Math.abs(diff) <= sectorRad;
        });
        const pool = inSector.length > 0 ? inSector : walkable;
        const best = pool.reduce((b, t) => {
          const da = Math.abs(dist2d(t, cx, cz) - distance);
          const db = Math.abs(dist2d(b, cx, cz) - distance);
          return da < db ? t : b;
        });
        const actualDist = Math.round(dist2d(best, cx, cz) * 100) / 100;

        // Skip if bot already exists
        if (bots.has(botName)) {
          ringResults.push({ name: botName, direction: dir, tileId: best.id, actualDist, freq, status: 'skipped' });
          continue;
        }

        try {
          const bot = new BotConnection(wsUrl, botName, '', {});
          await bot.connect();
          bots.set(botName, bot);
          await sleep(300);
          bot.moveToTile(best.id);
          await sleep(200);
          bot._send({ h: 'voiceState', a: [true] });

          if (startAudio) {
            await sleep(500);
            // Derive room ID
            const urlObj = new URL(wsUrl);
            const worldPath = urlObj.pathname.replace(/^\/socket\//, '').replace(/\/$/, '') || '0,0';
            const roomId = `${urlObj.hostname}@${worldPath}`;
            let session = null;
            try {
              session = new BotRTCSession(botName, roomId, `https://${urlObj.hostname}`, freq, gain);
              await session.start(freq, gain);
              rtcSessions.set(botName, session);
            } catch (audioErr) {
              if (session) { try { session.stop(); } catch (_) {} }
              throw audioErr;
            }
          }

          ringResults.push({ name: botName, direction: dir, tileId: best.id, actualDist, freq, gain, status: 'ready', audioStarted: startAudio });
        } catch (err) {
          const b = bots.get(botName);
          if (b) { b.close(); bots.delete(botName); }
          ringResults.push({ name: botName, direction: dir, tileId: best.id, actualDist, freq, status: 'failed', error: err.message });
        }

        if (i < dirs.length - 1) await sleep(600);
      }

      return {
        centerTile: { id: center.id, x: Math.round(cx * 10) / 10, z: Math.round(cz * 10) / 10 },
        distance,
        directions: dirs,
        bots: ringResults,
        tip: `Stand on tile ${center.id} (world center), enter FPP, and rotate. Each direction has a distinct tone: ${dirs.map((d, i) => `${d}=${Math.round(baseFreq * (freqMultipliers[i] ?? (i+1)))}Hz`).join(', ')}. You should hear clear left/right/front/behind panning.`,
      };
    }

    case 'bot_scene_audio': {
      const action = args.action || 'setup';
      const wsCheck = validateWsUrl(args.wsUrl);
      if (wsCheck.error) return wsCheck;
      const wsUrl = wsCheck.wsUrl;

      if (action === 'teardown') {
        // Stop conductor
        if (conductorInterval) { clearInterval(conductorInterval); conductorInterval = null; }
        if (conductorBot) { const n = conductorBot.username; conductorBot.close(); bots.delete(n); conductorBot = null; }
        // Stop all audio sessions
        for (const [n, s] of rtcSessions) { try { s.stop(); } catch (_) {} }
        rtcSessions.clear();
        // Kill all bots
        for (const [n, b] of bots) b.close();
        bots.clear();
        return { status: 'teardown_complete' };
      }

      if (action === 'status') {
        const audioStatus = [];
        for (const [n, s] of rtcSessions) audioStatus.push(s.getState());
        return {
          bots: [...bots.entries()].map(([n, b]) => ({ name: n, connected: b.connected, tile: b.ownTile })),
          audioSessions: audioStatus,
          conductor: conductorBot ? { name: conductorBot.username, connected: conductorBot.connected } : null,
          config: conductorConfig,
        };
      }

      if (action === 'setup_ring') {
        // Teardown first
        if (conductorInterval) { clearInterval(conductorInterval); conductorInterval = null; }
        if (conductorBot) { const n = conductorBot.username; conductorBot.close(); bots.delete(n); conductorBot = null; }
        for (const [n, s] of rtcSessions) { try { s.stop(); } catch (_) {} }
        rtcSessions.clear();
        for (const [n, b] of bots) b.close();
        bots.clear();
        await sleep(500);

        // Set up directional ring (N/E/S/W at 10u)
        const ringResult = await handleTool('bot_directional_ring', { wsUrl, distance: 10, count: 4, baseFreq: 220, gain: 0.7, startAudio: true });

        // Start conductor
        await sleep(500);
        const condResult = await handleTool('bot_conductor_start', { wsUrl, username: 'conductor' });

        return { action: 'setup_ring', ring: ringResult, conductor: condResult };
      }

      if (!['setup', 'teardown', 'status', 'setup_ring'].includes(action)) return { error: `Unknown scene_audio action: ${args.action}` };

      // Default: 'setup' — distance-based test
      if (conductorInterval) { clearInterval(conductorInterval); conductorInterval = null; }
      if (conductorBot) { const n = conductorBot.username; conductorBot.close(); bots.delete(n); conductorBot = null; }
      for (const [n, s] of rtcSessions) { try { s.stop(); } catch (_) {} }
      rtcSessions.clear();
      for (const [n, b] of bots) b.close();
      bots.clear();
      await sleep(500);

      const distanceBots = [
        { name: 'audio-5u',  tileId: 1950, freq: 220, gain: 0.6 },
        { name: 'audio-10u', tileId: 1763, freq: 330, gain: 0.6 },
        { name: 'audio-20u', tileId: 1498, freq: 440, gain: 0.6 },
        { name: 'audio-30u', tileId: 1257, freq: 550, gain: 0.6 },
      ];
      const setupResults = [];
      const urlObj = new URL(wsUrl);
      const worldPath = urlObj.pathname.replace(/^\/socket\//, '').replace(/\/$/, '') || '0,0';
      const roomId = `${urlObj.hostname}@${worldPath}`;
      const voiceServerBase = `https://${urlObj.hostname}`;

      for (const cfg of distanceBots) {
        try {
          const bot = new BotConnection(wsUrl, cfg.name, '', {});
          await bot.connect();
          bots.set(cfg.name, bot);
          await sleep(300);
          bot.moveToTile(cfg.tileId);
          await sleep(200);
          bot._send({ h: 'voiceState', a: [true] });
          await sleep(500);
          let audioSession = null;
          try {
            audioSession = new BotRTCSession(cfg.name, roomId, voiceServerBase, cfg.freq, cfg.gain);
            await audioSession.start(cfg.freq, cfg.gain);
          } catch (audioErr) {
            if (audioSession) { try { audioSession.stop(); } catch (_) {} }
            throw audioErr;
          }
          rtcSessions.set(cfg.name, audioSession);
          setupResults.push({ name: cfg.name, tileId: cfg.tileId, freq: cfg.freq, status: 'ready' });
        } catch (err) {
          const b = bots.get(cfg.name);
          if (b) { b.close(); bots.delete(cfg.name); }
          setupResults.push({ name: cfg.name, status: 'failed', error: err.message });
        }
        await sleep(500);
      }

      await sleep(300);
      const condResult = await handleTool('bot_conductor_start', { wsUrl, username: 'conductor' });

      return {
        action: 'setup',
        bots: setupResults,
        conductor: condResult,
        tip: 'Distance test ready. Enter FPP at world center and walk toward/away from bots. Chat: !ref N  !rolloff N  !vol N  !status  !reset',
      };
    }

    case 'bot_push_config': {
      const sender = bots.get(args.botName);
      if (!sender || !sender.connected) return { error: `Bot "${args.botName}" not connected` };

      const config = {};
      if (args.firstPerson) config.firstPerson = args.firstPerson;
      if (args.isometric) config.isometric = args.isometric;

      sender._send({ h: 'spatialAudioConfig', a: [config] });
      return { status: 'sent', config };
    }

    case 'bot_conductor_start': {
      const wsCheck2 = validateWsUrl(args.wsUrl);
      if (wsCheck2.error) return wsCheck2;
      const wsUrl = wsCheck2.wsUrl || 'wss://hubzz.xyz/socket/0,0/';
      const username = args.username || 'conductor';
      // Bug-3: capture generation before the async gap
      const myGen = ++conductorGen;

      // Stop any existing conductor
      if (conductorInterval) {
        clearInterval(conductorInterval);
        conductorInterval = null;
      }
      if (conductorBot) {
        const n = conductorBot.username; conductorBot.close(); bots.delete(n); conductorBot = null;
      }

      // Reset config to defaults
      conductorConfig = {
        firstPerson: { refDistance: 1, rolloffFactor: 0.75, distanceModel: 'exponential', volume: 1.0 },
        isometric: { volume: 1.0 },
      };

      const bot = new BotConnection(wsUrl, username, '', { autoReconnect: true });
      try {
        await bot.connect();
      } catch (err) {
        bot.close();
        return { error: `Failed to start conductor: ${err.message}` };
      }
      // Bug-3: if a newer start superseded us during the await, clean up and bail
      if (myGen !== conductorGen) {
        bot.close();
        return { error: 'Conductor start superseded by newer start' };
      }
      if (bots.has(username)) { const old = bots.get(username); old.close(); bots.delete(username); }
      bots.set(username, bot);
      conductorBot = bot;
      conductorChatCursor = bot.chatBuffer.length;

      const pushConfig = (partial) => {
        if (!bot.connected) return;
        bot._send({ h: 'spatialAudioConfig', a: [partial] });
      };

      const say = (text) => {
        if (!bot.connected) return;
        bot._send({ h: 'chat', a: [text] });
      };

      conductorInterval = setInterval(() => {
        if (!bot.connected) return;
        const msgs = bot.chatBuffer.slice(conductorChatCursor);
        conductorChatCursor = bot.chatBuffer.length;

        for (const entry of msgs) {
          // Ignore own messages
          if (entry.username.toLowerCase() === username.toLowerCase()) continue;

          const text = entry.message.trim();
          if (!text.startsWith('!')) continue;

          const parts = text.split(/\s+/);
          const cmd = parts[0].toLowerCase();

          if (cmd === '!ref') {
            const v = parseFloat(parts[1]);
            if (isNaN(v) || v <= 0) { say(`[conductor] !ref requires a positive number`); continue; }
            conductorConfig.firstPerson.refDistance = v;
            pushConfig({ firstPerson: { refDistance: v } });
            say(`[conductor] refDistance → ${v}`);

          } else if (cmd === '!rolloff') {
            const v = parseFloat(parts[1]);
            if (isNaN(v) || v < 0) { say(`[conductor] !rolloff requires a non-negative number`); continue; }
            conductorConfig.firstPerson.rolloffFactor = v;
            pushConfig({ firstPerson: { rolloffFactor: v } });
            say(`[conductor] rolloffFactor → ${v}`);

          } else if (cmd === '!vol') {
            const v = parseFloat(parts[1]);
            if (isNaN(v) || v < 0) { say(`[conductor] !vol requires a non-negative number`); continue; }
            conductorConfig.firstPerson.volume = v;
            pushConfig({ firstPerson: { volume: v } });
            say(`[conductor] FPP volume → ${v}`);

          } else if (cmd === '!isovol') {
            const v = parseFloat(parts[1]);
            if (isNaN(v) || v < 0) { say(`[conductor] !isovol requires a non-negative number`); continue; }
            conductorConfig.isometric.volume = v;
            pushConfig({ isometric: { volume: v } });
            say(`[conductor] isometric volume → ${v}`);

          } else if (cmd === '!louder') {
            // Scale FPP volume or specific bot gain up by 25%
            const target = parts[1];
            if (target) {
              const session = rtcSessions.get(target);
              if (!session) { say(`[conductor] no audio session for "${target}"`); continue; }
              const newGain = Math.min(session.gain * 1.25, 1.0);
              session.setTone(undefined, newGain);
              say(`[conductor] ${target} gain → ${newGain.toFixed(2)}`);
            } else {
              const v = Math.min((conductorConfig.firstPerson.volume || 1.0) * 1.25, 2.0);
              conductorConfig.firstPerson.volume = v;
              pushConfig({ firstPerson: { volume: v } });
              say(`[conductor] FPP volume → ${v.toFixed(2)}`);
            }

          } else if (cmd === '!quieter') {
            const target = parts[1];
            if (target) {
              const session = rtcSessions.get(target);
              if (!session) { say(`[conductor] no audio session for "${target}"`); continue; }
              const newGain = Math.max(session.gain * 0.8, 0.01);
              session.setTone(undefined, newGain);
              say(`[conductor] ${target} gain → ${newGain.toFixed(2)}`);
            } else {
              const v = Math.max((conductorConfig.firstPerson.volume || 1.0) * 0.8, 0.05);
              conductorConfig.firstPerson.volume = v;
              pushConfig({ firstPerson: { volume: v } });
              say(`[conductor] FPP volume → ${v.toFixed(2)}`);
            }

          } else if (cmd === '!status') {
            const fp = conductorConfig.firstPerson;
            say(`[conductor] ref=${fp.refDistance} rolloff=${fp.rolloffFactor} vol=${fp.volume} model=${fp.distanceModel}`);

          } else if (cmd === '!reset') {
            conductorConfig = {
              firstPerson: { refDistance: 1, rolloffFactor: 0.75, distanceModel: 'exponential', volume: 1.0 },
              isometric: { volume: 1.0 },
            };
            pushConfig(conductorConfig);
            say(`[conductor] reset to defaults`);

          } else if (cmd === '!help') {
            say(`[conductor] cmds: !ref N  !rolloff N  !vol N  !isovol N  !louder [bot]  !quieter [bot]  !status  !reset`);
          }
        }
      }, 500);

      return { status: 'started', username, wsUrl };
    }

    case 'bot_conductor_stop': {
      if (conductorInterval) {
        clearInterval(conductorInterval);
        conductorInterval = null;
      }
      if (conductorBot) {
        const name = conductorBot.username;
        conductorBot.close();
        bots.delete(name);
        conductorBot = null;
      }
      return { status: 'stopped' };
    }

    // === Cherry-picked from archived russfranky/hubzz-alpha (packages/bot-mcp) ===

    case 'bot_send_raw': {
      const r = getBot(args.name); if (r.error) return r;
      const h = args.h;
      if (typeof h !== 'string' || h.trim() === '') return { error: 'h (message handler name) is required' };
      const a = args.a === undefined ? [] : args.a;
      if (!Array.isArray(a)) return { error: 'a must be an array' };
      r._send({ h, a });
      return { status: 'sent', name: args.name, h, a };
    }

    case 'server_health': {
      const url = (args.apiUrl || DEFAULT_API_URL) + '/api/health';
      try {
        const { status, body } = await httpGetJson(url);
        return { httpStatus: status, ...(body && typeof body === 'object' ? body : { body }) };
      } catch (err) { return { error: err.message, url }; }
    }

    case 'server_spaces': {
      const url = (args.apiUrl || DEFAULT_API_URL) + '/api/spaces';
      try {
        const { status, body } = await httpGetJson(url);
        return { httpStatus: status, ...(body && typeof body === 'object' ? body : { body }) };
      } catch (err) { return { error: err.message, url }; }
    }

    case 'server_space_info': {
      if (args.path == null || String(args.path).trim() === '') return { error: 'path is required' };
      // Do NOT encodeURIComponent the path: the server's /api/space/* wildcard
      // splits on "/" and matches coordinates like "1,0" literally — an
      // encoded comma (%2C) 404s.
      const url = (args.apiUrl || DEFAULT_API_URL) + `/api/space/${String(args.path).trim()}`;
      try {
        const { status, body } = await httpGetJson(url);
        return { httpStatus: status, ...(body && typeof body === 'object' ? body : { body }) };
      } catch (err) { return { error: err.message, url }; }
    }

    case 'server_emotes': {
      const url = (args.apiUrl || DEFAULT_API_URL) + '/api/emotes';
      try {
        const { status, body } = await httpGetJson(url);
        const emotes = body?.emotes;
        return { httpStatus: status, count: Array.isArray(emotes) ? emotes.length : undefined, emotes: Array.isArray(emotes) ? emotes.slice(0, 50) : body };
      } catch (err) { return { error: err.message, url }; }
    }

    // === bot_sonar: the bot's eyes ===
    case 'bot_sonar': {
      const r = getBot(args.name); if (r.error) return r;
      const radius = Math.min(Math.max(Number(args.radius ?? 15) || 15, 1), 60);
      const includeAscii = args.includeAscii !== false;
      const maximumUsers = Math.min(Math.max(Number(args.maximumUsers ?? 20) || 20, 1), 100);
      const selfUsername = typeof args.selfUsername === 'string' && args.selfUsername.trim() !== ''
        ? args.selfUsername.trim() : null;
      const mapUrl = args.mapUrl || 'https://hubzz.xyz/data/maps/world_2.json';

      let map;
      try { map = await getCachedMap(mapUrl); }
      catch (e) { return { error: `Failed to load map: ${e.message}` }; }

      // Self position: prefer the true server-known position (via ownUserId),
      // fall back to the echo-corrected/optimistic ownTile.
      const ownPos = r.getOwnPosition();
      const selfTile = r.ownTile != null ? map.byId.get(Number(r.ownTile)) : null;
      const selfXZ = ownPos || (selfTile ? { x: selfTile.x, z: selfTile.z } : null);
      const selfEntry = selfUsername
        ? [...r.knownUsers.values()].find(u => u.username === selfUsername) : null;
      const ownAv = r.getOwnAvatar();
      const self = {
        username: r.username,
        serverUsername: selfEntry ? selfEntry.username : (selfUsername || null),
        connected: r.connected,
        tile: r.ownTile,
        position: selfXZ ? { x: Math.round(selfXZ.x * 100) / 100, z: Math.round(selfXZ.z * 100) / 100 } : null,
        positionSource: ownPos ? 'server' : (selfTile ? 'ownTile' : null),
        avatar: ownAv?.path ? { path: ownAv.path, collection: ownAv.collection } : null,
      };

      const note = selfXZ ? null
        : 'Bot has no known position yet. Move the bot once with bot_move so it can place itself on the map; distances and the ASCII map need a self position.';

      const usersWithPos = [...r.knownUsers.values()].filter(u => u.position && Number.isFinite(u.position.x) && Number.isFinite(u.position.z));
      const entitiesWithPos = [...r.entities.values()].filter(e => e.position && Number.isFinite(e.position.x) && Number.isFinite(e.position.z));

      const describeActor = (u, isSelf) => {
        const dx = u.position.x - self.position.x;
        const dz = u.position.z - self.position.z;
        const d = dist2(u.position.x, u.position.z, self.position.x, self.position.z);
        // Confidence decay (npc-engine convention): observations are fresh for
        // 30s, then stale. lastSeen is refreshed on w:add, w:move, and kbs.
        const agoSec = u.lastSeen ? Math.round((Date.now() - u.lastSeen) / 100) / 10 : null;
        return {
          username: u.username, userId: String(u.id),
          distance: Math.round(d * 10) / 10,
          direction: compassFromDelta(dx, dz),
          bearingDeg: Math.round(bearingFromDelta(dx, dz)),
          facing: facingFromRotation(u.rotation),
          tile: u.tile ?? null,
          position: { x: Math.round(u.position.x * 100) / 100, z: Math.round(u.position.z * 100) / 100 },
          animation: u.animation ?? null,
          avatar: u.avatarPath ? { path: u.avatarPath, collection: u.avatarCollection || null } : null,
          isBot: u.isBot === true,
          afk: u.afkState || null,
          lastSeenAgoSec: agoSec,
          stale: agoSec != null && agoSec > 30,
        };
      };

      let nearby = [];
      let entities = [];
      let openTiles = [];
      let openSeats = [];
      let ascii = null;
      let legend = null;

      if (selfXZ) {
        // The protocol never tells the bot its own server-side user id, so the
        // caller names it explicitly (e.g. the token account). Without it, the
        // bot's own entry stays in the list, flagged.
        nearby = usersWithPos
          .map(u => {
            const d = describeActor(u);
            d.isSelf = selfUsername ? u.username === selfUsername : null;
            return d;
          })
          .filter(u => u.isSelf !== true && u.distance <= radius)
          .sort((a, b) => a.distance - b.distance)
          .slice(0, maximumUsers);

        entities = entitiesWithPos
          .map(e => {
            const dx = e.position.x - self.position.x, dz = e.position.z - self.position.z;
            const d = dist2(e.position.x, e.position.z, self.position.x, self.position.z);
            return {
              type: e.type, id: String(e.id),
              distance: Math.round(d * 10) / 10,
              direction: compassFromDelta(dx, dz),
              position: { x: Math.round(e.position.x * 100) / 100, z: Math.round(e.position.z * 100) / 100 },
            };
          })
          .filter(e => e.distance <= radius)
          .sort((a, b) => a.distance - b.distance)
          .slice(0, 20);

        const occupied = (x, z) =>
          (selfXZ && dist2(selfXZ.x, selfXZ.z, x, z) < 1.5) ||
          usersWithPos.some(u => dist2(u.position.x, u.position.z, x, z) < 1.5);
        openTiles = map.walkable
          .filter(t => dist2(t.x, t.z, self.position.x, self.position.z) <= radius && !occupied(t.x, t.z))
          .map(t => {
            const dx = t.x - self.position.x, dz = t.z - self.position.z;
            return {
              tile: Number(t.id), x: t.x, z: t.z,
              distance: Math.round(dist2(t.x, t.z, self.position.x, self.position.z) * 10) / 10,
              direction: compassFromDelta(dx, dz),
            };
          })
          .sort((a, b) => a.distance - b.distance)
          .slice(0, 12);

        // Real seats: chair objects from the map whose tile is not occupied.
        openSeats = (map.seats || [])
          .map(s => {
            const t = map.byId.get(Number(s.tile));
            if (!t) return null;
            const dx = t.x - self.position.x, dz = t.z - self.position.z;
            return {
              name: s.name, type: s.type, tile: Number(s.tile), x: t.x, z: t.z,
              distance: Math.round(dist2(t.x, t.z, self.position.x, self.position.z) * 10) / 10,
              direction: compassFromDelta(dx, dz),
            };
          })
          .filter(s => s && s.distance <= radius && !occupied(s.x, s.z))
          .sort((a, b) => a.distance - b.distance)
          .slice(0, 12);

        if (includeAscii) {
          // Top-down map, 1 char per 2x2-unit cell (the tile spacing), centered on self.
          const half = Math.min(Math.ceil(radius / 2), 20);
          const cellOf = new Map();
          for (const t of map.tiles) {
            const gx = Math.round(t.x / 2), gz = Math.round(t.z / 2);
            cellOf.set(`${gx},${gz}`, t.walkable ? '.' : '#');
          }
          const cgx = Math.round(self.position.x / 2), cgz = Math.round(self.position.z / 2);
          const grid = [];
          for (let gz = cgz - half; gz <= cgz + half; gz++) {
            let row = '';
            for (let gx = cgx - half; gx <= cgx + half; gx++) {
              row += cellOf.get(`${gx},${gz}`) || ' ';
            }
            grid.push(row);
          }
          const plot = (x, z, ch) => {
            const gx = Math.round(x / 2) - (cgx - half);
            const gz = Math.round(z / 2) - (cgz - half);
            if (gz >= 0 && gz < grid.length && gx >= 0 && gx < grid[gz].length) {
              grid[gz] = grid[gz].slice(0, gx) + ch + grid[gz].slice(gx + 1);
            }
          };
          const letters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ';
          legend = { '@': `you (${r.username})` };
          nearby.forEach((u, i) => {
            const ch = letters[i % letters.length];
            plot(u.position.x, u.position.z, ch);
            legend[ch] = `${u.username} (${u.distance}u ${u.direction})`;
            u.marker = ch;
          });
          entities.forEach(e => plot(e.position.x, e.position.z, '*'));
          legend['*'] = 'entity';
          plot(self.position.x, self.position.z, '@');
          ascii = grid.join('\n');
        }
      }

      const recentChat = r.chatBuffer.slice(-10).map(m => ({
        username: m.username, message: m.message,
        agoSec: Math.round((Date.now() - m.timestamp) / 1000),
      }));

      // Plain-language scene summary.
      let summary;
      if (!selfXZ) {
        summary = `${r.username} is connected but has no known position yet. Move it once with bot_move so sonar can place it on the map.`;
      } else {
        const atTile = self.tile != null ? `tile ${self.tile} ` : '';
        const parts = [`${r.username} is at ${atTile}(${self.position.x}, ${self.position.z}).`];
        if (nearby.length === 0) parts.push(`No other users within ${radius} units.`);
        else {
          const names = nearby.slice(0, 5).map(u => `${u.username} ${u.distance}u ${u.direction}`).join(', ');
          parts.push(`${nearby.length} user${nearby.length === 1 ? '' : 's'} nearby: ${names}${nearby.length > 5 ? ', …' : ''}.`);
        }
        if (entities.length > 0) parts.push(`${entities.length} entit${entities.length === 1 ? 'y' : 'ies'} in range (${entities.slice(0, 3).map(e => e.type).join(', ')}).`);
        if (openTiles.length > 0) parts.push(`Nearest open tile: ${openTiles[0].tile} (${openTiles[0].distance}u ${openTiles[0].direction}).`);
        if (openSeats.length > 0) parts.push(`Nearest open seat: ${openSeats[0].name} (tile ${openSeats[0].tile}, ${openSeats[0].distance}u ${openSeats[0].direction}).`);
        if (recentChat.length > 0) {
          const last = recentChat[recentChat.length - 1];
          parts.push(`Last chat ${last.agoSec}s ago from ${last.username}: "${String(last.message).slice(0, 80)}".`);
        }
        summary = parts.join(' ');
      }

      return { self, nearby, entities, openTiles, openSeats, recentChat, ascii, legend, summary, note };
    }

    // === bot_navigate: closed-loop server-pathfind walk ===
    case 'bot_navigate': {
      // Closed-loop navigation. The server pathfinds from a single w:move and
      // streams per-step w:move echoes back, ending with st:true (arrival).
      // We send ONE move and wait for that arrival echo — no time estimates.
      const r = getBot(args.name); if (r.error) return r;
      const mapUrl = args.mapUrl || 'https://hubzz.xyz/data/maps/world_2.json';
      const arriveRadius = Number(args.arriveRadius ?? 3);
      const timeoutMs = Math.min(Math.max(Number(args.timeoutMs ?? 90000) || 90000, 5000), 300000);
      if (!Number.isFinite(arriveRadius) || arriveRadius < 0) return { error: 'arriveRadius must be a non-negative number' };

      let map;
      try { map = await getCachedMap(mapUrl); }
      catch (e) { return { error: `Failed to load map: ${e.message}` }; }

      const hasTileId = args.tileId != null;
      const hasUser = typeof args.username === 'string' && args.username.trim() !== '';
      const hasXZ = args.x != null && args.z != null;
      if ([hasTileId, hasUser, hasXZ].filter(Boolean).length !== 1) {
        return { error: 'Provide exactly one destination: tileId, username, or x+z' };
      }

      let targetTile, targetLabel;
      // Tiles occupied by other users reject moves (server sends no echoes),
      // so destination resolution avoids them.
      const occupiedByOther = (x, z) => [...r.knownUsers.values()].some(u => {
        if (r.ownUserId && String(u.id) === String(r.ownUserId)) return false;
        return u.position && dist2(u.position.x, u.position.z, x, z) < 2.25;
      });
      const nearestFree = (x, z) => {
        let best = null, bd = Infinity;
        for (const t of map.walkable) {
          if (occupiedByOther(t.x, t.z)) continue;
          const d = dist2(t.x, t.z, x, z);
          if (d < bd) { bd = d; best = t; }
        }
        return best;
      };
      if (hasTileId) {
        const t = map.byId.get(Number(args.tileId));
        if (!t) return { error: `Tile ${args.tileId} not found in map` };
        if (!t.walkable) {
          const near = nearestFree(t.x, t.z);
          if (!near) return { error: `Tile ${args.tileId} is not walkable and no free walkable tile is nearby` };
          targetTile = near; targetLabel = `tile ${args.tileId} (blocked; retargeted to walkable ${near.id})`;
        } else if (occupiedByOther(t.x, t.z)) {
          const near = nearestFree(t.x, t.z);
          if (!near) return { error: `Tile ${args.tileId} is occupied by another user and no free tile is nearby` };
          targetTile = near; targetLabel = `tile ${args.tileId} (occupied; retargeted to ${near.id})`;
        } else { targetTile = t; targetLabel = `tile ${args.tileId}`; }
      } else if (hasUser) {
        const u = [...r.knownUsers.values()].find(u => u.username === args.username || String(u.id) === String(args.username));
        if (!u || !u.position) return { error: `User "${args.username}" not visible` };
        const t = nearestFree(u.position.x, u.position.z);
        if (!t) return { error: 'No free walkable tile near user' };
        targetTile = t; targetLabel = `user ${args.username} (walkable tile ${t.id})`;
      } else {
        const t = nearestFree(Number(args.x), Number(args.z));
        if (!t) return { error: 'No free walkable tile near x,z' };
        targetTile = t; targetLabel = `position (${args.x}, ${args.z}) (walkable tile ${t.id})`;
      }

      const t0 = Date.now();
      // Already there? Prefer the server-known own position (via serverUsername
      // hint); fall back to the echo-corrected ownTile.
      const ownPos = r.getOwnPosition();
      const alreadyThere = (() => {
        if (ownPos) return dist2(ownPos.x, ownPos.z, targetTile.x, targetTile.z) <= arriveRadius;
        if (r.ownTile != null) {
          const cur = map.byId.get(Number(r.ownTile));
          if (cur) return dist2(cur.x, cur.z, targetTile.x, targetTile.z) <= arriveRadius;
        }
        return false;
      })();
      if (alreadyThere) {
        const d0 = ownPos
          ? dist2(ownPos.x, ownPos.z, targetTile.x, targetTile.z)
          : dist2(map.byId.get(Number(r.ownTile)).x, map.byId.get(Number(r.ownTile)).z, targetTile.x, targetTile.z);
        return {
          reached: true, reason: 'already_there', target: targetLabel,
          seconds: 0, finalTile: r.ownTile == null ? null : Number(r.ownTile), goalTile: Number(targetTile.id),
          finalDistance: Math.round(d0 * 10) / 10,
        };
      }
      // One move; the server walks the path and echoes arrival (st:true).
      let arrival;
      try {
        arrival = await r.moveToTileAndWait(Number(targetTile.id), timeoutMs, 5000, args.boost === true);
      } catch (e) {
        const cur = r.ownTile != null ? map.byId.get(Number(r.ownTile)) : null;
        const d = cur ? dist2(cur.x, cur.z, targetTile.x, targetTile.z) : null;
        return {
          reached: false, reason: 'timeout', target: targetLabel,
          seconds: Math.round((Date.now() - t0) / 100) / 10,
          finalTile: r.ownTile == null ? null : Number(r.ownTile), goalTile: Number(targetTile.id),
          finalDistance: d == null ? null : Math.round(d * 10) / 10,
          detail: e.message,
        };
      }
      if (!arrival.moved) {
        // Server sent no step echoes (already there / rejected) or the
        // explicit no-path signal (st:true for the current tile).
        const d = ownPos ? dist2(ownPos.x, ownPos.z, targetTile.x, targetTile.z) : null;
        const already = d != null && d <= arriveRadius;
        return {
          reached: already,
          reason: already ? 'already_there' : (arrival.reason === 'no_path' ? 'no_path' : 'no_movement'),
          target: targetLabel,
          seconds: Math.round(arrival.ms / 100) / 10,
          finalTile: r.ownTile == null ? null : Number(r.ownTile), goalTile: Number(targetTile.id),
          finalDistance: d == null ? null : Math.round(d * 10) / 10,
          detail: arrival.reason === 'no_path'
            ? 'server could not pathfind to the target (st:true for current tile)'
            : 'no step echoes from server within 5s of the move',
        };
      }
      const final = map.byId.get(Number(arrival.tileId));
      const d = final ? dist2(final.x, final.z, targetTile.x, targetTile.z) : null;
      return {
        reached: d == null ? true : d <= arriveRadius,
        reason: 'arrived', target: targetLabel,
        seconds: Math.round(arrival.ms / 100) / 10,
        finalTile: Number(arrival.tileId), goalTile: Number(targetTile.id),
        finalDistance: d == null ? null : Math.round(d * 10) / 10,
      };
    }

    default:
      return { error: `Unknown tool: ${name}` };
  }
}

// --- MCP Request Handler ---

async function handleRequest(request) {
  const { id, method, params } = request;

  switch (method) {
    case 'initialize':
      sendResponse(id, {
        protocolVersion: '2024-11-05',
        capabilities: { tools: {} },
        serverInfo: { name: 'hubzz-bot-mcp', version: '2.0.0' },
      });
      break;

    case 'notifications/initialized':
      break;

    case 'tools/list':
      sendResponse(id, { tools: TOOLS });
      break;

    case 'tools/call': {
      const { name, arguments: args } = params;
      try {
        const result = await handleTool(name, args || {});
        sendResponse(id, {
          content: [{ type: 'text', text: JSON.stringify(result, null, 2) }],
        });
      } catch (err) {
        sendResponse(id, {
          content: [{ type: 'text', text: JSON.stringify({ error: err.message }) }],
          isError: true,
        });
      }
      break;
    }

    default:
      if (id) sendError(id, -32601, `Method not found: ${method}`);
      break;
  }
}

// --- stdio Message Parser (newline-delimited JSON) ---
// Only start the MCP server loop when run directly (node bot-mcp.mjs), not
// when imported for unit tests.

let buffer = '';

if (isMainModule) {
process.stdin.setEncoding('utf8');
process.stdin.on('data', (chunk) => {
  buffer += chunk;

  let newlineIdx;
  while ((newlineIdx = buffer.indexOf('\n')) !== -1) {
    const line = buffer.slice(0, newlineIdx).trim();
    buffer = buffer.slice(newlineIdx + 1);

    if (!line) continue;

    try {
      const request = JSON.parse(line);
      handleRequest(request).catch(err => {
        console.error('Error handling request:', err);
        if (request.id) sendError(request.id, -32603, err.message);
      });
    } catch (err) {
      console.error('Failed to parse JSON-RPC:', err);
    }
  }
});
} // end isMainModule

// Cleanup on exit
process.on('SIGINT', () => {
  for (const [, bot] of bots) bot.close();
  process.exit(0);
});

process.on('SIGTERM', () => {
  for (const [, bot] of bots) bot.close();
  process.exit(0);
});

// Keep alive
if (isMainModule) process.stdin.resume();
