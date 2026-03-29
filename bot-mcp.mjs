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

const DEFAULT_WS_URL = process.env.HUBZZ_WS_URL || 'wss://hubzz.xyz/socket/';
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

function fetchJson(url) {
  return new Promise((resolve, reject) => {
    https.get(url, res => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => { try { resolve(JSON.parse(data)); } catch (e) { reject(e); } });
    }).on('error', reject);
  });
}

// Exponential falloff gain: (refDistance / max(refDistance, d))^rolloffFactor
function spatialGain(d, refDistance = 1, rolloffFactor = 0.75) {
  if (d <= 0) return 1;
  return Math.pow(refDistance / Math.max(refDistance, d), rolloffFactor);
}

// --- Bot Connection ---

class BotConnection extends EventEmitter {
  constructor(wsUrl, username, vrmUrl = '', opts = {}) {
    super();
    this.wsUrl = wsUrl;
    this.username = username;
    this.vrmUrl = vrmUrl;
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

    // Keepalive ping (nginx default timeout is 60s)
    this.keepaliveTimer = null;
  }

  connect() {
    return new Promise((resolve, reject) => {
      this.intentionallyClosed = false;

      try {
        this.ws = new WebSocket(this.wsUrl);
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
        this._send({ h: 'login', a: ['iamar0b0t', this.username, this.vrmUrl] });
      });

      this.ws.on('message', (data) => {
        const raw = data.toString();
        const parts = raw.split(DELIMITER).filter(m => m.length > 0);
        for (const part of parts) {
          try {
            const msg = JSON.parse(part);
            this.messageCount.received++;
            this._handleMessage(msg, resolve, reject);
          } catch (_) { /* skip unparseable */ }
        }
      });

      this.ws.on('close', () => {
        clearTimeout(this.connectionTimeout);
        const wasConnected = this.connected;
        this.connected = false;
        if (wasConnected) this.disconnectCount++;
        this._bufferEvent('disconnect', { wasConnected, intentional: this.intentionallyClosed });
        this.emit('disconnected');

        // Auto-reconnect
        if (!this.intentionallyClosed && this.autoReconnect && this.reconnectAttempts < this.maxReconnectAttempts) {
          this.reconnectAttempts++;
          const delay = Math.min(1000 * Math.pow(2, this.reconnectAttempts), 30000);
          setTimeout(() => this.connect().catch(() => {}), delay);
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
        if (resolve) resolve();
        break;

      case 'acc:fail':
        clearTimeout(this.connectionTimeout);
        this._trackError('auth', JSON.stringify(msg.a));
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
        });
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
        const [userId, tileId] = msg.a;
        const user = this.knownUsers.get(String(userId));
        if (user) user.tile = Number(tileId);
        this._bufferEvent('w:move', { userId, tileId });
        break;
      }

      case 'chat': {
        const [userId, message] = msg.a;
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
        const [target, func, ...callArgs] = msg.a || [];
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
    if (this.eventSubscriptions.has('*') || this.eventSubscriptions.has(type)) {
      this.eventBuffer.push({ type, data, timestamp: Date.now() });
      if (this.eventBuffer.length > MAX_EVENT_BUFFER) this.eventBuffer.shift();
    }
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
    }
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

  moveToTile(tileId) { this._send({ h: 'w:move', a: [tileId] }); }
  sendChat(message) { this._send({ h: 'chat', a: [message] }); }
  sendEmote(animation) { this._send({ h: 'emote', a: [animation, false] }); }
  sendRotation(x, y, z) { this._send({ h: 'w:rot', a: [{ x, y, z }] }); }
  sendLookAt(x, y, z) { this._send({ h: 'w:lookAt', a: [{ x, y, z }] }); }
  sendTypingStatus(isTyping) { this._send({ h: 'ts', a: [isTyping] }); }
  sendSetAvatar(asset) { this._send({ h: 'setAvatar', a: [asset] }); }

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
    this.stopPatrol();
    this._stopKeepalive();
    clearTimeout(this.connectionTimeout);
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
      this.socket.on('connect', resolve);
      this.socket.on('connect_error', e => reject(new Error(`Socket.IO connect error: ${e.message}`)));
      setTimeout(() => reject(new Error('Socket.IO connection timed out')), 8000);
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
    const track = this.audioSource.createTrack();
    this.producer = await this.sendTransport.produce({ track });
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
    if (this.socket) { this.socket.disconnect(); this.socket = null; }
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
        autoReconnect: { type: 'boolean', description: 'Enable auto-reconnect on disconnect (default: false)' },
      },
      required: ['name'],
    },
  },
  {
    name: 'bot_move',
    description: 'Move a bot to a specific tile in the world.',
    inputSchema: {
      type: 'object',
      properties: {
        name: { type: 'string', description: 'Bot name' },
        tileId: { type: 'number', description: 'Tile ID to move to' },
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
    description: 'Change a bot\'s avatar via setAvatar protocol or !shuffle chat command.',
    inputSchema: {
      type: 'object',
      properties: {
        name: { type: 'string', description: 'Bot name' },
        method: { type: 'string', enum: ['vrm', 'shuffle'], description: 'vrm = set VRM URL, shuffle = random avatar' },
        vrmUrl: { type: 'string', description: 'VRM URL (required if method is vrm)' },
        collection: { type: 'string', description: 'Collection slug for shuffle (optional)' },
      },
      required: ['name', 'method'],
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
];

// --- Tool Handlers ---

async function handleTool(name, args) {
  switch (name) {

    // === Original tools ===

    case 'bot_spawn': {
      const botName = args.name;
      if (bots.has(botName)) return { error: `Bot "${botName}" already exists. Close it first or use a different name.` };
      const wsUrl = args.wsUrl || DEFAULT_WS_URL;
      const bot = new BotConnection(wsUrl, botName, args.vrmUrl || '', { autoReconnect: args.autoReconnect || false });
      try {
        await bot.connect();
        bots.set(botName, bot);
        await sleep(1000);
        return { status: 'connected', name: botName, wsUrl, usersInWorld: bot.knownUsers.size };
      } catch (err) {
        bot.close();
        return { error: `Failed to spawn bot: ${err.message}` };
      }
    }

    case 'bot_move': {
      const r = getBot(args.name); if (r.error) return r;
      r.moveToTile(args.tileId);
      r.ownTile = args.tileId;
      return { status: 'moved', name: args.name, tileId: args.tileId };
    }

    case 'bot_chat': {
      const r = getBot(args.name); if (r.error) return r;
      r.sendChat(args.message);
      return { status: 'sent', name: args.name, message: args.message };
    }

    case 'bot_emote': {
      const r = getBot(args.name); if (r.error) return r;
      r.sendEmote(args.animation);
      return { status: 'emote_sent', name: args.name, animation: args.animation };
    }

    case 'bot_voice': {
      const r = getBot(args.name); if (r.error) return r;
      const state = args.state ?? args.voiceState ?? true;
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
      bots.clear();
      return { status: 'all_closed', closed: names };
    }

    // === New tools ===

    case 'bot_batch_spawn': {
      const count = Math.min(Math.max(1, args.count || 1), MAX_BATCH_SIZE);
      const staggerMs = args.staggerMs ?? 500;
      const wsUrl = args.wsUrl || DEFAULT_WS_URL;
      const results = [];
      const startTime = Date.now();

      for (let i = 0; i < count; i++) {
        const botName = `${args.prefix}-${i}`;
        if (bots.has(botName)) {
          results.push({ name: botName, status: 'skipped', error: 'already exists' });
          continue;
        }
        const bot = new BotConnection(wsUrl, botName, args.vrmUrl || '', { autoReconnect: args.autoReconnect || false });
        try {
          await bot.connect();
          bots.set(botName, bot);
          results.push({ name: botName, status: 'connected' });
        } catch (err) {
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
      if (!args.tiles || args.tiles.length === 0) return { error: 'tiles array is required for start action' };
      r.startPatrol(args.tiles, args.intervalMs || 2000, args.loop !== false);
      return { status: 'patrolling', name: args.name, tiles: args.tiles.length, intervalMs: args.intervalMs || 2000, loop: args.loop !== false };
    }

    case 'bot_stress_test': {
      const count = Math.min(Math.max(1, args.count || 1), MAX_BATCH_SIZE);
      const durationSec = Math.min(Math.max(1, args.durationSec || 10), MAX_STRESS_DURATION);
      const mps = Math.min(Math.max(0.1, args.messagesPerSec || 1), 5);
      const wsUrl = args.wsUrl || DEFAULT_WS_URL;
      const test = args.test;
      const testBots = [];
      const metrics = { botsSpawned: 0, botsFailed: 0, messagesAttempted: 0, messagesFailed: 0, droppedConnections: 0, errors: [] };

      // Spawn
      const spawnStart = Date.now();
      for (let i = 0; i < count; i++) {
        const botName = `_stress_${args.prefix}-${i}`;
        if (bots.has(botName)) { bots.get(botName).close(); bots.delete(botName); }
        const bot = new BotConnection(wsUrl, botName, '', {});
        try {
          await bot.connect();
          bots.set(botName, bot);
          testBots.push(bot);
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
        for (const bot of testBots) { bot.close(); bots.delete(`_stress_${args.prefix}-${testBots.indexOf(bot)}`); }
        return { test: 'connect', ...metrics, spawnTimeMs, avgSpawnMs: metrics.botsSpawned > 0 ? Math.round(spawnTimeMs / metrics.botsSpawned) : null };
      }

      // Wait for world state
      await sleep(1000);

      // Run test
      const intervalMs = Math.round(1000 / mps);
      const timers = [];
      const testStart = Date.now();

      for (const bot of testBots) {
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
      const latencies = testBots.filter(b => b.pingLatencies.length > 0).map(b => b.pingLatencies.reduce((a, c) => a + c, 0) / b.pingLatencies.length);
      const avgLatency = latencies.length > 0 ? Math.round(latencies.reduce((a, c) => a + c, 0) / latencies.length) : null;

      // Close test bots
      for (let i = 0; i < testBots.length; i++) {
        const botName = `_stress_${args.prefix}-${i}`;
        testBots[i].close();
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
      r.sendSetAvatar(args.vrmUrl);
      return { status: 'avatar_set', name: args.name, vrmUrl: args.vrmUrl };
    }

    case 'bot_nick': {
      const r = getBot(args.name); if (r.error) return r;
      r.sendChat(`!nick ${args.newNick}`);
      return { status: 'nick_sent', name: args.name, newNick: args.newNick };
    }

    case 'bot_dance': {
      const r = getBot(args.name); if (r.error) return r;
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
      const wsUrl = args.wsUrl || DEFAULT_WS_URL;
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
      if (rtcSessions.has(botName)) return { error: `Bot "${botName}" already has an active audio session. Use bot_audio_stop first.` };

      // Derive room_id from wsUrl: wss://hubzz.xyz/socket/0,0/ → hubzz.xyz@0,0
      const wsUrl = args.wsUrl || DEFAULT_WS_URL;
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

    case 'bot_spawn_at': {
      const wsUrl = args.wsUrl || DEFAULT_WS_URL;
      const bot = new BotConnection(wsUrl, args.name, args.vrmUrl || '', {});
      try {
        await bot.connect();
        bots.set(args.name, bot);
        await sleep(300);
        bot.moveToTile(args.tileId);
        bot.ownTile = args.tileId;
        if (args.voiceOn) {
          await sleep(200);
          bot._send({ h: 'voiceState', a: [true] });
        }
        return { status: 'ready', name: args.name, tileId: args.tileId, voiceOn: !!args.voiceOn };
      } catch (err) {
        bot.close();
        bots.delete(args.name);
        return { error: err.message };
      }
    }

    case 'bot_directional_ring': {
      const wsUrl = args.wsUrl || DEFAULT_WS_URL;
      const mapUrl = args.mapUrl || 'https://hubzz.xyz/data/maps/world_2.json';
      const distance = args.distance ?? 10;
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
            const session = new BotRTCSession(botName, roomId, `https://${urlObj.hostname}`, freq, gain);
            await session.start(freq, gain);
            rtcSessions.set(botName, session);
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
      const wsUrl = args.wsUrl || DEFAULT_WS_URL;

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
          const session = new BotRTCSession(cfg.name, roomId, voiceServerBase, cfg.freq, cfg.gain);
          await session.start(cfg.freq, cfg.gain);
          rtcSessions.set(cfg.name, session);
          setupResults.push({ name: cfg.name, tileId: cfg.tileId, freq: cfg.freq, status: 'ready' });
        } catch (err) {
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
      const wsUrl = args.wsUrl || 'wss://hubzz.xyz/socket/0,0/';
      const username = args.username || 'conductor';

      // Stop any existing conductor
      if (conductorInterval) {
        clearInterval(conductorInterval);
        conductorInterval = null;
      }
      if (conductorBot) {
        conductorBot.close();
        conductorBot = null;
      }

      // Reset config to defaults
      conductorConfig = {
        firstPerson: { refDistance: 1, rolloffFactor: 0.75, distanceModel: 'exponential', volume: 1.0 },
        isometric: { volume: 1.0 },
      };

      const bot = new BotConnection(wsUrl, username, '', { autoReconnect: true });
      await bot.connect();
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

let buffer = '';

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
process.stdin.resume();
