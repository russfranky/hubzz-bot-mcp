#!/usr/bin/env node
/**
 * bot-mcp-http.mjs
 *
 * HTTP/SSE transport wrapper for bot-mcp.mjs.
 * Allows remote MCP clients (e.g. Jules) to use bot-mcp over the network.
 *
 * Each SSE connection spawns an isolated bot-mcp child process.
 * Messages POST to /message?sessionId=X, responses stream back via SSE.
 *
 * Usage:
 *   BOT_MCP_PORT=37778 BOT_MCP_TOKEN=secret node bot-mcp-http.mjs
 */

import http from 'http';
import { spawn } from 'child_process';
import { fileURLToPath } from 'url';
import { dirname, join, resolve } from 'path';

const __dir = dirname(fileURLToPath(import.meta.url));
const PORT = parseInt(process.env.BOT_MCP_PORT || '37778', 10);
const TOKEN = process.env.BOT_MCP_TOKEN || '';
const CHILD_PATH = resolve(process.env.BOT_MCP_CHILD_PATH || join(__dir, 'bot-mcp.mjs'));

/** sessionId → { child, res } */
const sessions = new Map();

function auth(req, url) {
  if (!TOKEN) return true;
  const t = url.searchParams.get('token') || req.headers['x-api-token'];
  return t === TOKEN;
}

const server = http.createServer((req, res) => {
  const url = new URL(req.url, `http://localhost`);

  // CORS preflight
  if (req.method === 'OPTIONS') {
    res.writeHead(204, {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type, x-api-token',
    });
    res.end();
    return;
  }

  if (!auth(req, url)) {
    res.writeHead(401, { 'Content-Type': 'text/plain' });
    res.end('Unauthorized');
    return;
  }

  // --- SSE connection ---
  if (req.method === 'GET' && url.pathname === '/sse') {
    const sessionId = Math.random().toString(36).slice(2, 10);

    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      'Connection': 'keep-alive',
      'Access-Control-Allow-Origin': '*',
    });

    // MCP SSE protocol: first event tells client where to POST
    const postPath = TOKEN
      ? `/message?sessionId=${sessionId}&token=${TOKEN}`
      : `/message?sessionId=${sessionId}`;
    res.write(`event: endpoint\ndata: ${postPath}\n\n`);

    // Spawn isolated bot-mcp child
    const child = spawn(process.execPath, [CHILD_PATH], {
      cwd: dirname(CHILD_PATH),
      stdio: ['pipe', 'pipe', 'inherit'],
      env: { ...process.env },
    });

    sessions.set(sessionId, { child, res });
    console.error(`[http] session ${sessionId} opened (pid ${child.pid})`);

    // Forward child stdout → SSE data events
    let buf = '';
    child.stdout.setEncoding('utf8');
    child.stdout.on('data', chunk => {
      buf += chunk;
      let i;
      while ((i = buf.indexOf('\n')) !== -1) {
        const line = buf.slice(0, i).trim();
        buf = buf.slice(i + 1);
        if (line) {
          res.write(`data: ${line}\n\n`);
        }
      }
    });

    child.on('exit', (code) => {
      console.error(`[http] session ${sessionId} child exited (code ${code})`);
      sessions.delete(sessionId);
      try { res.end(); } catch {}
    });

    req.on('close', () => {
      console.error(`[http] session ${sessionId} client disconnected`);
      sessions.delete(sessionId);
      try { child.kill(); } catch {}
    });

    return;
  }

  // --- POST message ---
  if (req.method === 'POST' && url.pathname === '/message') {
    const sessionId = url.searchParams.get('sessionId');
    const session = sessions.get(sessionId);

    if (!session) {
      res.writeHead(404, { 'Content-Type': 'text/plain' });
      res.end('Session not found');
      return;
    }

    let body = '';
    req.on('data', d => body += d);
    req.on('end', () => {
      try {
        session.child.stdin.write(body.trim() + '\n');
        res.writeHead(202);
        res.end('Accepted');
      } catch (e) {
        res.writeHead(500, { 'Content-Type': 'text/plain' });
        res.end(e.message);
      }
    });

    return;
  }

  // --- Health check ---
  if (req.method === 'GET' && url.pathname === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'ok', sessions: sessions.size }));
    return;
  }

  res.writeHead(404);
  res.end('Not found');
});

server.listen(PORT, '127.0.0.1', () => {
  console.error(`[bot-mcp-http] listening on 127.0.0.1:${PORT}`);
});
