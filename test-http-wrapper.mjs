#!/usr/bin/env node

import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';

if (process.env.BOT_MCP_FIXTURE_CHILD === '1') {
  let buffer = '';
  process.stdin.setEncoding('utf8');
  process.stdin.on('data', chunk => {
    buffer += chunk;
    let newline;
    while ((newline = buffer.indexOf('\n')) !== -1) {
      const line = buffer.slice(0, newline).trim();
      buffer = buffer.slice(newline + 1);
      if (!line) continue;
      const request = JSON.parse(line);
      if (request.id == null) continue;
      process.stdout.write(JSON.stringify({
        jsonrpc: '2.0',
        id: request.id,
        result: {
          protocolVersion: '2024-11-05',
          capabilities: {},
          serverInfo: { name: 'bot-mcp-http-test-child', version: '1' },
        },
      }) + '\n');
    }
  });
} else {
  const port = 38_000 + (process.pid % 1_000);
  const token = 'wrapper-test-token';
  const controller = new AbortController();
  const wrapper = spawn(process.execPath, ['bot-mcp-http.mjs'], {
    cwd: fileURLToPath(new URL('.', import.meta.url)),
    env: {
      ...process.env,
      BOT_MCP_PORT: String(port),
      BOT_MCP_TOKEN: token,
      BOT_MCP_CHILD_PATH: fileURLToPath(import.meta.url),
      BOT_MCP_FIXTURE_CHILD: '1',
    },
    stdio: ['ignore', 'ignore', 'pipe'],
  });

  const stderr = [];
  wrapper.stderr.setEncoding('utf8');
  wrapper.stderr.on('data', chunk => stderr.push(chunk));

  try {
    const deadline = Date.now() + 5_000;
    while (Date.now() < deadline) {
      try {
        const health = await fetch(`http://127.0.0.1:${port}/health`, {
          headers: { 'x-api-token': token },
        });
        if (health.ok) break;
      } catch {}
      await new Promise(resolve => setTimeout(resolve, 50));
    }

    const sse = await fetch(`http://127.0.0.1:${port}/sse`, {
      headers: { 'x-api-token': token },
      signal: controller.signal,
    });
    assert.equal(sse.status, 200);

    const reader = sse.body.getReader();
    const decoder = new TextDecoder();
    let stream = '';
    let endpoint = '';
    const responseDeadline = Date.now() + 5_000;
    while (!endpoint && Date.now() < responseDeadline) {
      const { value, done } = await reader.read();
      if (done) break;
      stream += decoder.decode(value, { stream: true });
      endpoint = stream.match(/event: endpoint\ndata: ([^\n]+)/)?.[1] ?? '';
    }
    assert.ok(endpoint, 'SSE transport did not provide a message endpoint');

    const post = await fetch(`http://127.0.0.1:${port}${endpoint}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: 1,
        method: 'initialize',
        params: {
          protocolVersion: '2024-11-05',
          capabilities: {},
          clientInfo: { name: 'wrapper-test', version: '1' },
        },
      }),
    });
    assert.equal(post.status, 202);

    let response;
    while (!response && Date.now() < responseDeadline) {
      const { value, done } = await reader.read();
      if (done) break;
      stream += decoder.decode(value, { stream: true });
      for (const line of stream.split('\n')) {
        if (!line.startsWith('data: ')) continue;
        try {
          const message = JSON.parse(line.slice(6));
          if (message.id === 1) response = message;
        } catch {}
      }
    }

    assert.equal(response?.result?.serverInfo?.name, 'bot-mcp-http-test-child');
    console.log('bot-mcp HTTP wrapper child override: PASS');
  } finally {
    controller.abort();
    wrapper.kill('SIGTERM');
  }
}
