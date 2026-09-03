#!/usr/bin/env node

import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';

if (process.env.BOT_MCP_FIXTURE_CHILD === '1') {
  let buffer = Buffer.alloc(0);
  process.stdin.on('data', chunk => {
    buffer = Buffer.concat([buffer, chunk]);
    while (true) {
      const headerEnd = buffer.indexOf('\r\n\r\n');
      if (headerEnd === -1) break;
      const header = buffer.subarray(0, headerEnd).toString('ascii');
      const contentLength = Number(header.match(/Content-Length:\s*(\d+)/i)?.[1]);
      const bodyStart = headerEnd + 4;
      if (!Number.isInteger(contentLength) || buffer.length < bodyStart + contentLength) break;
      const request = JSON.parse(buffer.subarray(bodyStart, bodyStart + contentLength).toString('utf8'));
      buffer = buffer.subarray(bodyStart + contentLength);
      if (request.id == null) continue;
      const response = JSON.stringify({
        jsonrpc: '2.0',
        id: request.id,
        result: {
          protocolVersion: '2024-11-05',
          capabilities: {},
          serverInfo: { name: 'bot-mcp-http-test-child', version: '1' },
        },
      });
      process.stdout.write(`Content-Length: ${Buffer.byteLength(response)}\r\n\r\n${response}`);
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
    console.log('bot-mcp HTTP wrapper canonical framing: PASS');
  } finally {
    controller.abort();
    wrapper.kill('SIGTERM');
  }
}
