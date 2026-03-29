#!/usr/bin/env node
/**
 * Launches the spatial audio test suite:
 * - 4 audio bots at 5u, 10u, 20u, 30u from center
 * - 1 conductor bot listening to chat for !ref, !rolloff, !vol, etc.
 */

import { spawn } from 'child_process';
import { createInterface } from 'readline';

const MCP_PATH = new URL('./bot-mcp.mjs', import.meta.url).pathname;

let msgId = 1;
const pending = new Map();
let buffer = '';

const proc = spawn('node', [MCP_PATH], { stdio: ['pipe', 'pipe', 'inherit'] });

const rl = createInterface({ input: proc.stdout });
rl.on('line', (line) => {
  try {
    const msg = JSON.parse(line);
    if (msg.id && pending.has(msg.id)) {
      const { resolve, reject } = pending.get(msg.id);
      pending.delete(msg.id);
      if (msg.error) reject(new Error(msg.error.message));
      else resolve(msg.result);
    }
  } catch (_) {}
});

function call(method, params) {
  return new Promise((resolve, reject) => {
    const id = msgId++;
    pending.set(id, { resolve, reject });
    proc.stdin.write(JSON.stringify({ jsonrpc: '2.0', id, method, params }) + '\n');
    setTimeout(() => {
      if (pending.has(id)) {
        pending.delete(id);
        reject(new Error(`Timeout: ${method} ${JSON.stringify(params)}`));
      }
    }, 30000);
  });
}

function tool(name, args) {
  return call('tools/call', { name, arguments: args });
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function main() {
  // Initialize MCP
  await call('initialize', { protocolVersion: '2024-11-05', capabilities: {}, clientInfo: { name: 'launcher' } });
  console.log('MCP initialized');

  // Audio bots: tile, frequency, gain, label
  const audioBots = [
    { name: 'audio-5u',  tile: 1950, freq: 220, gain: 0.6, label: '5u'  },
    { name: 'audio-10u', tile: 1763, freq: 330, gain: 0.6, label: '10u' },
    { name: 'audio-20u', tile: 1498, freq: 440, gain: 0.6, label: '20u' },
    { name: 'audio-30u', tile: 1257, freq: 550, gain: 0.6, label: '30u' },
  ];

  const WS = 'wss://hubzz.xyz/socket/0,0/';
  const VRM = 'https://cdn.glitch.global/b5f5d7c7-9a2b-4e3a-b6c1-4a5e8f3d2b7a/robot.vrm';

  // Spawn and position audio bots
  for (const bot of audioBots) {
    try {
      console.log(`Spawning ${bot.name}...`);
      const spawnRes = await tool('bot_spawn', { name: bot.name, wsUrl: WS, vrmUrl: VRM });
      const r1 = JSON.parse(spawnRes.content[0].text);
      console.log(`  spawn: ${r1.status}`);

      await sleep(1000);

      const moveRes = await tool('bot_move', { name: bot.name, tileId: bot.tile });
      const r2 = JSON.parse(moveRes.content[0].text);
      console.log(`  move to tile ${bot.tile}: ${r2.status}`);

      await sleep(500);

      const voiceRes = await tool('bot_voice', { name: bot.name, state: true });
      const r3 = JSON.parse(voiceRes.content[0].text);
      console.log(`  voiceState: ${r3.status}`);

      await sleep(1000);

      const audioRes = await tool('bot_audio_start', { name: bot.name, frequency: bot.freq, gain: bot.gain });
      const r4 = JSON.parse(audioRes.content[0].text);
      console.log(`  audio ${bot.freq}Hz: ${r4.status || JSON.stringify(r4)}`);

      await sleep(500);
    } catch (err) {
      console.error(`  ERROR for ${bot.name}:`, err.message);
    }
  }

  // Start conductor
  console.log('\nStarting conductor...');
  try {
    const condRes = await tool('bot_conductor_start', { wsUrl: WS, username: 'conductor' });
    const r = JSON.parse(condRes.content[0].text);
    console.log('  conductor:', r.status);
  } catch (err) {
    console.error('  conductor ERROR:', err.message);
  }

  console.log('\n=== Audio test suite running ===');
  console.log('Bots: audio-5u (220Hz), audio-10u (330Hz), audio-20u (440Hz), audio-30u (550Hz)');
  console.log('Chat commands: !ref N  !rolloff N  !vol N  !louder [bot]  !quieter [bot]  !status  !reset  !help');
  console.log('Press Ctrl+C to stop all bots\n');

  // Keep alive
  process.on('SIGINT', async () => {
    console.log('\nShutting down...');
    try {
      await tool('bot_conductor_stop', {});
      for (const bot of audioBots) {
        await tool('bot_audio_stop', { name: bot.name });
        await tool('bot_kill', { name: bot.name });
      }
    } catch (_) {}
    proc.kill();
    process.exit(0);
  });
}

main().catch(err => {
  console.error('Fatal:', err);
  process.exit(1);
});
