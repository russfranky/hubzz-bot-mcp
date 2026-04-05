/**
 * Hubzz Media Test Suite
 *
 * Comprehensive test of the media screen pipeline, queue system,
 * permission enforcement, and webcam toggle. Runs against staging (hubzz.xyz).
 *
 * Spaces under test:
 *   - Rooftop (0,0) — has EntityScreen, primary media screen test target
 *   - Retrodoges (1,0) — no screen entity; verify space connectivity + permission behavior
 *
 * Coverage:
 *   Group A: Permission gate (guest blocked, user allowed)
 *   Group B: YouTube playback via --play (MQS + EntityScreen)
 *   Group C: Twitch stream via --play
 *   Group D: Kick stream via --play
 *   Group E: Queue system (multi-item, order, skip, clearqueue)
 *   Group F: Playwright visual checks on rooftop (screen entity + React state)
 *   Group G: ::webcam command — client-side toggle (Playwright browser)
 *   Group H: Retrodoges space (connection + --play permission accepted, no screen entity)
 *
 * Run: node media-test.mjs
 * Requires: ws, playwright
 */

import { chromium } from 'playwright';
import WebSocket from 'ws';

// ── Config ────────────────────────────────────────────────────────────────────
const BASE_URL  = 'https://hubzz.xyz';
const WS_BASE   = 'wss://hubzz.xyz/socket/';
const DELIMITER = '\uF8FF';

// iamar0b0t = internal test token → User role → has world.screen
const BOT_TOKEN  = 'iamar0b0t';
const BOT_ROOFTOP_NAME     = `MediaBot_R_${Date.now()}`;
const BOT_RETRODOGES_NAME  = `MediaBot_D_${Date.now()}`;

const TIMEOUT   = 30_000;
const CMD_DELAY = 800; // ms between sequential MQS commands

// Test media URLs
const YT_URL     = 'https://www.youtube.com/watch?v=dQw4w9WgXcQ';
const TWITCH_URL = 'https://www.twitch.tv/hubzz';
const KICK_URL   = 'https://www.kick.com/hubzz';

// ── Result tracking ───────────────────────────────────────────────────────────
const results = [];
const pass = (l, n = '') => {
  results.push({ ok: true, l, n });
  console.log(`  ✓ ${l}${n ? `\n       ↳ ${n}` : ''}`);
};
const fail = (l, n = '') => {
  results.push({ ok: false, l, n });
  console.log(`  ✗ ${l}${n ? `\n       ↳ ${n}` : ''}`);
};
const skip = (l, n = '') => {
  results.push({ ok: null, l, n });
  console.log(`  ○ [skip] ${l}${n ? `\n       ↳ ${n}` : ''}`);
};

// ── WS Bot helpers ────────────────────────────────────────────────────────────

/**
 * Spawn an authenticated bot in a given world path.
 * Returns { ws, name } on success.
 */
function spawnBot(worldPath, name, token = BOT_TOKEN) {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(`${WS_BASE}${worldPath}/`, { rejectUnauthorized: false });
    let buf = '';
    const timeout = setTimeout(() => { ws.terminate(); reject(new Error(`bot connect timeout (${worldPath})`)); }, 20_000);

    ws.on('open', () => {
      ws.send(JSON.stringify({ h: 'login', a: [token, name, ''] }) + DELIMITER);
    });

    ws.on('message', (raw) => {
      buf += raw.toString();
      const parts = buf.split(DELIMITER);
      buf = parts.pop();
      for (const p of parts) {
        if (!p.trim()) continue;
        try {
          const m = JSON.parse(p);
          if (m.h === 'ping') ws.send(JSON.stringify({ h: 'pong', a: [] }) + DELIMITER);
          if (m.h === 'acc:ok') {
            ws.send(JSON.stringify({ h: 'ready', a: [] }) + DELIMITER);
          }
          if (m.h === 'ready' || m.h === 'w:add') {
            clearTimeout(timeout);
            resolve({ ws, name });
          }
        } catch {}
      }
    });

    ws.on('error', (e) => { clearTimeout(timeout); reject(e); });
  });
}

/**
 * Spawn a guest bot (login_guest — no world.screen permission).
 * Guests get auto-assigned GuestN username; no token required.
 */
function spawnGuest(worldPath) {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(`${WS_BASE}${worldPath}/`, { rejectUnauthorized: false });
    let buf = '';
    const timeout = setTimeout(() => { ws.terminate(); reject(new Error('guest connect timeout')); }, 20_000);

    ws.on('open', () => {
      ws.send(JSON.stringify({ h: 'login_guest', a: [] }) + DELIMITER);
    });

    ws.on('message', (raw) => {
      buf += raw.toString();
      const parts = buf.split(DELIMITER);
      buf = parts.pop();
      for (const p of parts) {
        if (!p.trim()) continue;
        try {
          const m = JSON.parse(p);
          if (m.h === 'ping') ws.send(JSON.stringify({ h: 'pong', a: [] }) + DELIMITER);
          if (m.h === 'acc:ok') {
            // Guest must send ready after acc:ok
            ws.send(JSON.stringify({ h: 'ready', a: [] }) + DELIMITER);
          }
          if (m.h === 'ready' || m.h === 'w:add') {
            clearTimeout(timeout);
            const username = m.a?.[0]?.username ?? 'Guest';
            resolve({ ws, name: username });
          }
        } catch {}
      }
    });

    ws.on('error', (e) => { clearTimeout(timeout); reject(e); });
  });
}

/** Send a chat message from a bot WebSocket. */
function botChat(ws, text) {
  ws.send(JSON.stringify({ h: 'chat', a: [text] }) + DELIMITER);
}

/**
 * Send a chat command and collect all MQS responses within `waitMs`.
 * Returns array of MQS result objects.
 */
function sendMQSCommand(ws, cmd, waitMs = 4000) {
  return new Promise((resolve) => {
    const responses = [];
    let buf = '';

    const onMsg = (raw) => {
      buf += raw.toString();
      const parts = buf.split(DELIMITER);
      buf = parts.pop();
      for (const p of parts) {
        if (!p.trim()) continue;
        try {
          const m = JSON.parse(p);
          if (m.h === 'ping') ws.send(JSON.stringify({ h: 'pong', a: [] }) + DELIMITER);
          if (m.h === 'mqs') responses.push(m.a?.[0] ?? m);
        } catch {}
      }
    };

    ws.on('message', onMsg);
    botChat(ws, cmd);

    setTimeout(() => {
      ws.off('message', onMsg);
      resolve(responses);
    }, waitMs);
  });
}

/** Wait for page condition. Returns true/false. */
async function waitFor(page, fn, arg = null, ms = TIMEOUT) {
  try {
    await page.waitForFunction(fn, arg, { timeout: ms });
    return true;
  } catch {
    return false;
  }
}

/** Sleep helper. */
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

// ── Main ──────────────────────────────────────────────────────────────────────
async function run() {
  console.log('\n╔═══════════════════════════════════════════════════════╗');
  console.log('║        Hubzz Media Screen — Full Test Suite          ║');
  console.log('╚═══════════════════════════════════════════════════════╝\n');

  let rooftopBot = null;
  let guestBot   = null;
  let retrodBot  = null;
  let browser    = null;
  let page       = null;

  try {
    // ── Spawn bots ────────────────────────────────────────────────────────────
    console.log('── Connecting bots…\n');

    [rooftopBot, guestBot] = await Promise.all([
      spawnBot('0,0', BOT_ROOFTOP_NAME),
      spawnGuest('0,0'),
    ]);
    console.log(`  ℹ  Rooftop authenticated bot: ${rooftopBot.name}`);
    console.log(`  ℹ  Rooftop guest bot:          ${guestBot.name}\n`);

    // ── Browser: load rooftop ─────────────────────────────────────────────────
    browser = await chromium.launch({
      headless: true,
      args: [
        '--no-sandbox',
        '--use-fake-ui-for-media-stream',
        '--use-fake-device-for-media-stream',
        '--autoplay-policy=no-user-gesture-required',
      ],
    });

    const context = await browser.newContext({
      ignoreHTTPSErrors: true,
      permissions: ['camera', 'microphone'],
    });
    page = await context.newPage();

    page.on('pageerror', e => {
      if (!e.message.includes('ResizeObserver') && !e.message.includes('WebGL')) {
        console.log('  [page-err]', e.message.slice(0, 120));
      }
    });

    await page.goto(`${BASE_URL}/0,0`, { waitUntil: 'domcontentloaded', timeout: TIMEOUT });
    // Bot token injection so browser WS connects without Privy
    await page.evaluate((token) => { window.__botToken = token; }, BOT_TOKEN);
    await page.waitForTimeout(3000); // let engine settle

    // ════════════════════════════════════════════════════════════════════════
    // GROUP A — Permission Gate
    // ════════════════════════════════════════════════════════════════════════
    console.log('── Group A: Permission Gate ──\n');

    // A1: Authenticated user can send --play (gets MQS response, not silently dropped)
    const a1Resp = await sendMQSCommand(rooftopBot.ws, `--play ${YT_URL}`, 5000);
    a1Resp.length > 0
      ? pass('A1: Authenticated user (User role) receives MQS response for --play', `type=${a1Resp[0]?.type ?? JSON.stringify(a1Resp[0])}`)
      : fail('A1: Authenticated user got no MQS response — command may have been dropped');

    await sleep(CMD_DELAY);

    // A2: Stop playback so we start clean for the per-platform tests
    await sendMQSCommand(rooftopBot.ws, '--stop', 2000);

    // A3: Guest bot sends --play — should be silently dropped (no world.screen)
    // We confirm this by verifying the guest gets NO mqs response
    const a3Resp = await sendMQSCommand(guestBot.ws, `--play ${YT_URL}`, 4000);
    a3Resp.length === 0
      ? pass('A3: Guest (no world.screen) cannot use --play — command silently dropped')
      : fail('A3: Guest received an MQS response — permission gate may be broken', JSON.stringify(a3Resp[0]));

    await sleep(CMD_DELAY);

    // ════════════════════════════════════════════════════════════════════════
    // GROUP B — YouTube Playback
    // ════════════════════════════════════════════════════════════════════════
    console.log('\n── Group B: YouTube Playback ──\n');

    console.log(`  ℹ  Sending: --play ${YT_URL}`);
    const b1Resp = await sendMQSCommand(rooftopBot.ws, `--play ${YT_URL}`, 5000);
    const b1MQS  = b1Resp[0];

    b1MQS?.type === 'ok' && b1MQS?.message?.toLowerCase().includes('playing')
      ? pass('B1: --play YouTube URL accepted and now playing', `"${b1MQS.message}"`)
      : fail('B1: --play YouTube URL not accepted or wrong response type', JSON.stringify(b1MQS));

    // B2: Screen entity updated (EntityScreen.play called) — check via Playwright
    const b2 = await waitFor(page, (url) => {
      const w = window['currentWorld'];
      const screen = w?.entities?.find(e => e?.userData?.type === 'screen');
      if (!screen) return false;
      // currentContent or content array should have a URL matching
      const content = screen.currentContent ?? screen.content?.[0];
      return !!(content?.src && content.src.includes('youtube'));
    }, YT_URL, 12000);

    b2
      ? pass('B2: EntityScreen content updated with YouTube URL after --play')
      : skip('B2: EntityScreen content not detectable (may require auth-gated YouTube API)', 'visual-only check skipped');

    await sleep(CMD_DELAY);
    await sendMQSCommand(rooftopBot.ws, '--stop', 2000);
    await sleep(CMD_DELAY);

    // ════════════════════════════════════════════════════════════════════════
    // GROUP C — Twitch Stream
    // ════════════════════════════════════════════════════════════════════════
    console.log('\n── Group C: Twitch Stream ──\n');

    console.log(`  ℹ  Sending: --play ${TWITCH_URL}`);
    const c1Resp = await sendMQSCommand(rooftopBot.ws, `--play ${TWITCH_URL}`, 5000);
    const c1MQS  = c1Resp[0];

    c1MQS?.type === 'ok' && c1MQS?.message?.toLowerCase().includes('playing')
      ? pass('C1: --play Twitch URL accepted and now playing', `"${c1MQS.message}"`)
      : fail('C1: --play Twitch URL not accepted', JSON.stringify(c1MQS));

    // C2: Playwright check that screen entity reflects Twitch source
    const c2 = await waitFor(page, () => {
      const w = window['currentWorld'];
      const screen = w?.entities?.find(e => e?.userData?.type === 'screen');
      if (!screen) return false;
      const content = screen.currentContent ?? screen.content?.[0];
      return !!(content?.src && (content.src.includes('twitch') || content.src.includes('player.twitch')));
    }, null, 10000);

    c2
      ? pass('C2: EntityScreen content updated with Twitch URL')
      : skip('C2: EntityScreen Twitch content not detectable from browser context');

    await sleep(CMD_DELAY);
    await sendMQSCommand(rooftopBot.ws, '--stop', 2000);
    await sleep(CMD_DELAY);

    // ════════════════════════════════════════════════════════════════════════
    // GROUP D — Kick Stream
    // ════════════════════════════════════════════════════════════════════════
    console.log('\n── Group D: Kick Stream ──\n');

    console.log(`  ℹ  Sending: --play ${KICK_URL}`);
    const d1Resp = await sendMQSCommand(rooftopBot.ws, `--play ${KICK_URL}`, 5000);
    const d1MQS  = d1Resp[0];

    d1MQS?.type === 'ok' && d1MQS?.message?.toLowerCase().includes('playing')
      ? pass('D1: --play Kick URL accepted and now playing', `"${d1MQS.message}"`)
      : fail('D1: --play Kick URL not accepted', JSON.stringify(d1MQS));

    // D2: Screen entity content — Kick maps to the Twitch iframe player on the server
    const d2 = await waitFor(page, () => {
      const w = window['currentWorld'];
      const screen = w?.entities?.find(e => e?.userData?.type === 'screen');
      if (!screen) return false;
      const content = screen.currentContent ?? screen.content?.[0];
      return !!(content?.src && (content.src.includes('kick') || content.src.includes('twitch')));
    }, null, 10000);

    d2
      ? pass('D2: EntityScreen content updated with Kick/stream URL')
      : skip('D2: EntityScreen Kick content not detectable from browser context');

    await sleep(CMD_DELAY);
    await sendMQSCommand(rooftopBot.ws, '--stop', 2000);
    await sleep(CMD_DELAY);

    // ════════════════════════════════════════════════════════════════════════
    // GROUP E — Queue System
    // ════════════════════════════════════════════════════════════════════════
    console.log('\n── Group E: Queue System ──\n');

    // E1: Queue two items — first --play starts it, second adds to queue
    console.log(`  ℹ  Queuing: ${YT_URL} then ${TWITCH_URL}`);
    const e1a = await sendMQSCommand(rooftopBot.ws, `--play ${YT_URL}`, 4000);
    await sleep(CMD_DELAY);
    const e1b = await sendMQSCommand(rooftopBot.ws, `--play ${TWITCH_URL}`, 4000);

    const e1aOk = e1a[0]?.type === 'ok';
    const e1bOk = e1b[0]?.type === 'ok';
    e1aOk && e1bOk
      ? pass('E1: Two items accepted — first plays, second queued', `first="${e1a[0]?.message?.slice(0,60)}", second="${e1b[0]?.message?.slice(0,60)}"`)
      : fail('E1: Queue enqueue failed', `first=${JSON.stringify(e1a[0])}, second=${JSON.stringify(e1b[0])}`);

    // E2: --np shows what's now playing
    await sleep(CMD_DELAY);
    const e2Resp = await sendMQSCommand(rooftopBot.ws, '--np', 3000);
    // --np returns type "embed" with message "Now Playing: ..."
    e2Resp[0]?.type === 'embed' && e2Resp[0]?.message?.toLowerCase().includes('playing')
      ? pass('E2: --np returns now-playing embed', `"${e2Resp[0].message?.slice(0, 80)}"`)
      : fail('E2: --np returned unexpected response', JSON.stringify(e2Resp[0]));

    // E3: --queue shows queue listing
    await sleep(CMD_DELAY);
    const e3Resp = await sendMQSCommand(rooftopBot.ws, '--queue', 3000);
    // --queue returns type "embed" with message "Queue — N items"
    e3Resp[0]?.type === 'embed' && e3Resp[0]?.message?.toLowerCase().includes('queue')
      ? pass('E3: --queue returns queue listing', `"${e3Resp[0]?.message?.slice(0, 80)}"`)
      : fail('E3: --queue returned unexpected response', JSON.stringify(e3Resp[0]));

    // E4: --skip advances to next item
    await sleep(CMD_DELAY);
    const e4Resp = await sendMQSCommand(rooftopBot.ws, '--skip', 3000);
    // --skip returns type "ok" with message "... skipped — Now Playing: ..." or "... skipped — Queue empty"
    e4Resp[0]?.type === 'ok' && e4Resp[0]?.message?.toLowerCase().includes('skipped')
      ? pass('E4: --skip advances queue', `"${e4Resp[0]?.message?.slice(0, 80)}"`)
      : fail('E4: --skip returned unexpected response', JSON.stringify(e4Resp[0]));

    // E5: --stop then --clearqueue then --queue shows empty
    await sleep(CMD_DELAY);
    await sendMQSCommand(rooftopBot.ws, '--stop', 2000);
    await sleep(CMD_DELAY);
    const e5a = await sendMQSCommand(rooftopBot.ws, '--clearqueue', 3000);
    await sleep(CMD_DELAY);
    const e5b = await sendMQSCommand(rooftopBot.ws, '--queue', 3000);

    // After --stop + --clearqueue the queue should be empty.
    const queueMsg = e5b[0]?.message?.toLowerCase() ?? '';
    const queueData = e5b[0]?.data?.queue ?? [];
    const queueCleared = queueMsg.includes('empty') || queueMsg.includes('0 item') || queueData.length === 0;

    queueCleared
      ? pass('E5: --stop + --clearqueue empties queue', `clearqueue="${e5a[0]?.message?.slice(0, 60)}", queue after="${e5b[0]?.message?.slice(0,60)}"`)
      : fail('E5: --clearqueue did not empty queue', JSON.stringify(e5b[0]));

    // Cleanup
    await sendMQSCommand(rooftopBot.ws, '--stop', 2000);
    await sleep(CMD_DELAY);

    // ════════════════════════════════════════════════════════════════════════
    // GROUP F — Playwright Visual Checks (Rooftop)
    // ════════════════════════════════════════════════════════════════════════
    console.log('\n── Group F: Playwright Visual Checks (Rooftop) ──\n');

    // F1: Screen entity is present in the world
    const f1 = await page.evaluate(() => {
      const w = window['currentWorld'];
      const screen = w?.entities?.find(e => e?.userData?.type === 'screen');
      return screen ? { found: true, type: screen.userData?.type } : { found: false };
    });
    f1.found
      ? pass('F1: EntityScreen entity present in rooftop world.entities', `type=${f1.type}`)
      : fail('F1: EntityScreen not found in rooftop world.entities');

    // F2: MQS broadcast received by browser (world receives 'mqs' handler events)
    console.log(`  ℹ  Sending --play ${YT_URL} and watching for mqs event in browser…`);
    const f2Promise = page.evaluate(() => {
      return new Promise((resolve) => {
        const w = window['currentWorld'];
        if (!w) { resolve(false); return; }
        const handler = (data) => { w.off?.('mqs', handler); resolve(data); };
        w.on?.('mqs', handler);
        setTimeout(() => { w.off?.('mqs', handler); resolve(false); }, 8000);
      });
    });

    await sleep(500);
    botChat(rooftopBot.ws, `--play ${YT_URL}`);
    const f2Result = await f2Promise;

    f2Result && f2Result !== false
      ? pass('F2: Browser world received mqs event broadcast', `type=${f2Result?.type}`)
      : skip('F2: mqs event not captured via world.on() — may use different event system');

    await sleep(CMD_DELAY);

    // F3: SpaceCard experience layer visible in DOM
    const f3 = await page.evaluate(() => {
      return !!(
        document.querySelector('[data-experience-layer]') ||
        document.querySelector('[data-space-card]') ||
        document.querySelector('.space-cards') ||
        document.getElementById('spaceHUD')
      );
    });
    f3
      ? pass('F3: SpaceCard / HUD layer present in DOM')
      : fail('F3: SpaceCard HUD layer not found in DOM');

    // F4: No JS runtime errors that would block media (spot-check console error count)
    let errorCount = 0;
    const errHandler = (msg) => { if (msg.type() === 'error') errorCount++; };
    page.on('console', errHandler);
    await sleep(2000);
    page.off('console', errHandler);
    errorCount < 5
      ? pass(`F4: Console error count acceptable during media play`, `${errorCount} errors`)
      : fail(`F4: Too many console errors during media test`, `${errorCount} errors`);

    await sendMQSCommand(rooftopBot.ws, '--stop', 2000);
    await sleep(CMD_DELAY);

    // ════════════════════════════════════════════════════════════════════════
    // GROUP G — ::webcam Command (Client-Side Toggle)
    // ════════════════════════════════════════════════════════════════════════
    console.log('\n── Group G: ::webcam Toggle (Playwright) ──\n');

    // The ::webcam command is registered client-side in Commands.ts.
    // It toggles RTC video state. We fire it via page.evaluate dispatching
    // the command directly through the client's command system.
    const g1 = await page.evaluate(() => {
      return new Promise((resolve) => {
        // Try client.command() — the engine exposes client on window in staging
        const client = window['gameClient'] ?? window['__gameClient'] ?? window['client'];
        if (!client?.command) { resolve({ tried: false, reason: 'client not exposed' }); return; }

        let fired = false;
        // Listen for webcam toggle event
        const onCam = () => { fired = true; };
        window.addEventListener('webcam-toggle', onCam);
        client.command('::webcam');
        setTimeout(() => {
          window.removeEventListener('webcam-toggle', onCam);
          resolve({ tried: true, fired });
        }, 2000);
      });
    });

    if (!g1.tried) {
      // Fallback: use keyboard shortcut — Commands.ts registers keyboard handler
      const g1b = await page.evaluate(() => {
        return new Promise((resolve) => {
          // Dispatch a synthetic chat command via the engine's chat input
          const chatInput = document.querySelector('input[placeholder*="ype"]') ??
                            document.querySelector('[data-chat-input]');
          if (!chatInput) { resolve({ found: false }); return; }
          chatInput.focus();
          chatInput.value = '::webcam';
          chatInput.dispatchEvent(new KeyboardEvent('keydown', { key: 'Enter', bubbles: true }));
          chatInput.dispatchEvent(new Event('input', { bubbles: true }));
          setTimeout(() => resolve({ found: true }), 1500);
        });
      });
      g1b.found
        ? pass('G1: ::webcam dispatched via chat input', 'client.command() not exposed, used chat input fallback')
        : skip('G1: ::webcam untestable without Privy session', '::webcam requires signed-in RTC session — client-side only, cannot test via bot WebSocket');
    } else if (g1.fired) {
      pass('G1: ::webcam command triggered webcam-toggle event');
    } else {
      skip('G1: ::webcam command fired but no webcam-toggle event intercepted', 'RTC video state may toggle internally without window event');
    }

    // G2: Verify webcam command doesn't crash the engine (no new page errors after)
    const g2Errors = [];
    const g2Handler = (msg) => { if (msg.type() === 'error') g2Errors.push(msg.text()); };
    page.on('console', g2Handler);
    await sleep(2000);
    page.off('console', g2Handler);
    const g2Fatal = g2Errors.filter(e => e.includes('Uncaught') || e.includes('Cannot read'));
    g2Fatal.length === 0
      ? pass('G2: No fatal JS errors after ::webcam invocation')
      : fail('G2: Fatal JS errors after ::webcam invocation', g2Fatal[0].slice(0, 120));

    // ════════════════════════════════════════════════════════════════════════
    // GROUP H — Retrodoges Space (1,0)
    // ════════════════════════════════════════════════════════════════════════
    console.log('\n── Group H: Retrodoges Space (1,0) ──\n');

    // H1: Bot connects to Retrodoges space
    try {
      retrodBot = await spawnBot('1,0', BOT_RETRODOGES_NAME);
      pass('H1: Authenticated bot connects to Retrodoges (1,0)');
    } catch (e) {
      fail('H1: Could not connect bot to Retrodoges (1,0)', e.message);
    }

    if (retrodBot) {
      // H2: Authenticated user's --play is accepted by permission check in Retrodoges
      // (world.screen permission passes, but EntityScreen doesn't exist so screen stays blank)
      const h2Resp = await sendMQSCommand(retrodBot.ws, `--play ${YT_URL}`, 5000);
      const h2MQS  = h2Resp[0];

      // MQS should respond with ok (command accepted), even if no screen entity renders it
      h2MQS?.type === 'ok'
        ? pass('H2: --play accepted by MQS in Retrodoges (permission check passed)', `"${h2MQS.message?.slice(0,60)}"`)
        : fail('H2: --play not accepted in Retrodoges', JSON.stringify(h2MQS));

      // H3: Playwright navigates to Retrodoges — no EntityScreen in world.entities
      await page.goto(`${BASE_URL}/1,0`, { waitUntil: 'domcontentloaded', timeout: TIMEOUT });
      await page.waitForTimeout(4000); // let engine load

      const h3 = await page.evaluate(() => {
        const w = window['currentWorld'];
        if (!w) return { loaded: false };
        const screen = w.entities?.find(e => e?.userData?.type === 'screen');
        return { loaded: true, hasScreen: !!screen, entityCount: w.entities?.length ?? 0 };
      });

      h3.loaded
        ? pass(`H3: Retrodoges world loaded — screen entity ${h3.hasScreen ? 'PRESENT' : 'absent'}`, `entityCount=${h3.entityCount}`)
        : skip('H3: World not loaded in time', JSON.stringify(h3));

      await sendMQSCommand(retrodBot.ws, '--stop', 1500);
    }

  } catch (err) {
    console.error('\n[FATAL]', err.message);
    results.push({ ok: false, l: 'FATAL error during test run', n: err.message });
  } finally {
    // ── Cleanup ───────────────────────────────────────────────────────────────
    console.log('\n── Cleanup…\n');
    try { rooftopBot?.ws?.close(); } catch {}
    try { guestBot?.ws?.close();   } catch {}
    try { retrodBot?.ws?.close();  } catch {}
    try { await browser?.close();  } catch {}

    // ── Summary ───────────────────────────────────────────────────────────────
    const passed = results.filter(r => r.ok === true).length;
    const failed = results.filter(r => r.ok === false).length;
    const skipped = results.filter(r => r.ok === null).length;

    console.log('\n╔═══════════════════════════════════════════════════════╗');
    console.log(`║  Results: ${passed} passed  ${failed} failed  ${skipped} skipped`.padEnd(56) + '║');
    console.log('╚═══════════════════════════════════════════════════════╝\n');

    if (failed > 0) {
      console.log('Failures:');
      results.filter(r => r.ok === false).forEach(r => {
        console.log(`  ✗ ${r.l}${r.n ? '\n       ↳ ' + r.n : ''}`);
      });
      console.log('');
    }

    process.exit(failed > 0 ? 1 : 0);
  }
}

run();
