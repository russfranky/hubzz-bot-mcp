/**
 * Voice Mic Indicator — Playwright E2E Tests
 *
 * Approach: spawn a WS bot so the spectator page has an avatar to observe.
 * The Playwright browser connects as a guest spectator; the bot connects
 * via WebSocket and appears as an EntityAvatar in world.entities.
 * We then test the full nametag/voiceState pipeline on that bot's avatar.
 *
 * Tests:
 *   T1  World loads and entities are present
 *   T2  toggleMic handler is registered on client (new SpaceHUD code)
 *   T3  Bot avatar appears in world.entities as an EntityAvatar
 *   T4  Bot has a nametag DOM element (#nametag-{entityId})
 *   T5  Nametag CSS3DObject starts hidden (visible=false)
 *   T6  Mic icon inside nametag starts hidden (display: none)
 *   T7  voiceState true → mic icon becomes visible
 *   T8  Nametag CSS3DObject becomes visible when voice is active
 *   T9  voiceState false → mic icon hides, nametag hides
 *   T10 Nametag CSS3DObject position is above avatar's head (y offset > 2)
 *   T11 Nametag has CSS matrix3d transform (rendered in 3D scene, not flat)
 */

import { chromium }  from 'playwright';
import WebSocket      from 'ws';

const BASE_URL  = 'https://hubzz.xyz';
const WS_BASE   = 'wss://hubzz.xyz/socket/';
const DELIMITER = '\uF8FF';
const BOT_TOKEN = 'iamar0b0t';
const BOT_NAME  = `VoiceTestBot_${Date.now()}`;
const TIMEOUT   = 45000;

const results = [];
const pass = (l, n = '') => { results.push({ ok: true,  l, n }); console.log(`  ✓ ${l}${n ? `\n       ↳ ${n}` : ''}`); };
const fail = (l, n = '') => { results.push({ ok: false, l, n }); console.log(`  ✗ ${l}${n ? `\n       ↳ ${n}` : ''}`); };

// ── Spawn a WS bot and return a handle ──────────────────────────────────────
function spawnBot(name) {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(`${WS_BASE}0,0/`, { rejectUnauthorized: false });
    const timeout = setTimeout(() => { ws.terminate(); reject(new Error('bot connect timeout')); }, 15000);
    ws.on('open', () => ws.send(JSON.stringify({ h: 'login', a: [BOT_TOKEN, name, ''] }) + DELIMITER));
    ws.on('message', raw => {
      raw.toString().split(DELIMITER).filter(Boolean).forEach(p => {
        try {
          const m = JSON.parse(p);
          if (m.h === 'ping') ws.send(JSON.stringify({ h: 'pong', a: [] }) + DELIMITER);
          if (m.h === 'acc:ok') {
            ws.send(JSON.stringify({ h: 'ready', a: [] }) + DELIMITER);
            clearTimeout(timeout);
            resolve({ ws, entityId: m.a?.[0]?.id });
          }
        } catch {}
      });
    });
    ws.on('error', reject);
  });
}

async function run() {
  console.log('\n=== Voice Mic Indicator — Playwright E2E Tests ===\n');

  let bot = null;
  const browser = await chromium.launch({
    headless: true,
    args: ['--no-sandbox', '--use-fake-ui-for-media-stream', '--use-fake-device-for-media-stream'],
  });
  const context = await browser.newPage();
  const page = context;

  page.on('pageerror', e => {
    if (!e.message.includes('ResizeObserver') && !e.message.includes('WebGL')) {
      console.log('[page error]', e.message.slice(0, 120));
    }
  });

  try {
    // ── Spawn WS bot first so it's in the world when the page loads ──────────
    bot = await spawnBot(BOT_NAME);
    console.log(`  ℹ  Bot connected (entityId=${bot.entityId}, name=${BOT_NAME})\n`);

    // ── Load page ─────────────────────────────────────────────────────────────
    await page.goto(`${BASE_URL}/0,0`, { waitUntil: 'domcontentloaded', timeout: TIMEOUT });

    // Wait for world + the specific bot entity to appear and fully load its nametag
    // Uses evaluate polling (more reliable than waitForFunction on slow headless loads)
    let nametagFound = false;
    for (let attempt = 0; attempt < 20; attempt++) {
      await page.waitForTimeout(3000);
      const ready = await page.evaluate((n) => {
        const w = window['currentWorld'];
        const e = w?.entities?.find(x => x?.userData?.username === n);
        return !!(e?.nametag);
      }, BOT_NAME);
      if (ready) { nametagFound = true; break; }
    }
    if (!nametagFound) throw Object.assign(new Error('Nametag never appeared after 60s'), { name: 'TimeoutError' });

    await page.waitForTimeout(500); // brief settle after nametag creation

    // ── T1: World is ready and has entities ──────────────────────────────────
    const t1 = await page.evaluate(() => {
      const w = window['currentWorld'];
      return { worldExists: !!w, entityCount: w?.entities?.length ?? 0, worldState: w?.state };
    });
    t1.entityCount > 0
      ? pass('T1: World ready with entities loaded', `${t1.entityCount} entities, state="${t1.worldState}"`)
      : fail('T1: No entities in world');

    // ── T2: toggleMic handler registered on client (new SpaceHUD code) ────────
    const t2 = await page.evaluate(() => {
      const client = window['currentWorld']?.client;
      if (!client) return { registered: false, reason: 'no client' };
      // Custom EventEmitter (EventEmitter.ts) uses this.listeners[event], not Node's _events
      const listeners = client.listeners?.['toggleMic'];
      const count = Array.isArray(listeners) ? listeners.length : 0;
      return { registered: count > 0, count };
    });
    t2.registered
      ? pass('T2: toggleMic handler is registered on client', `count=${t2.count}`)
      : fail('T2: No toggleMic listener on client — SpaceHUD handler not mounted', JSON.stringify(t2));

    // ── T3: Bot avatar visible in world.entities ──────────────────────────────
    const botEntityId = await page.evaluate((botName) => {
      const w = window['currentWorld'];
      const EntityAvatar = w?.entities?.find?.(e => e?.userData?.username === botName || e?.username === botName);
      if (EntityAvatar) return EntityAvatar.entityId ?? EntityAvatar.id;
      // Fallback: scan all entities for the bot name
      const all = w?.entities ?? [];
      for (const e of all) {
        if (e?.userData?.username === botName) return e.entityId;
      }
      return null;
    }, BOT_NAME);

    botEntityId
      ? pass('T3: Bot avatar found in world.entities', `entityId=${botEntityId}`)
      : fail('T3: Bot avatar not found in world.entities', `looking for username="${BOT_NAME}"`);

    if (!botEntityId) {
      fail('T4–T11: Skipped (no bot entity found)');
    } else {
      // ── T4: Bot nametag DOM element exists ─────────────────────────────────
      // CSS3DRenderer only inserts the element into the page when it first renders
      // (hidden objects are skipped). Read from avatar.nametag.object.element directly.
      const nametagElem = await page.evaluate((id) => {
        const w = window['currentWorld'];
        const avatar = w?.entities?.find?.(e => e?.entityId === id);
        const elem = avatar?.nametag?.object?.element ?? document.getElementById(`nametag-${id}`);
        return { found: !!elem, id: elem?.id ?? null, className: elem?.className ?? null };
      }, botEntityId);
      nametagElem.found
        ? pass('T4: Bot nametag DOM element present', `#${nametagElem.id ?? `nametag-${botEntityId}`} class="${nametagElem.className}"`)
        : fail('T4: Bot nametag DOM element not found', `#nametag-${botEntityId}`);

      // ── T5: Nametag CSS3DObject starts hidden ──────────────────────────────
      const t5 = await page.evaluate((id) => {
        const w = window['currentWorld'];
        const avatar = w?.entities?.find?.(e => e?.entityId === id);
        return avatar?.nametag?.object?.visible;
      }, botEntityId);
      t5 === false
        ? pass('T5: Nametag CSS3DObject.visible is false initially')
        : fail('T5: Nametag should start hidden', `visible=${t5}`);

      // ── T6: Mic icon starts hidden ─────────────────────────────────────────
      // Read inline style.display — element may not be in DOM yet so getComputedStyle
      // is unreliable; AvatarNametag sets display via element.style.display directly.
      const t6 = await page.evaluate((id) => {
        const w = window['currentWorld'];
        const avatar = w?.entities?.find?.(e => e?.entityId === id);
        const elem   = avatar?.nametag?.object?.element ?? document.getElementById(`nametag-${id}`);
        const micDiv = elem?.firstElementChild;
        return micDiv ? (micDiv.style.display || 'not-set') : 'elem-missing';
      }, botEntityId);
      t6 === 'none'
        ? pass('T6: Mic icon is hidden initially (display: none)')
        : fail('T6: Mic icon initial state wrong', `display: ${t6}`);

      // ── T7: voiceState true → mic icon visible ─────────────────────────────
      await page.evaluate((id) => {
        window['currentWorld']?.connection?.emit('voiceState', { id, state: true });
      }, botEntityId);
      await page.waitForTimeout(300);

      const t7 = await page.evaluate((id) => {
        const elem   = document.getElementById(`nametag-${id}`);
        const micDiv = elem?.firstElementChild;
        return micDiv ? getComputedStyle(micDiv).display : 'elem-missing';
      }, botEntityId);
      t7 !== 'none' && t7 !== 'elem-missing'
        ? pass('T7: Mic icon visible after voiceState true', `display: ${t7}`)
        : fail('T7: Mic icon not visible after voiceState true', `display: ${t7}`);

      // ── T8: Nametag CSS3DObject visible while voice active ─────────────────
      const t8 = await page.evaluate((id) => {
        const avatar = window['currentWorld']?.entities?.find?.(e => e?.entityId === id);
        return avatar?.nametag?.object?.visible;
      }, botEntityId);
      t8 === true
        ? pass('T8: Nametag CSS3DObject.visible becomes true while speaking')
        : fail('T8: Nametag not visible while voice is active', `visible=${t8}`);

      // ── T9: voiceState false → mic icon + nametag hide ─────────────────────
      await page.evaluate((id) => {
        window['currentWorld']?.connection?.emit('voiceState', { id, state: false });
      }, botEntityId);
      await page.waitForTimeout(300);

      const t9mic = await page.evaluate((id) => {
        const elem   = document.getElementById(`nametag-${id}`);
        const micDiv = elem?.firstElementChild;
        return getComputedStyle(micDiv).display;
      }, botEntityId);
      const t9vis = await page.evaluate((id) => {
        const avatar = window['currentWorld']?.entities?.find?.(e => e?.entityId === id);
        return avatar?.nametag?.object?.visible;
      }, botEntityId);

      t9mic === 'none'
        ? pass('T9: Mic icon hides after voiceState false')
        : fail('T9: Mic icon still showing after voiceState false', `display: ${t9mic}`);
      t9vis === false
        ? pass('T9b: Nametag hides after voiceState false')
        : fail('T9b: Nametag still visible after voiceState false', `visible=${t9vis}`);

      // ── T10: Nametag positioned above avatar's head in 3D world space ───────
      const posCheck = await page.evaluate((id) => {
        const avatar = window['currentWorld']?.entities?.find?.(e => e?.entityId === id);
        if (!avatar?.nametag) return null;
        const tagY  = avatar.nametag.object.position.y;
        const avY   = avatar.position.y;
        const offset = tagY - avY;
        return {
          tagY:   +tagY.toFixed(2),
          avY:    +avY.toFixed(2),
          offset: +offset.toFixed(2),
          nametagHeight: +(avatar.nametagHeight ?? 4.0).toFixed(2),
        };
      }, botEntityId);

      if (posCheck) {
        posCheck.offset > 2
          ? pass('T10: Nametag is above avatar in 3D world space',
                 `avatar.y=${posCheck.avY}, nametag.y=${posCheck.tagY}, offset=+${posCheck.offset} (target ~${posCheck.nametagHeight})`)
          : fail('T10: Nametag not above avatar', `offset=${posCheck.offset}`);
      } else {
        fail('T10: Could not read nametag/avatar 3D positions');
      }

      // ── T11: Nametag element carries a CSS matrix3d transform ───────────────
      // CSS3DRenderer wraps each object's DOM element with a matrix3d CSS transform.
      const transformCheck = await page.evaluate((id) => {
        let el = document.getElementById(`nametag-${id}`);
        if (!el) return { found: false, reason: 'element missing' };
        for (let depth = 0; depth < 8 && el; depth++) {
          const t = el.style.transform || getComputedStyle(el).transform;
          if (t && t.includes('matrix3d')) {
            return { found: true, depth, preview: t.slice(0, 70) };
          }
          el = el.parentElement;
        }
        return { found: false, reason: 'no matrix3d in 8 ancestor levels' };
      }, botEntityId);

      transformCheck.found
        ? pass('T11: Nametag has CSS matrix3d transform — rendered in 3D scene',
               `depth=${transformCheck.depth}, "${transformCheck.preview}..."`)
        : fail('T11: No matrix3d transform on nametag or ancestors', transformCheck.reason);
    }

  } catch (e) {
    fail('Test error', e.message);
    console.error(e);
  } finally {
    bot?.ws?.close();
    await browser.close();
  }

  console.log();
  let p = 0, f = 0;
  for (const r of results) { r.ok ? p++ : f++; }
  console.log(`${'═'.repeat(57)}`);
  console.log(`  Results: ${p} passed, ${f} failed`);
  console.log(`${'═'.repeat(57)}\n`);
  process.exit(f > 0 ? 1 : 0);
}

run().catch(e => { console.error(e); process.exit(1); });
