/**
 * Hubzz Staging Integration Test Suite
 *
 * Pass ADMIN_TOKEN=<privy_jwt> env var to enable full admin endpoint tests.
 * Without it, admin tests verify auth enforcement (401/403 behaviour).
 */
import WebSocket from 'ws';
import https from 'https';

const WS_BASE      = 'wss://hubzz.xyz/socket/';
const API_BASE     = 'https://hubzz.xyz';
const DELIMITER    = '\uF8FF';
const BOT_TOKEN    = 'iamar0b0t';
const CONNECT_TIMEOUT = 15000;
const ADMIN_TOKEN  = process.env.ADMIN_TOKEN ?? null;

const sleep = ms => new Promise(r => setTimeout(r, ms));

// ─── HTTP helpers ──────────────────────────────────────────────────────────────

function fetchJson(url) {
    return new Promise((resolve, reject) => {
        https.get(url, { rejectUnauthorized: false }, res => {
            let data = '';
            res.on('data', c => data += c);
            res.on('end', () => { try { resolve(JSON.parse(data)); } catch (e) { reject(e); } });
        }).on('error', reject);
    });
}

function apiRequest(path, { method = 'GET', body, token } = {}) {
    return new Promise((resolve, reject) => {
        const payload = body ? JSON.stringify(body) : null;
        const url = new URL(API_BASE + path);
        const authHeaders = token ? { Authorization: `Bearer ${token}` } : {};
        const opts = {
            hostname: url.hostname,
            path: url.pathname + url.search,
            method,
            rejectUnauthorized: false,
            headers: {
                'Content-Type': 'application/json',
                ...authHeaders,
                ...(payload ? { 'Content-Length': Buffer.byteLength(payload) } : {}),
            },
        };
        const req = https.request(opts, res => {
            let data = '';
            res.on('data', c => data += c);
            res.on('end', () => {
                try { resolve({ status: res.statusCode, body: JSON.parse(data) }); }
                catch { resolve({ status: res.statusCode, body: data }); }
            });
        });
        req.on('error', reject);
        if (payload) req.write(payload);
        req.end();
    });
}

// Shorthand for authenticated admin requests
const adminReq = (path, opts = {}) =>
    apiRequest(path, { ...opts, token: ADMIN_TOKEN });

// ─── TestBot ───────────────────────────────────────────────────────────────────

class TestBot {
    constructor(name, worldPath = '', opts = {}) {
        this.name       = name;
        this.worldPath  = worldPath;
        this.isGuest    = opts.isGuest ?? false;
        this.wsUrl      = `${WS_BASE}${worldPath}${worldPath ? '/' : ''}`;
        this.ws         = null;
        this.connected  = false;
        this.knownUsers = new Map();
        this.events     = [];
        this.errors     = [];
        this.chatLog    = [];
    }

    connect() {
        return new Promise((resolve, reject) => {
            this.ws = new WebSocket(this.wsUrl, { rejectUnauthorized: false });
            const timeout = setTimeout(() => {
                this.ws?.terminate();
                reject(new Error(`${this.name}: timeout (${CONNECT_TIMEOUT / 1000}s)`));
            }, CONNECT_TIMEOUT);

            this.ws.on('open', () => {
                if (this.isGuest) {
                    this._send({ h: 'login_guest', a: [] });
                } else {
                    this._send({ h: 'login', a: [BOT_TOKEN, this.name, ''] });
                }
            });

            this.ws.on('message', (raw) => {
                raw.toString().split(DELIMITER).filter(Boolean).forEach(part => {
                    try {
                        const msg = JSON.parse(part);
                        this.events.push(msg);
                        if (msg.h === 'ping') this._send({ h: 'pong', a: [] });
                        if (msg.h === 'acc:ok') {
                            clearTimeout(timeout);
                            this._send({ h: 'ready', a: [] });
                            this.connected = true;
                            resolve(msg);
                        }
                        if (msg.h === 'acc:fail') {
                            clearTimeout(timeout);
                            reject(new Error(`${this.name}: acc:fail: ${JSON.stringify(msg.a)}`));
                        }
                        if (msg.h === 'w:add') {
                            const d = msg.a?.[0];
                            if (d && (!d.type || d.type === 'avatar')) {
                                this.knownUsers.set(String(d.id ?? d), d.username ?? 'unknown');
                            }
                        }
                        if (msg.h === 'w:rem') {
                            this.knownUsers.delete(String(msg.a?.[0]?.id ?? msg.a?.[0]));
                        }
                        // Chat arrives as w:call { id, f:'chat', a:[text] }
                        if (msg.h === 'w:call' && msg.a?.[0]?.f === 'chat') {
                            this.chatLog.push({ from: msg.a[0].id, text: msg.a[0].a?.[0] });
                        }
                    } catch {}
                });
            });

            this.ws.on('error', (e) => {
                this.errors.push(e.message);
                clearTimeout(timeout);
                if (!this.connected) reject(new Error(`${this.name}: ${e.message}`));
            });

            this.ws.on('close', () => { this.connected = false; });
        });
    }

    _send(obj) {
        if (this.ws?.readyState === WebSocket.OPEN)
            this.ws.send(JSON.stringify(obj) + DELIMITER);
    }

    chat(text) { this._send({ h: 'chat', a: [text] }); }
    move(tileId) { this._send({ h: 'w:move', a: [tileId] }); }

    waitForEvent(h, timeoutMs = 3000) {
        return new Promise((resolve, reject) => {
            const existing = this.events.find(e => e.h === h);
            if (existing) { resolve(existing); return; }
            const t = setTimeout(() => reject(new Error(`Timeout waiting for ${h}`)), timeoutMs);
            const interval = setInterval(() => {
                const found = this.events.find(e => e.h === h);
                if (found) { clearTimeout(t); clearInterval(interval); resolve(found); }
            }, 50);
        });
    }

    waitForChat(matchFn, timeoutMs = 3000) {
        return new Promise((resolve, reject) => {
            const t = setTimeout(() => reject(new Error('Timeout waiting for chat')), timeoutMs);
            const interval = setInterval(() => {
                const found = this.chatLog.find(matchFn);
                if (found) { clearTimeout(t); clearInterval(interval); resolve(found); }
            }, 50);
        });
    }

    close() { this.ws?.close(); this.connected = false; }
    getUsernames() { return [...this.knownUsers.values()]; }
    hasEvent(h) { return this.events.some(e => e.h === h); }
    eventTypes() { return [...new Set(this.events.map(e => e.h))].join(', '); }
}

// ─── Warmup ────────────────────────────────────────────────────────────────────

async function warmupSpaces(paths) {
    console.log(`  ℹ  Warming up ${paths.length} spaces...`);
    await Promise.allSettled(paths.map(path => {
        return new Promise(resolve => {
            const ws = new WebSocket(`${WS_BASE}${path}/`, { rejectUnauthorized: false });
            ws.on('open', () => ws.send(JSON.stringify({ h: 'login', a: [BOT_TOKEN, 'Warmup', ''] }) + DELIMITER));
            ws.on('message', raw => {
                raw.toString().split(DELIMITER).filter(Boolean).forEach(part => {
                    try {
                        const m = JSON.parse(part);
                        if (m.h === 'acc:ok') { ws.close(); resolve(); }
                    } catch {}
                });
            });
            ws.on('error', () => resolve());
            ws.on('close', resolve);
            setTimeout(() => { ws.terminate(); resolve(); }, CONNECT_TIMEOUT);
        });
    }));
    await sleep(500);
    console.log('  ℹ  Warmup complete.');
}

// ─── Assertions ────────────────────────────────────────────────────────────────

let passed = 0, failed = 0;
const failures = [];

function assert(cond, label, detail = '') {
    if (cond) { console.log(`  ✓ ${label}`); passed++; }
    else {
        const msg = detail ? `${label} [${detail}]` : label;
        console.error(`  ✗ FAIL: ${msg}`);
        failures.push(msg);
        failed++;
    }
}

// ─── Tests ─────────────────────────────────────────────────────────────────────

async function runTests() {
    console.log('\n═══════════════════════════════════════════════════════');
    console.log('  Hubzz Staging Integration Tests');
    console.log('  Target: hubzz.xyz (staging)');
    if (ADMIN_TOKEN) console.log('  Admin token: provided ✓');
    else console.log('  Admin token: not set — auth-enforcement tests only');
    console.log('═══════════════════════════════════════════════════════\n');

    const allBots = [];

    try {

        // ── TEST 1: /api/spaces ──────────────────────────────────────────────
        console.log('TEST 1: GET /api/spaces');
        let spaces = [];
        try {
            const r = await fetchJson(`${API_BASE}/api/spaces`);
            spaces = r.spaces ?? [];
            assert(Array.isArray(spaces) && spaces.length > 0, `Returns ${spaces.length} spaces`);
            assert(spaces.every(s => typeof s.id === 'number' && typeof s.path === 'string'), 'All have id + path');
            const coords = spaces.filter(s => /^-?\d+,-?\d+$/.test(s.path));
            const slugs  = spaces.filter(s => !/^-?\d+,-?\d+$/.test(s.path));
            console.log(`  ℹ  ${spaces.length} total — ${coords.length} coord paths, ${slugs.length} slug paths`);
            if (coords.length) console.log(`  ℹ  Coord paths: ${coords.map(s => s.path).join(', ')}`);
        } catch (e) { assert(false, '/api/spaces', e.message); }

        // ── TEST 2: /api/health ──────────────────────────────────────────────
        console.log('\nTEST 2: GET /api/health');
        try {
            const h = await fetchJson(`${API_BASE}/api/health`);
            assert(h.status === 'ok', 'status: ok');
            console.log(`  ℹ  uptime: ${h.uptime}s | connections: ${h.connections} | worlds: ${h.worlds}`);
        } catch (e) { assert(false, '/api/health', e.message); }

        const editorSpace = spaces.find(s => s.path && s.name?.trim());
        const editorPath  = editorSpace?.path ?? 'testspacepleaseignore';
        const coordSpace  = spaces.find(s => /^-?\d+,-?\d+$/.test(s.path));
        const concPaths   = ['0,0', editorPath,
            ...(spaces.slice(2, 4).map(s => s.path).filter(p => p && p !== editorPath))
        ].filter((p, i, a) => a.indexOf(p) === i).slice(0, 4);
        const warmPaths   = [...new Set(['0,0', editorPath, ...(coordSpace ? [coordSpace.path] : []), ...concPaths])];

        console.log('\nWARMUP: Pre-loading worlds...');
        await warmupSpaces(warmPaths);

        // ── TEST 3: Main world ───────────────────────────────────────────────
        console.log('\nTEST 3: Bot → main world (0,0)');
        const botMain = new TestBot('TBot_Main', '0,0');
        allBots.push(botMain);
        try {
            await botMain.connect();
            await sleep(1500);
            assert(botMain.connected, 'Connected');
            assert(botMain.hasEvent('acc:ok'), 'Received acc:ok');
            assert(botMain.hasEvent('w:ready'), 'World ready event received');
            console.log(`  ℹ  Events: ${botMain.eventTypes()}`);
        } catch (e) { assert(false, 'Connect to main world', e.message); }

        // ── TEST 4: EditorSpace connection ───────────────────────────────────
        console.log(`\nTEST 4: Bot → EditorSpace (/${editorPath})`);
        const botEditor = new TestBot('TBot_Editor', editorPath);
        allBots.push(botEditor);
        try {
            await botEditor.connect();
            await sleep(1500);
            assert(botEditor.connected, `Connected to /${editorPath}`);
            assert(botEditor.hasEvent('acc:ok'), 'Received acc:ok');
        } catch (e) { assert(false, `Connect to /${editorPath}`, e.message); }

        // ── TEST 5: Space isolation ──────────────────────────────────────────
        console.log('\nTEST 5: Space isolation');
        if (botMain.connected && botEditor.connected) {
            assert(!botMain.getUsernames().includes('TBot_Editor'), 'Main does NOT see editor bot');
            assert(!botEditor.getUsernames().includes('TBot_Main'), 'Editor does NOT see main bot');
        } else {
            console.log('  ⚠  Skipped (bots not connected)');
        }

        // ── TEST 6: Co-presence ──────────────────────────────────────────────
        console.log(`\nTEST 6: Co-presence — two bots in /${editorPath}`);
        const botEditor2 = new TestBot('TBot_Editor2', editorPath);
        allBots.push(botEditor2);
        try {
            await botEditor2.connect();
            await sleep(1500);
            assert(botEditor2.connected, 'Second bot connected');
            const e1SeesE2 = botEditor.getUsernames().includes('TBot_Editor2');
            const e2SeesE1 = botEditor2.getUsernames().includes('TBot_Editor');
            assert(e1SeesE2 || e2SeesE1, 'Bots in same space see each other',
                `E1→E2:${e1SeesE2} E2→E1:${e2SeesE1}`);
        } catch (e) { assert(false, 'Second editor bot', e.message); }

        // ── TEST 7: isBot flag ───────────────────────────────────────────────
        console.log('\nTEST 7: isBot flag (iamar0b0t token)');
        const botDiscord = new TestBot('TBot_Discord', '0,0');
        allBots.push(botDiscord);
        try {
            await botDiscord.connect();
            await sleep(500);
            assert(botDiscord.connected, 'Bot accepted with iamar0b0t token');
            assert(botDiscord.hasEvent('acc:ok'), 'Received acc:ok (isBot path works)');
        } catch (e) { assert(false, 'isBot token connect', e.message); }

        // ── TEST 8: Guest bot ────────────────────────────────────────────────
        console.log('\nTEST 8: Guest (spectator) bot');
        const botGuest = new TestBot('TBot_Guest', '0,0', { isGuest: true });
        allBots.push(botGuest);
        try {
            await botGuest.connect();
            await sleep(1000);
            assert(botGuest.connected, 'Guest connected');
            assert(botGuest.hasEvent('acc:ok'), 'Guest received acc:ok');
        } catch (e) { assert(false, 'Guest bot connect', e.message); }

        // ── TEST 9: Chat delivery between two bots ──────────────────────────
        console.log('\nTEST 9: Chat delivery — bot A sends, bot B receives');
        if (botEditor.connected && botEditor2.connected) {
            const msg = `ping-${Date.now()}`;
            try {
                botEditor.chat(msg);
                const received = await botEditor2.waitForChat(c => c.text === msg, 3000);
                assert(!!received, 'Chat message delivered cross-bot');
                assert(received.text === msg, `Exact message content matches ("${received.text}")`);
            } catch (e) { assert(false, 'Chat delivery', e.message); }
        } else {
            console.log('  ⚠  Skipped (bots not connected)');
        }

        botMain.close(); botEditor.close(); botEditor2.close();
        botDiscord.close(); botGuest.close();
        await sleep(300);

        // ── TEST 10: Concurrent multi-space ─────────────────────────────────
        console.log('\nTEST 10: Concurrent connections — multiple spaces');
        const concBots = concPaths.map((p, i) => {
            const b = new TestBot(`Conc_${i}`, p);
            allBots.push(b);
            return b;
        });
        const concResults = await Promise.allSettled(concBots.map(b => b.connect()));
        await sleep(1000);
        const concOk = concResults.filter(r => r.status === 'fulfilled').length;
        assert(concOk === concBots.length, `All ${concBots.length} concurrent bots connected (${concOk}/${concBots.length})`);
        concBots.forEach(b => b.close());
        await sleep(300);

        // ── TEST 11: x,y coordinate path routing ────────────────────────────
        console.log('\nTEST 11: x,y coordinate path routing');
        if (coordSpace) {
            const botCoord = new TestBot('TBot_Coord', coordSpace.path);
            allBots.push(botCoord);
            try {
                await botCoord.connect();
                await sleep(1000);
                assert(botCoord.connected, `Connected via /${coordSpace.path}`);
                console.log(`  ℹ  Space "${coordSpace.name}" loaded via /${coordSpace.path}`);
                botCoord.close();
            } catch (e) { assert(false, `Coord path /${coordSpace.path}`, e.message); }
        } else {
            console.log('  ⚠  Skipped (no coord spaces in DB)');
        }
        await sleep(300);

        // ── TEST 12: Multiple guests in same world ───────────────────────────
        console.log('\nTEST 12: Multiple guests co-exist in main world');
        {
            // Sentinel non-guest bot must be present first — guests only receive
            // w:add for non-guests (Ready.ts line 21: !user.session.isGuest guard)
            const sentinel = new TestBot('TBot_Sentinel12', '0,0');
            allBots.push(sentinel);
            await sentinel.connect();
            await sleep(500);

            const guests = [1, 2, 3].map(i => {
                const b = new TestBot(`GBot_${i}`, '0,0', { isGuest: true });
                allBots.push(b);
                return b;
            });
            const gResults = await Promise.allSettled(guests.map(b => b.connect()));
            await sleep(1200);
            const gOk = gResults.filter(r => r.status === 'fulfilled').length;
            assert(gOk === 3, `All 3 guests connected (${gOk}/3)`);
            // Guests should see the sentinel non-guest bot via w:add
            const anySeesOther = guests.some(g => g.knownUsers.size > 0);
            assert(anySeesOther, 'Guests receive world state (w:add events)');
            guests.forEach(b => b.close());
            sentinel.close();
            await sleep(300);
        }

        // ── TEST 13: Non-existent void path → fast acc:fail ─────────────────
        console.log('\nTEST 13: Non-existent void path → instant acc:fail rejection');
        {
            const start = Date.now();
            const botGhost = new TestBot('TBot_Ghost', '_,_/does-not-exist-xyz999');
            allBots.push(botGhost);
            let rejected = false, gotAccFail = false;
            try {
                await botGhost.connect();
                botGhost.close();
            } catch (e) {
                rejected = true;
                gotAccFail = botGhost.hasEvent('acc:fail');
                const elapsed = Date.now() - start;
                console.log(`  ℹ  Rejected in ${elapsed}ms: ${e.message}`);
                assert(elapsed < 3000, `Rejection is fast (<3s, got ${elapsed}ms)`);
            }
            assert(rejected, 'Non-existent space rejects connection');
            assert(gotAccFail, 'Server sends acc:fail for unknown space');
        }
        await sleep(200);

        // ── TEST 14: Reconnect after disconnect ──────────────────────────────
        console.log('\nTEST 14: Reconnect after clean disconnect');
        {
            const botRecon = new TestBot('TBot_Recon', editorPath);
            allBots.push(botRecon);
            try {
                await botRecon.connect();
                await sleep(500);
                assert(botRecon.connected, 'Initial connection succeeds');
                botRecon.close();
                await sleep(800);
                assert(!botRecon.connected, 'Bot is disconnected');
                // Reconnect with a fresh bot on same path
                const botRecon2 = new TestBot('TBot_Recon2', editorPath);
                allBots.push(botRecon2);
                await botRecon2.connect();
                await sleep(500);
                assert(botRecon2.connected, 'Reconnect to same space succeeds');
                botRecon2.close();
            } catch (e) { assert(false, 'Reconnect', e.message); }
            await sleep(200);
        }

        // ── TEST 15: Path traversal blocked ─────────────────────────────────
        console.log('\nTEST 15: Path traversal in /api/space-overrides is blocked');
        {
            try {
                const r = await apiRequest('/api/space-overrides/test?path=uploads/../../config.json');
                assert(r.status === 400, `Traversal attempt rejected (got ${r.status})`);
                console.log(`  ℹ  Response: ${JSON.stringify(r.body)}`);
            } catch (e) { assert(false, 'Path traversal block', e.message); }

            try {
                const r2 = await apiRequest('/api/space-overrides/test?path=uploads/%2e%2e%2fconfig.json');
                assert(r2.status === 400, `URL-encoded traversal rejected (got ${r2.status})`);
            } catch (e) { assert(false, 'URL-encoded path traversal block', e.message); }

            try {
                const r3 = await apiRequest('/api/space-overrides/test?path=../config.json');
                assert(r3.status === 400, `Prefix-bypass traversal rejected (got ${r3.status})`);
            } catch (e) { assert(false, 'Prefix-bypass traversal block', e.message); }
        }

        // ── TEST 16: Admin endpoints require auth ────────────────────────────
        console.log('\nTEST 16: Admin endpoints enforce auth (401 without token)');
        {
            const adminRoutes = [
                '/api/admin/spaces',
                '/api/admin/emotes',
                '/api/admin/spaces/999',
            ];
            for (const route of adminRoutes) {
                try {
                    const r = await apiRequest(route);
                    assert(r.status === 401, `${route} → 401 without token (got ${r.status})`);
                } catch (e) { assert(false, `${route} auth check`, e.message); }
            }
            // Invalid token also rejected
            try {
                const r = await apiRequest('/api/admin/spaces', { token: 'not-a-valid-jwt' });
                assert(r.status === 401, `Invalid token → 401 (got ${r.status})`);
            } catch (e) { assert(false, 'Invalid token rejected', e.message); }
        }

        // ── TEST 17: GLB upload validates file type ──────────────────────────
        console.log('\nTEST 17: GLB upload rejects non-GLB files');
        {
            // No auth needed to test 401, but with auth to test actual validation
            try {
                // Without auth → 401 (not 500 or 200)
                const noAuth = await apiRequest('/api/admin/spaces/upload-glb', {
                    method: 'POST',
                    body: { filename: 'shell.php', data: btoa('<?php echo shell_exec($_GET["cmd"]); ?>') },
                });
                assert(noAuth.status === 401, `Upload without auth → 401 (got ${noAuth.status})`);
            } catch (e) { assert(false, 'Upload auth check', e.message); }

            if (ADMIN_TOKEN) {
                // Wrong extension
                try {
                    const r = await adminReq('/api/admin/spaces/upload-glb', {
                        method: 'POST',
                        body: { filename: 'evil.exe', data: btoa('MZ\x90\x00') },
                    });
                    assert(r.status === 400, `Non-GLB extension rejected (got ${r.status})`);
                    console.log(`  ℹ  ${r.body?.error}`);
                } catch (e) { assert(false, 'Non-GLB extension', e.message); }

                // Correct extension but wrong magic bytes
                try {
                    const r = await adminReq('/api/admin/spaces/upload-glb', {
                        method: 'POST',
                        body: { filename: 'fake.glb', data: btoa('NOTGLB\x00\x00') },
                    });
                    assert(r.status === 400, `Invalid GLB magic bytes rejected (got ${r.status})`);
                    console.log(`  ℹ  ${r.body?.error}`);
                } catch (e) { assert(false, 'Invalid GLB magic bytes', e.message); }
            } else {
                console.log('  ⚠  Full GLB validation tests skipped (no ADMIN_TOKEN)');
            }
        }

        // ── TEST 18: Race condition — 6 simultaneous bots ───────────────────
        console.log('\nTEST 18: Race condition — 6 simultaneous bots, same space');
        {
            const raceBots = Array.from({ length: 6 }, (_, i) => {
                const b = new TestBot(`Race_${i}`, editorPath);
                allBots.push(b);
                return b;
            });
            const raceResults = await Promise.allSettled(raceBots.map(b => b.connect()));
            await sleep(1200);
            const raceOk = raceResults.filter(r => r.status === 'fulfilled').length;
            assert(raceOk === 6, `All 6 simultaneous connections succeed (${raceOk}/6)`);
            const raceConnected = raceBots.filter(b => b.connected);
            if (raceConnected.length >= 2) {
                const first = raceConnected[0];
                const seen = raceConnected.filter(b => b !== first && first.getUsernames().includes(b.name)).length;
                console.log(`  ℹ  First bot sees ${seen}/${raceConnected.length - 1} peers`);
                assert(seen === raceConnected.length - 1, `First bot sees all ${raceConnected.length - 1} peers`);
            }
            raceBots.forEach(b => b.close());
            await sleep(300);
        }

        // ── TEST 19: w:remove fires when bot leaves ──────────────────────────
        console.log('\nTEST 19: w:remove event fires when a bot disconnects');
        {
            const watcher = new TestBot('Watcher', editorPath);
            const leaver  = new TestBot('Leaver', editorPath);
            allBots.push(watcher, leaver);
            try {
                await Promise.all([watcher.connect(), leaver.connect()]);
                await sleep(1000);
                const leaverKnownBefore = watcher.getUsernames().includes('Leaver');
                assert(leaverKnownBefore, 'Watcher sees Leaver before disconnect');
                leaver.close();
                await sleep(800);
                const leaverGone = !watcher.getUsernames().includes('Leaver');
                assert(leaverGone, 'Watcher no longer sees Leaver after disconnect');
                assert(watcher.hasEvent('w:rem'), 'w:rem event was received');
                watcher.close();
            } catch (e) { assert(false, 'w:remove on disconnect', e.message); }
            await sleep(200);
        }

        // ══════════════════════════════════════════════════════════════════
        //  Admin functional tests (require ADMIN_TOKEN)
        // ══════════════════════════════════════════════════════════════════

        const testSpace = spaces.find(s => s.name?.trim() && s.path);
        const testSpaceId = testSpace?.id;
        const testSpacePath = testSpace?.path;

        if (!ADMIN_TOKEN) {
            console.log('\nTESTs 20-25: Skipped — set ADMIN_TOKEN env var to run admin functional tests');
        } else if (!testSpaceId) {
            console.log('\nTESTs 20-25: Skipped (no usable editor spaces in DB)');
        } else {

            // ── TEST 20: Admin GET includes published/visibility ───────────
            console.log('\nTEST 20: Admin GET /api/admin/spaces — auth + fields');
            try {
                const r = await adminReq('/api/admin/spaces');
                assert(r.status === 200, `Authenticated request returns 200 (got ${r.status})`);
                const flat = [...(r.body?.editorSpaces ?? []), ...(r.body?.worldInstances ?? [])];
                assert(flat.length > 0, `Returns ${flat.length} entries`);
                const sample = flat.find(s => s.id === testSpaceId);
                if (sample) {
                    assert(sample.published !== undefined, `published field present (=${sample.published})`);
                    assert(sample.visibility !== undefined, `visibility field present (="${sample.visibility}")`);
                    assert(typeof sample.liveUsers === 'number', `liveUsers is numeric (=${sample.liveUsers})`);
                }
            } catch (e) { assert(false, 'Admin spaces authenticated', e.message); }

            const originalCount = spaces.length;

            // ── TEST 21: Unpublish → absent from /api/spaces ──────────────
            console.log(`\nTEST 21: PATCH published=false → disappears from /api/spaces`);
            try {
                const patch = await adminReq(`/api/admin/spaces/${testSpaceId}`, {
                    method: 'PATCH', body: { published: false },
                });
                assert(patch.status === 200 && patch.body?.success, `PATCH succeeds (status=${patch.status})`);
                await sleep(300);
                const after = (await fetchJson(`${API_BASE}/api/spaces`)).spaces ?? [];
                assert(after.length === originalCount - 1, `Count ${originalCount} → ${originalCount - 1} (got ${after.length})`);
                assert(!after.some(s => s.id === testSpaceId), 'Unpublished space absent from listing');
            } catch (e) { assert(false, 'Unpublish filter', e.message); }

            // ── TEST 22: Draft still connectable via WS ───────────────────
            console.log(`\nTEST 22: Draft space — WS still works when unpublished`);
            try {
                const botDraft = new TestBot('TBot_Draft', testSpacePath);
                allBots.push(botDraft);
                await botDraft.connect();
                await sleep(800);
                assert(botDraft.connected, 'Draft space accepts WS connections');
                assert(botDraft.hasEvent('acc:ok'), 'acc:ok received on draft space');
                botDraft.close();
                await sleep(200);
            } catch (e) { assert(false, 'Draft space WS access', e.message); }

            // ── TEST 23: Republish → returns ─────────────────────────────
            console.log(`\nTEST 23: PATCH published=true → space returns to listing`);
            try {
                const patch = await adminReq(`/api/admin/spaces/${testSpaceId}`, {
                    method: 'PATCH', body: { published: true },
                });
                assert(patch.status === 200, 'Republish PATCH succeeds');
                await sleep(300);
                const restored = (await fetchJson(`${API_BASE}/api/spaces`)).spaces ?? [];
                assert(restored.length === originalCount, `Count restored to ${originalCount} (got ${restored.length})`);
                assert(restored.some(s => s.id === testSpaceId), 'Space back in listing');
            } catch (e) { assert(false, 'Republish', e.message); }

            // ── TEST 24: visibility=unlisted ──────────────────────────────
            console.log(`\nTEST 24: visibility=unlisted → hidden from listing, WS works`);
            try {
                await adminReq(`/api/admin/spaces/${testSpaceId}`, {
                    method: 'PATCH', body: { visibility: 'unlisted' },
                });
                await sleep(300);
                const unlisted = (await fetchJson(`${API_BASE}/api/spaces`)).spaces ?? [];
                assert(!unlisted.some(s => s.id === testSpaceId), 'Unlisted space absent from directory');
                const botUnlisted = new TestBot('TBot_Unlisted', testSpacePath);
                allBots.push(botUnlisted);
                await botUnlisted.connect();
                await sleep(800);
                assert(botUnlisted.connected, 'Unlisted space still accepts WS connections');
                botUnlisted.close();
                await sleep(200);
            } catch (e) {
                assert(false, 'Unlisted visibility', e.message);
            } finally {
                await adminReq(`/api/admin/spaces/${testSpaceId}`, {
                    method: 'PATCH', body: { visibility: 'public', published: true },
                }).catch(() => {});
            }

            // ── TEST 25: Invalid visibility → 400 ────────────────────────
            console.log('\nTEST 25: Invalid visibility value → 400');
            try {
                const bad = await adminReq(`/api/admin/spaces/${testSpaceId}`, {
                    method: 'PATCH', body: { visibility: 'secret' },
                });
                assert(bad.status === 400, `Rejected with 400 (got ${bad.status})`);
                assert(!!bad.body?.error, `Error message present: "${bad.body?.error}"`);
            } catch (e) { assert(false, 'Invalid visibility 400', e.message); }
        }

        // ── TEST 26: /api/spaces stable ordering ──────────────────────────────
        console.log('\nTEST 26: /api/spaces stable ordering across consecutive calls');
        try {
            const r1 = await fetchJson(`${API_BASE}/api/spaces`);
            await sleep(100);
            const r2 = await fetchJson(`${API_BASE}/api/spaces`);
            const ids1 = (r1.spaces ?? []).map(s => s.id).join(',');
            const ids2 = (r2.spaces ?? []).map(s => s.id).join(',');
            assert(ids1 === ids2, 'Same spaces, same order on two consecutive calls');
        } catch (e) { assert(false, '/api/spaces consistency', e.message); }

        // ── TEST 27: Stress — 12 simultaneous bots across 3 spaces ───────────
        console.log('\nTEST 27: Stress — 12 bots spread across 3 spaces');
        {
            const stressPaths = ['0,0', editorPath, coordSpace?.path ?? editorPath];
            const stressBots = Array.from({ length: 12 }, (_, i) => {
                const b = new TestBot(`Stress_${i}`, stressPaths[i % stressPaths.length]);
                allBots.push(b);
                return b;
            });
            const stressResults = await Promise.allSettled(stressBots.map(b => b.connect()));
            await sleep(1500);
            const stressOk = stressResults.filter(r => r.status === 'fulfilled').length;
            assert(stressOk >= 10, `At least 10/12 stress bots connected (got ${stressOk}/12)`);
            console.log(`  ℹ  ${stressOk}/12 connected across ${stressPaths.length} spaces`);
            stressBots.forEach(b => b.close());
            await sleep(400);
        }

        // ── TEST 28: Ping latency sanity check ───────────────────────────────
        console.log('\nTEST 28: Ping/pong latency via bot');
        {
            const botPing = new TestBot('TBot_Ping', '0,0');
            allBots.push(botPing);
            try {
                await botPing.connect();
                await sleep(200);
                const start = Date.now();
                botPing._send({ h: 'ping', a: [] });
                await botPing.waitForEvent('pong', 3000).catch(() => null);
                const rtt = Date.now() - start;
                assert(botPing.hasEvent('pong') || rtt < 3000, `Ping round-trip acceptable (${rtt}ms)`);
                console.log(`  ℹ  RTT estimate: ${rtt}ms`);
                botPing.close();
            } catch (e) { assert(false, 'Ping latency', e.message); }
            await sleep(200);
        }

    } finally {
        console.log('\nCleaning up...');
        allBots.forEach(b => { try { b.close(); } catch {} });
        await sleep(300);
    }

    console.log('\n═══════════════════════════════════════════════════════');
    console.log(`  Results: ${passed} passed, ${failed} failed`);
    if (failures.length) {
        console.log('\n  Failures:');
        failures.forEach(f => console.log(`    ✗ ${f}`));
    }
    console.log('═══════════════════════════════════════════════════════\n');

    process.exit(failed > 0 ? 1 : 0);
}

runTests().catch(e => {
    console.error('Fatal:', e.message);
    process.exit(1);
});
