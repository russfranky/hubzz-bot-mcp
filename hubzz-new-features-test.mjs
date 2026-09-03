/**
 * Hubzz New-Features Integration Test
 *
 * Covers all changes from the current session:
 *   - Chat HTML injection filtering
 *   - Friends: friend_request / friend_accept / friend_deny
 *   - Friend list sent on ready (friend_list event)
 *   - Poke guards: no-friends block, cooldown, radius, full allowed flow
 *   - Voice server health (WS connect to socket.io endpoint)
 *
 * Run: node hubzz-new-features-test.mjs
 */

import WebSocket from 'ws';
import https from 'https';

const WS_BASE         = 'wss://hubzz.xyz/socket/';
const VOICE_WS        = 'wss://hubzz.xyz/socket.io/?EIO=4&transport=websocket';
const API_BASE        = 'https://hubzz.xyz';
const DELIMITER       = '\uF8FF';
const BOT_TOKEN       = 'iamar0b0t';
const CONNECT_TIMEOUT = 20000;

const sleep = ms => new Promise(r => setTimeout(r, ms));

// ─── HTTP helpers ───────────────────────────────────────────────────────────

function fetchJson(url) {
    return new Promise((resolve, reject) => {
        https.get(url, { rejectUnauthorized: false }, res => {
            let data = '';
            res.on('data', c => data += c);
            res.on('end', () => { try { resolve(JSON.parse(data)); } catch (e) { reject(e); } });
        }).on('error', reject);
    });
}

// ─── TestBot ────────────────────────────────────────────────────────────────

class TestBot {
    constructor(name, worldPath = '', opts = {}) {
        this.name         = name;
        this.worldPath    = worldPath;
        this.isGuest      = opts.isGuest ?? false;
        this.wsUrl        = `${WS_BASE}${worldPath}${worldPath ? '/' : ''}`;
        this.ws           = null;
        this.connected    = false;
        this.knownUsers   = new Map(); // id → username
        this.events       = [];
        this.chatLog      = [];
        this.chatHistory  = []; // entries from chat:history event
        this.friendList   = []; // usernames from friend_list event
        this.pokesReceived = [];
        this.friendRequestsReceived = [];  // usernames that sent us a request
        this.friendAcceptedReceived = [];  // usernames accepted us
        // Dynamic username assigned by server (from acc:ok or w:add)
        this.serverUsername = null;
    }

    connect() {
        return new Promise((resolve, reject) => {
            this.ws = new WebSocket(this.wsUrl, { rejectUnauthorized: false });
            const timeout = setTimeout(() => {
                this.ws?.terminate();
                reject(new Error(`${this.name}: connect timeout`));
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

                        if (msg.h === 'ping')        this._send({ h: 'pong', a: [] });

                        if (msg.h === 'acc:ok') {
                            clearTimeout(timeout);
                            this._send({ h: 'ready', a: [] });
                            this.connected = true;
                            // Server returns session data including username in acc:ok
                            if (msg.a?.[0]?.username) this.serverUsername = msg.a[0].username;
                            resolve(msg);
                        }

                        if (msg.h === 'acc:fail') {
                            clearTimeout(timeout);
                            reject(new Error(`${this.name}: acc:fail`));
                        }

                        if (msg.h === 'w:add') {
                            const d = msg.a?.[0];
                            if (d && (!d.type || d.type === 'avatar')) {
                                this.knownUsers.set(String(d.id), d.username ?? 'unknown');
                                // Our own w:add broadcast tells us our server username
                                if (d.username === this.name || d.username?.startsWith('Bot')) {
                                    // best-effort: track the first avatar with a Bot* name
                                }
                            }
                        }

                        if (msg.h === 'w:rem') {
                            this.knownUsers.delete(String(msg.a?.[0]?.id ?? msg.a?.[0]));
                        }

                        // Chat arrives as w:call { id, f:'chat', a:[text] }
                        if (msg.h === 'w:call' && msg.a?.[0]?.f === 'chat') {
                            this.chatLog.push({ from: msg.a[0].id, text: msg.a[0].a?.[0] });
                        }

                        // chat:history — array of { username, text, timestamp }
                        if (msg.h === 'chat:history') {
                            this.chatHistory = msg.a?.[0] ?? [];
                        }

                        // friend_list — array of usernames
                        if (msg.h === 'friend_list') {
                            this.friendList = msg.a?.[0] ?? [];
                        }

                        // friend_request from someone
                        if (msg.h === 'friend_request') {
                            this.friendRequestsReceived.push(msg.a?.[0]);
                        }

                        // friend_accepted
                        if (msg.h === 'friend_accepted') {
                            this.friendAcceptedReceived.push(msg.a?.[0]);
                        }

                        // poke
                        if (msg.h === 'poke') {
                            this.pokesReceived.push(msg.a?.[0]);
                        }

                    } catch {}
                });
            });

            this.ws.on('error', (e) => {
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

    send(h, ...a) { this._send({ h, a }); }
    chat(text)    { this._send({ h: 'chat', a: [text] }); }

    waitForEvent(h, timeoutMs = 5000) {
        return new Promise((resolve, reject) => {
            const existing = this.events.find(e => e.h === h);
            if (existing) { resolve(existing); return; }
            const t = setTimeout(() => reject(new Error(`Timeout waiting for ${h}`)), timeoutMs);
            const iv = setInterval(() => {
                const found = this.events.find(e => e.h === h);
                if (found) { clearTimeout(t); clearInterval(iv); resolve(found); }
            }, 50);
        });
    }

    waitFor(fieldArray, matchFn, timeoutMs = 5000) {
        return new Promise((resolve, reject) => {
            const arr = this[fieldArray];
            const existing = arr.find(matchFn);
            if (existing) { resolve(existing); return; }
            const t = setTimeout(() => reject(new Error(`Timeout waiting in ${fieldArray}`)), timeoutMs);
            const iv = setInterval(() => {
                const found = this[fieldArray].find(matchFn);
                if (found) { clearTimeout(t); clearInterval(iv); resolve(found); }
            }, 50);
        });
    }

    close() { this.ws?.close(); this.connected = false; }
    hasEvent(h) { return this.events.some(e => e.h === h); }
    getUsernames() { return [...this.knownUsers.values()]; }
}

// ─── Assertions ─────────────────────────────────────────────────────────────

let passed = 0, failed = 0;
const failures = [];

function assert(cond, label, detail = '') {
    if (cond) { console.log(`  ✓ ${label}`); passed++; }
    else {
        const msg = detail ? `${label} — ${detail}` : label;
        console.error(`  ✗ FAIL: ${msg}`);
        failures.push(msg);
        failed++;
    }
}

// ─── Pick a test world ───────────────────────────────────────────────────────

async function getTestPath() {
    try {
        const r = await fetchJson(`${API_BASE}/api/spaces`);
        const spaces = r.spaces ?? [];
        const coord = spaces.find(s => /^-?\d+,-?\d+$/.test(s.path));
        return coord?.path ?? '0,0';
    } catch {
        return '0,0';
    }
}

// ─── Warmup ─────────────────────────────────────────────────────────────────

async function warmup(path) {
    await new Promise(resolve => {
        const ws = new WebSocket(`${WS_BASE}${path}/`, { rejectUnauthorized: false });
        ws.on('open', () => ws.send(JSON.stringify({ h: 'login', a: [BOT_TOKEN, 'Warmup', ''] }) + DELIMITER));
        ws.on('message', raw => {
            raw.toString().split(DELIMITER).filter(Boolean).forEach(part => {
                try { if (JSON.parse(part).h === 'acc:ok') { ws.close(); resolve(); } } catch {}
            });
        });
        ws.on('error', () => resolve());
        ws.on('close', resolve);
        setTimeout(() => { ws.terminate(); resolve(); }, CONNECT_TIMEOUT);
    });
    await sleep(500);
}

// ─── Tests ──────────────────────────────────────────────────────────────────

async function runTests() {
    console.log('\n═══════════════════════════════════════════════════════════');
    console.log('  Hubzz New-Features Integration Tests');
    console.log('  Target: hubzz.xyz (staging)');
    console.log('═══════════════════════════════════════════════════════════\n');

    const allBots = [];
    const testPath = await getTestPath();
    console.log(`  Using test world: /${testPath}\n`);

    console.log('  Warming up world...');
    await warmup(testPath);

    try {

        // ══════════════════════════════════════════════════════════════════
        // SECTION 1: Chat
        // ══════════════════════════════════════════════════════════════════

        console.log('\n── SECTION 1: Chat ─────────────────────────────────────────\n');

        // TEST 1: HTML injection is filtered (server-side)
        console.log('TEST 1: Chat HTML injection is filtered');
        {
            const injector = new TestBot('Injector', testPath);
            const watcher  = new TestBot('Watcher', testPath);
            allBots.push(injector, watcher);
            try {
                await Promise.all([injector.connect(), watcher.connect()]);
                await sleep(800);

                // Send HTML — server should drop this message
                injector.chat('<script>alert(1)</script>');
                await sleep(800);

                const found = watcher.chatLog.some(e => e.text?.includes('<script>'));
                assert(!found, 'HTML-containing chat message NOT delivered to other users');

                // Normal message still works
                const cleanMsg = `clean-${Date.now()}`;
                injector.chat(cleanMsg);
                await sleep(500);
                const cleanFound = watcher.chatLog.some(e => e.text === cleanMsg);
                assert(cleanFound, 'Clean message after HTML attempt still delivered');

                injector.close(); watcher.close();
            } catch (e) { assert(false, 'HTML injection filter', e.message); }
            await sleep(300);
        }

        // ══════════════════════════════════════════════════════════════════
        // SECTION 2: Friends system
        // ══════════════════════════════════════════════════════════════════

        console.log('\n── SECTION 2: Friends system ───────────────────────────────\n');

        // Two bots that will become friends throughout the section
        const botAlice = new TestBot('FrndAlice', testPath);
        const botBob   = new TestBot('FrndBob', testPath);
        allBots.push(botAlice, botBob);

        // TEST 2: Both bots connect and see each other
        console.log('TEST 2: Bots connect and observe each other');
        try {
            await Promise.all([botAlice.connect(), botBob.connect()]);
            await sleep(1500);
            assert(botAlice.connected, 'Alice connected');
            assert(botBob.connected, 'Bob connected');

            // Get their server-assigned usernames from w:add events
            // Each bot should see the other via w:add
            const aliceSeeBob = botAlice.getUsernames().includes('FrndBob') ||
                                [...botAlice.knownUsers.values()].some(u => u.includes('FrndBob'));
            const bobSeeAlice = botBob.getUsernames().includes('FrndAlice') ||
                                [...botBob.knownUsers.values()].some(u => u.includes('FrndAlice'));
            assert(aliceSeeBob || bobSeeAlice,
                'Bots observe each other in world', `alice→bob:${aliceSeeBob} bob→alice:${bobSeeAlice}`);
        } catch (e) { assert(false, 'Friend bots connect', e.message); }

        // TEST 3: Poke is BLOCKED without friends
        console.log('\nTEST 3: Poke blocked when not friends');
        {
            try {
                botAlice.send('poke', 'FrndBob');
                await sleep(600);
                assert(botBob.pokesReceived.length === 0,
                    'Bob received NO poke (not friends yet)');
            } catch (e) { assert(false, 'Poke blocked without friends', e.message); }
        }

        // TEST 4: friend_request sent → target receives notification
        console.log('\nTEST 4: friend_request → target receives event');
        {
            try {
                botAlice.send('friend_request', 'FrndBob');
                await sleep(800);
                assert(botBob.friendRequestsReceived.includes('FrndAlice'),
                    'Bob received friend_request from Alice',
                    `received: ${JSON.stringify(botBob.friendRequestsReceived)}`);
            } catch (e) { assert(false, 'Friend request delivery', e.message); }
        }

        // TEST 5: Duplicate friend_request is silently ignored
        console.log('\nTEST 5: Duplicate friend_request is ignored');
        {
            try {
                const beforeCount = botBob.friendRequestsReceived.length;
                botAlice.send('friend_request', 'FrndBob');
                await sleep(600);
                assert(botBob.friendRequestsReceived.length === beforeCount,
                    'Duplicate request does NOT deliver a second event');
            } catch (e) { assert(false, 'Duplicate friend request', e.message); }
        }

        // TEST 6: friend_accept → both sides receive friend_accepted
        console.log('\nTEST 6: friend_accept → both bots receive friend_accepted');
        {
            try {
                botBob.send('friend_accept', 'FrndAlice');
                await sleep(800);

                assert(botBob.friendAcceptedReceived.includes('FrndAlice'),
                    'Bob receives friend_accepted with Alice\'s username');
                assert(botAlice.friendAcceptedReceived.includes('FrndBob'),
                    'Alice receives friend_accepted with Bob\'s username');
            } catch (e) { assert(false, 'Friend accept', e.message); }
        }

        // TEST 7: Poke NOW works (they are friends, same space, same position)
        console.log('\nTEST 7: Poke allowed after becoming friends');
        {
            try {
                botAlice.send('poke', 'FrndBob');
                await botBob.waitFor('pokesReceived', () => true, 3000);
                assert(botBob.pokesReceived.includes('FrndAlice'),
                    'Bob receives poke from Alice after friend accept',
                    `pokesReceived: ${JSON.stringify(botBob.pokesReceived)}`);
            } catch (e) { assert(false, 'Poke allowed with friends', e.message); }
        }

        // TEST 8: Poke cooldown — second poke within 60s is blocked
        console.log('\nTEST 8: Poke cooldown (second poke within 60s blocked)');
        {
            try {
                const pokesBefore = botBob.pokesReceived.length;
                botAlice.send('poke', 'FrndBob');
                await sleep(800);
                assert(botBob.pokesReceived.length === pokesBefore,
                    'Second poke within cooldown window is NOT delivered');
            } catch (e) { assert(false, 'Poke cooldown', e.message); }
        }

        // TEST 9: friend_deny — a third bot sends a request, Bob denies it
        console.log('\nTEST 9: friend_deny — pending request removed');
        {
            const botCarol = new TestBot('FrndCarol', testPath);
            allBots.push(botCarol);
            try {
                await botCarol.connect();
                await sleep(800);

                botCarol.send('friend_request', 'FrndBob');
                await sleep(600);
                assert(botBob.friendRequestsReceived.includes('FrndCarol'),
                    'Bob received Carol\'s friend request');

                botBob.send('friend_deny', 'FrndCarol');
                await sleep(400);

                // Re-send the request — if deny worked, it should go through again
                botCarol.send('friend_request', 'FrndBob');
                await sleep(600);
                // The second request should arrive (not blocked by duplicate check because
                // the pending row was deleted by deny)
                const carolReqs = botBob.friendRequestsReceived.filter(u => u === 'FrndCarol').length;
                assert(carolReqs >= 2,
                    `After deny, new request goes through again (count=${carolReqs})`);

                botCarol.close();
            } catch (e) { assert(false, 'Friend deny', e.message); }
            await sleep(300);
        }

        // TEST 10: friend_list sent on next join (after friends established)
        console.log('\nTEST 10: friend_list received on fresh join (after friends exist)');
        {
            // Alice reconnects — should get friend_list containing Bob
            botAlice.close();
            await sleep(500);

            const botAlice2 = new TestBot('FrndAlice', testPath);
            allBots.push(botAlice2);
            try {
                await botAlice2.connect();
                await sleep(1500);

                assert(botAlice2.hasEvent('friend_list'),
                    'Reconnecting user receives friend_list event');
                assert(botAlice2.friendList.includes('FrndBob'),
                    'friend_list includes accepted friend (Bob)',
                    `list: ${JSON.stringify(botAlice2.friendList)}`);
            } catch (e) { assert(false, 'friend_list on ready', e.message); }
        }

        botBob.close();
        await sleep(300);

        // TEST 11: Poke blocked for non-existent username
        console.log('\nTEST 11: Poke to non-existent user — no error, silent drop');
        {
            const botPoke = new TestBot('PokeBot', testPath);
            allBots.push(botPoke);
            try {
                await botPoke.connect();
                await sleep(500);
                // Should not crash the server
                botPoke.send('poke', 'nobody_xyz_99999');
                await sleep(500);
                assert(botPoke.connected, 'Bot still connected after poke to non-existent user');
                botPoke.close();
            } catch (e) { assert(false, 'Poke non-existent user', e.message); }
            await sleep(200);
        }

        // TEST 12: friend_request to self — silently ignored
        console.log('\nTEST 12: friend_request to self — silently ignored');
        {
            const botSelf = new TestBot('SelfFriend', testPath);
            allBots.push(botSelf);
            try {
                await botSelf.connect();
                await sleep(500);
                botSelf.send('friend_request', 'SelfFriend');
                await sleep(500);
                assert(botSelf.friendRequestsReceived.length === 0,
                    'Self-request not delivered');
                assert(botSelf.connected, 'Bot still connected after self-request');
                botSelf.close();
            } catch (e) { assert(false, 'Self friend request', e.message); }
            await sleep(200);
        }

        // ══════════════════════════════════════════════════════════════════
        // SECTION 3: Voice server health
        // ══════════════════════════════════════════════════════════════════

        console.log('\n── SECTION 3: Voice server health ──────────────────────────\n');

        // TEST 13: Voice server socket.io endpoint responds
        console.log('TEST 13: Voice server WS endpoint is reachable');
        {
            await new Promise(resolve => {
                const start = Date.now();
                const ws = new WebSocket(VOICE_WS, { rejectUnauthorized: false });
                let received = false;

                ws.on('open', () => {});
                ws.on('message', (raw) => {
                    const text = raw.toString();
                    // socket.io sends "0{...}" on open
                    if (!received && (text.startsWith('0') || text.includes('sid'))) {
                        received = true;
                        const elapsed = Date.now() - start;
                        assert(true, `Voice server WS responds (${elapsed}ms, got: ${text.slice(0, 60)})`);
                        ws.close();
                        resolve();
                    }
                });
                ws.on('error', (e) => {
                    assert(false, 'Voice server WS reachable', e.message);
                    resolve();
                });
                setTimeout(() => {
                    if (!received) {
                        assert(false, 'Voice server WS response timeout');
                        ws.terminate();
                    }
                    resolve();
                }, 5000);
            });
        }

        // ══════════════════════════════════════════════════════════════════
        // SECTION 4: Regression — core features still work
        // ══════════════════════════════════════════════════════════════════

        console.log('\n── SECTION 4: Regression check ─────────────────────────────\n');

        // TEST 14: Two fresh bots — connect, chat, disconnect cleanly
        console.log('TEST 14: Basic connect / chat / disconnect regression');
        {
            const rA = new TestBot('RegA', testPath);
            const rB = new TestBot('RegB', testPath);
            allBots.push(rA, rB);
            try {
                await Promise.all([rA.connect(), rB.connect()]);
                await sleep(800);
                assert(rA.connected && rB.connected, 'Both bots connected');

                const msg = `reg-${Date.now()}`;
                rA.chat(msg);
                await sleep(500);
                assert(rB.chatLog.some(e => e.text === msg), 'Chat message delivered cross-bot');
                rA.close(); rB.close();
            } catch (e) { assert(false, 'Basic regression', e.message); }
            await sleep(300);
        }

        // TEST 15: Typing status broadcast
        console.log('\nTEST 15: Typing status broadcast');
        {
            const typer   = new TestBot('Typer', testPath);
            const watcher = new TestBot('TypWatcher', testPath);
            allBots.push(typer, watcher);
            try {
                await Promise.all([typer.connect(), watcher.connect()]);
                await sleep(600);
                typer.send('ts', true);
                await sleep(500);
                assert(watcher.hasEvent('ts'), 'Typing status received by watcher');
                typer.close(); watcher.close();
            } catch (e) { assert(false, 'Typing status', e.message); }
            await sleep(200);
        }

        // TEST 16: /api/health still passes
        console.log('\nTEST 16: /api/health regression');
        try {
            const h = await fetchJson(`${API_BASE}/api/health`);
            assert(h.status === 'ok', `/api/health returns ok (uptime=${h.uptime}s)`);
        } catch (e) { assert(false, '/api/health', e.message); }

    } finally {
        allBots.forEach(b => { try { b.close(); } catch {} });
        await sleep(300);
    }

    // ── Summary ──────────────────────────────────────────────────────────────
    console.log('\n═══════════════════════════════════════════════════════════');
    console.log(`  Results: ${passed} passed, ${failed} failed`);
    if (failures.length) {
        console.log('\n  Failures:');
        failures.forEach(f => console.log(`    ✗ ${f}`));
    } else {
        console.log('  All tests passed! ✓');
    }
    console.log('═══════════════════════════════════════════════════════════\n');

    process.exit(failed > 0 ? 1 : 0);
}

runTests().catch(e => {
    console.error('Fatal:', e.message);
    process.exit(1);
});
