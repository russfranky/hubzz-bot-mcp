// Deterministic unit tests for the Hubzz bot protocol helpers.
// These mirror the server's authoritative implementations:
//   - packages/server/src/Messages/moveTiming.ts (stepDurationMs)
//   - packages/server/src/Messages/Move.ts (w:move broadcast, rotation)
// Run: node --test test-protocol.mjs
import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  bearingFromDelta,
  compassFromDelta,
  dist2,
  facingFromRotation,
  stepDurationMs,
} from './bot-mcp.mjs';

// --- bearing / compass ---
// Convention: 0 deg = East (+X), 90 = North (-Z), clockwise-positive.

test('bearing: cardinal directions', () => {
  assert.equal(Math.round(bearingFromDelta(1, 0)), 0);    // +X = East
  assert.equal(Math.round(bearingFromDelta(0, -1)), 90);   // -Z = North
  assert.equal(Math.round(bearingFromDelta(-1, 0)), 180);  // -X = West
  assert.equal(Math.round(bearingFromDelta(0, 1)), 270);   // +Z = South
});

test('bearing: diagonals', () => {
  assert.equal(Math.round(bearingFromDelta(1, -1)), 45);   // NE
  assert.equal(Math.round(bearingFromDelta(1, 1)), 315);   // SE
});

test('compass: 8-way labels', () => {
  assert.equal(compassFromDelta(1, 0), 'E');
  assert.equal(compassFromDelta(1, -1), 'NE');
  assert.equal(compassFromDelta(0, -1), 'N');
  assert.equal(compassFromDelta(-1, -1), 'NW');
  assert.equal(compassFromDelta(-1, 0), 'W');
  assert.equal(compassFromDelta(-1, 1), 'SW');
  assert.equal(compassFromDelta(0, 1), 'S');
  assert.equal(compassFromDelta(1, 1), 'SE');
});

test('dist2: euclidean', () => {
  assert.equal(dist2(0, 0, 3, 4), 5);
  assert.equal(dist2(36, 14, 38, 14), 2);
});

// --- facing (server yaw = atan2(dir.x, dir.z)) ---

test('facing: from server rotation yaw', () => {
  // Moving +X (east): yaw = atan2(1, 0) = PI/2 → faces E
  assert.equal(facingFromRotation({ x: 0, y: Math.PI / 2, z: 0 }), 'E');
  // Moving -Z (north): yaw = atan2(0, -1) = PI → faces N
  assert.equal(facingFromRotation({ x: 0, y: Math.PI, z: 0 }), 'N');
  // Moving +Z (south): yaw = atan2(0, 1) = 0 → faces S
  assert.equal(facingFromRotation({ x: 0, y: 0, z: 0 }), 'S');
  // Moving -X (west): yaw = atan2(-1, 0) = -PI/2 → faces W
  assert.equal(facingFromRotation({ x: 0, y: -Math.PI / 2, z: 0 }), 'W');
});

test('facing: null-safe', () => {
  assert.equal(facingFromRotation(null), null);
  assert.equal(facingFromRotation({}), null);
  assert.equal(facingFromRotation({ y: NaN }), null);
});

// --- step timing (mirrors server moveTiming.ts) ---

test('stepDurationMs: cardinal = base', () => {
  // Tile size 2: a 2-unit orthogonal step takes exactly baseStepMs.
  assert.equal(stepDurationMs({ x: 0, z: 0 }, { x: 2, z: 0 }, 500, 2), 500);
});

test('stepDurationMs: diagonal = base * sqrt(2)', () => {
  // 2.828-unit diagonal → 707ms. This is the 707 observed live.
  assert.equal(stepDurationMs({ x: 0, z: 0 }, { x: 2, z: 2 }, 500, 2), 707);
});

test('stepDurationMs: boost halves it', () => {
  assert.equal(stepDurationMs({ x: 0, z: 0 }, { x: 2, z: 0 }, 250, 2), 250);
  assert.equal(stepDurationMs({ x: 0, z: 0 }, { x: 2, z: 2 }, 250, 2), 354);
});

test('stepDurationMs: never below 1ms', () => {
  assert.equal(stepDurationMs({ x: 0, z: 0 }, { x: 0, z: 0 }, 500, 2), 1);
});

// --- w:move broadcast parsing ---
// Real server format: {id, g, t, st, s, r[, a][, b]}.
// The parser lives in the w:move handler; these document the contract.

function parseMoveBroadcast(data) {
  // Mirrors the handler's parsing logic for test purposes.
  let userId, tileId, arrived, stepMs;
  if (typeof data === 'object' && data.t != null) {
    userId = String(data.id);
    tileId = Number(data.t);
    arrived = data.st === true;
    stepMs = Number(data.s) || null;
  } else {
    userId = String(data);
    tileId = NaN;
    arrived = false;
    stepMs = null;
  }
  return { userId, tileId, arrived, stepMs };
}

test('w:move: step echo parses', () => {
  const p = parseMoveBroadcast({ id: 562, g: 'a', t: 3047, st: false, s: 500, r: {} });
  assert.equal(p.userId, '562');
  assert.equal(p.tileId, 3047);
  assert.equal(p.arrived, false);
  assert.equal(p.stepMs, 500);
});

test('w:move: arrival echo parses', () => {
  const p = parseMoveBroadcast({ id: 562, g: 'a', t: 2465, st: true, s: 707, r: {} });
  assert.equal(p.arrived, true);
  assert.equal(p.stepMs, 707);
});

test('w:move: no-path signal has no s field', () => {
  // Server broadcasts {id, g, t: from.id, st: true, r} with NO s when
  // findPath fails — distinguishable from a real arrival.
  const p = parseMoveBroadcast({ id: 562, g: 'a', t: 3175, st: true, r: { x: 0, y: 1.5, z: 0 } });
  assert.equal(p.arrived, true);
  assert.equal(p.stepMs, null);
});
