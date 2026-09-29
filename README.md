# hubzz-bot-mcp

MCP server for spawning and controlling bots in [Hubzz](https://hubzz.xyz) worlds. Drive bot avatars over the Hubzz game protocol: connect, move, chat, emote, play spatial audio, and run load tests — all as callable tools from any MCP client.

Built for three jobs: **spatial audio testing** (directional rings, distance grids, live config tuning), **stress testing** (batch spawns, scripted connect/chat/move runs), and **world observation** (snapshots, event watching, assertions).

## Quickstart

```bash
npm install
export HUBZZ_BOT_TOKEN=<your-bot-token>   # required
node bot-mcp.mjs                            # MCP server on stdio
```

Point at a different world server with `HUBZZ_WS_URL` (default `wss://hubzz.xyz/socket/`).

Remote MCP clients can reach the server over the network through the HTTP/SSE wrapper (each SSE connection spawns an isolated bot-mcp child process):

```bash
BOT_MCP_PORT=37778 BOT_MCP_TOKEN=<secret> npm run start:http
npm test   # runs the wrapper tests
```

## Example session

`bot_spawn` a QA bot, `bot_move` it across tiles, `bot_chat` to verify delivery, `bot_directional_ring` to check left/right panning, `bot_stress_test` to spin up 20 bots for a mixed load run, `bot_close_all` to clean up.

## What it does

- **Lifecycle** — `bot_spawn`, `bot_spawn_at`, `bot_guest`, `bot_close`, `bot_close_all`, `bot_list`, `bot_batch_spawn`
- **Movement & actions** — `bot_move`, `bot_patrol`, `bot_rotate`, `bot_chat`, `bot_typing`, `bot_dance`, `bot_emote_loop`, `bot_set_avatar`, `bot_nick`, `bot_screen`, `bot_upload`, moderation probes (`bot_kick_test`)
- **Observation** — `bot_look`, `bot_observe`, `bot_subscribe`, `bot_report`, event watching (`bot_watch_events`), world conditions (`bot_world_wait`), assertions (`bot_assert`), ping latency (`bot_ping_latency`)
- **Load testing** — `bot_stress_test` (spawn N bots, run a scripted connect/chat/move/mixed test, auto-cleanup)
- **Voice & spatial audio** — `bot_voice`, `bot_audio_start`/`bot_audio_stop`/`bot_audio_tune` (mediasoup RTC, sine-wave tones), directional rings (`bot_directional_ring`), distance grids (`bot_spatial_grid`), tile finding (`bot_find_tiles`), scene manager (`bot_scene_audio`), live spatial-config pushes (`bot_push_config`)

## Protocol notes

- MCP transport: stdio, newline-delimited JSON-RPC 2.0.
- Game protocol: JSON frames with a U+F8FF delimiter over WebSocket.
- The server exits on startup if `HUBZZ_BOT_TOKEN` is not set. The token is read from the environment only — never commit one.

## Repo layout

| File | Purpose |
|---|---|
| `bot-mcp.mjs` | MCP stdio server (main entry, `npm start`) |
| `bot-mcp-http.mjs` | HTTP/SSE wrapper for remote MCP clients (`npm run start:http`) |
| `bot-mcp.cjs` | CommonJS bundle of the server |
| `test-http-wrapper.mjs` | Wrapper tests (`npm test`) |
| `launch-audio-test.mjs` | Spatial audio test scene: 4 tone bots at 5/10/20/30u + a conductor bot |
| `hubzz-test.mjs` | Staging integration test suite (WebSocket bot tests) |
| `daily-regression-test.mjs` | Daily regression run for hubzz-alpha |
| `hubzz-new-features-test.mjs` | Integration tests for new world features |
| `media-test.mjs` | Media screen pipeline, queue, and permission tests |
| `voice-indicator-playwright-test.mjs` | Playwright E2E for the voice mic indicator and nametags |

See `CLAUDE.md` for the full tool reference.
