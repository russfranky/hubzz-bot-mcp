# CLAUDE.md — bot-mcp

## What This Is

MCP stdio server (`bot-mcp.mjs`) that exposes tools for spawning and controlling bots in Hubzz worlds. Used for spatial audio testing, stress testing, and general world observation.

- **Protocol:** MCP stdio JSON-RPC 2.0
- **Game protocol:** JSON + U+F8FF delimiter over WebSocket
- **Default target:** `wss://hubzz.xyz/socket/0,0/` (staging)

## Running

```bash
node bot-mcp.mjs          # MCP server (stdio)
node launch-audio-test.mjs  # Launch full spatial audio test scene
```

The MCP server is registered in Claude Code's MCP config and runs as a background process (PID visible via `ps aux | grep bot-mcp`).

## Tool Reference

### Bot Lifecycle
| Tool | Description |
|------|-------------|
| `bot_spawn` | Connect a bot to a world |
| `bot_spawn_at` | Spawn + move to tile in one call |
| `bot_close` | Disconnect a bot |
| `bot_close_all` | Kill all bots |
| `bot_list` | List all active bots and status |
| `bot_batch_spawn` | Spawn N bots with staggered connections |

### Movement & Actions
| Tool | Description |
|------|-------------|
| `bot_move` | Move bot to a tile ID |
| `bot_patrol` | Walk a tile route on a timer (loops) |
| `bot_chat` | Send a chat message |
| `bot_dance` | Play any animation |
| `bot_emote` | Legacy animation (use bot_dance) |
| `bot_set_avatar` | Change VRM or shuffle avatar |
| `bot_nick` | Change display name via !nick |

### Observation
| Tool | Description |
|------|-------------|
| `bot_look` | Quick world state snapshot |
| `bot_observe` | Full state: users, chat, events, latency |
| `bot_subscribe` | Subscribe bot to collect specific event types |
| `bot_report` | Summary/detailed report across all bots |

### Load Testing
| Tool | Description |
|------|-------------|
| `bot_stress_test` | Spawn N bots, run connect/chat/move/mixed test, auto-cleanup |

### Voice & Audio
| Tool | Description |
|------|-------------|
| `bot_voice` | Toggle voice indicator (mic on/off). Accepts `state` or `voiceState`. |
| `bot_voice_all` | Toggle voice on all connected bots |
| `bot_audio_start` | Connect bot to mediasoup RTC, produce sine wave tone |
| `bot_audio_stop` | Stop audio production |
| `bot_audio_tune` | Hot-swap frequency or gain while producing |
| `bot_audio_status` | Get RTC status for one or all bots |

### Spatial Audio Testing
| Tool | Description |
|------|-------------|
| `bot_find_tiles` | Find walkable tiles at target distances from center. Supports `direction` param (N/S/E/W/NE/NW/SE/SW or degrees) to constrain to a compass sector. |
| `bot_spatial_grid` | Spawn bots at specific tiles with voice on — manual distance grid |
| `bot_directional_ring` | Place 4 or 8 bots in a circle at equal distance, evenly spaced compass directions, each with a distinct tone. **The primary tool for testing left/right/front/behind panning in FPP.** |
| `bot_scene_audio` | All-in-one scene manager — see below |

### Spatial Config (Live Tuning)
| Tool | Description |
|------|-------------|
| `bot_push_config` | Push `spatialAudioConfig` to all clients via a connected bot. Updates `RTCClient.SpatialConfig` live. |
| `bot_conductor_start` | Spawn a conductor bot that listens to world chat and processes tuning commands |
| `bot_conductor_stop` | Stop and disconnect the conductor bot |

## bot_scene_audio Actions

The `bot_scene_audio` tool manages the full spatial audio test lifecycle:

| Action | What it does |
|--------|-------------|
| `setup` | Teardown existing → spawn 4 distance bots (5u/10u/20u/30u) + conductor |
| `setup_ring` | Teardown existing → spawn 4 directional bots (N/E/S/W at 10u) + conductor |
| `status` | Show all bots, audio sessions, conductor state, current spatial config |
| `teardown` | Kill everything |

## Conductor Bot Chat Commands

When the conductor bot is running, type these in Hubzz world chat:

```
!ref N        → set refDistance (default 1)
!rolloff N    → set rolloffFactor (default 0.75)
!vol N        → set FPP volume multiplier
!isovol N     → set isometric volume
!louder       → FPP volume +25%
!quieter      → FPP volume -20%
!louder <bot> → that bot's tone gain +25%
!quieter <bot>→ that bot's tone gain -20%
!status       → print current config
!reset        → reset to defaults
!help         → show all commands
```

## Spatial Audio Architecture

Two modes, switched automatically on FPP toggle:

**Isometric** → Global audio. `PositionalAudio` reparented to `world.listener` at position 0,0,0. No distance falloff. Volume controlled by `RTCClient.SpatialConfig.isometric.volume`.

**First Person (FPP)** → True spatial audio. `PositionalAudio` attached to remote avatar. `AudioListener` parented to `firstPersonCamera` so orientation updates as you look around. Falloff model:

```
gain = (refDistance / max(refDistance, distance)) ^ rolloffFactor
```

Default: `refDistance=1, rolloffFactor=0.75, distanceModel=exponential, volume=1.0`

Config changes pushed via `bot_push_config` are received by `World.ts → connection.on("spatialAudioConfig")`, merged into `RTCClient.SpatialConfig`, then `rtcSettingsUpdated` fires so all active `PositionalAudio` nodes re-apply immediately.

## Directional Ring Test Layout

```
         dir-n (220Hz)
              |
dir-w --------+-------- dir-e (330Hz)
(440Hz)       |
         dir-s (550Hz)
```

Stand at world center tile, enter FPP, rotate 360°. Each direction has a distinct tone — panning should clearly shift as you turn. Use `!ref` and `!rolloff` to tune while walking.

## World Coordinate System

- Tile IDs: sequential in a 64×64 grid
- Center tile: ~2016 (nearest walkable to world origin 0,0)
- +X = East, -Z = North (standard Three.js convention)
- Tile distances are Euclidean in world units
- `bot_find_tiles` fetches `https://hubzz.xyz/data/maps/world_2.json`

## Known Issues / Notes

- `@roamhq/wrtc` segfaults on process exit (native GC) — harmless for long-running daemon
- `bot_audio_start` requires bot to already be spawned and connected
- Mediasoup voice server: `https://demo.hubzz.com/` (derived from wsUrl hostname)
- Room ID format: `hostname@worldPath` (e.g. `hubzz.xyz@0,0`)
- The conductor bot must be signed in (non-guest) to send chat — bot login uses `iamar0b0t` which grants signed-in status

## Files

| File | Purpose |
|------|---------|
| `bot-mcp.mjs` | MCP server — all tools and bot logic |
| `launch-audio-test.mjs` | Convenience launcher for the distance-based audio test scene |
