# TurtleSpeak Design Spec

> LAN voice chat — near-zero latency through raw PCM relay over WebSocket.

## Overview

TurtleSpeak is a LAN voice chat application. A single Rust server runs on one machine, serves a web page, and relays raw PCM audio between browser clients over WebSocket. Clients capture microphone audio, stream it to the server, and mix all received streams locally using the Web Audio API. The server performs zero audio processing — it is a pure byte forwarder.

**First milestone**: Single-room chat, continuous (always-on) microphone transmission, simple web UI.

## Architecture

```
Browser A (capture + playback) ──WebSocket──┐
                                            ├── Rust Server (relay) ── Static HTML/JS
Browser B (capture + playback) ──WebSocket──┘
```

### Audio Data Flow

1. Browser captures mic via `getUserMedia` → `ScriptProcessorNode` produces 10ms Float32 frames
2. Float32 converted to Int16 LE → sent as WebSocket binary frame (960 bytes)
3. Server receives binary frame, identifies sender by session ID, forwards bytes to all other connected clients
4. Each client receives binary frames, converts Int16 back to Float32, pushes to per-peer ring buffer
5. Per-peer `AudioBuffer` + `AudioBufferSourceNode` schedules playback, mixed via `GainNode`s to `AudioContext.destination`

### PCM Format

- 48kHz sample rate
- Mono (single channel)
- Signed 16-bit integer, little-endian
- Frame size: 10ms = 480 samples = 960 bytes
- Bandwidth per sender: ~96 KB/s (100 packets/sec × 960 bytes)

## Server Design

**Language**: Rust  
**Framework**: Axum (Tokio-based HTTP + WebSocket)  
**Key crates**: `tokio`, `axum`, `serde`, `serde_json`, `tower-http`

### Routes

| Route | Method | Description |
|-------|--------|-------------|
| `/` | GET | Serves `static/index.html` |
| `/static/*` | GET | Serves static assets |
| `/ws` | GET (Upgrade) | WebSocket endpoint for audio + control |

### WebSocket Protocol

**Binary frames**: Raw PCM Int16 bytes. No header, no metadata — pure audio samples.

**Text frames** (JSON):

Client → Server:
```json
{"type": "join", "name": "Alice"}
{"type": "leave"}
```

Server → Client:
```json
{"type": "peer_joined", "id": "abc123", "name": "Bob"}
{"type": "peer_left", "id": "abc123", "name": "Bob"}
{"type": "user_list", "users": [{"id": "abc123", "name": "Alice"}, {"id": "def456", "name": "Bob"}]}
```

### Internal State

A single `Room` struct containing a client map (`HashMap<ClientId, Client>`). Protected by `tokio::sync::Mutex`.

Each `Client` holds:
- Unique ID (ULID)
- Display name
- `mpsc::UnboundedSender<Message>` for writing to the WebSocket

### Broadcast Logic

On receiving a binary frame from client X:
1. Look up X in the client map
2. Iterate all clients where id != X.id
3. Send the binary frame to each via their mpsc sender

On receiving a `join` text frame:
1. Assign a ULID, store the client in the room map
2. Send `user_list` to the new client (full current list)
3. Broadcast `peer_joined` to all other clients

On disconnect (WebSocket close):
1. Remove client from room map
2. Broadcast `peer_left` to remaining clients

### Connection Lifecycle

1. Browser opens WebSocket to `ws://<host>:<port>/ws`
2. Client sends `{"type":"join","name":"..."}`
3. Server registers client, assigns ID, broadcasts presence
4. Client begins streaming binary audio frames
5. On disconnect (browser close, network drop), server cleans up and notifies peers

## Client Design

### Technology

- Single `static/index.html` file
- Embedded CSS + JavaScript (no framework, no build step)
- All audio processing via Web Audio API

### Capture Pipeline

```
getUserMedia({audio: true})
  → AudioContext.createMediaStreamSource(stream)
  → ScriptProcessorNode (bufferSize=512, ~10ms frames)
  → onaudioprocess: Float32 → Int16 conversion
  → ws.send(int16ArrayBuffer)  // Binary frame (960 bytes)
```

Note: `ScriptProcessorNode` is deprecated but widely supported and simpler than `AudioWorklet` for v1. Upgradable to `AudioWorklet` in v2 without external API changes.

### Playback Pipeline

```
ws.onmessage(binary)
  → Int16 → Float32 conversion
  → push to per-peer ring buffer (Int16Array backed)
  → pull 10ms of Float32 samples
  → AudioContext.createBuffer(1, 480, 48000)
  → buffer.getChannelData(0).set(float32Samples)
  → AudioBufferSourceNode.start(scheduledTime)
  → GainNode → AudioContext.destination
```

Per-peer management:
- On `peer_joined`: create ring buffer + dedicated GainNode
- On `peer_left`: stop scheduled sources, disconnect GainNode, free buffer

### Clock Drift Handling

Mic capture clock and audio playback clock are independent hardware oscillators — over time, a sender's 48kHz may be 48,001Hz and the receiver's 48,000Hz, or vice versa. Without correction, the per-peer ring buffer will grow or drain.

**Approach**: adaptive buffering

1. Target buffer depth: 50ms (5 frames). Start playback only after 3 frames buffered.
2. Each pull cycle, check buffer depth:
   - **Depth > 100ms** (10 frames): Discard oldest frame(s) to catch up (brief click, inaudible on LAN)
   - **Depth < ~1ms** (underrun): Insert one silent frame to avoid glitching. This covers the case where the receiver clock is slightly faster than sender.
3. This bounds latency at ~50ms nominal, worst-case ~100ms, with smooth recovery from drift.


### UI Layout

```
┌─────────────────────────────────┐
│         TurtleSpeak             │
│                                 │
│  Your name: [___________]       │
│  [     Join Room     ]          │
│                                 │
│  Connected Users          Stats │
│  • Alice (you)           2ms   │
│  • Bob — speaking        23K/s │
│  • Charlie — silent      46K/s │
└─────────────────────────────────┘
```

### Reconnection

Client maintains connection state machine:
- `connecting` → `connected` on WS open
- `connected` → `disconnected` on close
- `disconnected` → auto-reconnect: 1s, 2s, 4s, max 10s backoff
- UI shows connection status badge

## Error Handling

| Scenario | Server Behavior | Client Behavior |
|----------|----------------|-----------------|
| WebSocket disconnect | Remove client, broadcast `peer_left` | Show disconnected state, auto-reconnect |
| Server crash | Process exits (managed by OS or supervisor) | `ws.onclose`, reconnect loop |
| Mic permission denied | N/A | Show inline error: "Microphone access required" |
| Malformed JSON message | Log warning, ignore message | N/A |
| Unexpected binary (no join) | Log warning, ignore frame | N/A |
| AudioContext not available | N/A | Show error: "Browser not supported" |
| Room overflow (too many clients) | No cap. Practical limit: bandwidth × N. Log warning at high count. | N/A |

## Testing Strategy

- **Unit tests** (`room.rs`): Add client, remove client, forward audio to correct recipients, sender exclusion, broadcast `peer_joined`/`peer_left` to correct clients
- **Integration test**: Spawn test server, open 2 WebSocket connections, client A sends binary frame, verify client B receives it, verify client A does NOT receive echo
- **Manual**: Browser → server loopback (two tabs, same machine), two machines on same LAN

## Project Structure

```
TurtleSpeak/
├── Cargo.toml
├── src/
│   ├── main.rs          # Axum server startup, route definitions
│   ├── room.rs          # Room state, client map, broadcast logic
│   ├── ws_handler.rs    # Per-connection WebSocket handler, message routing
│   └── models.rs        # Message types (Join, Leave, UserList, etc.)
└── static/
    └── index.html       # Full web client (HTML + CSS + JS)
```

## Out of Scope (v2 / Future)

- Multi-room support
- Voice Activity Detection (VAD)
- Push-to-talk
- AudioWorklet migration
- Opus/compressed audio
- Desktop/mobile native client
- Authentication / access control
- Channel bandwidth limiting
- Audio recording
- Mute/deafen controls
- Echo cancellation (may not be needed since each peer only receives others' audio, not their own)
