# TurtleSpeak v1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a LAN voice chat server (Rust + Axum) and web client (HTML/JS) that relays raw PCM audio between browsers with sub-100ms latency.

**Architecture:** Single Rust binary serving static files and a WebSocket endpoint. Browser captures mic as PCM Int16 (10ms frames), sends over WebSocket. Server forwards bytes to all other clients. Browser mixes all received streams locally via Web Audio API. Zero server-side audio processing.

**Tech Stack:** Rust, Tokio, Axum, Serde, Web Audio API, vanilla JS/CSS.

**File Map:**

| File | Responsibility |
|------|---------------|
| `Cargo.toml` | Dependencies, lib + bin targets |
| `src/lib.rs` | Re-exports modules, exposes `make_app()` for tests |
| `src/models.rs` | JSON message types for WebSocket control messages |
| `src/room.rs` | Room state: client map, join/leave/broadcast + tests |
| `src/ws_handler.rs` | Per-connection WebSocket task |
| `src/main.rs` | Binary entry point — starts server |
| `tests/integration.rs` | Integration test: two clients, audio relay, notifications |
| `static/index.html` | Complete web client |

---

### Task 1: Initialize Rust project

**Files:**
- Create: `Cargo.toml`
- Create: `src/lib.rs`
- Create: `src/main.rs`
- Create: `static/index.html`

- [ ] **Step 1: Create Cargo.toml**

```toml
[package]
name = "turtlespeak"
version = "0.1.0"
edition = "2021"

[lib]
name = "turtlespeak_lib"
path = "src/lib.rs"

[[bin]]
name = "turtlespeak"
path = "src/main.rs"

[dependencies]
tokio = { version = "1", features = ["full"] }
axum = { version = "0.8", features = ["ws"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
tower-http = { version = "0.6", features = ["fs"] }
ulid = "1"
tracing = "0.1"
tracing-subscriber = "0.3"
futures-util = "0.3"

[dev-dependencies]
tokio-tungstenite = "0.24"
```

- [ ] **Step 2: Create src/lib.rs**

```rust
pub mod models;
pub mod room;
pub mod ws_handler;

mod main_entry;

pub use main_entry::make_app;
```

- [ ] **Step 3: Create src/main_entry.rs** (shared app constructor)

```rust
use axum::{routing::get, Router};

pub fn make_app() -> Router {
    let shared_room = crate::room::make_shared_room();

    Router::new()
        .route("/ws", get({
            let room = shared_room.clone();
            move |ws| {
                let room = room.clone();
                crate::ws_handler::handler(ws, room)
            }
        }))
        .nest_service("/", tower_http::services::ServeDir::new("static"))
}
```

- [ ] **Step 4: Create src/main.rs**

```rust
use std::net::SocketAddr;

#[tokio::main]
async fn main() {
    tracing_subscriber::init();

    let app = turtlespeak_lib::make_app();

    let addr = SocketAddr::from(([0, 0, 0, 0], 8080));
    tracing::info!("TurtleSpeak server listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
```

- [ ] **Step 5: Create placeholder static/index.html**

```html
<!DOCTYPE html>
<html><head><meta charset="UTF-8"></head><body>TurtleSpeak</body></html>
```

- [ ] **Step 6: Build**

```bash
cargo build
```

Expected: successful compile.

- [ ] **Step 7: Commit**

```bash
git add Cargo.toml src/ static/
git commit -m "feat: initialize Rust project with Axum skeleton"
```

---

### Task 2: Define models and Room state

**Files:**
- Create: `src/models.rs`
- Create: `src/room.rs`

- [ ] **Step 1: Write src/models.rs**

```rust
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
pub enum ClientMessage {
    #[serde(rename = "join")]
    Join { name: String },
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type")]
pub enum ServerMessage {
    #[serde(rename = "peer_joined")]
    PeerJoined { id: String, name: String },
    #[serde(rename = "peer_left")]
    PeerLeft { id: String, name: String },
    #[serde(rename = "user_list")]
    UserList { users: Vec<UserInfo> },
}

#[derive(Debug, Clone, Serialize)]
pub struct UserInfo {
    pub id: String,
    pub name: String,
}
```

- [ ] **Step 2: Write src/room.rs**

```rust
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use ulid::Ulid;
use crate::models::UserInfo;

pub struct Client {
    pub id: String,
    pub name: String,
    pub audio_tx: mpsc::UnboundedSender<Vec<u8>>,
    pub msg_tx: mpsc::UnboundedSender<String>,
}

pub struct Room {
    clients: HashMap<String, Arc<Client>>,
}

impl Room {
    pub fn new() -> Self {
        Self { clients: HashMap::new() }
    }

    pub fn len(&self) -> usize {
        self.clients.len()
    }

    pub fn join(
        &mut self,
        name: String,
        audio_tx: mpsc::UnboundedSender<Vec<u8>>,
        msg_tx: mpsc::UnboundedSender<String>,
    ) -> (String, Vec<UserInfo>) {
        let id = Ulid::new().to_string();
        let users: Vec<UserInfo> = self.clients.values().map(|c| {
            UserInfo { id: c.id.clone(), name: c.name.clone() }
        }).collect();

        self.clients.insert(id.clone(), Arc::new(Client {
            id: id.clone(),
            name,
            audio_tx,
            msg_tx,
        }));

        (id, users)
    }

    pub fn leave(&mut self, id: &str) -> Option<String> {
        self.clients.remove(id).map(|c| c.name.clone())
    }

    pub fn broadcast_audio(&self, sender_id: &str, audio: &[u8]) {
        for (cid, client) in &self.clients {
            if cid != sender_id {
                let _ = client.audio_tx.send(audio.to_vec());
            }
        }
    }

    pub fn broadcast_json(&self, exclude_id: Option<&str>, msg: &str) {
        for (cid, client) in &self.clients {
            if exclude_id.map_or(true, |eid| cid != eid) {
                let _ = client.msg_tx.send(msg.to_string());
            }
        }
    }

    pub fn user_list(&self) -> Vec<UserInfo> {
        self.clients.values().map(|c| {
            UserInfo { id: c.id.clone(), name: c.name.clone() }
        }).collect()
    }
}

pub type SharedRoom = Arc<Mutex<Room>>;

pub fn make_shared_room() -> SharedRoom {
    Arc::new(Mutex::new(Room::new()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_channels() -> (mpsc::UnboundedSender<Vec<u8>>, mpsc::UnboundedReceiver<Vec<u8>>, mpsc::UnboundedSender<String>, mpsc::UnboundedReceiver<String>) {
        let (atx, arx) = mpsc::unbounded_channel::<Vec<u8>>();
        let (mtx, mrx) = mpsc::unbounded_channel::<String>();
        (atx, arx, mtx, mrx)
    }

    #[test]
    fn test_join_and_leave() {
        let mut room = Room::new();
        assert_eq!(room.len(), 0);

        let (atx, _arx, mtx, _mrx) = make_channels();
        let (id, users) = room.join("Alice".into(), atx, mtx);
        assert_eq!(room.len(), 1);
        assert_eq!(users.len(), 0);

        let name = room.leave(&id).unwrap();
        assert_eq!(name, "Alice");
        assert_eq!(room.len(), 0);
        assert!(room.leave("nobody").is_none());
    }

    #[test]
    fn test_broadcast_audio_excludes_sender() {
        let mut room = Room::new();
        let (atx1, mut arx1, mtx1, _) = make_channels();
        let (atx2, mut arx2, mtx2, _) = make_channels();
        let (id1, _) = room.join("Alice".into(), atx1, mtx1);
        room.join("Bob".into(), atx2, mtx2);

        room.broadcast_audio(&id1, &[1, 2, 3]);
        assert!(arx1.try_recv().is_err());
        assert_eq!(arx2.try_recv().unwrap(), vec![1, 2, 3]);
    }

    #[test]
    fn test_broadcast_json_exclude() {
        let mut room = Room::new();
        let (atx1, _, _mtx1, mut mrx1) = make_channels();
        let (atx2, _, _mtx2, mut mrx2) = make_channels();
        room.join("Alice".into(), atx1, _mtx1);
        let (id2, _) = room.join("Bob".into(), atx2, _mtx2);

        room.broadcast_json(Some(&id2), "hello");
        assert!(mrx1.try_recv().is_ok());
        assert!(mrx2.try_recv().is_err());
    }

    #[test]
    fn test_user_list() {
        let mut room = Room::new();
        let (atx1, _, mtx1, _) = make_channels();
        let (atx2, _, mtx2, _) = make_channels();
        room.join("Alice".into(), atx1, mtx1);
        room.join("Bob".into(), atx2, mtx2);

        let users = room.user_list();
        assert_eq!(users.len(), 2);
        let names: Vec<&str> = users.iter().map(|u| u.name.as_str()).collect();
        assert!(names.contains(&"Alice"));
        assert!(names.contains(&"Bob"));
    }
}
```

- [ ] **Step 3: Build and test**

```bash
cargo test
```

Expected: all 4 tests pass.

- [ ] **Step 4: Commit**

```bash
git add src/models.rs src/room.rs
git commit -m "feat: define message types and Room state with tests"
```

---

### Task 3: Implement WebSocket handler

**Files:**
- Create: `src/ws_handler.rs`

- [ ] **Step 1: Write src/ws_handler.rs**

```rust
use axum::extract::ws::{Message, WebSocket};
use axum::extract::WebSocketUpgrade;
use axum::response::IntoResponse;
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use crate::models::{ClientMessage, ServerMessage};
use crate::room::SharedRoom;

pub async fn handler(ws: WebSocketUpgrade, room: SharedRoom) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle(socket, room))
}

async fn handle(socket: WebSocket, room: SharedRoom) {
    let (mut ws_tx, mut ws_rx) = socket.split();

    let (audio_tx, mut audio_rx) = mpsc::unbounded_channel::<Vec<u8>>();
    let (msg_tx, mut msg_rx) = mpsc::unbounded_channel::<String>();

    // Phase 1: wait for join message
    let (client_id, client_name) = loop {
        match ws_rx.next().await {
            Some(Ok(Message::Text(text))) => {
                if let Ok(ClientMessage::Join { name }) =
                    serde_json::from_str::<ClientMessage>(&text)
                {
                    let mut room = room.lock().await;
                    let (id, _) = room.join(name.clone(), audio_tx, msg_tx);

                    let user_list = serde_json::to_string(&ServerMessage::UserList {
                        users: room.user_list(),
                    })
                    .unwrap();
                    if ws_tx.send(Message::Text(user_list.into())).await.is_err() {
                        return;
                    }

                    let peer_msg = serde_json::to_string(&ServerMessage::PeerJoined {
                        id: id.clone(),
                        name: name.clone(),
                    })
                    .unwrap();
                    room.broadcast_json(Some(&id), &peer_msg);

                    break (id, name);
                }
            }
            Some(Ok(Message::Close(_))) | None => return,
            _ => continue,
        }
    };

    tracing::info!("Client {} ({}) joined", client_id, client_name);

    // Phase 2: spawn sender task (room channels -> WebSocket)
    let send_task = tokio::spawn(async move {
        loop {
            tokio::select! {
                Some(audio) = audio_rx.recv() => {
                    if ws_tx.send(Message::Binary(audio.into())).await.is_err() {
                        break;
                    }
                }
                Some(json) = msg_rx.recv() => {
                    if ws_tx.send(Message::Text(json.into())).await.is_err() {
                        break;
                    }
                }
                else => break,
            }
        }
    });

    // Phase 3: main loop (WebSocket -> room)
    while let Some(msg) = ws_rx.next().await {
        match msg {
            Ok(Message::Binary(data)) => {
                let room = room.lock().await;
                room.broadcast_audio(&client_id, &data);
            }
            Ok(Message::Close(_)) => break,
            Ok(Message::Ping(_)) | Ok(Message::Pong(_)) => {}
            Ok(Message::Text(_)) => {
                tracing::debug!("Unexpected text from {}", client_name);
            }
            Err(e) => {
                tracing::warn!("{}: WS error: {}", client_name, e);
                break;
            }
        }
    }

    send_task.abort();

    // Cleanup
    let mut room = room.lock().await;
    room.leave(&client_id);
    let leave_msg = serde_json::to_string(&ServerMessage::PeerLeft {
        id: client_id.clone(),
        name: client_name.clone(),
    })
    .unwrap();
    room.broadcast_json(None, &leave_msg);

    tracing::info!("Client {} ({}) left", client_id, client_name);
}
```

- [ ] **Step 2: Build**

```bash
cargo build
```

Expected: successful compile.

- [ ] **Step 3: Commit**

```bash
git add src/ws_handler.rs
git commit -m "feat: implement WebSocket handler with join/audio-relay/leave"
```

---

### Task 4: Add integration test

**Files:**
- Create: `tests/integration.rs`

- [ ] **Step 1: Create tests/integration.rs**

```rust
use std::time::Duration;
use tokio::net::TcpListener;
use futures_util::SinkExt;
use futures_util::StreamExt;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

async fn start_test_server() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let app = turtlespeak_lib::make_app();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    port
}

#[tokio::test]
async fn test_two_clients_audio_relay() {
    let port = start_test_server().await;
    let url = format!("ws://127.0.0.1:{}/ws", port);

    let (mut a, _) = connect_async(&url).await.unwrap();
    a.send(Message::Text(r#"{"type":"join","name":"Alice"}"#.into())).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let (mut b, _) = connect_async(&url).await.unwrap();
    b.send(Message::Text(r#"{"type":"join","name":"Bob"}"#.into())).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let audio = vec![0u8; 960];
    a.send(Message::Binary(audio.clone().into())).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Bob receives Alice's audio
    let msg = tokio::time::timeout(Duration::from_secs(2), b.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(msg.is_binary());
    assert_eq!(msg.into_data(), audio);

    // Alice does NOT receive echo
    assert!(
        tokio::time::timeout(Duration::from_millis(500), a.next())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn test_peer_join_notification() {
    let port = start_test_server().await;
    let url = format!("ws://127.0.0.1:{}/ws", port);

    let (mut a, _) = connect_async(&url).await.unwrap();
    a.send(Message::Text(r#"{"type":"join","name":"Alice"}"#.into())).await.unwrap();

    // Alice gets user_list
    let msg = a.next().await.unwrap().unwrap();
    let text = msg.into_text().unwrap();
    assert!(text.contains("user_list"));
    assert!(text.contains("Alice"));

    let (mut b, _) = connect_async(&url).await.unwrap();
    b.send(Message::Text(r#"{"type":"join","name":"Bob"}"#.into())).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Alice gets peer_joined
    let msg = a.next().await.unwrap().unwrap();
    let text = msg.into_text().unwrap();
    assert!(text.contains("peer_joined"));
    assert!(text.contains("Bob"));

    // Bob gets user_list
    let msg = b.next().await.unwrap().unwrap();
    let text = msg.into_text().unwrap();
    assert!(text.contains("user_list"));
}
```

- [ ] **Step 2: Run integration tests**

```bash
cargo test --test integration
```

Expected: both tests pass.

- [ ] **Step 3: Commit**

```bash
git add tests/ Cargo.toml
git commit -m "test: add integration tests for audio relay and notifications"
```

---

### Task 5: Create the web client

**Files:**
- Overwrite: `static/index.html`

- [ ] **Step 1: Write static/index.html**

```html
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>TurtleSpeak</title>
<style>
* { margin: 0; padding: 0; box-sizing: border-box; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #0f172a; color: #e2e8f0; min-height: 100vh; display: flex; align-items: center; justify-content: center; }
#app { width: 360px; padding: 24px; }
h1 { font-size: 24px; margin-bottom: 4px; color: #38bdf8; }
.subtitle { font-size: 13px; color: #64748b; margin-bottom: 20px; }
.card { background: #1e293b; border: 1px solid #334155; border-radius: 8px; padding: 16px; margin-bottom: 12px; }
input { width: 100%; padding: 10px 12px; background: #0f172a; border: 1px solid #334155; border-radius: 6px; color: #e2e8f0; font-size: 14px; outline: none; }
input:focus { border-color: #38bdf8; }
button { width: 100%; padding: 10px; background: #38bdf8; color: #0f172a; border: none; border-radius: 6px; font-weight: 600; font-size: 14px; cursor: pointer; }
button:disabled { opacity: 0.4; cursor: not-allowed; }
button.leave { background: #ef4444; color: #fff; }
.user { display: flex; justify-content: space-between; align-items: center; padding: 6px 0; font-size: 14px; }
.user .dot { width: 8px; height: 8px; border-radius: 50%; background: #22c55e; margin-right: 8px; }
.user .dot.silent { background: #475569; }
.stats { font-size: 11px; color: #64748b; text-align: right; }
.status { font-size: 12px; color: #64748b; text-align: center; margin-top: 4px; }
.error { color: #ef4444; font-size: 13px; margin-top: 8px; display: none; }
</style>
</head>
<body>
<div id="app">
  <h1>TurtleSpeak</h1>
  <p class="subtitle">LAN voice chat</p>

  <div id="login" class="card">
    <input id="nameInput" placeholder="Your name" maxlength="24" autocomplete="off">
    <button id="joinBtn" style="margin-top:10px">Join Room</button>
    <div id="loginError" class="error"></div>
  </div>

  <div id="room" style="display:none">
    <div class="card">
      <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:8px;">
        <span style="font-size:14px; font-weight:600">Room</span>
        <span class="stats" id="stats">--</span>
      </div>
      <div id="userList"></div>
      <div id="roomStatus" class="status">connected</div>
    </div>
    <button class="leave" id="leaveBtn">Leave</button>
  </div>
</div>

<script>
const SAMPLE_RATE = 48000;
const FRAME_SAMPLES = 480;
const FRAME_BYTES = FRAME_SAMPLES * 2;
const RING_CAP_SAMPLES = FRAME_SAMPLES * 50;
const MAX_DEPTH_SAMPLES = FRAME_SAMPLES * 10;

let ws = null;
let audioCtx = null;
let processor = null;
let micStream = null;
let remoteGain = null;
let myId = null;
let myName = null;
let reconnectTimer = null;
let reconnectDelay = 1000;
let peers = {};
let ringBuf = new Int16Array(RING_CAP_SAMPLES);
let writeIdx = 0;
let playIdx = 0;
let nextScheduleTime = 0;
let bytesReceived = 0;
let lastBytesReset = 0;
let scheduleTimer = null;

const login = document.getElementById('login');
const room = document.getElementById('room');
const nameInput = document.getElementById('nameInput');
const joinBtn = document.getElementById('joinBtn');
const leaveBtn = document.getElementById('leaveBtn');
const userListEl = document.getElementById('userList');
const statsEl = document.getElementById('stats');
const loginError = document.getElementById('loginError');
const roomStatus = document.getElementById('roomStatus');

joinBtn.onclick = function () {
  const name = nameInput.value.trim();
  if (!name) {
    loginError.textContent = 'Enter a name';
    loginError.style.display = 'block';
    return;
  }
  loginError.style.display = 'none';
  myName = name;
  connect();
};

leaveBtn.onclick = function () { disconnect(true); };

function connect() {
  const protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
  ws = new WebSocket(protocol + '//' + location.host + '/ws');
  ws.binaryType = 'arraybuffer';
  ws.onopen = onOpen;
  ws.onmessage = onMessage;
  ws.onclose = onClose;
  ws.onerror = function () {};
}

function onOpen() {
  reconnectDelay = 1000;
  ws.send(JSON.stringify({ type: 'join', name: myName }));
}

function onMessage(evt) {
  if (typeof evt.data === 'string') {
    handleControl(JSON.parse(evt.data));
  } else if (evt.data instanceof ArrayBuffer) {
    handleAudio(evt.data);
  }
}

function handleControl(msg) {
  if (msg.type === 'user_list') {
    login.style.display = 'none';
    room.style.display = 'block';
    roomStatus.textContent = 'connected';
    peers = {};
    for (var i = 0; i < msg.users.length; i++) {
      var u = msg.users[i];
      if (u.name === myName) {
        myId = u.id;
      } else {
        peers[u.id] = { id: u.id, name: u.name };
      }
    }
    renderUserList();
    startAudio();
  } else if (msg.type === 'peer_joined') {
    if (msg.name === myName) return;
    peers[msg.id] = { id: msg.id, name: msg.name };
    renderUserList();
  } else if (msg.type === 'peer_left') {
    delete peers[msg.id];
    renderUserList();
  }
}

function handleAudio(data) {
  var src = new Int16Array(data);

  while (((writeIdx - playIdx + RING_CAP_SAMPLES) % RING_CAP_SAMPLES) > MAX_DEPTH_SAMPLES) {
    for (var i = 0; i < FRAME_SAMPLES; i++) {
      ringBuf[(playIdx + i) % RING_CAP_SAMPLES] = 0;
    }
    playIdx = (playIdx + FRAME_SAMPLES) % RING_CAP_SAMPLES;
  }

  for (var i = 0; i < FRAME_SAMPLES; i++) {
    var idx = (writeIdx + i) % RING_CAP_SAMPLES;
    var sample = ringBuf[idx] + src[i];
    if (sample > 32767) sample = 32767;
    else if (sample < -32768) sample = -32768;
    ringBuf[idx] = sample;
  }
  writeIdx = (writeIdx + FRAME_SAMPLES) % RING_CAP_SAMPLES;

  bytesReceived += FRAME_BYTES;
  var now = Date.now();
  if (!lastBytesReset) lastBytesReset = now;
  if (now - lastBytesReset >= 1000) {
    lastBytesReset = now;
    bytesReceived = 0;
  }
}

function startAudio() {
  if (audioCtx) return;
  audioCtx = new AudioContext({ sampleRate: SAMPLE_RATE });

  remoteGain = audioCtx.createGain();
  remoteGain.gain.value = 1.0;
  remoteGain.connect(audioCtx.destination);

  nextScheduleTime = audioCtx.currentTime + 0.02;

  navigator.mediaDevices.getUserMedia({
    audio: {
      sampleRate: SAMPLE_RATE,
      channelCount: 1,
      echoCancellation: false,
      noiseSuppression: false,
      autoGainControl: false
    }
  }).then(function (stream) {
    micStream = stream;
    var source = audioCtx.createMediaStreamSource(stream);
    processor = audioCtx.createScriptProcessor(FRAME_SAMPLES, 1, 1);
    processor.onaudioprocess = function (e) {
      if (!ws || ws.readyState !== WebSocket.OPEN) return;
      var input = e.inputBuffer.getChannelData(0);
      var int16 = new Int16Array(FRAME_SAMPLES);
      for (var i = 0; i < FRAME_SAMPLES; i++) {
        var s = Math.max(-1, Math.min(1, input[i]));
        int16[i] = s < 0 ? s * 0x8000 : s * 0x7FFF;
      }
      ws.send(int16.buffer);
    };
    source.connect(processor);
    processor.connect(audioCtx.destination);
  }).catch(function () {
    loginError.textContent = 'Microphone access required';
    loginError.style.display = 'block';
  });

  schedulePlayback();
}

function schedulePlayback() {
  if (!audioCtx) return;

  nextScheduleTime += FRAME_SAMPLES / SAMPLE_RATE;

  var buf = audioCtx.createBuffer(1, FRAME_SAMPLES, SAMPLE_RATE);
  var channel = buf.getChannelData(0);

  if (((writeIdx - playIdx + RING_CAP_SAMPLES) % RING_CAP_SAMPLES) >= FRAME_SAMPLES) {
    for (var i = 0; i < FRAME_SAMPLES; i++) {
      var idx = (playIdx + i) % RING_CAP_SAMPLES;
      channel[i] = ringBuf[idx] / 32768;
      ringBuf[idx] = 0;
    }
  }

  var src = audioCtx.createBufferSource();
  src.buffer = buf;
  src.connect(remoteGain);
  src.start(nextScheduleTime);

  playIdx = (playIdx + FRAME_SAMPLES) % RING_CAP_SAMPLES;

  var delay = (nextScheduleTime - audioCtx.currentTime) * 1000;
  scheduleTimer = setTimeout(schedulePlayback, Math.max(1, delay));
}

function renderUserList() {
  var users = [{ name: myName, id: myId, me: true }];
  for (var pid in peers) {
    if (Object.prototype.hasOwnProperty.call(peers, pid)) {
      users.push({ name: peers[pid].name, id: pid, me: false });
    }
  }
  userListEl.innerHTML = users.map(function (u) {
    return '<div class="user"><span><span class="dot' + (u.me ? '' : ' silent') + '"></span>' + u.name + (u.me ? ' (you)' : '') + '</span></div>';
  }).join('');
  updateStats();
}

function updateStats() {
  statsEl.textContent = Math.round(bytesReceived / 1024) + ' KB/s | ' + Object.keys(peers).length + ' peers';
}

function disconnect(intentional) {
  if (reconnectTimer) {
    clearTimeout(reconnectTimer);
    reconnectTimer = null;
  }
  if (scheduleTimer) {
    clearTimeout(scheduleTimer);
    scheduleTimer = null;
  }
  if (processor) {
    processor.disconnect();
    processor = null;
  }
  if (micStream) {
    micStream.getTracks().forEach(function (t) { t.stop(); });
    micStream = null;
  }
  if (remoteGain) {
    remoteGain.disconnect();
    remoteGain = null;
  }
  if (audioCtx) {
    audioCtx.close();
    audioCtx = null;
  }
  peers = {};
  myId = null;
  ringBuf = new Int16Array(RING_CAP_SAMPLES);
  writeIdx = 0;
  playIdx = 0;
  nextScheduleTime = 0;
  bytesReceived = 0;
  lastBytesReset = 0;

  if (ws) {
    ws.onclose = null;
    ws.onerror = null;
    ws.onmessage = null;
    ws.onopen = null;
    ws.close();
    ws = null;
  }

  if (intentional) {
    login.style.display = 'block';
    room.style.display = 'none';
  } else {
    roomStatus.textContent = 'reconnecting...';
    scheduleReconnect();
  }
}

function onClose() {
  disconnect(false);
}

function scheduleReconnect() {
  if (reconnectTimer) return;
  reconnectTimer = setTimeout(function () {
    reconnectTimer = null;
    reconnectDelay = Math.min(reconnectDelay * 2, 10000);
    connect();
  }, reconnectDelay);
}
</script>
</body>
</html>
```
