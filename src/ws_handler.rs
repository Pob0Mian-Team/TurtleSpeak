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
                        room.leave(&id);
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
