use axum::extract::ws::WebSocketUpgrade;

pub async fn handler(ws: WebSocketUpgrade, _room: crate::room::SharedRoom) -> impl axum::response::IntoResponse {
    ws.on_upgrade(|_socket| async {})
}
