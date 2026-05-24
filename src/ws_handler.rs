use axum::extract::ws::WebSocketUpgrade;

pub async fn handler(ws: WebSocketUpgrade, _room: std::sync::Arc<crate::room::Room>) -> impl axum::response::IntoResponse {
    ws.on_upgrade(|_socket| async {})
}
