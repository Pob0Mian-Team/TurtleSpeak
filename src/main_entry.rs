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
        .fallback_service(tower_http::services::ServeDir::new("static"))
}
