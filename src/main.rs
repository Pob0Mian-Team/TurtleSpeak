use std::net::SocketAddr;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let app = turtlespeak_lib::make_app();

    let addr = SocketAddr::from(([0, 0, 0, 0], 8080));
    tracing::info!("TurtleSpeak server listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
