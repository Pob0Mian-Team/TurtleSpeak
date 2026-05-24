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

    // Drain join-related text messages: Alice gets user_list + peer_joined (2), Bob gets user_list (1)
    for _ in 0..2 {
        let msg = a.next().await.unwrap().unwrap();
        assert!(msg.is_text());
    }
    for _ in 0..1 {
        let msg = b.next().await.unwrap().unwrap();
        assert!(msg.is_text());
    }

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
