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
