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

impl Default for Room {
    fn default() -> Self {
        Self::new()
    }
}

impl Room {
    pub fn new() -> Self {
        Self { clients: HashMap::new() }
    }

    pub fn len(&self) -> usize {
        self.clients.len()
    }

    pub fn is_empty(&self) -> bool {
        self.clients.is_empty()
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
            if exclude_id.is_none_or(|eid| cid != eid) {
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
