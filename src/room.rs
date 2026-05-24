pub struct Room {}

impl Room {
    pub fn new() -> Self {
        Room {}
    }
}

pub fn make_shared_room() -> std::sync::Arc<Room> {
    std::sync::Arc::new(Room::new())
}
