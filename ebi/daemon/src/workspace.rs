use crate::shelf::shelf::{ShelfId, ShelfInfo};
use iroh::NodeId;
use std::collections::HashMap;
use uuid::Uuid;

pub type WorkspaceId = Uuid;

pub struct Workspace {
    pub id: WorkspaceId,
    pub name: String,
    pub description: String,
    pub local_shelves: HashMap<ShelfId, ShelfInfo>,
    pub remote_shelves: HashMap<ShelfId, (ShelfInfo, NodeId)>,
}

impl Workspace {
    pub fn edit_shelf(
        &mut self,
        shelf_id: ShelfId,
        new_name: Option<String>,
        new_description: Option<String>,
    ) -> bool {
        if let Some(shelf) = self.local_shelves.get_mut(&shelf_id) {
            if let Some(name) = new_name {
                shelf.name = name;
            }
            if let Some(description) = new_description {
                shelf.description = description;
            }
            return true;
        }
        if let Some((shelf, _)) = self.remote_shelves.get_mut(&shelf_id) {
            if let Some(name) = new_name {
                shelf.name = name;
            }
            if let Some(description) = new_description {
                shelf.description = description;
            }
            return true;
        }
        false
    }

    pub fn contains(&self, shelf_id: ShelfId) -> (bool, bool) {
        let remote = self.remote_shelves.contains_key(&shelf_id);
        let exists = self.local_shelves.contains_key(&shelf_id) || remote;
        return (exists, remote);
    }
}
