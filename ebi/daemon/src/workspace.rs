use crate::shelf::shelf::ShelfInfo;
use iroh::NodeId;
use std::collections::HashMap;

pub struct Workspace {
    pub id: WorkspaceId,
    pub name: String,
    pub description: String,
    pub local_shelves: HashMap<u64, ShelfInfo>,
    pub remote_shelves: HashMap<u64, (ShelfInfo, NodeId)>,
}

impl Workspace {
    pub fn edit_shelf(
        &mut self,
        shelf_id: u64,
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
}

pub type WorkspaceId = u64;
