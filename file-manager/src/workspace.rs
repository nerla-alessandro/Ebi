use crate::{shelf::shelf::ShelfInfo, Shelf};
use iroh::NodeId;
use std::path::PathBuf;

pub struct Workspace {
    pub id: WorkspaceId,
    pub name: String,
    pub description: String,
    pub local_shelves: Vec<ShelfInfo>,
    pub remote_shelves: Vec<(ShelfInfo, NodeId)>,
}

impl Workspace {
    pub fn edit_shelf(
        &mut self,
        shelf_id: u64,
        new_name: Option<String>,
        new_description: Option<String>,
    ) -> bool {
        if let Some(shelf) = self.local_shelves.iter_mut().find(|s| s.id == shelf_id) {
            if let Some(name) = new_name {
                shelf.name = name;
            }
            if let Some(description) = new_description {
                shelf.description = description;
            }
            return true;
        }
        if let Some((shelf, _)) = self
            .remote_shelves
            .iter_mut()
            .find(|(s, _)| s.id == shelf_id)
        {
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

    pub fn contains(&self, shelf_id: u64) -> bool {
        self.local_shelves.iter().any(|s| s.id == shelf_id)
            || self.remote_shelves.iter().any(|(s, _)| s.id == shelf_id)
    }

    pub fn get_peer_id(&self, shelf_id: u64) -> Option<NodeId> {
        if let Some((_, peer_id)) = self.remote_shelves.iter().find(|(s, _)| s.id == shelf_id) {
            Some(*peer_id)
        } else {
            None
        }
    }
}

pub type WorkspaceId = u64;
