use crate::shelf::shelf::ShelfManager;
use crate::shelf::shelf::{ShelfId, ShelfInfo};
use crate::tag::Autotagger;
use iroh::NodeId;
use std::sync::Arc;
use std::{collections::HashMap, path::PathBuf};
use tokio::sync::RwLock;
use uuid::Uuid; // Make sure ShelfManager is defined or imported correctly

pub type WorkspaceId = Uuid;

pub struct Workspace {
    pub id: WorkspaceId,
    pub name: String,
    pub description: String,
    pub local_shelves: HashMap<ShelfId, ShelfInfo>,
    pub remote_shelves: HashMap<ShelfId, (ShelfInfo, NodeId)>,
    pub auto_taggers: Vec<Autotagger>,
    pub shelf_manager: Arc<RwLock<ShelfManager>>,
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

    pub async fn attach_autotagger(&mut self, tagger: Autotagger) -> () {
        self.auto_taggers.push(tagger);
        let mut shelf_manager = self.shelf_manager.write().await;
        for shelf in self.local_shelves.values_mut() {
            let shelf = shelf_manager.shelves.get_mut(&shelf.id).unwrap();
            shelf.write().await.apply(self.id, &mut self.auto_taggers.last_mut().unwrap()).await;
        }
    }

    pub async fn add_shelf(
        &mut self,
        id: ShelfId,
        name: String,
        description: String,
        root_path: PathBuf,
    ) -> () {
        self.local_shelves.insert(
            id,
            ShelfInfo {
                id,
                name,
                description,
                root_path,
            },
        );
        for tagger in &mut self.auto_taggers {
            self.shelf_manager
                .write()
                .await
                .shelves
                .get_mut(&id)
                .unwrap()
                .write()
                .await
                .apply(self.id, tagger)
                .await;
        }
    }

    pub fn contains(&self, shelf_id: ShelfId) -> (bool, bool) {
        let remote = self.remote_shelves.contains_key(&shelf_id);
        let exists = self.local_shelves.contains_key(&shelf_id) || remote;
        return (exists, remote);
    }
}
