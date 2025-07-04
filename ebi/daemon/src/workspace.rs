use crate::shelf::file::{FileRef, FileSummary};
use crate::shelf::shelf::{ShelfId, Shelf, ShelfInfo};
use crate::tag::{Tag, TagData, TagId, TagRef};
use iroh::NodeId;
use std::sync::Arc;
use std::{collections::HashMap, path::PathBuf};
use tokio::sync::RwLock;
use uuid::Uuid;

pub type WorkspaceId = Uuid;

pub type WorkspaceRef = Arc<RwLock<Workspace>>;

//[#] Workspace calls Shelf.refresh(), Shelf returns a ChangeSummary, Workspace applies Autotaggers


pub struct Workspace {
    // Workspace Info
    pub id: WorkspaceId,
    pub name: String,
    pub description: String,
    // Shelf Management
    pub local_shelves: HashMap<ShelfId, Arc<RwLock<Shelf>>>,
    pub remote_shelves: HashMap<ShelfId, (ShelfInfo, NodeId)>,
    // Tag Management
    pub tags: HashMap<TagId, TagRef>,
    pub lookup: HashMap<String, TagId>,
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

    pub async fn add_shelf_info(
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
    }

    pub fn contains(&self, shelf_id: ShelfId) -> (bool, bool) {
        let remote = self.remote_shelves.contains_key(&shelf_id);
        let exists = self.local_shelves.contains_key(&shelf_id) || remote;
        return (exists, remote);
    }

    pub fn get_tags(&mut self) -> Vec<TagRef> {
        self.tags.values().cloned().collect()
    }

    pub fn create_tag(
        &mut self,
        name: &str,
        priority: u64,
        parent: Option<TagId>,
    ) -> Result<TagId, TagErr> {
        if parent.is_some() && !self.tags.contains_key(&parent.unwrap().clone()) {
            return Err(TagErr::ParentMissing(parent.unwrap()));
        }

        let id = loop {
            let id = Uuid::now_v7();
            if !self.tags.contains_key(&id) {
                break id;
            }
        };
        if self.lookup.contains_key(&name.to_string()) {
            return Err(TagErr::DuplicateTag((name.to_string(), self.id.clone())));
        }
        let parent = match parent {
            Some(p) => Some(self.tags.get(&p.clone()).unwrap().clone()),
            None => None,
        };
        let tag = Tag {
            id,
            priority,
            name: name.to_string(),
            parent,
        };
        self.tags.insert(
            id.clone(),
            TagRef {
                tag_ref: Arc::new(std::sync::RwLock::new(tag)),
            },
        );
        self.lookup.insert(name.to_string(), id.clone());
        Ok(id)
    }

    pub fn get_tag(
        &mut self,
        name: &str,
    ) -> Result<TagRef, TagErr> {
        let id = self.lookup.get(&name.to_string());
        if let Some(id) = id {
            let tag_ref = self.tags.get(id).cloned();
            if tag_ref.is_some() {
                Ok(tag_ref.unwrap())
            } else {
                Err(TagErr::InconsistentTagManager((*id, self.id)))
            }
        } else {
            Err(TagErr::TagMissing(Vec::new()))
        }
    }

    pub async fn refresh(&mut self, shelf: ShelfId, change: ChangeSummary) -> () {
        todo!();
    }

    // Private function to be used in refresh
    fn get_or_create(&mut self, tag_data: TagData) -> TagRef {
        match self.lookup.get(&tag_data.name) {
            Some(tag_id) => self.tags.get(&tag_id).unwrap().clone(),
            None => {
                let tag_id = self.create_tag(&tag_data.name, tag_data.priority, None).unwrap();
                self.tags.get(&tag_id).unwrap().clone()
            }
        }
    }
}

pub struct ChangeSummary {
    pub added_files: Vec<FileRef>,
    pub removed_files: Vec<FileRef>,
    pub modified_files: Vec<FileRef>,
}

#[derive(Debug)]
pub enum TagErr {
    TagMissing(Vec<TagId>),
    ParentMissing(TagId),
    DuplicateTag((String, WorkspaceId)),
    InconsistentTagManager((TagId, WorkspaceId)),
}
