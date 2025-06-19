use crate::{shelf, tag};
use crate::shelf::file::{FileRef, FileSummary};
use crate::shelf::shelf::ShelfManager;
use crate::shelf::shelf::{ShelfId, ShelfInfo};
use crate::tag::{Tag, TagData, TagId, TagRef, Tagger};
use iroh::NodeId;
use std::hash::Hash;
use std::io;
use std::sync::Arc;
use std::{collections::HashMap, path::PathBuf};
use tokio::sync::RwLock;
use uuid::Uuid;

pub type WorkspaceId = Uuid;

type DynTagger = Box<dyn Tagger + Send + Sync>;

//[#] Workspace calls Shelf.refresh(), Shelf returns a ChangeSummary, Workspace applies Autotaggers 

pub struct Workspace {
    // Workspace Info 
    pub id: WorkspaceId,
    pub name: String,
    pub description: String,
    // Shelf Management
    pub local_shelves: HashMap<ShelfId, ShelfInfo>,
    pub remote_shelves: HashMap<ShelfId, (ShelfInfo, NodeId)>,
    pub shelf_manager: Arc<RwLock<ShelfManager>>, //[!] Move to RPC, addShelf takes a Shelf object 
    // Tag Management
    pub tags: HashMap<TagId, TagRef>,
    pub lookup: HashMap<String, TagId>,
    pub auto_taggers: Vec<DynTagger>,
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

    pub async fn attach_autotagger(&mut self, tagger: DynTagger) -> () {
        self.auto_taggers.push(tagger);
        let mut shelf_manager = self.shelf_manager.write().await;
        for shelf in self.local_shelves.values_mut() {
            let shelf = shelf_manager.shelves.get_mut(&shelf.id).unwrap();
            shelf
                .write()
                .await
                .apply(self.id, &mut self.auto_taggers.last_mut().unwrap())
                .await;
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
        let shelf_manager = self.shelf_manager.read().await;
        let mut shelf_write = shelf_manager
                .shelves
                .get(&id)
                .unwrap()
                .write()
                .await;
        for tagger in &mut self.auto_taggers {
                shelf_write
                .apply(self.id, tagger)
                .await;
        }
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
        if parent.is_some()
            && !self
                .tags
                .contains_key(&parent.unwrap().clone())
        {
            return Err(TagErr::ParentMissing(parent.unwrap()));
        }

        let id = loop {
            let id = Uuid::now_v7();
            if !self.tags.contains_key(&id) {
                break id;
            }
        };
        if self
            .lookup
            .contains_key(&name.to_string())
        {
            return Err(TagErr::DuplicateTag((
                name.to_string(),
                self.id.clone(),
            )));
        }
        let parent = match parent {
            Some(p) => Some(
                self.tags
                    .get(&p.clone())
                    .unwrap()
                    .clone(),
            ),
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
        self.lookup
            .insert(name.to_string(), id.clone());
        Ok(id)
    }

    pub fn get_tag(
        &mut self,
        name: &str,
        priority: u64,
        parent: Option<TagId>,
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
            let tag_id = self.create_tag(name, priority, parent);
            if let Ok(tag_id) = tag_id {
                let tag_ref = self.tags.get(&tag_id).cloned();
                if let Some(tag_ref) = tag_ref {
                    Ok(tag_ref)
                } else {
                    Err(TagErr::InconsistentTagManager((tag_id, self.id)))
                }
            } else {
                Err(tag_id.err().unwrap())
            }
        }
    }

    pub async fn refresh(&mut self) -> () {
        let mut errors: Vec<io::Error> = Vec::new();
        let mut changes: Vec<Result<ChangeSummary, io::Error>> = Vec::new();
        for shelf in self.local_shelves.values_mut() {
            let shelf_manager_wlock = self.shelf_manager.write().await;
            let shelf_ref = shelf_manager_wlock.shelves.get(&shelf.id).unwrap();
            let change_summary = shelf_ref.write().await.refresh().await;
            changes.push(change_summary);
        }
        for change in changes {
            if let Ok(change) = change {
                for file in change.added_files {
                    let auto_taggers = self.auto_taggers.clone();
                    for tagger in auto_taggers {
                        let tag_data: Option<TagData>;
                        if tagger.requires_data() {
                            todo!();
                        } else {
                            tag_data = tagger.generate_tag(FileSummary::from(file.clone()), None).await;
                        }
                        if let Some(tag_data) = tag_data { 
                            self.get_tag(&tag_data.name, tag_data.priority, None); // Recursively get/create parent(s), I can't be arsed today
                        }
                    }
                }
                // Handle removed and modified files if needed
            } else {
                errors.push(change.err().unwrap());
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
