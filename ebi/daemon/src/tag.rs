use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::{Arc, RwLock};
use std::vec;
use crate::shelf::file::FileSummary;

use uuid::Uuid;

use crate::workspace::WorkspaceId;

pub type TagId = Uuid;

//[#] Automatic Tagging 

pub enum AutoTagErr {
    MissingAttribute,
    ParentMissing,
    ManagerLockError,
    InconsistentTagManager
}

pub trait Tagger {
    fn generate_tag(
        &mut self,
        workspace_id: WorkspaceId,
        file: FileSummary,
        tag_manager: Arc<RwLock<TagManager>>,
        priority: Option<u64>, // 0 is the highest priority
        parent: Option<TagId>,
    ) -> Result<Option<TagRef>, AutoTagErr>; 
}

struct ExtensionTagger {}

impl Tagger for ExtensionTagger {
    fn generate_tag(
        &mut self,
        workspace_id: WorkspaceId,
        file: FileSummary,
        tag_manager: Arc<RwLock<TagManager>>,
        priority: Option<u64>,
        parent: Option<TagId>,
    ) -> Result<Option<TagRef>, AutoTagErr> {
        if let Some(ext) = file.path.extension() {
            let mut manager = tag_manager.write().map_err(|_| AutoTagErr::ManagerLockError)?;
            let tag = manager.get_tag(
                workspace_id,
                &ext.to_string_lossy().to_string(),
                priority.unwrap_or(u64::MAX),
                parent,
            );
            match tag {
                Ok(tag_ref) => Ok(Some(tag_ref)),
                Err(TagErr::ParentMissing(_)) => Err(AutoTagErr::ParentMissing),
                Err(TagErr::InconsistentTagManager(_)) => Err(AutoTagErr::InconsistentTagManager),
                Err(TagErr::TagMissing(_)) => todo!(),
                Err(TagErr::DuplicateTag(_)) => Ok(None), // File already tagged (or duplicate tag) //[!] Where should we check if it has alreaddy been tagged? 
            }
        }
        else {
            Err(AutoTagErr::MissingAttribute)
        }
    }
}

struct NameTagger {}

impl Tagger for NameTagger {
    fn generate_tag(
        &mut self,
        workspace_id: WorkspaceId,
        file: FileSummary,
        tag_manager: Arc<RwLock<TagManager>>,
        priority: Option<u64>,
        parent: Option<TagId>,
    ) -> Result<Option<TagRef>, AutoTagErr> {
        if let Some(name) = file.path.file_stem() {
            let mut manager = tag_manager.write().map_err(|_| AutoTagErr::ManagerLockError)?;
            let tag = manager.get_tag(
                workspace_id,
                &name.to_string_lossy().to_string(),
                priority.unwrap_or(u64::MAX),
                parent,
            );
            match tag {
                Ok(tag_ref) => Ok(Some(tag_ref)),
                Err(TagErr::ParentMissing(_)) => Err(AutoTagErr::ParentMissing),
                Err(TagErr::InconsistentTagManager(_)) => Err(AutoTagErr::InconsistentTagManager),
                Err(TagErr::TagMissing(_)) => todo!(),
                Err(TagErr::DuplicateTag(_)) => Ok(None), // File already tagged (or duplicate tag) //[!] Where should we check if it has alreaddy been tagged? 
            }
        }
        else {
            Err(AutoTagErr::MissingAttribute)
        }
    }
}

struct ModifiedDateTagger {}

impl Tagger for ModifiedDateTagger {
    fn generate_tag(
        &mut self,
        workspace_id: WorkspaceId,
        file: FileSummary,
        tag_manager: Arc<RwLock<TagManager>>,
        priority: Option<u64>,
        parent: Option<TagId>,
    ) -> Result<Option<TagRef>, AutoTagErr> {
        if let Some(modified) = file.metadata.modified {
            let mut manager = tag_manager.write().map_err(|_| AutoTagErr::ManagerLockError)?;
            let tag = manager.get_tag(
                workspace_id,
                &modified.format("%Y-%m-%d").to_string(),
                priority.unwrap_or(u64::MAX),
                parent,
            );
            match tag {
                Ok(tag_ref) => Ok(Some(tag_ref)),
                Err(TagErr::ParentMissing(_)) => Err(AutoTagErr::ParentMissing),
                Err(TagErr::InconsistentTagManager(_)) => Err(AutoTagErr::InconsistentTagManager),
                Err(TagErr::TagMissing(_)) => todo!(),
                Err(TagErr::DuplicateTag(_)) => Ok(None), // File already tagged (or duplicate tag) //[!] Where should we check if it has alreaddy been tagged? 
            }
        }
        else {
            Err(AutoTagErr::MissingAttribute)
        }
    }
    
}


//[#] Tag 

#[derive(Debug, Eq, PartialOrd, PartialEq, Ord, Hash, Default)]
pub struct Tag {
    pub id: TagId,
    pub priority: u64,
    pub name: String,
    pub parent: Option<TagRef>,
}

#[derive(Debug)]
pub struct TagRef {
    pub tag_ref: Arc<RwLock<Tag>>,
}

impl Clone for TagRef {
    fn clone(&self) -> Self {
        TagRef {
            tag_ref: Arc::clone(&self.tag_ref),
        }
    }
}

impl PartialEq for TagRef {
    fn eq(&self, other: &Self) -> bool {
        self.tag_ref.read().unwrap().id == other.tag_ref.read().unwrap().id
    }
}

impl PartialOrd for TagRef {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(
            self.tag_ref
                .read()
                .unwrap()
                .priority
                .cmp(&other.tag_ref.read().unwrap().priority),
        )
    }
}

impl Hash for TagRef {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.tag_ref.read().unwrap().id.hash(state);
    }
}

impl Ord for TagRef {
    fn cmp(&self, other: &Self) -> Ordering {
        self.tag_ref
            .read()
            .unwrap()
            .priority
            .cmp(&other.tag_ref.read().unwrap().priority)
    }
}

impl Eq for TagRef {}

//[#] We require a tag manmager
#[derive(Debug)]
pub struct TagManager {
    pub tags: HashMap<(TagId, WorkspaceId), TagRef>, // Global Tags have Workspace_ID = 0
    pub lookup: HashMap<(String, WorkspaceId), TagId>, // Used by Taggers, mostly
}

impl TagManager {
    pub fn new() -> Self {
        TagManager {
            tags: HashMap::new(),
            lookup: HashMap::new(),
        }
    }

    pub fn get_tag(
        &mut self,
        workspace_id: WorkspaceId,
        name: &str,
        priority: u64,
        parent: Option<TagId>,
    ) -> Result<TagRef, TagErr> {
        let id = self.lookup.get(&(name.to_string(), workspace_id.clone()));
        if let Some(id) = id {
            let key = (id.clone(), workspace_id.clone());
            let tag_ref = self.tags.get(&key).cloned();
            if tag_ref.is_some() {
                Ok(tag_ref.unwrap())
            } else {
                Err(TagErr::InconsistentTagManager(key))
            }
        } else {
            let tag_id = self.create_tag(name, workspace_id, priority, parent);
            if tag_id.is_ok() {
                let key = (tag_id.unwrap(), workspace_id.clone());
                let tag_ref = self.tags.get(&key).cloned();
                if tag_ref.is_some() {
                    Ok(tag_ref.unwrap())
                } else {
                Err(TagErr::InconsistentTagManager(key))
                }
            } else {
                Err(tag_id.err().unwrap())
            }
        }
    }

    pub fn validate(
        &self,
        tag_set: HashSet<TagId>,
        workspace_id: WorkspaceId,
    ) -> Result<(), TagErr> {
        let mut missing: Vec<TagId> = vec![];
        for tag in tag_set {
            if !self.tags.contains_key(&(tag.clone(), workspace_id)) {
                missing.push(tag.clone());
            }
        }
        if missing.is_empty() {
            Ok(())
        } else {
            Err(TagErr::TagMissing(missing))
        }
    }

    pub fn create_tag(
        &mut self,
        name: &str,
        workspace_id: WorkspaceId,
        priority: u64,
        parent: Option<TagId>,
    ) -> Result<TagId, TagErr> {
        if parent.is_some()
            && !self
                .tags
                .contains_key(&(parent.unwrap().clone(), workspace_id.clone()))
        {
            return Err(TagErr::ParentMissing(parent.unwrap()));
        }

        let id = loop {
            let id = Uuid::now_v7();
            if !self.tags.contains_key(&(id.clone(), workspace_id.clone())) {
                break id;
            }
        };
        if self.lookup.contains_key(&(name.to_string(), workspace_id.clone())) {
            return Err(TagErr::DuplicateTag((name.to_string(), workspace_id.clone())));
        }
        let parent = match parent {
            Some(p) => Some(
                self.tags
                    .get(&(p.clone(), workspace_id.clone()))
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
            (id.clone(), workspace_id.clone()),
            TagRef {
                tag_ref: Arc::new(RwLock::new(tag)),
            },
        );
        self.lookup.insert(
            (name.to_string(), workspace_id.clone()),
            id.clone(),
        );
        Ok(id)
    }

    pub fn get_tags(&mut self, workspace_id: WorkspaceId) -> Vec<TagRef> {
        let mut tags: Vec<TagRef> = vec![];
        for (key, tag) in self.tags.iter() {
            if key.1 == workspace_id {
                tags.push(tag.clone());
            }
        }
        tags
    }
}

#[derive(Debug)]
pub enum TagErr {
    TagMissing(Vec<TagId>),
    ParentMissing(TagId),
    DuplicateTag((String, WorkspaceId)),
    InconsistentTagManager((TagId, WorkspaceId)),
}
