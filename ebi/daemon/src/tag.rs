use crate::shelf::file::FileSummary;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::vec;
use uuid::Uuid;

use crate::workspace::WorkspaceId;
use async_trait::async_trait;

pub type TagId = Uuid;

//[#] Automatic Tagging

pub enum AutoTagErr {
    MissingAttribute,
    ParentMissing,
    ManagerLockError,
    InconsistentTagManager,
}

#[derive(Clone)]
pub struct TagData {
    pub name: String,
    pub priority: u64,
    pub parent: Option<Box<TagData>>,
}

pub trait Tagger {
    fn requires_data(&self) -> bool {
        false
    }

    fn generate_tag(
        &mut self,
        summary: FileSummary,
        data: Option<Vec<u8>>,
    ) -> Option<TagData>;
}

pub type DynTagger = Box<dyn Tagger + Send + Sync>;

/*
pub struct Autotagger {
    pub wrapped_tagger: DynTagger,
    pub tag_manager: Arc<tokio::sync::RwLock<TagManager>>,
}

impl Autotagger {
    pub fn new(tagger: DynTagger, tag_manager: Arc<tokio::sync::RwLock<TagManager>>) -> Self {
        Autotagger {
            wrapped_tagger: tagger,
            tag_manager,
        }
    }

    pub async fn generate_tag(
        &mut self,
        workspace_id: WorkspaceId,
        file: FileSummary,
    ) -> Result<Option<TagRef>, AutoTagErr> {
        self.wrapped_tagger
            .generate_tag(workspace_id, file, self.tag_manager.clone())
            .await
    }
}

struct ExtensionTagger {}

#[async_trait]
impl Tagger for ExtensionTagger {
    async fn generate_tag(
        &mut self,
        workspace_id: WorkspaceId,
        file: FileSummary,
        tag_manager: Arc<tokio::sync::RwLock<TagManager>>,
    ) -> Result<Option<TagRef>, AutoTagErr> {
        if let Some(ext) = file.path.extension() {
            let mut manager = tag_manager.write().await;
            let tag = manager.get_tag(
                workspace_id,
                &ext.to_string_lossy().to_string(),
                u64::MAX,
                None,
            );
            match tag {
                Ok(tag_ref) => Ok(Some(tag_ref)),
                Err(TagErr::ParentMissing(_)) => Err(AutoTagErr::ParentMissing),
                Err(TagErr::InconsistentTagManager(_)) => Err(AutoTagErr::InconsistentTagManager),
                Err(TagErr::TagMissing(_)) => todo!(),
                Err(TagErr::DuplicateTag(_)) => Ok(None), // File already tagged (or duplicate tag) //[!] Where should we check if it has already been tagged?
            }
        } else {
            Err(AutoTagErr::MissingAttribute)
        }
    }
}

struct NameTagger {}

#[async_trait]
impl Tagger for NameTagger {
    async fn generate_tag(
        &mut self,
        workspace_id: WorkspaceId,
        file: FileSummary,
        tag_manager: Arc<tokio::sync::RwLock<TagManager>>,
    ) -> Result<Option<TagRef>, AutoTagErr> {
        if let Some(name) = file.path.file_stem() {
            let mut manager = tag_manager.write().await;
            let tag = manager.get_tag(
                workspace_id,
                &name.to_string_lossy().to_string(),
                u64::MAX,
                None,
            );
            match tag {
                Ok(tag_ref) => Ok(Some(tag_ref)),
                Err(TagErr::ParentMissing(_)) => Err(AutoTagErr::ParentMissing),
                Err(TagErr::InconsistentTagManager(_)) => Err(AutoTagErr::InconsistentTagManager),
                Err(TagErr::TagMissing(_)) => todo!(),
                Err(TagErr::DuplicateTag(_)) => Ok(None), // File already tagged (or duplicate tag) //[!] Where should we check if it has alreaddy been tagged?
            }
        } else {
            Err(AutoTagErr::MissingAttribute)
        }
    }
}
struct ModifiedDateTagger {}

#[async_trait]
impl Tagger for ModifiedDateTagger {
    async fn generate_tag(
        &mut self,
        workspace_id: WorkspaceId,
        file: FileSummary,
        tag_manager: Arc<tokio::sync::RwLock<TagManager>>,
    ) -> Result<Option<TagRef>, AutoTagErr> {
        if let Some(modified) = file.metadata.modified {
            let mut manager = tag_manager.write().await;
            let tag = manager.get_tag(
                workspace_id,
                &modified.format("%Y-%m-%d").to_string(),
                u64::MAX,
                None,
            );
            match tag {
                Ok(tag_ref) => Ok(Some(tag_ref)),
                Err(TagErr::ParentMissing(_)) => Err(AutoTagErr::ParentMissing),
                Err(TagErr::InconsistentTagManager(_)) => Err(AutoTagErr::InconsistentTagManager),
                Err(TagErr::TagMissing(_)) => todo!(),
                Err(TagErr::DuplicateTag(_)) => Ok(None), // File already tagged (or duplicate tag) //[!] Where should we check if it has alreaddy been tagged?
            }
        } else {
            Err(AutoTagErr::MissingAttribute)
        }
    }
}
*/
//[#] Tag

#[derive(Debug, Eq, PartialOrd, PartialEq, Ord, Hash, Default)]
pub struct Tag {
    pub id: TagId,
    pub priority: u64,
    pub name: String,
    pub parent: Option<TagRef>,
    //[+] pub visible: bool, // Whether the tag is visible in the UI
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
