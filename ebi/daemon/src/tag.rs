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

#[derive(Clone)]
pub struct TagData {
    pub name: String,
    pub priority: u64,
    pub parent: Option<Box<TagData>>,
}

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
