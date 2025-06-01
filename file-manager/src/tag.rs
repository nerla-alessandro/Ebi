use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::{Arc, RwLock};
use std::vec;

#[derive(Debug, Eq, PartialOrd, PartialEq, Ord, Hash, Default)]
pub struct Tag {
    pub id: u64,
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
pub struct TagManager {
    pub tags: HashMap<(u64, u64), TagRef>, // (Tag_ID, Workspace_ID) -> TagRef - [Global Tags have Workspace_ID = 0]
}

impl TagManager {
    pub fn new() -> Self {
        TagManager {
            tags: HashMap::new(),
        }
    }

    pub fn validate(&self, tag_set: HashSet<u64>, workspace_id: u64) -> Result<(), TagErr> {
        let mut missing: Vec<u64> = vec![];
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
        workspace_id: u64,
        priority: u64,
        parent: Option<u64>,
    ) -> Result<u64, TagErr> {
        if parent.is_some()
            && !self
                .tags
                .contains_key(&(parent.unwrap().clone(), workspace_id.clone()))
        {
            return Err(TagErr::ParentMissing(parent.unwrap()));
        }

        let id = loop {
            let id = rand::random::<u64>();
            if !self.tags.contains_key(&(id.clone(), workspace_id.clone())) {
                break id;
            }
        };
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
        Ok(id)
    }

    pub fn get_tags(&mut self, workspace_id: u64) -> Vec<TagRef> {
        let mut tags: Vec<TagRef> = vec![];
        for (key, tag) in self.tags.iter() {
            if key.1 == workspace_id {
                tags.push(tag.clone());
            }
        }
        tags
    }
}

pub enum TagErr {
    TagMissing(Vec<u64>),
    ParentMissing(u64),
}
