use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, RwLock};

#[derive(Debug, Eq, PartialOrd, PartialEq, Ord, Hash, Default)]
pub struct Tag {
    id: u64,
    priority: u64,
    name: String,
    parent: Option<TagRef>,
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
