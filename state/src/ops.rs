use std::hash::Hash;

use chrono::{DateTime, Utc};
use ebi_types::Uuid;
use rbf_ebi::rateless_bloom::RatelessBF;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Target {
    Shelves,
    Workspaces,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Kind {
    Add,
    Update,
    Remove,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Op {
    pub uuid: Uuid,
    pub target: Target,
    pub kind: Kind,
    pub data: Vec<u8>,
    pub timestamp: DateTime<Utc>,
}

impl Hash for Op {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.uuid.hash(state)
    }
}

#[derive(Debug)]
pub struct Operations {
    _ops: RatelessBF<Op>,
}

impl Operations {
    pub fn new() -> Self {
        Operations {
            _ops: RatelessBF::<Op>::new(Vec::<Op>::new(), 100),
        }
    }
}
