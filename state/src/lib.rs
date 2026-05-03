#![doc = include_str!("../README.md")]

pub mod cache;
pub mod ops;
pub mod redb;
pub mod service;

use crate::ops::Operations;
use ebi_filesystem::shelf::TagFilter;
use ebi_types::{Uuid, sharedref::*};

pub type Workspace = ebi_types::workspace::Workspace<TagFilter>;
pub type Shelf = ebi_types::shelf::Shelf<TagFilter>;

#[derive(PartialEq, Eq, Debug)]
pub struct SyncState<T> {
    pub staged: StatefulRef<T>, // Locally modified state - contains uncommitted changes
    pub committed: SharedRef<Operations, ()>, // committed operations that have been applied to "staged" but not seen by all
    pub synced: Option<ImmutRef<T>>,          // latest globally seen synced state
}

impl<T> SyncState<T> {
    pub fn new(new_state: T) -> Self {
        SyncState {
            staged: StatefulRef::new_ref(Uuid::new_v4(), new_state),
            committed: SharedRef::new_ref((), Operations::new()),
            synced: None,
        }
    }

    pub fn hash_synced(&self) -> Vec<u128> {
        self.synced
            .iter()
            .map(|state_ref| state_ref.id.as_u128())
            .collect()
    }
}
