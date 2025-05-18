use crate::Shelf;
use iroh::NodeId;
use std::path::PathBuf;


struct ShelfInfo {
    id: u64,
    root_path: PathBuf,
    //summary: ShelfSummary
}

pub struct Workspace {
    id: WorkspaceId,
    name: String,
    description: String,
    local_shelves: ShelfInfo,
    remote_shelves: (ShelfInfo, NodeId)
}

pub type WorkspaceId = u64;
