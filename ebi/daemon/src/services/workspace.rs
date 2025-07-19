use ebi_proto::rpc::*;
use std::collections::HashMap;
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::sync::RwLock;
use tower::Service;
use uuid::Uuid;
use crate::workspace::{Workspace, WorkspaceId, WorkspaceRef};
use crate::tag::{TagId, TagRef};
use crate::shelf::shelf::{ShelfId, ShelfRef};
use std::path::PathBuf;


const HEADER_SIZE: usize = 10; //[!] Move to Constant file 

#[derive(Clone)]
pub struct WorkspaceService {
    pub workspaces: Arc<RwLock<HashMap<WorkspaceId, WorkspaceRef>>>,
    pub shelf_assignation: Arc<RwLock<HashMap<ShelfId, Vec<WorkspaceId>>>>,
    pub paths: Arc<RwLock<HashMap<PathBuf, ShelfId>>> // [!] should this be HashMap<NodeId, HashMap<PathBuf, ShelfId>> ?,
}

enum Operations {
    GetWorkspace(WorkspaceId),
}

pub struct GetWorkspace {
    pub id: WorkspaceId
}

impl Service<GetWorkspace> for WorkspaceService {
    type Response = WorkspaceRef;
    type Error = ReturnCode;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: GetWorkspace) -> Self::Future {
        let workspaces = self.workspaces.clone();
        Box::pin(async move {
            if let Some(workspace) = workspaces.read().await.get(&req.id) {
                Ok(workspace.clone())
            } else {
                Err(ReturnCode::WorkspaceNotFound)
            }
        })
    }
}

pub struct CreateWorkspace {
    pub name: String,
    pub description: String,
}

impl Service<CreateWorkspace> for WorkspaceService {
    type Response = WorkspaceId;
    type Error = ReturnCode;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: CreateWorkspace) -> Self::Future {
        let workspaces = self.workspaces.clone();
        Box::pin(async move {
            let id = Uuid::now_v7();

            let workspace = Workspace {
                id,
                name: req.name,
                description: req.description,
                shelves: HashMap::new(), // Placeholder for local shelves
                tags: HashMap::new(),
                lookup: HashMap::new(),
            };

            workspaces.write().await.insert(id, Arc::new(RwLock::new(workspace)));

            Ok(id)
        })
    }
}

pub struct GetWorkspaces {}

impl Service<GetWorkspaces> for WorkspaceService {
    type Response = Vec<ebi_proto::rpc::Workspace>;
    type Error = ReturnCode;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _req: GetWorkspaces) -> Self::Future {
        let workspaces = self.workspaces.clone();
        Box::pin(async move {
            let mut workspace_ls = Vec::new();
            for (_, workspace) in workspaces.read().await.iter() {
                let mut tag_ls = Vec::new();
                for tag_ref in workspace.read().await.tags.values() {
                    let tag = tag_ref.tag_ref.read().unwrap();
                    let tag_id = tag.id;
                    let name = tag.name.clone();
                    let priority = tag.priority;
                    let parent_id = match tag.parent.clone() {
                        Some(parent) => Some(parent.tag_ref.read().unwrap().id.as_bytes().to_vec()),
                        None => None,
                    };
                    tag_ls.push(Tag {
                        tag_id: tag_id.as_bytes().to_vec(),
                        name,
                        priority,
                        parent_id,
                    });
                }
                let workspace = workspace.read().await;
                let ws = ebi_proto::rpc::Workspace {
                    workspace_id: workspace.id.as_bytes().to_vec(),
                    name: workspace.name.clone(),
                    description: workspace.description.clone(),
                    tags: tag_ls,
                };
                workspace_ls.push(ws);
            }
            Ok(workspace_ls)
        })
    }
}






pub struct GetShelf {
    pub shelf_id: ShelfId,
    pub workspace_id: WorkspaceId
}

impl Service<GetShelf> for WorkspaceService {
    type Response = ShelfRef;
    type Error = ReturnCode;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: GetShelf) -> Self::Future {
        let workspaces = self.workspaces.clone();
        Box::pin(async move {
            let workspace_r = workspaces.read().await;
            let Some(workspace) = workspace_r.get(&req.workspace_id) else {
                return Err(ReturnCode::WorkspaceNotFound);
            };

            if let Some(shelf) = workspace.clone().read().await.shelves.get(&req.shelf_id) {
                Ok(shelf.clone())
            } else {
                return Err(ReturnCode::ShelfNotFound)
            }
        })
    }
}

pub struct UnassignShelf {
    pub shelf_id: ShelfId,
    pub workspace_id: WorkspaceId
}

impl Service<UnassignShelf> for WorkspaceService {
    type Response = bool; // True if the unassgnied workspace was the last
    type Error = ReturnCode;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: UnassignShelf) -> Self::Future {
        let shelf_assignation = self.shelf_assignation.clone();
        let paths = self.paths.clone();
        Box::pin(async move {
            let mut shelf_assignation_w = shelf_assignation.write().await;
            let Some(workspace_list) = shelf_assignation_w.get_mut(&req.shelf_id) else {
                return Err(ReturnCode::ShelfNotFound)
            };
            workspace_list.retain(|&w_id| w_id != req.workspace_id);
            if workspace_list.is_empty() {
                paths.write().await.retain(|_, s_id| s_id != &req.shelf_id);
                return Ok(true);
            }
            Ok(false)
        })
    }
}

pub struct RemoveWorkspace {
    pub workspace_id: WorkspaceId
}

impl Service<RemoveWorkspace> for WorkspaceService {
    type Response = (); 
    type Error = ReturnCode;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: RemoveWorkspace) -> Self::Future {
        let shelf_assignation = self.shelf_assignation.clone();
        let paths = self.paths.clone();
        Box::pin(async move {
            let mut shelf_assignation_w = shelf_assignation.write().await;
            for (shelf, workspaces) in shelf_assignation_w.iter_mut() {
                workspaces.retain(|&w_id| w_id != req.workspace_id); 
                if workspaces.is_empty() {
                    paths.write().await.retain(|_, s_id| s_id != shelf);
                }
            }
            Ok(())
        })
    }
}


pub struct GetTag {
    pub tag_id: TagId,
    pub workspace_id: WorkspaceId
}


impl Service<GetTag> for WorkspaceService {
    type Response = TagRef;
    type Error = ReturnCode;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: GetTag) -> Self::Future {
        let workspaces = self.workspaces.clone();
        Box::pin(async move {
            let workspace_r = workspaces.read().await;
            let Some(workspace) = workspace_r.get(&req.workspace_id) else {
                return Err(ReturnCode::WorkspaceNotFound);
            };

            if let Some(tag) = workspace.clone().read().await.tags.get(&req.tag_id) {
                Ok(tag.clone())
            } else {
                return Err(ReturnCode::ShelfNotFound)
            }
        })
    }
}
