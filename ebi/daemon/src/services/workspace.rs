use ebi_proto::rpc::*;
use iroh::NodeId;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::sync::RwLock;
use tokio::sync::mpsc::Sender;
use tokio::sync::watch::Receiver;
use tokio::time::{Duration, sleep};
use tower::Service;
use uuid::Uuid;
use crate::workspace::{TagErr, Workspace, WorkspaceId, WorkspaceRef};
use crate::shelf::shelf::{ShelfId, ShelfInfo};
use std::path::PathBuf;

use crate::services::rpc::RequestId;

const HEADER_SIZE: usize = 10; //[!] Move to Constant file 

#[derive(Clone)]
pub struct SharedStateService {
    pub workspaces: Arc<RwLock<HashMap<WorkspaceId, WorkspaceRef>>>,
    pub shelf_assignation: Arc<RwLock<HashMap<ShelfId, Vec<WorkspaceId>>>>,
    pub paths: Arc<RwLock<HashMap<PathBuf, ShelfId>>>,
}

enum Operations {
    GetWorkspace(WorkspaceId),
}

struct GetWorkspace {
    id: WorkspaceId
}

impl Service<GetWorkspace> for SharedStateService {
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

struct GetShelf {
    shelf_id: ShelfId,
    workspace_id: WorkspaceId
}

enum ShelfKind {
    Local(Arc<RwLock<Shelf>>),
    Remote(ShelfInfo)
}

impl Service<GetShelf> for SharedStateService {
    type Response = ShelfKind;
    type Error = ReturnCode;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: GetShelf) -> Self::Future {
        let workspaces = self.workspaces.clone();
        Box::pin(async move {
            let Some(workspace) = workspaces.read().await.get(&req.id) else {
                return Err(ReturnCode::WorkspaceNotFound);
            };

            let Some(shelf) = workspace.clone().read().await.local_shelves
        })
    }
}
