use crate::query::{FileOrder, OrderedFileSummary};
use crate::services::peer::PeerService;
use crate::tag::TagManager;
use crate::tag::TagRef;
use crate::workspace::{Workspace, WorkspaceId};
use std::collections::{BTreeSet, HashMap};
use std::path::PathBuf;
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    sync::RwLock,
    task::{Context, Poll},
};
use tower::Service;

#[derive(Clone)]
pub struct CacheService {
    peer_service: PeerService,
    tag_manager: Arc<RwLock<TagManager>>,
    workspaces: Arc<RwLock<HashMap<WorkspaceId, Workspace>>>,
}

enum RetrieveFiles<T: FileOrder + Clone> {
    GetAll(WorkspaceId, T),
    GetTag(WorkspaceId, T, TagRef),
}

enum Caching {
    IsCacheValid(Option<TagRef>, HashCache),
}
enum RetrieveData {
    GetDir(PathBuf),
    GetFile(PathBuf),
}
enum RetrieveInfo {
    GetFileInfo(PathBuf),
    GetDirInfo(PathBuf),
}

struct HashCache {
    hash: u64,
}

enum CommandRes<T: FileOrder + Clone> {
    OrderedFiles(BTreeSet<OrderedFileSummary<T>>),
}

enum CacheError {
    WorkspaceNotFound,
}

impl<T: FileOrder + Clone> Service<RetrieveFiles<T>> for CacheService {
    type Response = BTreeSet<OrderedFileSummary<T>>;
    type Error = CacheError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: RetrieveFiles<T>) -> Self::Future {
        let _ = Box::pin(async move {
            match req {
                RetrieveFiles::GetAll(work_id, _ord) => {
                    if let Some(_workspace) = &self.workspaces.read().unwrap().get(&work_id) {
                        todo!();
                    } else {
                        return CacheError::WorkspaceNotFound;
                    }
                }
                RetrieveFiles::GetTag(_work_id, _ord, _tag) => {}
            }
            todo!();
        });
        todo!();
    }
}
