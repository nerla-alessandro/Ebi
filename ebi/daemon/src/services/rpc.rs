use crate::services::peer::PeerService;
use crate::shelf::shelf::{ShelfId, ShelfInfo, ShelfManager, UpdateErr};
use crate::tag::TagId;
use crate::workspace::{TagErr, Workspace, WorkspaceId, WorkspaceRef};
use ebi_proto::rpc::*;
use iroh::NodeId;
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::watch::{Receiver, Sender};
use tokio::sync::{RwLock, RwLockReadGuard};
use tokio::task::JoinHandle;
use tower::Service;

use uuid::Uuid;

pub type RequestId = Uuid;

//[!] Potentially, we could have a validation

macro_rules! return_error {
    ($return_code:path, $response:ident, $request_uuid:expr, $error_data:ident) => {
        let return_code = $return_code;
        let metadata = ResponseMetadata {
            request_uuid: $request_uuid,
            return_code: return_code as u32,
            $error_data,
        };
        let mut res = $response::default();
        res.metadata = Some(metadata);
        return Ok(res);
    };
}

fn val_workspace_id(
    workspaces: &RwLockReadGuard<HashMap<WorkspaceId, WorkspaceRef>>,
    bytes: &Vec<u8>,
) -> Option<WorkspaceId> {
    uuid(bytes.clone())
        .ok()
        .filter(|id| workspaces.contains_key(id))
}

fn val_shelf_id(workspace: &RwLockReadGuard<Workspace>, bytes: &Vec<u8>) -> (Option<Uuid>, bool) {
    if let Ok(id) = uuid(bytes.clone()) {
        let (found, is_remote) = workspace.contains(id);
        if found {
            (Some(id), is_remote)
        } else {
            (None, false)
        }
    } else {
        (None, false)
    }
}

fn val_tag_id(workspace: &RwLockReadGuard<Workspace>, bytes: &Vec<u8>) -> Option<TagId> {
    uuid(bytes.clone())
        .ok()
        .filter(|id| workspace.tags.contains_key(&id))
}

//[!] Handle poisoned locks

#[derive(Clone)]
pub struct RpcService {
    pub daemon_info: Arc<DaemonInfo>,
    pub peer_service: PeerService,
    pub tasks: Arc<HashMap<TaskID, JoinHandle<()>>>,
    pub shelf_manager: Arc<RwLock<ShelfManager>>,
    pub workspaces: Arc<RwLock<HashMap<WorkspaceId, WorkspaceRef>>>,

    // [!] This should be Mutexes. reason about read-write ratio
    pub responses: Arc<RwLock<HashMap<RequestId, Response>>>,
    pub notify_queue: Arc<RwLock<VecDeque<Notification>>>,
    pub broadcast: Sender<Uuid>,
    pub watcher: Receiver<Uuid>,
}
pub type TaskID = u64;

pub enum Notification {
    Heartbeat(Heartbeat),
    Operation(Operation),
    PeerConnected(NodeId),
}

pub struct DaemonInfo {
    pub id: NodeId,
    pub name: RwLock<String>,
}

impl DaemonInfo {
    pub fn new(id: NodeId, name: String) -> Self {
        DaemonInfo {
            id,
            name: RwLock::new(name),
        }
    }
}

fn parse_peer_id(bytes: &[u8]) -> Result<NodeId, ()> {
    let bytes: &[u8; 32] = bytes.try_into().map_err(|_| ())?;
    NodeId::from_bytes(bytes).map_err(|_| ())
}

enum UuidErr {
    SizeMismatch,
}

fn uuid(bytes: Vec<u8>) -> Result<Uuid, UuidErr> {
    let bytes: Result<[u8; 16], _> = bytes.try_into();
    if let Ok(bytes) = bytes {
        Ok(Uuid::from_bytes(bytes))
    } else {
        Err(UuidErr::SizeMismatch)
    }
}

impl Service<Response> for RpcService {
    type Response = ();
    type Error = ();
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Response) -> Self::Future {
        let responses = self.responses.clone();
        let broadcast = self.broadcast.clone();
        Box::pin(async move {
            let res = Response::from(req);
            let metadata = res.metadata().ok_or(())?;
            let uuid = Uuid::try_from(metadata.request_uuid).map_err(|_| ())?;
            responses.write().await.insert(uuid, res);
            broadcast.send(uuid).map_err(|_| ())?;
            Ok(())
        })
    }
}

impl Service<DeleteTag> for RpcService {
    type Response = DeleteTagResponse;
    type Error = ();
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: DeleteTag) -> Self::Future {
        let metadata = req.metadata.clone().unwrap();
        let notify_queue = self.notify_queue.clone();
        let shelf_manager = self.shelf_manager.clone();
        let workspaces = self.workspaces.clone();
        Box::pin(async move {
            let error_data: Option<ErrorData> = None;

            let hashmap_workspaces_r = workspaces.read().await;
            let Some(workspace_id) = val_workspace_id(&hashmap_workspaces_r, &req.workspace_id)
            else {
                return_error!(
                    ReturnCode::WorkspaceNotFound,
                    DeleteTagResponse,
                    metadata.request_uuid,
                    error_data
                );
            };
            let workspace = hashmap_workspaces_r.get(&workspace_id).unwrap().clone();

            drop(hashmap_workspaces_r); // We don't need to lock the HashMap anymore.

            let workspace_r = workspace.read().await;

            let Some(tag_id) = val_tag_id(&workspace_r, &req.tag_id) else {
                return_error!(
                    ReturnCode::TagNotFound,
                    DeleteTagResponse,
                    metadata.request_uuid,
                    error_data
                );
            };

            //[/] Business Logic
            let tag = workspace_r.tags.get(&tag_id).unwrap();

            let local_shelves: Vec<&ShelfInfo> = workspace_r.local_shelves.values().collect();
            let remote_shelves: Vec<&(ShelfInfo, NodeId)> =
                workspace_r.remote_shelves.values().collect();

            // Fast local operation. Require shelf_manager lock once
            let shelf_manager_r = shelf_manager.read().await;
            for shelf_info in local_shelves {
                let shelf = shelf_manager_r.shelves.get(&shelf_info.id);
                if let Some(shelf) = shelf {
                    let _ = shelf.write().await.strip(PathBuf::from(""), tag.clone());
                }
            }

            // Remote operations. We ask for self_manager each time as timeouts may be high.
            // [!] we might want to clone remote_shelves if we want to release the workspace read lock early
            for (shelf_info, peer_id) in remote_shelves {
                let shelf_manager = shelf_manager.read().await;
                let shelf = shelf_manager.shelves.get(&shelf_info.id);
                if let Some(shelf) = shelf {
                    let _shelf = shelf;
                    let _peer_id = peer_id;
                    //[TODO] Remote Request
                    // Relay the request via the peer service
                    // Await for the response to be inserted into the relay_responses map
                    // Handle the responses
                    // think of when to remove RwLocks
                }
            }

            drop(workspace_r); // Drop read Workspace lock before acquiring write
            workspace.write().await.tags.remove(&tag_id);

            notify_queue.write().await.push_back({
                Notification::Operation(Operation {
                    target: ActionTarget::Tag.into(),
                    id: tag_id.as_bytes().to_vec(),
                    action: ActionType::Delete.into(),
                })
            });

            let metadata = ResponseMetadata {
                request_uuid: metadata.request_uuid,
                return_code: ReturnCode::Success as u32,
                error_data,
            };
            Ok(DeleteTagResponse {
                metadata: Some(metadata),
            })
        })
    }
}

impl Service<StripTag> for RpcService {
    type Response = StripTagResponse;
    type Error = ();
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: StripTag) -> Self::Future {
        let metadata = req.metadata.clone().unwrap();
        let notify_queue = self.notify_queue.clone();
        let shelf_manager = self.shelf_manager.clone();
        let workspaces = self.workspaces.clone();
        let mut peer_service = self.peer_service.clone();
        Box::pin(async move {
            let error_data: Option<ErrorData> = None;

            let hashmap_workspaces_r = workspaces.read().await;
            let Some(workspace_id) = val_workspace_id(&hashmap_workspaces_r, &req.workspace_id)
            else {
                return_error!(
                    ReturnCode::WorkspaceNotFound,
                    StripTagResponse,
                    metadata.request_uuid,
                    error_data
                );
            };

            let workspace = hashmap_workspaces_r.get(&workspace_id).unwrap().clone();
            drop(hashmap_workspaces_r); // We don't need to lock the HashMap anymore.
            let workspace_r = workspace.read().await;

            let (opt_shelf_id, remote) = val_shelf_id(&workspace_r, &req.shelf_id);
            let Some(shelf_id) = opt_shelf_id else {
                return_error!(
                    ReturnCode::ShelfNotFound,
                    StripTagResponse,
                    metadata.request_uuid,
                    error_data
                );
            };

            let Some(tag_id) = val_tag_id(&workspace_r, &req.tag_id) else {
                return_error!(
                    ReturnCode::TagNotFound,
                    StripTagResponse,
                    metadata.request_uuid,
                    error_data
                );
            };

            let return_code = {
                if remote {
                    // We can unwrap because we checked before.
                    let peer_id: NodeId =
                        workspace_r.remote_shelves.get(&shelf_id).unwrap().1.clone();
                    drop(workspace_r); // Workspace read lock no longer needed.
                    match peer_service.call((peer_id, Request::from(req))).await {
                        Ok(res) => parse_code(res.metadata().unwrap().return_code),
                        Err(_) => ReturnCode::PeerServiceError, // [!] Unknown error, expand with errors from peer service
                    }
                } else {
                    let tag_ref = workspace_r.tags.get(&tag_id).unwrap().clone();
                    drop(workspace_r);
                    let shelf_manager_r = shelf_manager.read().await;
                    let shelf = shelf_manager_r.shelves.get(&shelf_id).unwrap().clone();
                    drop(shelf_manager_r);
                    let mut shelf_w = shelf.write().await;
                    let path = PathBuf::from(req.path);
                    match shelf_w.strip(path, tag_ref.clone()) {
                        Ok(_) => ReturnCode::Success,
                        Err(UpdateErr::PathNotDir) => ReturnCode::PathNotDir, // Path not a Directory
                        Err(UpdateErr::PathNotFound) => ReturnCode::PathNotFound, // Path not Found
                        Err(UpdateErr::FileNotFound) => ReturnCode::FileNotFound, // "Nothing ever happens" -Chudda
                    }
                }
            };

            if return_code == ReturnCode::Success {
                notify_queue.write().await.push_back({
                    Notification::Operation(Operation {
                        target: ActionTarget::Shelf.into(),
                        id: shelf_id.as_bytes().to_vec(),
                        action: ActionType::Edit.into(),
                    })
                });
            };

            let metadata = ResponseMetadata {
                request_uuid: metadata.request_uuid,
                return_code: return_code as u32,
                error_data,
            };
            Ok(StripTagResponse {
                metadata: Some(metadata),
            })
        })
    }
}

impl Service<DetachTag> for RpcService {
    type Response = DetachTagResponse;
    type Error = ();
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: DetachTag) -> Self::Future {
        let metadata = req.metadata.clone().unwrap();
        let notify_queue = self.notify_queue.clone();
        let shelf_manager = self.shelf_manager.clone();
        let workspaces = self.workspaces.clone();
        let mut peer_service = self.peer_service.clone();
        Box::pin(async move {
            let error_data: Option<ErrorData> = None;

            let hashmap_workspaces_r = workspaces.read().await;
            let Some(workspace_id) = val_workspace_id(&hashmap_workspaces_r, &req.workspace_id)
            else {
                return_error!(
                    ReturnCode::WorkspaceNotFound,
                    DetachTagResponse,
                    metadata.request_uuid,
                    error_data
                );
            };
            let workspace = hashmap_workspaces_r.get(&workspace_id).unwrap().clone();
            drop(hashmap_workspaces_r); // We don't need to lock the HashMap anymore.
            let workspace_r = workspace.read().await;

            let Some(tag_id) = val_tag_id(&workspace_r, &req.tag_id) else {
                return_error!(
                    ReturnCode::TagNotFound,
                    DetachTagResponse,
                    metadata.request_uuid,
                    error_data
                );
            };

            let (opt_shelf_id, remote) = val_shelf_id(&workspace_r, &req.shelf_id);
            let Some(shelf_id) = opt_shelf_id else {
                return_error!(
                    ReturnCode::ShelfNotFound,
                    DetachTagResponse,
                    metadata.request_uuid,
                    error_data
                );
            };

            //[/] Business Logic
            let return_code = {
                if remote {
                    let peer_id: NodeId =
                        workspace_r.remote_shelves.get(&shelf_id).unwrap().1.clone();
                    drop(workspace_r);
                    match peer_service.call((peer_id, Request::from(req))).await {
                        Ok(res) => parse_code(res.metadata().unwrap().return_code),
                        Err(_) => ReturnCode::PeerServiceError, // [!] Unknown error, expand with errors from peer service
                    }
                } else {
                    let tag_ref = workspace_r.tags.get(&tag_id).unwrap().clone();

                    drop(workspace_r);
                    let shelf_manager_r = shelf_manager.read().await;
                    let shelf = shelf_manager_r.shelves.get(&shelf_id).unwrap().clone();
                    drop(shelf_manager_r);
                    let mut shelf_w = shelf.write().await;
                    let path = PathBuf::from(&req.path);
                    let result = if path.is_file() {
                        shelf_w.detach(path, tag_ref)
                    } else {
                        shelf_w.detach_dtag(path, tag_ref)
                    };

                    match result {
                        Ok(true) => ReturnCode::Success,                          // Success
                        Ok(false) => ReturnCode::NotTagged,                       // File not tagged
                        Err(UpdateErr::PathNotFound) => ReturnCode::PathNotFound, // Path not found
                        Err(UpdateErr::FileNotFound) => ReturnCode::FileNotFound, // File not found
                        Err(UpdateErr::PathNotDir) => ReturnCode::PathNotDir, // "Nothing ever happens" -Chudda
                    }
                }
            };

            if return_code == ReturnCode::Success {
                notify_queue.write().await.push_back({
                    Notification::Operation(Operation {
                        target: ActionTarget::Shelf.into(),
                        id: shelf_id.as_bytes().to_vec(),
                        action: ActionType::Edit.into(),
                    })
                });
            };

            let metadata = ResponseMetadata {
                request_uuid: metadata.request_uuid,
                return_code: return_code as u32,
                error_data,
            };
            Ok(DetachTagResponse {
                metadata: Some(metadata),
            })
        })
    }
}

impl Service<AttachTag> for RpcService {
    type Response = AttachTagResponse;
    type Error = ();
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: AttachTag) -> Self::Future {
        let metadata = req.metadata.clone().unwrap();
        let notify_queue = self.notify_queue.clone();
        let shelf_manager = self.shelf_manager.clone();
        let workspaces = self.workspaces.clone();
        let mut peer_service = self.peer_service.clone();
        Box::pin(async move {
            let error_data: Option<ErrorData> = None;

            let hashmap_workspaces_r = workspaces.read().await;
            let Some(workspace_id) = val_workspace_id(&hashmap_workspaces_r, &req.workspace_id)
            else {
                return_error!(
                    ReturnCode::WorkspaceNotFound,
                    AttachTagResponse,
                    metadata.request_uuid,
                    error_data
                );
            };

            let workspace = hashmap_workspaces_r.get(&workspace_id).unwrap().clone();
            drop(hashmap_workspaces_r); // We don't need to lock the HashMap anymore.
            let workspace_r = workspace.read().await;

            let Some(tag_id) = val_tag_id(&workspace_r, &req.tag_id) else {
                return_error!(
                    ReturnCode::TagNotFound,
                    AttachTagResponse,
                    metadata.request_uuid,
                    error_data
                );
            };

            let (opt_shelf_id, remote) = val_shelf_id(&workspace_r, &req.shelf_id);
            let Some(shelf_id) = opt_shelf_id else {
                return_error!(
                    ReturnCode::ShelfNotFound,
                    AttachTagResponse,
                    metadata.request_uuid,
                    error_data
                );
            };

            let return_code = {
                if remote {
                    let peer_id: NodeId =
                        workspace_r.remote_shelves.get(&shelf_id).unwrap().1.clone();
                    drop(workspace_r); // Workspace read lock no longer needed.
                    match peer_service.call((peer_id, Request::from(req))).await {
                        Ok(res) => parse_code(res.metadata().unwrap().return_code),
                        Err(_) => ReturnCode::PeerServiceError, // [!] Unknown error, expand with errors from peer service
                    }
                } else {
                    let tag_ref = workspace_r.tags.get(&tag_id).unwrap().clone();
                    drop(workspace_r);
                    let shelf_manager_r = shelf_manager.read().await;
                    let shelf = shelf_manager_r.shelves.get(&shelf_id).unwrap().clone();
                    drop(shelf_manager_r);
                    let mut shelf_w = shelf.write().await;

                    let path = PathBuf::from(&req.path);
                    let result = if path.is_file() {
                        shelf_w.attach(path, tag_ref)
                    } else {
                        shelf_w.attach_dtag(path, tag_ref)
                    };

                    match result {
                        Ok(true) => ReturnCode::Success,                          // Success
                        Ok(false) => ReturnCode::TagAlreadyAttached, // File already tagged
                        Err(UpdateErr::PathNotFound) => ReturnCode::PathNotFound, // Path not found
                        Err(UpdateErr::FileNotFound) => ReturnCode::FileNotFound, // File not found
                        Err(UpdateErr::PathNotDir) => ReturnCode::PathNotDir, // "Nothing ever happens" -Chudda
                    }
                }
            };

            if return_code == ReturnCode::Success {
                notify_queue.write().await.push_back({
                    Notification::Operation(Operation {
                        target: ActionTarget::Shelf.into(),
                        id: shelf_id.as_bytes().to_vec(),
                        action: ActionType::Edit.into(),
                    })
                });
            };

            let metadata = ResponseMetadata {
                request_uuid: metadata.request_uuid,
                return_code: return_code as u32,
                error_data,
            };
            Ok(AttachTagResponse {
                metadata: Some(metadata),
            })
        })
    }
}

impl Service<RemoveShelf> for RpcService {
    type Response = RemoveShelfResponse;
    type Error = ();
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: RemoveShelf) -> Self::Future {
        let workspaces = self.workspaces.clone();
        let metadata = req.metadata.clone().unwrap();
        let shelf_manager = self.shelf_manager.clone();
        let notify_queue = self.notify_queue.clone();
        let mut peer_service = self.peer_service.clone();
        Box::pin(async move {
            let error_data: Option<ErrorData> = None;

            let hashmap_workspaces_r = workspaces.read().await;
            let Some(workspace_id) = val_workspace_id(&hashmap_workspaces_r, &req.workspace_id)
            else {
                return_error!(
                    ReturnCode::WorkspaceNotFound,
                    RemoveShelfResponse,
                    metadata.request_uuid,
                    error_data
                );
            };

            let workspace = hashmap_workspaces_r.get(&workspace_id).unwrap().clone();
            drop(hashmap_workspaces_r); // We don't need to lock the HashMap anymore.
            let workspace_r = workspace.read().await;

            let (opt_shelf_id, remote) = val_shelf_id(&workspace_r, &req.shelf_id);
            let Some(shelf_id) = opt_shelf_id else {
                return_error!(
                    ReturnCode::ShelfNotFound,
                    RemoveShelfResponse,
                    metadata.request_uuid,
                    error_data
                );
            };

            //[/] Business Logic
            let return_code = {
                if remote {
                    let peer_id: NodeId =
                        workspace_r.remote_shelves.get(&shelf_id).unwrap().1.clone();
                    drop(workspace_r); // Workspace read lock no longer needed.
                    match peer_service.call((peer_id, Request::from(req))).await {
                        Ok(res) => parse_code(res.metadata().unwrap().return_code),
                        Err(_) => ReturnCode::PeerServiceError, // [!] Unknown error, expand with errors from peer service
                    }
                } else {
                    drop(workspace_r);
                    workspace.write().await.local_shelves.remove(&shelf_id);

                    let mut shelf_manager = shelf_manager.write().await;
                    if shelf_manager.try_remove_shelf(shelf_id).await {
                        notify_queue.write().await.push_back({
                            Notification::Operation(Operation {
                                target: ActionTarget::Shelf.into(),
                                id: shelf_id.as_bytes().to_vec(),
                                action: ActionType::Delete.into(),
                            })
                        });
                    }
                    ReturnCode::Success
                }
            };

            if return_code == ReturnCode::Success {
                notify_queue.write().await.push_back({
                    Notification::Operation(Operation {
                        target: ActionTarget::Workspace.into(),
                        id: workspace_id.as_bytes().to_vec(),
                        action: ActionType::Edit.into(),
                    })
                });
            }

            let metadata = ResponseMetadata {
                request_uuid: metadata.request_uuid,
                return_code: return_code as u32,
                error_data,
            };
            Ok(RemoveShelfResponse {
                metadata: Some(metadata),
            })
        })
    }
}

impl Service<EditShelf> for RpcService {
    type Response = EditShelfResponse;
    type Error = ();
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: EditShelf) -> Self::Future {
        let workspaces = self.workspaces.clone();
        let metadata = req.metadata.clone();
        let metadata = metadata.unwrap();
        let notify_queue = self.notify_queue.clone();
        let mut peer_service = self.peer_service.clone();
        Box::pin(async move {
            let error_data: Option<ErrorData> = None;

            let hashmap_workspaces_r = workspaces.read().await;
            let Some(workspace_id) = val_workspace_id(&hashmap_workspaces_r, &req.workspace_id)
            else {
                return_error!(
                    ReturnCode::WorkspaceNotFound,
                    EditShelfResponse,
                    metadata.request_uuid,
                    error_data
                );
            };
            let workspace = hashmap_workspaces_r.get(&workspace_id).unwrap().clone();
            drop(hashmap_workspaces_r); // We don't need to lock the HashMap anymore.
            let workspace_r = workspace.read().await;

            let (opt_shelf_id, remote) = val_shelf_id(&workspace_r, &req.shelf_id);
            let Some(shelf_id) = opt_shelf_id else {
                return_error!(
                    ReturnCode::ShelfNotFound,
                    EditShelfResponse,
                    metadata.request_uuid,
                    error_data
                );
            };

            //[/] Business Logic
            let return_code = {
                if remote {
                    let peer_id: NodeId =
                        workspace_r.remote_shelves.get(&shelf_id).unwrap().1.clone();
                    drop(workspace_r); // Workspace read lock no longer needed.
                    match peer_service.call((peer_id, Request::from(req))).await {
                        Ok(res) => parse_code(res.metadata().unwrap().return_code),
                        Err(_) => ReturnCode::PeerServiceError, // [!] Unknown error, expand with errors from peer service
                    }
                } else {
                    drop(workspace_r);
                    workspace.write().await.edit_shelf(
                        shelf_id,
                        Some(req.name.clone()),
                        Some(req.description.clone()),
                    );
                    notify_queue.write().await.push_back({
                        Notification::Operation(Operation {
                            target: ActionTarget::Workspace.into(),
                            id: shelf_id.as_bytes().to_vec(),
                            action: ActionType::Edit.into(),
                        })
                    });
                    ReturnCode::Success
                }
            };

            let metadata = ResponseMetadata {
                request_uuid: metadata.request_uuid,
                return_code: return_code as u32,
                error_data,
            };
            Ok(EditShelfResponse {
                metadata: Some(metadata),
            })
        })
    }
}

impl Service<AddShelf> for RpcService {
    type Response = AddShelfResponse;
    type Error = ();
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: AddShelf) -> Self::Future {
        let workspaces = self.workspaces.clone();
        let metadata = req.metadata.clone();
        let metadata = metadata.unwrap();
        let shelf_manager = self.shelf_manager.clone();
        let notify_queue = self.notify_queue.clone();
        let daemon_info = self.daemon_info.clone();
        let mut peer_service = self.peer_service.clone();

        Box::pin(async move {
            let mut error_data: Option<ErrorData> = None;
            let mut shelf_id: Option<ShelfId> = None;

            let hashmap_workspaces_r = workspaces.read().await;
            let Some(workspace_id) = val_workspace_id(&hashmap_workspaces_r, &req.workspace_id)
            else {
                return_error!(
                    ReturnCode::WorkspaceNotFound,
                    AddShelfResponse,
                    metadata.request_uuid,
                    error_data
                );
            };

            let workspace = hashmap_workspaces_r.get(&workspace_id).unwrap().clone();
            drop(hashmap_workspaces_r); // We don't need to lock the HashMap anymore.

            //[/] Peer ID unwrap & validation
            //[!] missing peer validation (only parsing)
            let Ok(peer_id) = parse_peer_id(&req.peer_id.clone()) else {
                return_error!(
                    ReturnCode::PeerNotFound,
                    AddShelfResponse,
                    metadata.request_uuid,
                    error_data
                );
            };

            //[/] Business Logic
            let return_code = {
                if peer_id != daemon_info.id {
                    match peer_service
                        .call((peer_id, Request::from(req.clone())))
                        .await
                    {
                        Ok(res) => parse_code(res.metadata().unwrap().return_code),
                        Err(_) => ReturnCode::PeerServiceError, // [!] Unknown error, expand with errors from peer service
                    }
                } else {
                    let path = PathBuf::from(req.path.clone());
                    let shelf_id_res = shelf_manager.write().await.add_shelf(path.clone());
                    match shelf_id_res {
                        //[!] Shelf should be added via the Workspace to apply AutoTaggers
                        Ok(id) => {
                            workspace
                                .write()
                                .await
                                .add_shelf(
                                    id,
                                    req.name.unwrap_or_else(|| {
                                        PathBuf::from(req.path.clone())
                                            .components()
                                            .last()
                                            .and_then(|comp| comp.as_os_str().to_str())
                                            .unwrap_or_default()
                                            .to_string()
                                    }),
                                    req.description.unwrap_or_else(|| "".to_string()),
                                    req.path.into(),
                                )
                                .await;

                            shelf_id = Some(id);
                            ReturnCode::Success
                        }
                        Err(e) => {
                            error_data = Some(ErrorData {
                                error_data: vec![e.to_string()],
                            });
                            ReturnCode::ShelfCreationIOError
                        }
                    }
                }
            };

            let encoded_shelf_id;
            if shelf_id.is_some() {
                encoded_shelf_id = Some(shelf_id.unwrap().as_bytes().to_vec());
            } else {
                encoded_shelf_id = None;
            }

            if return_code == ReturnCode::Success {
                notify_queue.write().await.push_back({
                    Notification::Operation(Operation {
                        target: ActionTarget::Workspace.into(),
                        id: workspace_id.as_bytes().to_vec(),
                        action: ActionType::Edit.into(),
                    })
                });
            }
            let metadata = ResponseMetadata {
                request_uuid: metadata.request_uuid,
                return_code: return_code as u32,
                error_data,
            };
            Ok(AddShelfResponse {
                shelf_id: encoded_shelf_id,
                metadata: Some(metadata),
            })
        })
    }
}

impl Service<DeleteWorkspace> for RpcService {
    type Response = DeleteWorkspaceResponse;
    type Error = ();
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: DeleteWorkspace) -> Self::Future {
        let workspaces = self.workspaces.clone();
        let metadata = req.metadata.unwrap();
        let shelf_manager = self.shelf_manager.clone();
        let notify_queue = self.notify_queue.clone();
        Box::pin(async move {
            let error_data: Option<ErrorData> = None;

            let hashmap_workspaces_r = workspaces.read().await;
            let Some(workspace_id) = val_workspace_id(&hashmap_workspaces_r, &req.workspace_id)
            else {
                return_error!(
                    ReturnCode::WorkspaceNotFound,
                    DeleteWorkspaceResponse,
                    metadata.request_uuid,
                    error_data
                );
            };

            let workspace = hashmap_workspaces_r.get(&workspace_id).unwrap().clone();
            drop(hashmap_workspaces_r); // We don't need to lock the HashMap anymore.

            //[/] Business Logic
            let return_code = {
                let shelves: Vec<ShelfId> = workspace
                    .read()
                    .await
                    .local_shelves
                    .keys()
                    .map(|id| id.clone())
                    .collect();
                let mut shelf_manager_w = shelf_manager.write().await;
                for shelf_id in shelves {
                    shelf_manager_w.try_remove_shelf(shelf_id).await;
                }
                drop(shelf_manager_w);
                workspaces.write().await.remove(&workspace_id);
                ReturnCode::Success
            };

            notify_queue.write().await.push_back({
                // Always Success
                Notification::Operation(Operation {
                    target: ActionTarget::Workspace.into(),
                    id: workspace_id.as_bytes().to_vec(),
                    action: ActionType::Delete.into(),
                })
            });

            let metadata = ResponseMetadata {
                request_uuid: metadata.request_uuid,
                return_code: return_code as u32,
                error_data: None,
            };
            Ok(DeleteWorkspaceResponse {
                metadata: Some(metadata),
            })
        })
    }
}

impl Service<EditTag> for RpcService {
    type Response = EditTagResponse;
    type Error = ();
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: EditTag) -> Self::Future {
        let workspaces = self.workspaces.clone();
        let metadata = req.metadata.unwrap();
        let notify_queue = self.notify_queue.clone();
        Box::pin(async move {
            let error_data: Option<ErrorData> = None;

            let parent_id: Option<TagId>;

            let hashmap_workspaces_r = workspaces.read().await;
            let Some(workspace_id) = val_workspace_id(&hashmap_workspaces_r, &req.workspace_id)
            else {
                return_error!(
                    ReturnCode::WorkspaceNotFound,
                    EditTagResponse,
                    metadata.request_uuid,
                    error_data
                );
            };

            let workspace = hashmap_workspaces_r.get(&workspace_id).unwrap().clone();
            drop(hashmap_workspaces_r); // We don't need to lock the HashMap anymore.
            let workspace_r = workspace.read().await;

            let Some(tag_id) = val_tag_id(&workspace_r, &req.tag_id) else {
                return_error!(
                    ReturnCode::TagNotFound,
                    EditTagResponse,
                    metadata.request_uuid,
                    error_data
                );
            };

            //[/] Parent ID unwrap & validation
            //[?] Inconsistent: Unwrap and validate here or in TagManager ??
            if let Some(id) = req.parent_id.clone() {
                if let Ok(id) = uuid(id.clone()) {
                    if workspace_r.tags.contains_key(&id) {
                        parent_id = Some(id);
                    } else {
                        parent_id = None;
                    }
                } else {
                    parent_id = None;
                }
                if parent_id.is_none() {
                    return_error!(
                        ReturnCode::ParentNotFound,
                        EditTagResponse,
                        metadata.request_uuid,
                        error_data
                    );
                }
            } else {
                parent_id = None;
            }

            //[/] Tag Name Validation
            if req.name.clone().is_empty() {
                return_error!(
                    ReturnCode::TagNameEmpty,
                    EditTagResponse,
                    metadata.request_uuid,
                    error_data
                );
            }

            //[/] Business Logic
            let return_code = {
                let tag_ref = workspace_r.tags.get(&tag_id).unwrap().tag_ref.clone();

                let mut tag_w = tag_ref.write().unwrap();

                tag_w.name = req.name.clone();
                tag_w.priority = req.priority;
                if req.parent_id.is_some() {
                    tag_w.parent = Some(workspace_r.tags.get(&parent_id.unwrap()).unwrap().clone());
                } else {
                    tag_w.parent = None;
                }
                ReturnCode::Success
            };

            if return_code == ReturnCode::Success {
                notify_queue.write().await.push_back({
                    Notification::Operation(Operation {
                        target: ActionTarget::Tag.into(),
                        id: tag_id.as_bytes().to_vec(),
                        action: ActionType::Edit.into(),
                    })
                });
            }

            let metadata = ResponseMetadata {
                request_uuid: metadata.request_uuid,
                return_code: return_code as u32,
                error_data: None,
            };
            Ok(EditTagResponse {
                metadata: Some(metadata),
            })
        })
    }
}

impl Service<EditWorkspace> for RpcService {
    type Response = EditWorkspaceResponse;
    type Error = ();
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: EditWorkspace) -> Self::Future {
        let workspaces = self.workspaces.clone();
        let metadata = req.metadata.unwrap();
        let notify_queue = self.notify_queue.clone();
        Box::pin(async move {
            let error_data: Option<ErrorData> = None;

            let hashmap_workspaces_r = workspaces.read().await;
            let Some(workspace_id) = val_workspace_id(&hashmap_workspaces_r, &req.workspace_id)
            else {
                return_error!(
                    ReturnCode::WorkspaceNotFound,
                    EditWorkspaceResponse,
                    metadata.request_uuid,
                    error_data
                );
            };

            let workspace = hashmap_workspaces_r.get(&workspace_id).unwrap().clone();
            drop(hashmap_workspaces_r); // We don't need to lock the HashMap anymore.

            //[/] Workspace Name Validation
            if req.name.clone().is_empty() {
                let return_code = ReturnCode::WorkspaceNameEmpty; // Name is Empty 
                let metadata = ResponseMetadata {
                    request_uuid: metadata.request_uuid,
                    return_code: return_code as u32,
                    error_data,
                };
                return Ok(EditWorkspaceResponse {
                    metadata: Some(metadata),
                });
            }

            //[/] Business Logic
            let return_code = {
                let mut to_edit = workspace.write().await;
                to_edit.name = req.name;
                to_edit.description = req.description;
                ReturnCode::Success
            };

            if return_code == ReturnCode::Success {
                notify_queue.write().await.push_back({
                    Notification::Operation(Operation {
                        target: ActionTarget::Workspace.into(),
                        id: workspace_id.as_bytes().to_vec(),
                        action: ActionType::Edit.into(),
                    })
                });
            }

            let metadata = ResponseMetadata {
                request_uuid: metadata.request_uuid,
                return_code: return_code as u32,
                error_data: None,
            };
            return Ok(EditWorkspaceResponse {
                metadata: Some(metadata),
            });
        })
    }
}

impl Service<GetShelves> for RpcService {
    type Response = GetShelvesResponse;
    type Error = ();
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: GetShelves) -> Self::Future {
        let workspaces = self.workspaces.clone();
        let metadata = req.metadata.unwrap();
        let daemon_info = self.daemon_info.clone();
        Box::pin(async move {
            let error_data: Option<ErrorData> = None;

            let hashmap_workspaces_r = workspaces.read().await;
            let Some(workspace_id) = val_workspace_id(&hashmap_workspaces_r, &req.workspace_id)
            else {
                return_error!(
                    ReturnCode::WorkspaceNotFound,
                    GetShelvesResponse,
                    metadata.request_uuid,
                    error_data
                );
            };

            let workspace = hashmap_workspaces_r.get(&workspace_id).unwrap().clone();
            drop(hashmap_workspaces_r); // We don't need to lock the HashMap anymore.
            let workspace_r = workspace.read().await;

            let mut shelves = Vec::new();
            // [!] Can be changed with iter mut + impl Into<rpc::Shelf> for shelf::Shelf
            for (id, shelf_info) in &workspace_r.local_shelves {
                shelves.push(Shelf {
                        peer_id: daemon_info.id.as_bytes().to_vec(),
                        shelf_id: (*id).as_bytes().to_vec(),
                        name: shelf_info.name.clone(),
                        path: shelf_info.root_path.to_string_lossy().into_owned(),
                    });
                }
            for (id, (shelf_info, peer_id)) in &workspace_r.remote_shelves {
                shelves.push(Shelf {
                    peer_id: peer_id.as_bytes().to_vec(),
                    shelf_id: (*id).as_bytes().to_vec(),
                    name: shelf_info.name.clone(),
                    path: shelf_info.root_path.to_string_lossy().into_owned(),
                });
            }
            let metadata = ResponseMetadata {
                request_uuid: metadata.request_uuid,
                return_code: ReturnCode::Success as u32,
                error_data: None,
            };
            Ok(GetShelvesResponse {
                shelves,
                metadata: Some(metadata),
            })
        })
    }
}

impl Service<GetWorkspaces> for RpcService {
    type Response = GetWorkspacesResponse;
    type Error = ();
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: GetWorkspaces) -> Self::Future {
        let workspaces = self.workspaces.clone();
        let metadata = req.metadata.unwrap();
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

            let metadata = ResponseMetadata {
                request_uuid: metadata.request_uuid,
                return_code: ReturnCode::Success as u32,
                error_data: None,
            };
            Ok(GetWorkspacesResponse {
                workspaces: workspace_ls,
                metadata: Some(metadata),
            })
        })
    }
}

impl Service<CreateWorkspace> for RpcService {
    type Response = CreateWorkspaceResponse;
    type Error = ();
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: CreateWorkspace) -> Self::Future {
        let workspaces = self.workspaces.clone();
        let metadata = req.metadata.unwrap();
        let notify_queue = self.notify_queue.clone();
        let shelf_manager = self.shelf_manager.clone();
        Box::pin(async move {
            let mut id: WorkspaceId;
            loop {
                id = Uuid::now_v7();
                if !workspaces.read().await.contains_key(&id) {
                    break;
                }
            }

            notify_queue.write().await.push_back({
                Notification::Operation(Operation {
                    target: ActionTarget::Workspace.into(),
                    id: id.as_bytes().to_vec(),
                    action: ActionType::Create.into(),
                })
            });

            let workspace = Workspace {
                id,
                name: req.name,
                description: req.description,
                local_shelves: HashMap::new(), // Placeholder for local shelves
                remote_shelves: HashMap::new(), // Placeholder for remote shelves
                auto_taggers: Vec::new(),      // Placeholder for auto taggers
                shelf_manager: shelf_manager.clone(),
                tags: HashMap::new(),
                lookup: HashMap::new(),
            };
            workspaces.write().await.insert(id, Arc::new(RwLock::new(workspace)));

            let metadata = ResponseMetadata {
                request_uuid: metadata.request_uuid,
                return_code: ReturnCode::Success as u32,
                error_data: None,
            };
            return Ok(CreateWorkspaceResponse {
                workspace_id: id.as_bytes().to_vec(),
                metadata: Some(metadata),
            });
        })
    }
}

impl Service<CreateTag> for RpcService {
    type Response = CreateTagResponse;
    type Error = ();
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: CreateTag) -> Self::Future {
        let workspaces = self.workspaces.clone();
        let notify_queue = self.notify_queue.clone();
        let metadata = req.metadata.clone().unwrap();
        Box::pin(async move {
            let error_data: Option<ErrorData> = None;

            let parent_id: Option<TagId>;

            let hashmap_workspaces_r = workspaces.read().await;
            let Some(workspace_id) = val_workspace_id(&hashmap_workspaces_r, &req.workspace_id)
            else {
                return_error!(
                    ReturnCode::WorkspaceNotFound,
                    CreateTagResponse,
                    metadata.request_uuid,
                    error_data
                );
            };

            let workspace = hashmap_workspaces_r.get(&workspace_id).unwrap().clone();
            drop(hashmap_workspaces_r); // We don't need to lock the HashMap anymore.

            //[/] Parent ID unwrap & validation
            if let Some(id) = req.parent_id.clone() {
                if let Ok(id) = uuid(id.clone()) {
                    if workspace
                        .read()
                        .await
                        .tags
                        .contains_key(&id)
                    {
                        parent_id = Some(id);
                    } else {
                        parent_id = None;
                    }
                } else {
                    parent_id = None;
                }
                if parent_id.is_none() {
                    let return_code = ReturnCode::ParentNotFound; // Parent does not exist 
                    let metadata = ResponseMetadata {
                        request_uuid: metadata.request_uuid,
                        return_code: return_code as u32,
                        error_data,
                    };
                    return Ok(CreateTagResponse {
                        tag_id: None,
                        metadata: Some(metadata),
                    });
                }
            } else {
                parent_id = None;
            }

            // Create the tag using the tag manager
            let id = workspace
                .write()
                .await
                .create_tag(&req.name, req.priority, parent_id);
            match id {
                Ok(tag_id) => {
                    notify_queue.write().await.push_back({
                        Notification::Operation(Operation {
                            target: ActionTarget::Tag.into(),
                            id: tag_id.as_bytes().to_vec(),
                            action: ActionType::Delete.into(),
                        })
                    });
                    // If the tag was created successfully, return the response with the tag ID
                    let metadata = ResponseMetadata {
                        request_uuid: metadata.request_uuid,
                        return_code: ReturnCode::Success as u32, // Success
                        error_data: None,
                    };
                    return Ok(CreateTagResponse {
                        tag_id: Some(tag_id.as_bytes().to_vec()),
                        metadata: Some(metadata),
                    });
                }
                Err(e) => {
                    let return_code: ReturnCode;
                    return_code = match e {
                        TagErr::DuplicateTag(_) => ReturnCode::DuplicateTag, // Tag name already exists
                        TagErr::ParentMissing(_) => ReturnCode::ParentNotFound, // Parent tag does not exist
                        TagErr::InconsistentTagManager(_) => ReturnCode::InternalStateError, // Inconsistent tag manager state
                        TagErr::TagMissing(_) => todo!(), //[!] Fix unreachable errors in Error Enums
                    };
                    // If there was an error creating the tag, return an error response
                    let metadata = ResponseMetadata {
                        request_uuid: metadata.request_uuid,
                        return_code: return_code as u32, // Requested parent does not exist
                        error_data: None,
                    };
                    return Ok(CreateTagResponse {
                        tag_id: None,
                        metadata: Some(metadata),
                    });
                }
            }
        })
    }
}
