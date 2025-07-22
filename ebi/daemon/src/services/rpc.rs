use crate::services::peer::PeerService;
use crate::services::workspace::{WorkspaceService, GetWorkspace, GetShelf, GetTag, UnassignShelf, RemoveWorkspace};
use crate::shelf::shelf::{ShelfId, ShelfType, UpdateErr, ShelfRef};
use crate::tag::{TagId, TagRef};
use crate::workspace::{Workspace, WorkspaceRef};
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

async fn try_get_workspace(rawid: &Vec<u8>, srv: &mut WorkspaceService) -> Result<WorkspaceRef, ReturnCode> {
    let id = uuid(rawid.clone()).map_err(|_| ReturnCode::WorkspaceNotFound)?; //[!] Change to UuidParseErr //[?] Why ??
    srv.call(GetWorkspace { id }).await
}

async fn try_get_shelf(w_rawid: &Vec<u8>, s_rawid: &Vec<u8>, srv: &mut WorkspaceService) -> Result<ShelfRef, ReturnCode> {
    let shelf_id = uuid(s_rawid.clone()).map_err(|_| ReturnCode::ShelfNotFound)?;
    let workspace_id = uuid(w_rawid.clone()).map_err(|_| ReturnCode::WorkspaceNotFound)?;
    srv.call(GetShelf { shelf_id, workspace_id }).await
}

async fn try_get_tag(w_rawid: &Vec<u8>, t_rawid: &Vec<u8>, srv: &mut WorkspaceService) -> Result<TagRef, ReturnCode> {
    let tag_id = uuid(t_rawid.clone()).map_err(|_| ReturnCode::ShelfNotFound)?;
    let workspace_id = uuid(w_rawid.clone()).map_err(|_| ReturnCode::WorkspaceNotFound)?;
    srv.call(GetTag { tag_id, workspace_id }).await
}

fn val_tag_id(workspace: &RwLockReadGuard<Workspace>, bytes: &Vec<u8>) -> Option<TagId> {
    uuid(bytes.clone())
        .ok()
        .filter(|id| workspace.tags.contains_key(&id) )
}

//[!] Handle poisoned locks

#[derive(Clone)]
pub struct RpcService {
    pub daemon_info: Arc<DaemonInfo>,
    pub peer_srv: PeerService,
    pub workspace_srv: WorkspaceService,
    pub tasks: Arc<HashMap<TaskID, JoinHandle<()>>>,

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
        let mut workspace_srv = self.workspace_srv.clone();
        Box::pin(async move {
            let error_data: Option<ErrorData> = None;

            let Ok(workspace) = try_get_workspace(&req.workspace_id, &mut workspace_srv).await else {
                return_error!(
                    ReturnCode::WorkspaceNotFound, //[!] Change to UuidParseErr //[?] Why ??
                    DeleteTagResponse,
                    metadata.request_uuid,
                    error_data
                );
            };

            let workspace_r = workspace.read().await;

            let Some(tag_id) = val_tag_id(&workspace_r, &req.tag_id) else {
                return_error!(
                    ReturnCode::TagNotFound,
                    DeleteTagResponse,
                    metadata.request_uuid,
                    error_data
                );
            };

            let tag = workspace_r.tags.get(&tag_id).unwrap().clone();
            let shelves = workspace_r.shelves.clone();

            drop(workspace_r);
            
            for (_id, shelf) in shelves {
                let shelf_r = shelf.read().await;
                let shelf_type = shelf_r.shelf_type.clone();
                let shelf_config = shelf_r.config.clone();
                drop(shelf_r);
                
                match shelf_type {
                    ShelfType::Local(shelf_data) => {
                        let _ = shelf_data.write().await.strip(PathBuf::from(""), tag.clone()); //[/] Operation can't fail 
                        //[?] Are (remote) Sync'd shelves also in shelves ?? 
                        if shelf_config.sync_config.is_some() {
                            //[TODO] Sync Notification
                        }
                    }
                    ShelfType::Remote(_peer) => {
                        //[TODO] Remote Request
                        //[?] For all Nodes with Write-Permissions, relay ?? 
                        //[!] Request must be atomic, relaying may not be sufficient 
                        // Relay the request via the peer service
                        // Await for the response to be inserted into the relay_responses map
                        // Handle the responses
                        // think of when to remove RwLocks
                    }
                }
            }

            workspace.write().await.tags.remove(&tag_id);

            notify_queue.write().await.push_back({
                Notification::Operation(Operation {
                    target: ActionTarget::Tag.into(),
                    id: req.tag_id,
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
        let mut peer_srv = self.peer_srv.clone();
        let mut workspace_srv = self.workspace_srv.clone();
        Box::pin(async move {
            let error_data: Option<ErrorData> = None;

            let shelf_res = try_get_shelf(&req.workspace_id, &req.shelf_id, &mut workspace_srv).await;

            let shelf = match shelf_res {
                Ok(shelf) => shelf,
                Err(err) => {
                    return_error!(
                        err,
                        StripTagResponse,
                        metadata.request_uuid,
                        error_data
                    );
                }
            };

            let Ok(tag_ref) = try_get_tag(&req.workspace_id, &req.tag_id, &mut workspace_srv).await else {
                return_error!(
                    ReturnCode::TagNotFound,
                    StripTagResponse,
                    metadata.request_uuid,
                    error_data
                );
            };
            let shelf_r = shelf.read().await;
            let shelf_id = req.shelf_id.clone();
            let shelf_type = shelf_r.shelf_type.clone();
            let shelf_config = shelf_r.config.clone();
            drop(shelf_r);

            let return_code = {
                match shelf_type {
                    ShelfType::Local(shelf_data) => {
                        let path = PathBuf::from(req.path);
                        let mut shelf_w = shelf_data.write().await;
                        let result = match shelf_w.strip(path, tag_ref) {
                            Ok(_) => ReturnCode::Success,
                            Err(UpdateErr::PathNotDir) => ReturnCode::PathNotDir, // Path not a Directory
                            Err(UpdateErr::PathNotFound) => ReturnCode::PathNotFound, // Path not Found
                            Err(UpdateErr::FileNotFound) => ReturnCode::FileNotFound, // "Nothing ever happens" -Chudda
                        };
                        drop(shelf_w);
                        if shelf_config.sync_config.is_some() {
                            //[TODO] Sync Notification
                        }
                        result
                    }
                    ShelfType::Remote(peer) => {
                        //[/] Request can be relayed, it is already atomic 
                        match peer_srv.call((peer, Request::from(req))).await {
                            Ok(res) => parse_code(res.metadata().unwrap().return_code),
                            Err(_) => ReturnCode::PeerServiceError, //[!] Generic error, expand with PeerService errors 
                        }
                    }
                }
            };

            if return_code == ReturnCode::Success {
                notify_queue.write().await.push_back({
                    Notification::Operation(Operation {
                        target: ActionTarget::Shelf.into(),
                        id: shelf_id,
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
        let mut workspace_srv = self.workspace_srv.clone();
        let mut peer_srv = self.peer_srv.clone();
        Box::pin(async move {
            let error_data: Option<ErrorData> = None;
            let shelf_res = try_get_shelf(&req.workspace_id, &req.shelf_id, &mut workspace_srv).await;

            let shelf = match shelf_res {
                Ok(shelf) => shelf,
                Err(err) => {
                    return_error!(
                        err,
                        DetachTagResponse,
                        metadata.request_uuid,
                        error_data
                    );
                }
            };

            let Ok(tag_ref) = try_get_tag(&req.workspace_id, &req.tag_id, &mut workspace_srv).await else {
                return_error!(
                    ReturnCode::TagNotFound,
                    DetachTagResponse,
                    metadata.request_uuid,
                    error_data
                );
            };
            let shelf_r = shelf.read().await;
            let shelf_id = req.shelf_id.clone();
            let shelf_type = shelf_r.shelf_type.clone();
            let shelf_config = shelf_r.config.clone();
            drop(shelf_r);

            //[/] Business Logic
            let return_code = {
                match shelf_type {
                    ShelfType::Local(shelf_data) => {
                        let path = PathBuf::from(&req.path);
                        let mut shelf_w = shelf_data.write().await;
                        let result = if path.is_file() {
                            shelf_w.detach(path, tag_ref)
                        } else {
                            shelf_w.detach_dtag(path, tag_ref)
                        };
                        drop(shelf_w);
                        if shelf_config.sync_config.is_some() {
                            //[TODO] Sync Notification
                        }
                        match result {
                            Ok(true) => ReturnCode::Success,                            // Success
                            Ok(false) => ReturnCode::NotTagged,                         // File not tagged
                            Err(UpdateErr::PathNotFound) => ReturnCode::PathNotFound,   // Path not found
                            Err(UpdateErr::FileNotFound) => ReturnCode::FileNotFound,   // File not found
                            Err(UpdateErr::PathNotDir) => ReturnCode::PathNotDir,       // "Nothing ever happens" -Chudda
                        }
                    }
                    ShelfType::Remote(peer) => {
                        //[/] Request can be relayed, it is already atomic 
                        match peer_srv.call((peer, Request::from(req))).await {
                            Ok(res) => parse_code(res.metadata().unwrap().return_code),
                            Err(_) => ReturnCode::PeerServiceError, //[!] Generic error, expand with PeerService errors 
                        }
                    }
                }
            };

            if return_code == ReturnCode::Success {
                notify_queue.write().await.push_back({
                    Notification::Operation(Operation {
                        target: ActionTarget::Shelf.into(),
                        id: shelf_id,
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
        let mut workspace_srv = self.workspace_srv.clone();
        let mut peer_srv = self.peer_srv.clone();
        Box::pin(async move {
            let error_data: Option<ErrorData> = None;
            let shelf_res = try_get_shelf(&req.workspace_id, &req.shelf_id, &mut workspace_srv).await;

            let shelf = match shelf_res {
                Ok(shelf) => shelf,
                Err(err) => {
                    return_error!(
                        err,
                        AttachTagResponse,
                        metadata.request_uuid,
                        error_data
                    );
                }
            };

            let Ok(tag_ref) = try_get_tag(&req.workspace_id, &req.tag_id, &mut workspace_srv).await else {
                return_error!(
                    ReturnCode::TagNotFound,
                    AttachTagResponse,
                    metadata.request_uuid,
                    error_data
                );
            };
            let shelf_r = shelf.read().await;
            let shelf_id = req.shelf_id.clone();
            let shelf_type = shelf_r.shelf_type.clone();
            let shelf_config = shelf_r.config.clone();
            drop(shelf_r);

            //[/] Business Logic
            let return_code = {
                match shelf_type {
                    ShelfType::Local(shelf_data) => {
                        let path = PathBuf::from(&req.path);
                        let mut shelf_w = shelf_data.write().await;
                        let result = if path.is_file() {
                            shelf_w.attach(path, tag_ref)
                        } else {
                            shelf_w.attach_dtag(path, tag_ref)
                        };
                        drop(shelf_w);
                        if shelf_config.sync_config.is_some() {
                            //[TODO] Sync Notification
                        }
                        match result {
                            Ok(true) => ReturnCode::Success,                            // Success
                            Ok(false) => ReturnCode::TagAlreadyAttached,                // File not tagged
                            Err(UpdateErr::PathNotFound) => ReturnCode::PathNotFound,   // Path not found
                            Err(UpdateErr::FileNotFound) => ReturnCode::FileNotFound,   // File not found
                            Err(UpdateErr::PathNotDir) => ReturnCode::PathNotDir,       // "Nothing ever happens" -Chudda
                        }
                    }
                    ShelfType::Remote(peer) => {
                        //[/] Request can be relayed, it is already atomic 
                        match peer_srv.call((peer, Request::from(req))).await {
                            Ok(res) => parse_code(res.metadata().unwrap().return_code),
                            Err(_) => ReturnCode::PeerServiceError, //[!] Generic error, expand with PeerService errors 
                        }
                    }
                }
            };

            if return_code == ReturnCode::Success {
                notify_queue.write().await.push_back({
                    Notification::Operation(Operation {
                        target: ActionTarget::Shelf.into(),
                        id: shelf_id,
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
        let metadata = req.metadata.clone().unwrap();
        let notify_queue = self.notify_queue.clone();
        let mut peer_srv = self.peer_srv.clone();
        let mut workspace_srv = self.workspace_srv.clone();
        Box::pin(async move {
            let error_data: Option<ErrorData> = None;

            let Ok(workspace) = try_get_workspace(&req.workspace_id, &mut workspace_srv).await else {
                return_error!(
                    ReturnCode::WorkspaceNotFound,
                    RemoveShelfResponse,
                    metadata.request_uuid,
                    error_data
                );
            };

            let Ok(shelf) = try_get_shelf(&req.workspace_id, &req.shelf_id, &mut workspace_srv).await else {
                return_error!(
                    ReturnCode::ShelfNotFound,
                    RemoveShelfResponse,
                    metadata.request_uuid,
                    error_data
                );
            };
            let shelf_r = shelf.read().await;
            let shelf_id = shelf_r.info.id;
            let shelf_type = shelf_r.shelf_type.clone();
            let shelf_config = shelf_r.config.clone();
            drop(shelf_r);
            let workspace_id = workspace.read().await.id;

            //[/] Business Logic
            let return_code = {
                match shelf_type {
                    ShelfType::Local(_) => {
                        workspace.write().await.shelves.remove(&shelf_id);
                        let _ = workspace_srv.call(UnassignShelf { shelf_id, workspace_id }).await;
                        notify_queue.write().await.push_back({
                            Notification::Operation(Operation {
                                target: ActionTarget::Shelf.into(),
                                id: shelf_id.as_bytes().to_vec(),
                                action: ActionType::Delete.into(),
                            })
                        });
                        if shelf_config.sync_config.is_some() {
                            //[TODO] Sync Notification
                        }
                        ReturnCode::Success //[/] Operation can't fail 
                    }
                    ShelfType::Remote(peer) => {
                        //[/] Request can be relayed, it is already atomic 
                        match peer_srv.call((peer, Request::from(req))).await {
                            Ok(res) => {
                                workspace.write().await.shelves.remove(&shelf_id);
                                parse_code(res.metadata().unwrap().return_code)
                            },
                            Err(_) => ReturnCode::PeerServiceError, //[!] Generic error, expand with PeerService errors 
                        }
                    }
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
        let metadata = req.metadata.clone();
        let metadata = metadata.unwrap();
        let notify_queue = self.notify_queue.clone();
        let mut peer_srv = self.peer_srv.clone();
        let mut workspace_srv = self.workspace_srv.clone();
        Box::pin(async move {
            let error_data: Option<ErrorData> = None;

            let shelf_res = try_get_shelf(&req.shelf_id, &req.workspace_id, &mut workspace_srv).await;
            let shelf = match shelf_res {
                Ok(shelf) => shelf,
                Err(err) => {
                    return_error!(
                        err,
                        EditShelfResponse,
                        metadata.request_uuid,
                        error_data
                    );
                }
            };

            let shelf_r = shelf.read().await;
            let shelf_type = shelf_r.shelf_type.clone();
            let shelf_config = shelf_r.config.clone();
            drop(shelf_r);

            //[/] Business Logic
            let return_code = {
                match shelf_type {
                    ShelfType::Local(_) => {
                        shelf.write().await.edit_info(
                            Some(req.name.clone()),
                            Some(req.description.clone()),
                        ).await;
                        if shelf_config.sync_config.is_some() {
                            //[TODO] Sync Notification
                        }
                        ReturnCode::Success
                    }
                    ShelfType::Remote(peer) => {
                        //[/] Request can be relayed, it is already atomic 
                        match peer_srv.call((peer, Request::from(req.clone()))).await {
                        Ok(res) => {
                            parse_code(res.metadata().unwrap().return_code)
                        },
                            Err(_) => ReturnCode::PeerServiceError, //[!] Generic error, expand with PeerService errors 
                        }
                    }
                }
            };
            
            if return_code == ReturnCode::Success {
                notify_queue.write().await.push_back({
                    Notification::Operation(Operation {
                        target: ActionTarget::Workspace.into(),
                        id: req.shelf_id,
                        action: ActionType::Edit.into(),
                    })
                });
            }

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
        let mut workspace_srv = self.workspace_srv.clone();
        let metadata = req.metadata.clone();
        let metadata = metadata.unwrap();
        let notify_queue = self.notify_queue.clone();
        let daemon_info = self.daemon_info.clone();
        let mut peer_srv = self.peer_srv.clone();

        Box::pin(async move {
            let mut error_data: Option<ErrorData> = None;
            let mut shelf_id: Option<ShelfId> = None;

            let Ok(workspace) = try_get_workspace(&req.workspace_id, &mut workspace_srv).await else {
                return_error!(
                    ReturnCode::WorkspaceNotFound,
                    AddShelfResponse,
                    metadata.request_uuid,
                    error_data
                );
            };

            //[/] Peer ID unwrap & validation
            //[!] Missing Peer validation (only parsing)
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
                    match peer_srv
                        .call((peer_id, Request::from(req.clone())))
                        .await
                    {
                        Ok(res) => parse_code(res.metadata().unwrap().return_code),
                        Err(_) => ReturnCode::PeerServiceError, //[!] Generic error, expand with PeerService errors 
                    }
                } else {
                    let path = PathBuf::from(req.path.clone());
                    let res = workspace
                        .write()
                        .await
                        .add_local_shelf(
                            path,
                            req.name.unwrap_or_else(|| {
                                PathBuf::from(req.path.clone())
                                    .components()
                                    .last()
                                    .and_then(|comp| comp.as_os_str().to_str())
                                    .unwrap_or_default()
                                    .to_string()
                            }),
                            req.description.unwrap_or_else(|| "".to_string())
                        );
                    match res {
                        Ok(id) => {
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
                        id: req.workspace_id,
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
        let mut workspace_srv = self.workspace_srv.clone();
        let metadata = req.metadata.unwrap();
        let notify_queue = self.notify_queue.clone();
        Box::pin(async move {
            let error_data: Option<ErrorData> = None;

            let Ok(workspace_id) = uuid(req.workspace_id.clone()) else {
                return_error!(
                    ReturnCode::WorkspaceNotFound,
                    DeleteWorkspaceResponse,
                    metadata.request_uuid,
                    error_data
                );
            };

            let Ok(()) = workspace_srv.call(RemoveWorkspace{ workspace_id }).await else {
                return_error!(
                    ReturnCode::WorkspaceNotFound,
                    DeleteWorkspaceResponse,
                    metadata.request_uuid,
                    error_data
                );
            };

            notify_queue.write().await.push_back({
                Notification::Operation(Operation {
                    target: ActionTarget::Workspace.into(),
                    id: workspace_id.as_bytes().to_vec(),
                    action: ActionType::Delete.into(),
                })
            });

            let metadata = ResponseMetadata {
                request_uuid: metadata.request_uuid,
                return_code: ReturnCode::Success as u32,
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
        let mut workspace_srv = self.workspace_srv.clone();
        let metadata = req.metadata.unwrap();
        let notify_queue = self.notify_queue.clone();
        Box::pin(async move {
            let error_data: Option<ErrorData> = None;

            let tag_res = try_get_tag(&req.tag_id, &req.workspace_id, &mut workspace_srv).await;
            let tag = match tag_res {
                Ok(tag) => tag,
                Err(err) => {
                    return_error!(
                        err,
                        EditTagResponse,
                        metadata.request_uuid,
                        error_data
                    );
                }
            };

            let parent = {
                if let Some(id) = req.parent_id.clone() {
                    let Ok(parent_tag) = try_get_tag(&id, &req.workspace_id, &mut workspace_srv).await else {
                        return_error!(
                            ReturnCode::ParentNotFound,
                            EditTagResponse,
                            metadata.request_uuid,
                            error_data
                        );
                    };
                    Some(parent_tag)
                } else {
                    None
                }
            };

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
                let mut tag_w = tag.tag_ref.write().unwrap();
                tag_w.name = req.name.clone();
                tag_w.priority = req.priority;
                tag_w.parent = parent;
                ReturnCode::Success
            };

            if return_code == ReturnCode::Success {
                notify_queue.write().await.push_back({
                    Notification::Operation(Operation {
                        target: ActionTarget::Tag.into(),
                        id: req.tag_id,
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
        let mut workspace_srv = self.workspace_srv.clone();
        let metadata = req.metadata.unwrap();
        let notify_queue = self.notify_queue.clone();
        Box::pin(async move {
            let error_data: Option<ErrorData> = None;

            let Ok(workspace) = try_get_workspace(&req.workspace_id, &mut workspace_srv).await else {
                return_error!(
                    ReturnCode::WorkspaceNotFound,
                    EditWorkspaceResponse,
                    metadata.request_uuid,
                    error_data
                );
            };

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
                        id: req.workspace_id,
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
        let mut workspace_srv = self.workspace_srv.clone();
        let metadata = req.metadata.unwrap();
        let daemon_info = self.daemon_info.clone();
        Box::pin(async move {
            let error_data: Option<ErrorData> = None;

            let Ok(workspace) = try_get_workspace(&req.workspace_id, &mut workspace_srv).await else {
                return_error!(
                    ReturnCode::WorkspaceNotFound,
                    GetShelvesResponse,
                    metadata.request_uuid,
                    error_data
                );
            };

            let mut shelves = Vec::new();
            let workspace_r = workspace.read().await;
            // [!] Can be changed with iter mut + impl Into<rpc::Shelf> for shelf::Shelf
            for (id, shelf) in &workspace_r.shelves {
                let shelf_r = shelf.read().await;
                let shelf_type = shelf_r.shelf_type.clone();
                let shelf_info = shelf_r.info.clone();
                let shelf_config = shelf_r.config.clone();
                drop(shelf_r);
                let peer_id = match &shelf_type {
                    ShelfType::Local(_) => daemon_info.id.as_bytes().to_vec(),
                    ShelfType::Remote(peer) => peer.as_bytes().to_vec(),
                };
                let sync_id = match &shelf_config.sync_config {
                    Some(config) => Some(config.group_id.as_bytes().to_vec()),
                    None => None,
                };
                shelves.push(Shelf {
                    shelf_id: id.as_bytes().to_vec(),
                    peer_id,
                    name: shelf_info.name.clone(),
                    description: shelf_info.description.clone(),
                    path: shelf_info.root_path.to_string_lossy().into_owned(),
                    sync_id
                });
            }
            drop(workspace_r);

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
        let mut workspace_srv = self.workspace_srv.clone();
        let metadata = req.metadata.unwrap();
        Box::pin(async move {
            let workspace_ls = workspace_srv.call(crate::services::workspace::GetWorkspaces {}).await.unwrap();

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
        let mut workspace_srv = self.workspace_srv.clone();
        let notify_queue = self.notify_queue.clone();
        let metadata = req.metadata.clone().unwrap();
        Box::pin(async move {

            let id = workspace_srv.call(crate::services::workspace::CreateWorkspace { name: req.name, description: req.description }).await.unwrap();

            notify_queue.write().await.push_back({
                Notification::Operation(Operation {
                    target: ActionTarget::Workspace.into(),
                    id: id.as_bytes().to_vec(),
                    action: ActionType::Create.into(),
                })
            });

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
        let mut workspace_srv = self.workspace_srv.clone();
        let notify_queue = self.notify_queue.clone();
        let metadata = req.metadata.clone().unwrap();
        Box::pin(async move {
            let error_data: Option<ErrorData> = None;

            let Ok(workspace) = try_get_workspace(&req.workspace_id, &mut workspace_srv).await else {
                return_error!(
                    ReturnCode::WorkspaceNotFound,
                    CreateTagResponse,
                    metadata.request_uuid,
                    error_data
                );
            };

            let parent = {
                if let Some(id) = req.parent_id.clone() {
                    let Ok(parent) = try_get_tag(&id, &req.workspace_id, &mut workspace_srv).await else {
                        return_error!(
                            ReturnCode::ParentNotFound,
                            CreateTagResponse,
                            metadata.request_uuid,
                            error_data
                        );
                    };
                    Some(parent)
                } else {
                    None
                }
            };

            if workspace.read().await.lookup.contains_key(&req.name.to_string()) {
                return_error!(
                    ReturnCode::ParentNotFound,
                        CreateTagResponse,
                        metadata.request_uuid,
                        error_data
                );
            };

            let id = workspace.write().await.create_tag(req.priority, req.name, parent); //[!] Does not consider existing tags with same name

            notify_queue.write().await.push_back({
                Notification::Operation(Operation {
                    target: ActionTarget::Tag.into(),
                    id: id.as_bytes().to_vec(),
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
                tag_id: Some(id.as_bytes().to_vec()),
                metadata: Some(metadata),
            });
        })
    }
}
