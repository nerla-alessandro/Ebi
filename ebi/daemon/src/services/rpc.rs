use crate::services::peer::PeerService;
use crate::shelf::shelf::{ShelfId, ShelfInfo, ShelfManager, UpdateErr};
use crate::tag::{TagId, TagManager};
use crate::workspace::{Workspace, WorkspaceId};
use ebi_proto::rpc::*;
use iroh::NodeId;
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::RwLock;
use tokio::sync::watch::{Receiver, Sender};
use tokio::task::JoinHandle;
use tower::Service;
use uuid::Uuid;

pub type RequestId = Uuid;

//[!] Handle poisoned locks

#[derive(Clone)]
pub struct RpcService {
    pub daemon_info: Arc<DaemonInfo>,
    pub peer_service: PeerService,
    pub tasks: Arc<HashMap<TaskID, JoinHandle<()>>>,
    pub tag_manager: Arc<RwLock<TagManager>>,
    pub shelf_manager: Arc<RwLock<ShelfManager>>,
    pub workspaces: Arc<RwLock<HashMap<WorkspaceId, Workspace>>>,
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
        let tag_manager = self.tag_manager.clone();
        let metadata = req.metadata.clone().unwrap();
        let notify_queue = self.notify_queue.clone();
        let shelf_manager = self.shelf_manager.clone();
        let workspaces = self.workspaces.clone();
        Box::pin(async move {
            let error_data: Option<ErrorData> = None;
            let tag_id: Option<TagId>;
            let workspace_id: Option<WorkspaceId>;

            //[TODO] Macro for uuid unwrap from bytes & validation
            //[/] Workspace ID unwrap & validation
            if let Ok(id) = uuid(req.workspace_id) {
                if workspaces.read().await.contains_key(&id) {
                    workspace_id = Some(id);
                } else {
                    workspace_id = None;
                }
            } else {
                workspace_id = None;
            }

            if workspace_id.is_none() {
                let return_code = ReturnCode::WorkspaceNotFound; // Workspace does not exist 
                let metadata = ResponseMetadata {
                    request_uuid: metadata.request_uuid,
                    return_code: return_code as u32,
                    error_data,
                };
                return Ok(DeleteTagResponse {
                    metadata: Some(metadata),
                });
            }
            let workspace_id = workspace_id.unwrap();

            //[/] Tag ID unwrap & validation
            if let Ok(id) = uuid(req.tag_id) {
                if tag_manager
                    .read()
                    .await
                    .tags
                    .contains_key(&(id, workspace_id))
                {
                    tag_id = Some(id);
                } else {
                    tag_id = None;
                }
            } else {
                tag_id = None;
            }

            if tag_id.is_none() {
                let return_code = ReturnCode::TagNotFound; // Tag does not exist 
                let metadata = ResponseMetadata {
                    request_uuid: metadata.request_uuid,
                    return_code: return_code as u32,
                    error_data,
                };
                return Ok(DeleteTagResponse {
                    metadata: Some(metadata),
                });
            }
            let tag_id = tag_id.unwrap();

            //[/] Business Logic
            {
                let tag_manager_r = tag_manager.read().await;
                let workspaces_r = workspaces.read().await;
                let tag = tag_manager_r.tags.get(&(tag_id, workspace_id)).unwrap();
                let workspace = workspaces_r.get(&workspace_id).unwrap();
                for (_, shelf_info) in workspace.local_shelves.iter() {
                    let shelf_manager_r = shelf_manager.read().await;
                    let shelf = shelf_manager_r.shelves.get(&shelf_info.id);
                    if let Some(shelf) = shelf {
                        let _ = shelf.write().await.strip(PathBuf::from(""), tag.clone());
                    }
                }
                for (_, (shelf_info, peer_id)) in workspace.remote_shelves.iter() {
                    let shelf_manager = shelf_manager.read().await;
                    let shelf = shelf_manager.shelves.get(&shelf_info.id);
                    if let Some(shelf) = shelf {
                        let _shelf = shelf;
                        let _peer_id = peer_id;
                        //[TODO] Remote Request
                        // Relay the request via the peer service
                        // Await for the response to be inserted into the relay_responses map
                        // Handle the responses
                    }
                }
            } // Tag_manager read lock goes out of scope before requesting write
            tag_manager
                .write()
                .await
                .tags
                .retain(|id, _| !(id == &(tag_id, workspace_id)));

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
        let tag_manager = self.tag_manager.clone();
        let metadata = req.metadata.clone().unwrap();
        let notify_queue = self.notify_queue.clone();
        let shelf_manager = self.shelf_manager.clone();
        let workspaces = self.workspaces.clone();
        let mut peer_service = self.peer_service.clone();
        Box::pin(async move {
            let error_data: Option<ErrorData> = None;

            //[TODO] Macro for uuid unwrap from bytes & validation
            let tag_id: Option<TagId>;
            let workspace_id: Option<WorkspaceId>;
            let shelf_id: Option<ShelfId>;
            let mut remote: bool = false;

            //[/] Workspace ID unwrap & validation
            if let Ok(id) = uuid(req.workspace_id.clone()) {
                if workspaces.read().await.contains_key(&id) {
                    workspace_id = Some(id);
                } else {
                    workspace_id = None;
                }
            } else {
                workspace_id = None;
            }

            if workspace_id.is_none() {
                let return_code = ReturnCode::WorkspaceNotFound; // Workspace does not exist 
                let metadata = ResponseMetadata {
                    request_uuid: metadata.request_uuid,
                    return_code: return_code as u32,
                    error_data,
                };
                return Ok(StripTagResponse {
                    metadata: Some(metadata),
                });
            }
            let workspace_id = workspace_id.unwrap();

            //[/] Tag ID unwrap & validation
            if let Ok(id) = uuid(req.tag_id.clone()) {
                if tag_manager
                    .read()
                    .await
                    .tags
                    .contains_key(&(id, workspace_id))
                {
                    tag_id = Some(id);
                } else {
                    tag_id = None;
                }
            } else {
                tag_id = None;
            }

            if tag_id.is_none() {
                let return_code = ReturnCode::TagNotFound; // Tag does not exist 
                let metadata = ResponseMetadata {
                    request_uuid: metadata.request_uuid,
                    return_code: return_code as u32,
                    error_data,
                };
                return Ok(StripTagResponse {
                    metadata: Some(metadata),
                });
            }
            let tag_id = tag_id.unwrap();

            //[/] Shelf ID unwrap & validation
            if let Ok(id) = uuid(req.shelf_id.clone()) {
                let workspace_r = workspaces.read().await;
                let (shelf_exists, is_remote) =
                    workspace_r.get(&workspace_id).unwrap().contains(id); //Workspace ID already validated
                if shelf_exists {
                    shelf_id = Some(id);
                    remote = is_remote;
                } else {
                    shelf_id = None;
                }
            } else {
                shelf_id = None;
            }

            if shelf_id.is_none() {
                let return_code = ReturnCode::ShelfNotFound; // Shelf does not exist 
                let metadata = ResponseMetadata {
                    request_uuid: metadata.request_uuid,
                    return_code: return_code as u32,
                    error_data,
                };
                return Ok(StripTagResponse {
                    metadata: Some(metadata),
                });
            }
            let shelf_id = shelf_id.unwrap();

            //[/] Business Logic
            let return_code = {
                if remote {
                    let workspace = workspaces.read().await;
                    let workspace = workspace.get(&workspace_id).unwrap();
                    let remote_shelf = workspace.remote_shelves.get(&shelf_id);
                    if let Some((_, peer_id)) = remote_shelf {
                        match peer_service.call((*peer_id, Request::from(req))).await {
                            Ok(res) => parse_code(res.metadata().unwrap().return_code),
                            Err(_) => ReturnCode::PeerServiceError, // [!] Unknown error, expand with errors from peer service
                        }
                    } else {
                        ReturnCode::ShelfNotFound // Shelf not Found
                    }
                } else {
                    let tag_manager_w = tag_manager.write().await;
                    let tag_ref = tag_manager_w.tags.get(&(tag_id, workspace_id)).unwrap();
                    // we can unwrap because we checked that the shelf existed
                    let shelf_manager_r = shelf_manager.read().await;
                    let mut shelf = shelf_manager_r
                        .shelves
                        .get(&shelf_id)
                        .unwrap()
                        .write()
                        .await;
                    let path = PathBuf::from(req.path);
                    match shelf.strip(path, tag_ref.clone()) {
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
        let tag_manager = self.tag_manager.clone();
        let metadata = req.metadata.clone().unwrap();
        let notify_queue = self.notify_queue.clone();
        let shelf_manager = self.shelf_manager.clone();
        let workspaces = self.workspaces.clone();
        let mut peer_service = self.peer_service.clone();
        Box::pin(async move {
            let error_data: Option<ErrorData> = None;

            let tag_id: Option<TagId>;
            let workspace_id: Option<WorkspaceId>;
            let shelf_id: Option<ShelfId>;
            let mut remote: bool = false;

            //[TODO] Macro for uuid unwrap from bytes & validation
            //[/] Workspace ID unwrap & validation
            if let Ok(id) = uuid(req.workspace_id.clone()) {
                if workspaces.read().await.contains_key(&id) {
                    workspace_id = Some(id);
                } else {
                    workspace_id = None;
                }
            } else {
                workspace_id = None;
            }

            if workspace_id.is_none() {
                let return_code = ReturnCode::WorkspaceNotFound; // Workspace does not exist 
                let metadata = ResponseMetadata {
                    request_uuid: metadata.request_uuid,
                    return_code: return_code as u32,
                    error_data,
                };
                return Ok(DetachTagResponse {
                    metadata: Some(metadata),
                });
            }
            let workspace_id = workspace_id.unwrap();

            //[/] Tag ID unwrap & validation
            if let Ok(id) = uuid(req.tag_id.clone()) {
                if tag_manager
                    .read()
                    .await
                    .tags
                    .contains_key(&(id, workspace_id))
                {
                    tag_id = Some(id);
                } else {
                    tag_id = None;
                }
            } else {
                tag_id = None;
            }

            if tag_id.is_none() {
                let return_code = ReturnCode::TagNotFound; // Tag does not exist 
                let metadata = ResponseMetadata {
                    request_uuid: metadata.request_uuid,
                    return_code: return_code as u32,
                    error_data,
                };
                return Ok(DetachTagResponse {
                    metadata: Some(metadata),
                });
            }
            let tag_id = tag_id.unwrap();

            //[/] Shelf ID unwrap & validation
            if let Ok(id) = uuid(req.shelf_id.clone()) {
                let workspace_r = workspaces.read().await;
                let (shelf_exists, is_remote) =
                    workspace_r.get(&workspace_id).unwrap().contains(id); //Workspace ID already validated
                if shelf_exists {
                    shelf_id = Some(id);
                    remote = is_remote;
                } else {
                    shelf_id = None;
                }
            } else {
                shelf_id = None;
            }

            if shelf_id.is_none() {
                let return_code = ReturnCode::ShelfNotFound; // Shelf does not exist 
                let metadata = ResponseMetadata {
                    request_uuid: metadata.request_uuid,
                    return_code: return_code as u32,
                    error_data,
                };
                return Ok(DetachTagResponse {
                    metadata: Some(metadata),
                });
            }
            let shelf_id = shelf_id.unwrap();

            //[/] Business Logic
            let return_code = {
                if remote {
                    let workspace_r = workspaces.read().await;
                    let workspace = workspace_r.get(&workspace_id).unwrap();
                    let remote_shelf = workspace.remote_shelves.get(&shelf_id);
                    let (_, peer_id) = remote_shelf.unwrap(); // Already validated  
                    match peer_service.call((*peer_id, Request::from(req))).await {
                        Ok(res) => parse_code(res.metadata().unwrap().return_code),
                        Err(_) => ReturnCode::PeerServiceError, // [!] Unknown error, expand with errors from peer service
                    }
                } else {
                    let tag_ref = tag_manager
                        .write()
                        .await
                        .tags
                        .get(&(tag_id, workspace_id))
                        .unwrap()
                        .clone();

                    let shelf_manager_r = shelf_manager.read().await;
                    let mut shelf = shelf_manager_r
                        .shelves
                        .get(&shelf_id)
                        .unwrap()
                        .write()
                        .await;

                    let path = PathBuf::from(&req.path);
                    let result = if path.is_file() {
                        shelf.detach(path, tag_ref)
                    } else {
                        shelf.detach_dtag(path, tag_ref)
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
        let tag_manager = self.tag_manager.clone();
        let metadata = req.metadata.clone().unwrap();
        let notify_queue = self.notify_queue.clone();
        let shelf_manager = self.shelf_manager.clone();
        let workspaces = self.workspaces.clone();
        let mut peer_service = self.peer_service.clone();
        Box::pin(async move {
            let error_data: Option<ErrorData> = None;

            let tag_id: Option<TagId>;
            let workspace_id: Option<WorkspaceId>;
            let shelf_id: Option<ShelfId>;
            let mut remote: bool = false;

            //[TODO] Macro for uuid unwrap from bytes & validation
            //[/] Workspace ID unwrap & validation
            if let Ok(id) = uuid(req.workspace_id.clone()) {
                if workspaces.read().await.contains_key(&id) {
                    workspace_id = Some(id);
                } else {
                    workspace_id = None;
                }
            } else {
                workspace_id = None;
            }

            if workspace_id.is_none() {
                let return_code = ReturnCode::WorkspaceNotFound; // Workspace does not exist 
                let metadata = ResponseMetadata {
                    request_uuid: metadata.request_uuid,
                    return_code: return_code as u32,
                    error_data,
                };
                return Ok(AttachTagResponse {
                    metadata: Some(metadata),
                });
            }
            let workspace_id = workspace_id.unwrap();

            //[/] Tag ID unwrap & validation
            if let Ok(id) = uuid(req.tag_id.clone()) {
                if tag_manager
                    .read()
                    .await
                    .tags
                    .contains_key(&(id, workspace_id))
                {
                    tag_id = Some(id);
                } else {
                    tag_id = None;
                }
            } else {
                tag_id = None;
            }

            if tag_id.is_none() {
                let return_code = 2; // Tag does not exist 
                let metadata = ResponseMetadata {
                    request_uuid: metadata.request_uuid,
                    return_code,
                    error_data,
                };
                return Ok(AttachTagResponse {
                    metadata: Some(metadata),
                });
            }
            let tag_id = tag_id.unwrap();

            //[/] Shelf ID unwrap & validation
            if let Ok(id) = uuid(req.shelf_id.clone()) {
                let workspace_r = workspaces.read().await;
                let (shelf_exists, is_remote) =
                    workspace_r.get(&workspace_id).unwrap().contains(id); //Workspace ID already validated
                if shelf_exists {
                    shelf_id = Some(id);
                    remote = is_remote;
                } else {
                    shelf_id = None;
                }
            } else {
                shelf_id = None;
            }

            if shelf_id.is_none() {
                let return_code = ReturnCode::ShelfNotFound; // Shelf does not exist 
                let metadata = ResponseMetadata {
                    request_uuid: metadata.request_uuid,
                    return_code: return_code as u32,
                    error_data,
                };
                return Ok(AttachTagResponse {
                    metadata: Some(metadata),
                });
            }
            let shelf_id = shelf_id.unwrap();

            //[/] Business Logic
            let return_code = {
                if remote {
                    let workspace_r = workspaces.read().await;
                    let workspace = workspace_r.get(&workspace_id).unwrap();
                    let remote_shelf = workspace.remote_shelves.get(&shelf_id);
                    let (_, peer_id) = remote_shelf.unwrap(); // Already validated  
                    match peer_service.call((*peer_id, Request::from(req))).await {
                        Ok(res) => parse_code(res.metadata().unwrap().return_code),
                        Err(_) => ReturnCode::PeerServiceError, // [!] Unknown error, expand with errors from peer service
                    }
                } else {
                    let tag_ref = tag_manager
                        .write()
                        .await
                        .tags
                        .get(&(tag_id, workspace_id))
                        .unwrap()
                        .clone();

                    let shelf_manager_r = shelf_manager.read().await;
                    let mut shelf = shelf_manager_r
                        .shelves
                        .get(&shelf_id)
                        .unwrap()
                        .write()
                        .await;

                    let path = PathBuf::from(&req.path);
                    let result = if path.is_file() {
                        shelf.attach(path, tag_ref)
                    } else {
                        shelf.attach_dtag(path, tag_ref)
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

            let workspace_id: Option<WorkspaceId>;
            let shelf_id: Option<ShelfId>;
            let mut remote: bool = false;

            //[TODO] Macro for uuid unwrap from bytes & validation
            //[/] Workspace ID unwrap & validation
            if let Ok(id) = uuid(req.workspace_id.clone()) {
                if workspaces.read().await.contains_key(&id) {
                    workspace_id = Some(id);
                } else {
                    workspace_id = None;
                }
            } else {
                workspace_id = None;
            }

            if workspace_id.is_none() {
                let return_code = ReturnCode::WorkspaceNotFound; // Workspace does not exist 
                let metadata = ResponseMetadata {
                    request_uuid: metadata.request_uuid,
                    return_code: return_code as u32,
                    error_data,
                };
                return Ok(RemoveShelfResponse {
                    metadata: Some(metadata),
                });
            }
            let workspace_id = workspace_id.unwrap();

            //[/] Shelf ID unwrap & validation
            if let Ok(id) = uuid(req.shelf_id.clone()) {
                let workspace_r = workspaces.read().await;
                let (shelf_exists, is_remote) =
                    workspace_r.get(&workspace_id).unwrap().contains(id); //Workspace ID already validated
                if shelf_exists {
                    shelf_id = Some(id);
                    remote = is_remote;
                } else {
                    shelf_id = None;
                }
            } else {
                shelf_id = None;
            }

            if shelf_id.is_none() {
                let return_code = ReturnCode::ShelfNotFound; // Shelf does not exist 
                let metadata = ResponseMetadata {
                    request_uuid: metadata.request_uuid,
                    return_code: return_code as u32,
                    error_data,
                };
                return Ok(RemoveShelfResponse {
                    metadata: Some(metadata),
                });
            }
            let shelf_id = shelf_id.unwrap();

            //[/] Business Logic
            let return_code = {
                if remote {
                    let workspace_r = workspaces.read().await;
                    let workspace = workspace_r.get(&workspace_id).unwrap();
                    let remote_shelf = workspace.remote_shelves.get(&shelf_id);
                    let (_, peer_id) = remote_shelf.unwrap(); // Already validated  
                    match peer_service.call((*peer_id, Request::from(req))).await {
                        Ok(res) => parse_code(res.metadata().unwrap().return_code),
                        Err(_) => ReturnCode::PeerServiceError, // [!] Unknown error, expand with errors from peer service
                    }
                } else {
                    workspaces
                        .write()
                        .await
                        .get_mut(&workspace_id)
                        .unwrap()
                        .local_shelves
                        .remove(&shelf_id);
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
            let workspace_id: Option<WorkspaceId>;
            let shelf_id: Option<ShelfId>;
            let mut remote: bool = false;

            //[TODO] Macro for uuid unwrap from bytes & validation
            //[/] Workspace ID unwrap & validation
            if let Ok(id) = uuid(req.workspace_id.clone()) {
                if workspaces.read().await.contains_key(&id) {
                    workspace_id = Some(id);
                } else {
                    workspace_id = None;
                }
            } else {
                workspace_id = None;
            }

            if workspace_id.is_none() {
                let return_code = ReturnCode::WorkspaceNotFound; // Workspace does not exist 
                let metadata = ResponseMetadata {
                    request_uuid: metadata.request_uuid,
                    return_code: return_code as u32,
                    error_data,
                };
                return Ok(EditShelfResponse {
                    metadata: Some(metadata),
                });
            }
            let workspace_id = workspace_id.unwrap();

            //[/] Shelf ID unwrap & validation
            if let Ok(id) = uuid(req.shelf_id.clone()) {
                let workspace_r = workspaces.read().await;
                let (shelf_exists, is_remote) =
                    workspace_r.get(&workspace_id).unwrap().contains(id); //Workspace ID already validated
                if shelf_exists {
                    shelf_id = Some(id);
                    remote = is_remote;
                } else {
                    shelf_id = None;
                }
            } else {
                shelf_id = None;
            }

            if shelf_id.is_none() {
                let return_code = ReturnCode::ShelfNotFound; // Shelf does not exist 
                let metadata = ResponseMetadata {
                    request_uuid: metadata.request_uuid,
                    return_code: return_code as u32,
                    error_data,
                };
                return Ok(EditShelfResponse {
                    metadata: Some(metadata),
                });
            }
            let shelf_id = shelf_id.unwrap();

            //[/] Business Logic
            let return_code = {
                if remote {
                    let workspace_r = workspaces.read().await;
                    let workspace = workspace_r.get(&workspace_id).unwrap();
                    let remote_shelf = workspace.remote_shelves.get(&shelf_id);
                    let (_, peer_id) = remote_shelf.unwrap(); // Already validated  
                    match peer_service.call((*peer_id, Request::from(req))).await {
                        Ok(res) => parse_code(res.metadata().unwrap().return_code),
                        Err(_) => ReturnCode::PeerServiceError, // [!] Unknown error, expand with errors from peer service
                    }
                } else {
                    workspaces
                        .write()
                        .await
                        .get_mut(&workspace_id)
                        .unwrap()
                        .edit_shelf(
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

            let workspace_id: Option<WorkspaceId>;

            //[TODO] Macro for uuid unwrap from bytes & validation
            //[/] Workspace ID unwrap & validation
            if let Ok(id) = uuid(req.workspace_id.clone()) {
                if workspaces.read().await.contains_key(&id) {
                    workspace_id = Some(id);
                } else {
                    workspace_id = None;
                }
            } else {
                workspace_id = None;
            }

            if workspace_id.is_none() {
                let return_code = ReturnCode::WorkspaceNotFound; // Workspace does not exist 
                let metadata = ResponseMetadata {
                    request_uuid: metadata.request_uuid,
                    return_code: return_code as u32,
                    error_data,
                };
                return Ok(AddShelfResponse {
                    shelf_id: None,
                    metadata: Some(metadata),
                });
            }
            let workspace_id = workspace_id.unwrap();

            //[/] Peer ID unwrap & validation
            let peer_id = parse_peer_id(&req.peer_id.clone());

            if let Err(_) = peer_id {
                let return_code = ReturnCode::PeerNotFound; // Peer does not exist 
                let metadata = ResponseMetadata {
                    request_uuid: metadata.request_uuid,
                    return_code: return_code as u32,
                    error_data,
                };
                return Ok(AddShelfResponse {
                    shelf_id: None,
                    metadata: Some(metadata),
                });
            }
            let peer_id = peer_id.unwrap();

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
                        Ok(id) => {
                            workspaces
                                .write()
                                .await
                                .get_mut(&workspace_id)
                                .unwrap()
                                .local_shelves
                                .insert(
                                    id,
                                    ShelfInfo::new(id, req.name, req.description, req.path),
                                );

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
        let tag_manager = self.tag_manager.clone();
        let shelf_manager = self.shelf_manager.clone();
        let notify_queue = self.notify_queue.clone();
        Box::pin(async move {
            let error_data: Option<ErrorData> = None;

            let workspace_id: Option<WorkspaceId>;

            //[TODO] Macro for uuid unwrap from bytes & validation
            //[/] Workspace ID unwrap & validation
            if let Ok(id) = uuid(req.workspace_id.clone()) {
                if workspaces.read().await.contains_key(&id) {
                    workspace_id = Some(id);
                } else {
                    workspace_id = None;
                }
            } else {
                workspace_id = None;
            }

            if workspace_id.is_none() {
                let return_code = ReturnCode::WorkspaceNotFound; // Workspace does not exist 
                let metadata = ResponseMetadata {
                    request_uuid: metadata.request_uuid,
                    return_code: return_code as u32,
                    error_data,
                };
                return Ok(DeleteWorkspaceResponse {
                    metadata: Some(metadata),
                });
            }
            let workspace_id = workspace_id.unwrap();

            //[/] Business Logic
            let return_code = {
                let tag_rm: Vec<(TagId, WorkspaceId)> = tag_manager
                    .write()
                    .await
                    .tags
                    .keys()
                    .filter(|(_, ws_id)| *ws_id == workspace_id)
                    .cloned()
                    .collect();
                for key in tag_rm {
                    tag_manager.write().await.tags.remove(&key);
                }
                let mut shelves: Vec<ShelfId> = Vec::new();
                for (id, _) in workspaces
                    .read()
                    .await
                    .get(&workspace_id)
                    .unwrap()
                    .local_shelves
                    .iter()
                {
                    shelves.push(*id);
                }
                for shelf_id in shelves {
                    shelf_manager.write().await.try_remove_shelf(shelf_id).await;
                }
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
        let tag_manager = self.tag_manager.clone();
        let metadata = req.metadata.unwrap();
        let notify_queue = self.notify_queue.clone();
        Box::pin(async move {
            let error_data: Option<ErrorData> = None;

            let tag_id: Option<TagId>;
            let workspace_id: Option<WorkspaceId>;
            let parent_id: Option<TagId>;

            //[TODO] Macro for uuid unwrap from bytes & validation
            //[/] Workspace ID unwrap & validation
            if let Ok(id) = uuid(req.workspace_id.clone()) {
                if workspaces.read().await.contains_key(&id) {
                    workspace_id = Some(id);
                } else {
                    workspace_id = None;
                }
            } else {
                workspace_id = None;
            }

            if workspace_id.is_none() {
                let return_code = ReturnCode::WorkspaceNotFound; // Workspace does not exist 
                let metadata = ResponseMetadata {
                    request_uuid: metadata.request_uuid,
                    return_code: return_code as u32,
                    error_data,
                };
                return Ok(EditTagResponse {
                    metadata: Some(metadata),
                });
            }
            let workspace_id = workspace_id.unwrap();

            //[/] Tag ID unwrap & validation
            if let Ok(id) = uuid(req.tag_id.clone()) {
                if tag_manager
                    .read()
                    .await
                    .tags
                    .contains_key(&(id, workspace_id))
                {
                    tag_id = Some(id);
                } else {
                    tag_id = None;
                }
            } else {
                tag_id = None;
            }

            if tag_id.is_none() {
                let return_code = ReturnCode::TagNotFound; // Tag does not exist 
                let metadata = ResponseMetadata {
                    request_uuid: metadata.request_uuid,
                    return_code: return_code as u32,
                    error_data,
                };
                return Ok(EditTagResponse {
                    metadata: Some(metadata),
                });
            }
            let tag_id = tag_id.unwrap();

            //[/] Parent ID unwrap & validation
            //[?] Inconsistent: Unwrap and validate here or in TagManager ??
            if let Some(id) = req.parent_id.clone() {
                if let Ok(id) = uuid(id.clone()) {
                    if tag_manager
                        .read()
                        .await
                        .tags
                        .contains_key(&(id, workspace_id))
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
                    return Ok(EditTagResponse {
                        metadata: Some(metadata),
                    });
                }
            } else {
                parent_id = None;
            }

            //[/] Tag Name Validation
            if req.name.clone().is_empty() {
                let return_code = ReturnCode::TagNameEmpty; // Name is empty
                let metadata = ResponseMetadata {
                    request_uuid: metadata.request_uuid,
                    return_code: return_code as u32,
                    error_data,
                };
                return Ok(EditTagResponse {
                    metadata: Some(metadata),
                });
            }

            //[/] Business Logic
            let return_code = {
                let mut tag_manager_w = tag_manager.write().await;
                let tag = tag_manager_w.tags.get_mut(&(tag_id, workspace_id)).unwrap();
                tag.tag_ref.write().unwrap().name = req.name.clone();
                tag.tag_ref.write().unwrap().priority = req.priority;
                if req.parent_id.is_some() {
                    tag.tag_ref.write().unwrap().parent = Some(
                        tag_manager
                            .read()
                            .await
                            .tags
                            .get(&(parent_id.unwrap(), workspace_id))
                            .unwrap()
                            .clone(),
                    );
                } else {
                    tag.tag_ref.write().unwrap().parent = None;
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

            let workspace_id: Option<WorkspaceId>;

            //[TODO] Macro for uuid unwrap from bytes & validation
            //[/] Workspace ID unwrap & validation
            if let Ok(id) = uuid(req.workspace_id.clone()) {
                if workspaces.read().await.contains_key(&id) {
                    workspace_id = Some(id);
                } else {
                    workspace_id = None;
                }
            } else {
                workspace_id = None;
            }

            if workspace_id.is_none() {
                let return_code = ReturnCode::WorkspaceNotFound; // Workspace does not exist 
                let metadata = ResponseMetadata {
                    request_uuid: metadata.request_uuid,
                    return_code: return_code as u32,
                    error_data,
                };
                return Ok(EditWorkspaceResponse {
                    metadata: Some(metadata),
                });
            }
            let workspace_id = workspace_id.unwrap();

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
                let mut workspace_r = workspaces.write().await;
                let to_edit = workspace_r.get_mut(&workspace_id).unwrap();
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

            let workspace_id: Option<WorkspaceId>;

            //[TODO] Macro for uuid unwrap from bytes & validation
            //[/] Workspace ID unwrap & validation
            if let Ok(id) = uuid(req.workspace_id.clone()) {
                if workspaces.read().await.contains_key(&id) {
                    workspace_id = Some(id);
                } else {
                    workspace_id = None;
                }
            } else {
                workspace_id = None;
            }

            if workspace_id.is_none() {
                let return_code = ReturnCode::WorkspaceNotFound; // Workspace does not exist 
                let metadata = ResponseMetadata {
                    request_uuid: metadata.request_uuid,
                    return_code: return_code as u32,
                    error_data,
                };
                return Ok(GetShelvesResponse {
                    shelves: Vec::new(),
                    metadata: Some(metadata),
                });
            }
            let workspace_id = workspace_id.unwrap();

            let mut shelves = Vec::new();
            if let Some(workspace) = workspaces.read().await.get(&workspace_id) {
                for (id, shelf_info) in &workspace.local_shelves {
                    shelves.push(Shelf {
                        peer_id: daemon_info.id.as_bytes().to_vec(),
                        shelf_id: (*id).as_bytes().to_vec(),
                        name: shelf_info.name.clone(),
                        path: shelf_info.root_path.to_string_lossy().into_owned(),
                    });
                }
                for (id, (shelf_info, peer_id)) in &workspace.remote_shelves {
                    shelves.push(Shelf {
                        peer_id: peer_id.as_bytes().to_vec(),
                        shelf_id: (*id).as_bytes().to_vec(),
                        name: shelf_info.name.clone(),
                        path: shelf_info.root_path.to_string_lossy().into_owned(),
                    });
                }
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
        let tag_manager = self.tag_manager.clone();
        Box::pin(async move {
            let mut workspace_ls = Vec::new();
            for (_, workspace) in workspaces.read().await.iter() {
                let mut tag_ls = Vec::new();
                for tag_ref in tag_manager.write().await.get_tags(workspace.id) {
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
            };
            workspaces.write().await.insert(id, workspace);

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
        let tag_manager = self.tag_manager.clone();
        let workspaces = self.workspaces.clone();
        let notify_queue = self.notify_queue.clone();
        let metadata = req.metadata.clone().unwrap();
        Box::pin(async move {
            let error_data: Option<ErrorData> = None;

            let parent_id: Option<TagId>;
            let workspace_id: Option<WorkspaceId>;

            //[TODO] Macro for uuid unwrap from bytes & validation
            //[/] Workspace ID unwrap & validation
            if let Ok(id) = uuid(req.workspace_id.clone()) {
                if workspaces.read().await.contains_key(&id) {
                    workspace_id = Some(id);
                } else {
                    workspace_id = None;
                }
            } else {
                workspace_id = None;
            }

            if workspace_id.is_none() {
                let return_code = ReturnCode::WorkspaceNotFound; // Workspace does not exist 
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
            let workspace_id = workspace_id.unwrap();

            //[/] Parent ID unwrap & validation
            //[?] Inconsistent: Unwrap and validate here or in TagManager ??
            if let Some(id) = req.parent_id.clone() {
                if let Ok(id) = uuid(id.clone()) {
                    if tag_manager
                        .read()
                        .await
                        .tags
                        .contains_key(&(id, workspace_id))
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
            let id = tag_manager.write().await.create_tag(
                &req.name,
                workspace_id,
                req.priority,
                parent_id,
            );
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
                Err(_) => {
                    // If there was an error creating the tag, return an error response
                    let metadata = ResponseMetadata {
                        request_uuid: metadata.request_uuid,
                        return_code: ReturnCode::ParentNotFound as u32, // Requested parent does not exist
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
