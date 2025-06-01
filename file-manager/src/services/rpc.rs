use crate::rpc::{
    self, ActionTarget, ActionType, AddShelf, AddShelfResponse, AttachTag, AttachTagResponse,
    ClientQuery, ClientQueryData, CreateTag, CreateTagResponse, CreateWorkspace,
    CreateWorkspaceResponse, DeleteTag, DeleteTagResponse, DeleteWorkspace,
    DeleteWorkspaceResponse, DetachTag, DetachTagResponse, EditShelf, EditShelfResponse, EditTag,
    EditTagResponse, EditWorkspace, EditWorkspaceResponse, ErrorData, GetShelves,
    GetShelvesResponse, GetWorkspaces, GetWorkspacesResponse, Heartbeat, Operation, PeerQuery,
    PeerQueryData, QueryResponse, RemoveShelf, RemoveShelfResponse, RequestCode, ResponseMetadata,
    StripTag, StripTagResponse,
};
use crate::services::peer::PeerService;
use crate::shelf::shelf::{ShelfInfo, ShelfManager, UpdateErr};
use crate::tag::{self, TagManager, TagRef};
use crate::workspace::{Workspace, WorkspaceId};
use crate::{shelf, workspace};
use iroh::NodeId;
use std::collections::{HashMap, VecDeque};
use std::ffi::OsStr;
use std::fs::File;
use std::future::Future;
use std::io::Write;
use std::ops::Add;
use std::path::PathBuf;
use std::pin::Pin;
use std::ptr::read;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tonic::{metadata, Response};
use tower::Service;

use super::peer;

//[!] Handle poisoned locks

#[derive(Clone)]
pub struct RpcService {
    pub daemon_info: Arc<DaemonInfo>, //[!] DaemonInfo should be Arc<RwLock<DaemonInfo>>?
    pub peer_service: PeerService,
    pub tasks: Arc<HashMap<TaskID, JoinHandle<()>>>,
    pub tag_manager: Arc<RwLock<TagManager>>,
    pub shelf_manager: Arc<RwLock<ShelfManager>>,
    pub workspaces: Arc<RwLock<HashMap<WorkspaceId, Workspace>>>,
    pub relay_responses: Arc<RwLock<HashMap<u64, (RequestCode, Box<dyn prost::Message>)>>>, //[/] (Code, Box<...>) can be avoided by using an Enum
    pub notify_queue: Arc<RwLock<VecDeque<Notification>>>,
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

impl Service<DeleteTag> for RpcService {
    type Response = DeleteTagResponse;
    type Error = ();
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: DeleteTag) -> Self::Future {
        let tag_manager = self.tag_manager.clone();
        let metadata = req.metadata.unwrap();
        let notify_queue = self.notify_queue.clone();
        let shelf_manager = self.shelf_manager.clone();
        let workspaces = self.workspaces.clone();
        Box::pin(async move {
            let return_code: u32;
            let error_data: Option<ErrorData> = None;

            if tag_manager
                .read()
                .await
                .tags
                .contains_key(&(req.tag_id, req.workspace_id))
            {
                if workspaces.read().await.contains_key(&req.workspace_id) {
                    let tag_manager_read = tag_manager.read().await;
                    let tag = tag_manager_read
                        .tags
                        .get(&(req.tag_id, req.workspace_id))
                        .unwrap();
                    let workspaces = workspaces.read().await;
                    let workspace = workspaces.get(&req.workspace_id).unwrap();
                    for shelf_info in workspace.local_shelves.iter() {
                        let shelf_manager = shelf_manager.read().await;
                        let shelf = shelf_manager.shelves.get(&shelf_info.id);
                        if let Some(shelf) = shelf {
                            if shelf.read().await.contains(tag.clone()) && !req.force {
                                return_code = 1; // Tag still present, remove not forced
                                return Ok(DeleteTagResponse {
                                    metadata: Some(ResponseMetadata {
                                        request_id: metadata.request_id,
                                        return_code,
                                        error_data,
                                    }),
                                });
                            }
                        }
                    }
                    for shelf_info in workspace.local_shelves.iter() {
                        let shelf_manager = shelf_manager.read().await;
                        let shelf = shelf_manager.shelves.get(&shelf_info.id);
                        if let Some(shelf) = shelf {
                            let _ = shelf.write().await.strip(PathBuf::from(""), tag.clone());
                        }
                    }

                    for (shelf_info, peer_id) in workspace.remote_shelves.iter() {
                        //[?] How to avoid partial removals upon HALT ??
                        //[/] Remote shelves may delete the tag even though it is present in other peers' shelves
                        let shelf_manager = shelf_manager.read().await;
                        let shelf = shelf_manager.shelves.get(&shelf_info.id);
                        if let Some(shelf) = shelf {
                            let _shelf = shelf;
                            let _peer_id = peer_id;
                            //[!] Relay the request via the peer service
                            //[!] Handle the response(s), if any (especially HALT)
                        }
                    }
                    tag_manager
                        .write()
                        .await
                        .tags
                        .retain(|(tag_id, workspace_id), _| {
                            !(tag_id == &req.tag_id && workspace_id == &req.workspace_id)
                        });

                    notify_queue.write().await.push_back({
                        Notification::Operation(Operation {
                            target: ActionTarget::Tag.into(),
                            id: req.tag_id,
                            action: ActionType::Delete.into(),
                        })
                    });
                    return_code = 0; // Success
                } else {
                    return_code = 2; // Workspace not found
                }
            } else {
                return_code = 1; // Tag not found
            }
            let metadata = ResponseMetadata {
                request_id: metadata.request_id,
                return_code,
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
        let metadata = req.metadata.unwrap();
        let notify_queue = self.notify_queue.clone();
        let shelf_manager = self.shelf_manager.clone();
        let workspaces = self.workspaces.clone();
        Box::pin(async move {
            let return_code: u32;
            let error_data: Option<ErrorData> = None;

            if !workspaces.read().await.contains_key(&req.workspace_id) {
                let workspace = workspaces.read().await;
                let workspace = workspace.get(&req.workspace_id).unwrap();
                let peer_id = workspace.get_peer_id(req.shelf_id);
                if let Some(peer_id) = peer_id {
                    //[!] Remote Request
                    // Relay the request via the peer service
                    // Await for the response to be inserted into the relay_responses map
                    // Timeout?
                    let _peer_id = peer_id;
                    let relay_response = StripTagResponse { metadata: None };
                    return Ok(relay_response);
                } else {
                    //[/] Local Request
                    if tag_manager
                        .read()
                        .await
                        .tags
                        .contains_key(&(req.tag_id, req.workspace_id))
                    {
                        let tag_manager_write = tag_manager.write().await;
                        let tag_ref = tag_manager_write
                            .tags
                            .get(&(req.tag_id, req.workspace_id))
                            .unwrap();
                        match shelf_manager.read().await.shelves.get(&req.shelf_id) {
                            Some(shelf) => {
                                let mut shelf = shelf.write().await;
                                let path = PathBuf::from(req.path);
                                let detach_res: Result<(), UpdateErr>;
                                if path.is_dir() {
                                    detach_res = shelf.strip(path, tag_ref.clone());
                                    match detach_res {
                                        Ok(_) => {
                                            notify_queue.write().await.push_back({
                                                Notification::Operation(Operation {
                                                    target: ActionTarget::Shelf.into(),
                                                    id: req.shelf_id,
                                                    action: ActionType::Edit.into(),
                                                })
                                            });
                                            return_code = 0 // Success
                                        }
                                        Err(_) => {
                                            return_code = 5; // Path not found
                                        }
                                    }
                                } else {
                                    return_code = 4; // Path is not a directory
                                }
                            }
                            None => {
                                return_code = 3; // Shelf not found
                            }
                        }
                    } else {
                        return_code = 2; // Tag not found
                    }
                }
            } else {
                return_code = 1; // Workspace not found
            }
            let metadata = ResponseMetadata {
                request_id: metadata.request_id,
                return_code,
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
        let metadata = req.metadata.unwrap();
        let notify_queue = self.notify_queue.clone();
        let shelf_manager = self.shelf_manager.clone();
        let workspaces = self.workspaces.clone();
        Box::pin(async move {
            let return_code: u32;
            let error_data: Option<ErrorData> = None;

            if !workspaces.read().await.contains_key(&req.workspace_id) {
                let workspace = workspaces.read().await;
                let workspace = workspace.get(&req.workspace_id).unwrap();
                let peer_id = workspace.get_peer_id(req.shelf_id);
                if let Some(peer_id) = peer_id {
                    //[!] Remote Request
                    // Relay the request via the peer service
                    // Await for the response to be inserted into the relay_responses map
                    // Timeout?
                    let _peer_id = peer_id;
                    let relay_response = DetachTagResponse { metadata: None };
                    return Ok(relay_response);
                } else {
                    //[/] Local Request
                    if tag_manager
                        .read()
                        .await
                        .tags
                        .contains_key(&(req.tag_id, req.workspace_id))
                    {
                        let tag_manager_write = tag_manager.write().await;
                        let tag_ref = tag_manager_write
                            .tags
                            .get(&(req.tag_id, req.workspace_id))
                            .unwrap();
                        match shelf_manager.read().await.shelves.get(&req.shelf_id) {
                            Some(shelf) => {
                                let mut shelf = shelf.write().await;
                                let path = PathBuf::from(req.path);
                                let detach_res: Result<bool, UpdateErr>;
                                // Check if Tag or DTag
                                if path.is_file() {
                                    detach_res = shelf.detach(path, tag_ref.clone());
                                } else {
                                    detach_res = shelf.detach_dtag(path, tag_ref.clone())
                                }
                                match detach_res {
                                    Ok(detached) => {
                                        if detached {
                                            notify_queue.write().await.push_back({
                                                Notification::Operation(Operation {
                                                    target: ActionTarget::Shelf.into(),
                                                    id: req.shelf_id,
                                                    action: ActionType::Edit.into(),
                                                })
                                            });
                                            return_code = 0 // Success
                                        } else {
                                            return_code = 6; // File not tagged
                                        }
                                    }
                                    Err(e) => {
                                        match e {
                                            UpdateErr::FileNotFound => {
                                                return_code = 5; // File not found
                                            }
                                            UpdateErr::PathNotFound => {
                                                return_code = 4; // Path not found
                                            }
                                        }
                                    }
                                }
                            }
                            None => {
                                return_code = 3; // Shelf not found
                            }
                        }
                    } else {
                        return_code = 2; // Tag not found
                    }
                }
            } else {
                return_code = 1; // Workspace not found
            }
            let metadata = ResponseMetadata {
                request_id: metadata.request_id,
                return_code,
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
        let metadata = req.metadata.unwrap();
        let notify_queue = self.notify_queue.clone();
        let shelf_manager = self.shelf_manager.clone();
        let workspaces = self.workspaces.clone();
        Box::pin(async move {
            let return_code: u32;
            let error_data: Option<ErrorData> = None;

            if !workspaces.read().await.contains_key(&req.workspace_id) {
                let workspace = workspaces.read().await;
                let workspace = workspace.get(&req.workspace_id).unwrap();
                let peer_id = workspace.get_peer_id(req.shelf_id);
                if let Some(peer_id) = peer_id {
                    //[!] Remote Request
                    // Relay the request via the peer service
                    // Await for the response to be inserted into the relay_responses map
                    // Timeout?
                    let _peer_id = peer_id;
                    let relay_response = AttachTagResponse { metadata: None };
                    return Ok(relay_response);
                } else {
                    //[/] Local Request
                    if tag_manager
                        .read()
                        .await
                        .tags
                        .contains_key(&(req.tag_id, req.workspace_id))
                    {
                        let tag_manager_write = tag_manager.write().await;
                        let tag_ref = tag_manager_write
                            .tags
                            .get(&(req.tag_id, req.workspace_id))
                            .unwrap();
                        match shelf_manager.read().await.shelves.get(&req.shelf_id) {
                            Some(shelf) => {
                                let mut shelf = shelf.write().await;
                                let path = PathBuf::from(req.path);
                                let attach_res: Result<bool, UpdateErr>;
                                // Check if Tag or DTag
                                if path.is_file() {
                                    attach_res = shelf.attach(path, tag_ref.clone());
                                } else {
                                    attach_res = shelf.attach_dtag(path, tag_ref.clone())
                                }
                                match attach_res {
                                    Ok(attached) => {
                                        if attached {
                                            notify_queue.write().await.push_back({
                                                Notification::Operation(Operation {
                                                    target: ActionTarget::Shelf.into(),
                                                    id: req.shelf_id,
                                                    action: ActionType::Edit.into(),
                                                })
                                            });
                                            return_code = 0 // Success
                                        } else {
                                            return_code = 6; // File already tagged
                                        }
                                    }
                                    Err(e) => {
                                        match e {
                                            UpdateErr::FileNotFound => {
                                                return_code = 5; // File not found
                                            }
                                            UpdateErr::PathNotFound => {
                                                return_code = 4; // Path not found
                                            }
                                        }
                                    }
                                }
                            }
                            None => {
                                return_code = 3; // Shelf not found
                            }
                        }
                    } else {
                        return_code = 2; // Tag not found
                    }
                }
            } else {
                return_code = 1; // Workspace not found
            }
            let metadata = ResponseMetadata {
                request_id: metadata.request_id,
                return_code,
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
        let metadata = req.metadata.unwrap();
        let shelf_manager = self.shelf_manager.clone();
        let notify_queue = self.notify_queue.clone();
        Box::pin(async move {
            let return_code: u32;
            let error_data: Option<ErrorData> = None;
            if !workspaces.read().await.contains_key(&req.workspace_id) {
                return_code = 1; // Workspace not found
            } else if workspaces
                .read()
                .await
                .get(&req.workspace_id)
                .unwrap()
                .contains(req.shelf_id)
            {
                return_code = 0; // Success
                workspaces
                    .write()
                    .await
                    .get_mut(&req.workspace_id)
                    .unwrap()
                    .local_shelves
                    .retain(|s| s.id != req.shelf_id);
                let mut shelf_manager = shelf_manager.write().await;
                let deleted_shelf = shelf_manager.try_remove_shelf(req.shelf_id).await;
                notify_queue.write().await.push_back({
                    Notification::Operation(Operation {
                        target: ActionTarget::Workspace.into(),
                        id: req.workspace_id,
                        action: ActionType::Edit.into(),
                    })
                });
                if deleted_shelf {
                    notify_queue.write().await.push_back({
                        Notification::Operation(Operation {
                            target: ActionTarget::Shelf.into(),
                            id: req.shelf_id,
                            action: ActionType::Delete.into(),
                        })
                    });
                }
            } else {
                return_code = 2; // Shelf not found
            }
            let metadata = ResponseMetadata {
                request_id: metadata.request_id,
                return_code,
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
        let metadata = req.metadata.unwrap();
        let notify_queue = self.notify_queue.clone();
        Box::pin(async move {
            let return_code: u32;
            let error_data: Option<ErrorData> = None;
            if !workspaces.read().await.contains_key(&req.workspace_id) {
                return_code = 1; // Workspace not found
            } else if workspaces
                .read()
                .await
                .get(&req.workspace_id)
                .unwrap()
                .contains(req.shelf_id)
            {
                return_code = 0; // Success
                workspaces
                    .write()
                    .await
                    .get_mut(&req.workspace_id)
                    .unwrap()
                    .edit_shelf(
                        req.shelf_id,
                        Some(req.name.clone()),
                        Some(req.description.clone()),
                    );

                notify_queue.write().await.push_back({
                    Notification::Operation(Operation {
                        target: ActionTarget::Workspace.into(),
                        id: req.shelf_id,
                        action: ActionType::Edit.into(),
                    })
                });
            } else {
                return_code = 2; // Shelf not found
            }
            let metadata = ResponseMetadata {
                request_id: metadata.request_id,
                return_code,
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
        let metadata = req.metadata.unwrap();
        let shelf_manager = self.shelf_manager.clone();
        let notify_queue = self.notify_queue.clone();
        let daemon_info = self.daemon_info.clone();
        Box::pin(async move {
            let return_code: u32;

            let peer_id_error: bool;
            let mut peer_id: Option<NodeId> = None;
            let peer_id_bytes: Result<[u8; 32], _> = req.peer_id.as_slice().try_into();
            if let Ok(peer_id_bytes) = peer_id_bytes {
                let peer_id_res = NodeId::from_bytes(&peer_id_bytes);
                if let Ok(peer_id_res) = peer_id_res {
                    peer_id = Some(peer_id_res);
                    peer_id_error = false;
                } else {
                    peer_id_error = true;
                }
            } else {
                peer_id_error = true;
            }

            if peer_id_error {
                return_code = 1; // Invalid peer ID
                let metadata = ResponseMetadata {
                    request_id: metadata.request_id,
                    return_code,
                    error_data: None,
                };
                return Ok(AddShelfResponse {
                    shelf_id: None,
                    metadata: Some(metadata),
                });
            }
            let peer_id = peer_id.unwrap();
            if peer_id != daemon_info.id {
                //[!] Remote Request
                // Relay the request via the peer service
                // Await for the response to be inserted into the relay_responses map
                // Timeout?
                let relay_response = AddShelfResponse {
                    shelf_id: None,
                    metadata: None,
                };
                return Ok(relay_response);
            } else {
                //[/] Local Request
                let shelf_id: Option<u64>;
                let mut error_data: Option<ErrorData> = None;
                if !workspaces.read().await.contains_key(&req.workspace_id) {
                    return_code = 2; // Workspace not found
                    shelf_id = None;
                } else {
                    let path = PathBuf::from(req.path.clone());
                    let shelf_id_res = shelf_manager.write().await.add_shelf(path.clone());

                    match shelf_id_res {
                        Ok(id) => {
                            workspaces
                                .write()
                                .await
                                .get_mut(&req.workspace_id)
                                .unwrap()
                                .local_shelves
                                .push(ShelfInfo::new(id, req.name, req.description, req.path));
                            notify_queue.write().await.push_back({
                                Notification::Operation(Operation {
                                    target: ActionTarget::Workspace.into(),
                                    id: req.workspace_id,
                                    action: ActionType::Edit.into(),
                                })
                            });
                            return_code = 0; // Success
                            shelf_id = Some(id);
                        }
                        Err(e) => {
                            return_code = 3; // Path Error (IO Error propagated from Node)
                            shelf_id = None;
                            error_data = Some(ErrorData {
                                error_data: vec![e.to_string()],
                            });
                        }
                    }
                }
                let metadata = ResponseMetadata {
                    request_id: metadata.request_id,
                    return_code,
                    error_data,
                };
                Ok(AddShelfResponse {
                    shelf_id: shelf_id,
                    metadata: Some(metadata),
                })
            }
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
            let return_code: u32;
            if !workspaces.read().await.contains_key(&req.workspace_id) {
                return_code = 1; // Workspace not found
            } else {
                notify_queue.write().await.push_back({
                    Notification::Operation(Operation {
                        target: ActionTarget::Workspace.into(),
                        id: req.workspace_id,
                        action: ActionType::Delete.into(),
                    })
                });

                return_code = 0; // Success
                let tag_ls: Vec<(u64, u64)> = tag_manager
                    .write()
                    .await
                    .tags
                    .keys()
                    .filter(|(_, ws_id)| *ws_id == req.workspace_id)
                    .cloned()
                    .collect();
                for key in tag_ls {
                    tag_manager.write().await.tags.remove(&key);
                }
                let mut shelves: Vec<u64> = Vec::new();
                for shelf_info in workspaces
                    .read()
                    .await
                    .get(&req.workspace_id)
                    .unwrap()
                    .local_shelves
                    .iter()
                {
                    shelves.push(shelf_info.id);
                }
                for (shelf_info, _) in workspaces
                    .read()
                    .await
                    .get(&req.workspace_id)
                    .unwrap()
                    .remote_shelves
                    .iter()
                {
                    shelves.push(shelf_info.id);
                }
                for shelf_id in shelves {
                    shelf_manager.write().await.try_remove_shelf(shelf_id).await;
                }
                workspaces.write().await.remove(&req.workspace_id);
            }

            let metadata = ResponseMetadata {
                request_id: metadata.request_id,
                return_code,
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
        let tag_manager = self.tag_manager.clone();
        let metadata = req.metadata.unwrap();
        let notify_queue = self.notify_queue.clone();
        Box::pin(async move {
            let return_code: u32;
            let parent_id_exists: bool;
            match req.parent_id {
                Some(_) => parent_id_exists = true,
                None => parent_id_exists = false,
            }
            let tag_manager_read = tag_manager.read().await;
            if !tag_manager_read
                .tags
                .contains_key(&(req.tag_id, req.workspace_id))
            {
                return_code = 1; // Tag not found
            } else if parent_id_exists
                && !tag_manager_read
                    .tags
                    .contains_key(&(req.parent_id.unwrap(), req.workspace_id))
            {
                return_code = 2; // Requested parent does not exist
            } else if req.name.is_empty() {
                return_code = 3; // Name cannot be empty
            } else {
                notify_queue.write().await.push_back({
                    Notification::Operation(Operation {
                        target: ActionTarget::Tag.into(),
                        id: req.tag_id,
                        action: ActionType::Edit.into(),
                    })
                });

                return_code = 0; // Success
                let mut tag_manager_write = tag_manager.write().await;
                let tag = tag_manager_write
                    .tags
                    .get_mut(&(req.tag_id, req.workspace_id))
                    .unwrap();
                tag.tag_ref.write().unwrap().name = req.name.clone();
                tag.tag_ref.write().unwrap().priority = req.priority;
                if parent_id_exists {
                    tag.tag_ref.write().unwrap().parent = Some(
                        tag_manager_read
                            .tags
                            .get(&(req.parent_id.unwrap(), req.workspace_id))
                            .unwrap()
                            .clone(),
                    );
                } else {
                    tag.tag_ref.write().unwrap().parent = None;
                }
            }

            let metadata = ResponseMetadata {
                request_id: metadata.request_id,
                return_code,
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
            let return_code: u32;
            if !workspaces.read().await.contains_key(&req.workspace_id) {
                return_code = 1; // Workspace not found
            } else if req.name.is_empty() {
                return_code = 2; // Name cannot be empty
            } else {
                notify_queue.write().await.push_back({
                    Notification::Operation(Operation {
                        target: ActionTarget::Workspace.into(),
                        id: req.workspace_id,
                        action: ActionType::Edit.into(),
                    })
                });

                return_code = 0; // Success
                workspaces
                    .write()
                    .await
                    .get_mut(&req.workspace_id)
                    .unwrap()
                    .name = req.name;
                workspaces
                    .write()
                    .await
                    .get_mut(&req.workspace_id)
                    .unwrap()
                    .description = req.description;
            }

            let metadata = ResponseMetadata {
                request_id: metadata.request_id,
                return_code,
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
            let workspace_id = req.workspace_id;
            let mut shelves = Vec::new();
            if let Some(workspace) = workspaces.read().await.get(&workspace_id) {
                for shelf_info in &workspace.local_shelves {
                    shelves.push(rpc::Shelf {
                        peer_id: daemon_info.id.as_bytes().to_vec(),
                        shelf_id: shelf_info.id,
                        name: shelf_info.name.clone(),
                        path: shelf_info.root_path.to_string_lossy().to_string(), //[!] Better way to handle this?
                    });
                }
                for (shelf_info, peer_id) in &workspace.remote_shelves {
                    shelves.push(rpc::Shelf {
                        peer_id: peer_id.as_bytes().to_vec(),
                        shelf_id: shelf_info.id,
                        name: shelf_info.name.clone(),
                        path: shelf_info.root_path.to_string_lossy().to_string(), //[!] Better way to handle this?
                    });
                }
            }
            let metadata = ResponseMetadata {
                request_id: metadata.request_id,
                return_code: 0,
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
                        Some(parent) => Some(parent.tag_ref.read().unwrap().id),
                        None => None,
                    };
                    tag_ls.push(rpc::Tag {
                        tag_id,
                        name,
                        priority,
                        parent_id,
                    });
                }
                let ws = rpc::Workspace {
                    workspace_id: workspace.id,
                    name: workspace.name.clone(),
                    description: workspace.description.clone(),
                    tags: tag_ls,
                };
                workspace_ls.push(ws);
            }
            let metadata = ResponseMetadata {
                request_id: metadata.request_id,
                return_code: 0,
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
            let mut id: u64;
            loop {
                id = rand::random::<u64>();
                if !workspaces.read().await.contains_key(&id) {
                    break;
                }
            }

            notify_queue.write().await.push_back({
                Notification::Operation(Operation {
                    target: ActionTarget::Workspace.into(),
                    id,
                    action: ActionType::Create.into(),
                })
            });

            let workspace = Workspace {
                id,
                name: req.name,
                description: req.description,
                local_shelves: Vec::new(),  // Placeholder for local shelves
                remote_shelves: Vec::new(), // Placeholder for remote shelves
            };
            workspaces.write().await.insert(id, workspace);

            let metadata = ResponseMetadata {
                request_id: metadata.request_id,
                return_code: 0,
                error_data: None,
            };
            return Ok(CreateWorkspaceResponse {
                workspace_id: id,
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
        Box::pin(async move {
            let metadata = req.metadata.unwrap();

            // Check if the workspace exists
            let mut workspace_err = false;
            if !workspaces.read().await.contains_key(&req.workspace_id) {
                workspace_err = true;
            }

            if !workspace_err {
                // Create the tag using the tag manager
                let id = tag_manager.write().await.create_tag(
                    &req.name,
                    req.workspace_id,
                    req.priority,
                    req.parent_id,
                );
                match id {
                    Ok(tag_id) => {
                        notify_queue.write().await.push_back({
                            Notification::Operation(Operation {
                                target: ActionTarget::Tag.into(),
                                id: tag_id,
                                action: ActionType::Delete.into(),
                            })
                        });
                        // If the tag was created successfully, return the response with the tag ID
                        let metadata = ResponseMetadata {
                            request_id: metadata.request_id,
                            return_code: 0, // Success
                            error_data: None,
                        };
                        return Ok(CreateTagResponse {
                            tag_id: Some(tag_id),
                            metadata: Some(metadata),
                        });
                    }
                    Err(_) => {
                        // If there was an error creating the tag, return an error response
                        let metadata = ResponseMetadata {
                            request_id: metadata.request_id,
                            return_code: 2, // Requested parent does not exist
                            error_data: None,
                        };
                        return Ok(CreateTagResponse {
                            tag_id: None,
                            metadata: Some(metadata),
                        });
                    }
                }
            } else {
                let metadata = ResponseMetadata {
                    request_id: metadata.request_id,
                    return_code: 1, // Workspace not found
                    error_data: None,
                };
                return Ok(CreateTagResponse {
                    tag_id: None,
                    metadata: Some(metadata),
                });
            };
        })
    }
}
