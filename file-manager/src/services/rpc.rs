use crate::rpc::*;
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

fn parse_peer_id(bytes: &[u8]) -> Result<NodeId, ()> {
    let bytes: &[u8; 32] = bytes.try_into().map_err(|_| ())?;
    NodeId::from_bytes(bytes).map_err(|_| ())
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
            let error_data: Option<ErrorData> = None;

            let tag_found = tag_manager
                .read()
                .await
                .tags
                .contains_key(&(req.tag_id, req.workspace_id));
            let workspace_found = workspaces.read().await.contains_key(&req.workspace_id);

            if tag_found && workspace_found {
                {
                    let tag_manager_r = tag_manager.read().await;
                    let workspaces_r = workspaces.read().await;

                    let tag = tag_manager_r
                        .tags
                        .get(&(req.tag_id, req.workspace_id))
                        .unwrap();
                    let workspace = workspaces_r.get(&req.workspace_id).unwrap();

                    if !req.force {
                        // check if tags are present if remove is not forced
                        for (_, shelf_info) in workspace.local_shelves.iter() {
                            let shelf_manager = shelf_manager.read().await;
                            let shelf = shelf_manager.shelves.get(&shelf_info.id);
                            if let Some(shelf) = shelf {
                                if shelf.read().await.contains(tag.clone()) {
                                    return Ok(DeleteTagResponse {
                                        metadata: Some(ResponseMetadata {
                                            request_id: metadata.request_id,
                                            return_code: 1,
                                            error_data,
                                        }),
                                    });
                                }
                            }
                        }
                    }

                    for (_, shelf_info) in workspace.local_shelves.iter() {
                        let shelf_manager_r = shelf_manager.read().await;
                        let shelf = shelf_manager_r.shelves.get(&shelf_info.id);
                        if let Some(shelf) = shelf {
                            let _ = shelf.write().await.strip(PathBuf::from(""), tag.clone());
                        }
                    }

                    for (_, (shelf_info, peer_id)) in workspace.remote_shelves.iter() {
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
                } // Tag_manager read lock goes out of scope before requesting write
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
            }

            let return_code = match (workspace_found, tag_found) {
                (false, _) => 2,
                (true, false) => 1,
                _ => 0,
            };
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
            let error_data: Option<ErrorData> = None;

            let tag_found = tag_manager
                .read()
                .await
                .tags
                .contains_key(&(req.tag_id, req.workspace_id));
            let workspace_found = workspaces.read().await.contains_key(&req.workspace_id);
            let workspace_r = workspaces.read().await;
            let local_shelf_found = workspace_r
                .get(&req.workspace_id)
                .map(|ws| ws.local_shelves.contains_key(&req.shelf_id))
                .unwrap_or(false);

            let return_code = match (tag_found, workspace_found, local_shelf_found) {
                (_, false, _) => 1,
                (false, true, _) => 2,
                (true, true, false) => {
                    let workspace = workspaces.read().await;
                    let workspace = workspace.get(&req.workspace_id).unwrap();
                    let remote_shelf = workspace.remote_shelves.get(&req.shelf_id);
                    if let Some((_, peer_id)) = remote_shelf {
                        //[!] Remote Request
                        // Relay the request via the peer service
                        // Await for the response to be inserted into the relay_responses map
                        // Timeout?
                        let _peer_id = peer_id;
                        let _relay_response = StripTagResponse { metadata: None };
                        //return_code = relay_response.metadata.return_code;
                        0
                    } else {
                        3 // Shelf not found and not in remote peer
                    }
                }
                (true, true, true) => {
                    let tag_manager_w = tag_manager.write().await;
                    let tag_ref = tag_manager_w
                        .tags
                        .get(&(req.tag_id, req.workspace_id))
                        .unwrap();
                    // we can unwrap because we checked that the shelf existed
                    let shelf_manager_r = shelf_manager.read().await;
                    let mut shelf = shelf_manager_r
                        .shelves
                        .get(&req.shelf_id)
                        .unwrap()
                        .write()
                        .await;
                    let path = PathBuf::from(req.path);
                    if path.is_dir() {
                        //[!] this should be checked inside shelf.strip. Alongside
                        //if path is valid for the shelf
                        match shelf.strip(path, tag_ref.clone()) {
                            Ok(_) => 0,
                            Err(_) => 5, // Path not found
                        }
                    } else {
                        4 // Shelf not found
                    }
                }
            };

            if return_code == 0 {
                notify_queue.write().await.push_back({
                    Notification::Operation(Operation {
                        target: ActionTarget::Shelf.into(),
                        id: req.shelf_id,
                        action: ActionType::Edit.into(),
                    })
                });
            };

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
            let error_data: Option<ErrorData> = None;

            let tag_found = tag_manager
                .read()
                .await
                .tags
                .contains_key(&(req.tag_id, req.workspace_id));
            let workspace_found = workspaces.read().await.contains_key(&req.workspace_id);
            let local_shelf_found = workspaces
                .read()
                .await
                .get(&req.workspace_id)
                .map(|ws| ws.local_shelves.contains_key(&req.shelf_id))
                .unwrap_or(false);

            let return_code = match (tag_found, workspace_found, local_shelf_found) {
                (_, false, _) => {
                    1 // workspace_found
                }
                (false, true, _) => {
                    2 // tag_found
                }
                (_, _, false) => {
                    let workspace_r = workspaces.read().await;
                    let workspace = workspace_r.get(&req.workspace_id).unwrap();
                    let remote_shelf = workspace.remote_shelves.get(&req.shelf_id);
                    if let Some((_, peer_id)) = remote_shelf {
                        //[!] Remote Request
                        // Relay the request via the peer service
                        // Await for the response to be inserted into the relay_responses map
                        // Timeout?
                        let _peer_id = peer_id;
                        let _relay_response = DetachTagResponse { metadata: None };
                        //return_code = relay_response.metadata.return_code;
                        0 // response is 0
                    } else {
                        3 // Shelf not found and not in remote peer
                    }
                }
                (true, true, true) => {
                    let tag_ref = tag_manager
                        .write()
                        .await
                        .tags
                        .get(&(req.tag_id, req.workspace_id))
                        .unwrap()
                        .clone();

                    let shelf_manager_r = shelf_manager.read().await;
                    let mut shelf = shelf_manager_r
                        .shelves
                        .get(&req.shelf_id)
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
                        Ok(true) => 0,                     // Success
                        Ok(false) => 6,                    // File not tagged
                        Err(UpdateErr::FileNotFound) => 5, // File not found
                        Err(UpdateErr::PathNotFound) => 4, // Path not found
                    }
                }
            };

            if return_code == 0 {
                notify_queue.write().await.push_back({
                    Notification::Operation(Operation {
                        target: ActionTarget::Shelf.into(),
                        id: req.shelf_id,
                        action: ActionType::Edit.into(),
                    })
                });
            };

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
            let error_data: Option<ErrorData> = None;

            let tag_found = tag_manager
                .read()
                .await
                .tags
                .contains_key(&(req.tag_id, req.workspace_id));
            let workspace_found = workspaces.read().await.contains_key(&req.workspace_id);
            let local_shelf_found = workspaces
                .read()
                .await
                .get(&req.workspace_id)
                .map(|ws| ws.local_shelves.contains_key(&req.shelf_id))
                .unwrap_or(false);

            let return_code = match (tag_found, workspace_found, local_shelf_found) {
                (_, false, _) => 1,    // Workspace not found
                (false, true, _) => 2, // Tag not found
                (true, true, false) => {
                    let workspace_r = workspaces.read().await;
                    let workspace = workspace_r.get(&req.workspace_id).unwrap();
                    let remote_shelf = workspace.remote_shelves.get(&req.shelf_id);
                    if let Some((_, _peer_id)) = remote_shelf {
                        //[!] Remote Request
                        // Relay the request via the peer service
                        // Await for the response to be inserted into the relay_responses map
                        // Timeout?
                        let _relay_response = AttachTagResponse { metadata: None };
                        //return_code = relay_response.metadata.return_code;
                        0 // response is 0
                    } else {
                        3 // Shelf not found and not in remote peer
                    }
                }
                (true, true, true) => {
                    let tag_ref = tag_manager
                        .write()
                        .await
                        .tags
                        .get(&(req.tag_id, req.workspace_id))
                        .unwrap()
                        .clone();

                    let shelf_manager_r = shelf_manager.read().await;
                    let mut shelf = shelf_manager_r
                        .shelves
                        .get(&req.shelf_id)
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
                        Ok(true) => 0,                     // Success
                        Ok(false) => 6,                    // Already tagged
                        Err(UpdateErr::FileNotFound) => 5, // File not found
                        Err(UpdateErr::PathNotFound) => 4, // Path not found
                    }
                }
            };

            if return_code == 0 {
                notify_queue.write().await.push_back({
                    Notification::Operation(Operation {
                        target: ActionTarget::Shelf.into(),
                        id: req.shelf_id,
                        action: ActionType::Edit.into(),
                    })
                });
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
            let error_data: Option<ErrorData> = None;

            let workspace_found = workspaces.read().await.contains_key(&req.workspace_id);
            let local_shelf_found = workspaces
                .read()
                .await
                .get(&req.workspace_id)
                .map(|ws| ws.local_shelves.contains_key(&req.shelf_id))
                .unwrap_or(false);

            let return_code = match (workspace_found, local_shelf_found) {
                (false, _) => 1,
                (true, false) => {
                    let workspace_r = workspaces.read().await;
                    let workspace = workspace_r.get(&req.workspace_id).unwrap();
                    let remote_shelf = workspace.remote_shelves.get(&req.shelf_id);
                    if let Some((_, _peer_id)) = remote_shelf {
                        //[!] Remote Request
                        // Relay the request via the peer service
                        // Await for the response to be inserted into the relay_responses map
                        // Timeout?
                        let relay_response = RemoveShelfResponse { metadata: None };
                        //return_code = relay_response.metadata.return_code;
                        0
                    } else {
                        2 // Shelf not found
                    }
                }
                (true, true) => {
                    workspaces
                        .write()
                        .await
                        .get_mut(&req.workspace_id)
                        .unwrap()
                        .local_shelves
                        .remove(&req.shelf_id);
                    let mut shelf_manager = shelf_manager.write().await;
                    if shelf_manager.try_remove_shelf(req.shelf_id).await {
                        notify_queue.write().await.push_back({
                            Notification::Operation(Operation {
                                target: ActionTarget::Shelf.into(),
                                id: req.shelf_id,
                                action: ActionType::Delete.into(),
                            })
                        });
                    }
                    0
                }
            };

            if return_code == 0 {
                notify_queue.write().await.push_back({
                    Notification::Operation(Operation {
                        target: ActionTarget::Workspace.into(),
                        id: req.workspace_id,
                        action: ActionType::Edit.into(),
                    })
                });
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
            let error_data: Option<ErrorData> = None;

            let workspace_found = workspaces.read().await.contains_key(&req.workspace_id);
            let local_shelf_found = workspaces
                .read()
                .await
                .get(&req.workspace_id)
                .map(|ws| ws.local_shelves.contains_key(&req.shelf_id))
                .unwrap_or(false);

            let return_code = match (workspace_found, local_shelf_found) {
                (false, _) => 1,
                (true, false) => {
                    let workspace_r = workspaces.read().await;
                    let workspace = workspace_r.get(&req.workspace_id).unwrap();
                    let remote_shelf = workspace.remote_shelves.get(&req.shelf_id);
                    if let Some((_, _peer_id)) = remote_shelf {
                        //[!] Remote Request
                        // Relay the request via the peer service
                        // Await for the response to be inserted into the relay_responses map
                        // Timeout?
                        let relay_response = EditShelfResponse { metadata: None };
                        //return_code = relay_response.metadata.return_code;
                        0
                    } else {
                        2 // Shelf not found
                    }
                }
                (true, true) => {
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
                    0
                }
            };

            if return_code == 0 {
                notify_queue.write().await.push_back({
                    Notification::Operation(Operation {
                        target: ActionTarget::Workspace.into(),
                        id: req.shelf_id,
                        action: ActionType::Edit.into(),
                    })
                });
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
            let mut error_data: Option<ErrorData> = None;
            let mut shelf_id: Option<u64> = None;

            let peer_id = parse_peer_id(req.peer_id.as_slice());
            let workspace_found = workspaces.read().await.contains_key(&req.workspace_id);

            let return_code = match (workspace_found, peer_id) {
                (false, _) => 2,
                (true, Err(_)) => 1,
                (true, Ok(peer_id)) if peer_id != daemon_info.id => {
                    //[!] Remote Request
                    let relay_response = AddShelfResponse {
                        shelf_id: None,
                        metadata: None,
                    };
                    0
                }
                (true, Ok(peer_id)) => {
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
                                .insert(
                                    id,
                                    ShelfInfo::new(id, req.name, req.description, req.path),
                                );

                            shelf_id = Some(id);
                            0
                        }
                        Err(e) => {
                            error_data = Some(ErrorData {
                                error_data: vec![e.to_string()],
                            });
                            3
                        }
                    }
                }
            };

            if return_code == 0 {
                notify_queue.write().await.push_back({
                    Notification::Operation(Operation {
                        target: ActionTarget::Workspace.into(),
                        id: req.workspace_id,
                        action: ActionType::Edit.into(),
                    })
                });
            }
            let metadata = ResponseMetadata {
                request_id: metadata.request_id,
                return_code,
                error_data,
            };
            Ok(AddShelfResponse {
                shelf_id,
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
            let workspace_found = workspaces.read().await.contains_key(&req.workspace_id);

            let return_code = match workspace_found {
                false => 1,
                true => {
                    let tag_rm: Vec<(u64, u64)> = tag_manager
                        .write()
                        .await
                        .tags
                        .keys()
                        .filter(|(_, ws_id)| *ws_id == req.workspace_id)
                        .cloned()
                        .collect();
                    for key in tag_rm {
                        tag_manager.write().await.tags.remove(&key);
                    }
                    let mut shelves: Vec<u64> = Vec::new();
                    for (id, _) in workspaces
                        .read()
                        .await
                        .get(&req.workspace_id)
                        .unwrap()
                        .local_shelves
                        .iter()
                    {
                        shelves.push(*id);
                    }
                    for shelf_id in shelves {
                        shelf_manager.write().await.try_remove_shelf(shelf_id).await;
                    }
                    workspaces.write().await.remove(&req.workspace_id);
                    0
                }
            };

            if return_code == 0 {
                notify_queue.write().await.push_back({
                    Notification::Operation(Operation {
                        target: ActionTarget::Workspace.into(),
                        id: req.workspace_id,
                        action: ActionType::Delete.into(),
                    })
                });
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
        let workspaces = self.workspaces.clone();
        let tag_manager = self.tag_manager.clone();
        let metadata = req.metadata.unwrap();
        let notify_queue = self.notify_queue.clone();
        Box::pin(async move {
            let tag_found = tag_manager
                .read()
                .await
                .tags
                .contains_key(&(req.tag_id, req.workspace_id));
            let workspace_found = workspaces.read().await.contains_key(&req.workspace_id);

            let valid_parent = if let Some(id) = req.parent_id {
                tag_manager
                    .read()
                    .await
                    .tags
                    .contains_key(&(id, req.workspace_id))
            } else {
                true
            };

            let return_code = match (
                workspace_found,
                tag_found,
                req.name.is_empty(),
                valid_parent,
            ) {
                (false, _, _, _) => 2,
                (true, false, _, _) => 1,
                (true, true, false, _) => 3,

                (true, true, true, false) => 4,
                (true, true, true, true) => {
                    let mut tag_manager_w = tag_manager.write().await;
                    let tag = tag_manager_w
                        .tags
                        .get_mut(&(req.tag_id, req.workspace_id))
                        .unwrap();
                    tag.tag_ref.write().unwrap().name = req.name.clone();
                    tag.tag_ref.write().unwrap().priority = req.priority;
                    if req.parent_id.is_some() {
                        tag.tag_ref.write().unwrap().parent = Some(
                            tag_manager
                                .read()
                                .await
                                .tags
                                .get(&(req.parent_id.unwrap(), req.workspace_id))
                                .unwrap()
                                .clone(),
                        );
                    } else {
                        tag.tag_ref.write().unwrap().parent = None;
                    }
                    0
                }
            };

            if return_code == 0 {
                notify_queue.write().await.push_back({
                    Notification::Operation(Operation {
                        target: ActionTarget::Tag.into(),
                        id: req.tag_id,
                        action: ActionType::Edit.into(),
                    })
                });
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
            let workspace_found = workspaces.read().await.contains_key(&req.workspace_id);

            let return_code = match (workspace_found, req.name.is_empty()) {
                (false, _) => 1,
                (true, false) => 2,
                (true, true) => {
                    let mut workspace_r = workspaces.write().await;
                    let to_edit = workspace_r.get_mut(&req.workspace_id).unwrap();
                    to_edit.name = req.name;
                    to_edit.description = req.description;
                    0
                }
            };

            if return_code == 0 {
                notify_queue.write().await.push_back({
                    Notification::Operation(Operation {
                        target: ActionTarget::Workspace.into(),
                        id: req.workspace_id,
                        action: ActionType::Edit.into(),
                    })
                });
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
                for (id, shelf_info) in &workspace.local_shelves {
                    shelves.push(Shelf {
                        peer_id: daemon_info.id.as_bytes().to_vec(),
                        shelf_id: *id,
                        name: shelf_info.name.clone(),
                        path: shelf_info.root_path.to_string_lossy().to_string(), //[!] Better way to handle this?
                    });
                }
                for (id, (shelf_info, peer_id)) in &workspace.remote_shelves {
                    shelves.push(Shelf {
                        peer_id: peer_id.as_bytes().to_vec(),
                        shelf_id: *id,
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
                    tag_ls.push(Tag {
                        tag_id,
                        name,
                        priority,
                        parent_id,
                    });
                }
                let ws = crate::rpc::Workspace {
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
                local_shelves: HashMap::new(), // Placeholder for local shelves
                remote_shelves: HashMap::new(), // Placeholder for remote shelves
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
