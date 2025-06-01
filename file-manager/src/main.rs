#![allow(dead_code)]
use crate::rpc::*;
use crate::services::peer::{Client, PeerService};
use crate::services::rpc::{DaemonInfo, RpcService, TaskID};
use anyhow::Result;
use iroh::RelayMode;
use iroh::{endpoint::Connection, Endpoint, NodeId, SecretKey};
use prost::Message;
use rpc::MessageType;
use shelf::shelf::{Shelf, ShelfManager};
use std::collections::HashMap;
use std::collections::VecDeque;
use std::hash::Hash;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration};
use tower::{Service, ServiceBuilder};

mod query;
mod rpc;
mod services;
mod shelf;
mod tag;
mod workspace;

const HEADER_SIZE: usize = 10;
const ALPN: &[u8] = b"ebi";

macro_rules! generate_request_match {
    ($msg_code:expr, $buffer:expr, $service:expr, $socket:expr, $( ($code:path, $req_ty:ty) ),* $(,)?) => {
        match $msg_code.try_into() {
            $(
                Ok($code) => {
                    let req = <$req_ty>::decode(&$buffer[..]).unwrap();
                    if let Ok(response) = $service.call(req).await {
                        let mut payload = Vec::new();
                        response.encode(&mut payload).unwrap();
                        let mut response_buf = vec![0; HEADER_SIZE];
                        response_buf[0] = MessageType::Response as u8;
                        response_buf[1] = $code as u8;
                        let size = payload.len() as u64;
                        response_buf[2..HEADER_SIZE].copy_from_slice(&size.to_le_bytes());
                        response_buf.extend_from_slice(&payload);
                        let _ = $socket.write_all(&response_buf).await;
                    }
                }
            )*
            Ok(_) => {
                todo!();
            }
            Err(_) => {
                println!("Unknown response code: {}", $msg_code);
            }
        }
    };
}

#[tokio::main]
async fn main() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:3000").await.unwrap();
    let sec_key = get_secret_key();
    let ep = Endpoint::builder()
        .discovery_n0()
        .secret_key(sec_key)
        .alpns(vec![ALPN.to_vec()])
        .bind()
        .await?; //[!] Error if PK is already in use
    println!("{:?}", ep.node_addr().await?.node_id.as_bytes());
    println!("{:?}", ep.node_addr().await?.node_id);
    let peers = Arc::new(RwLock::new(HashMap::<NodeId, Connection>::new()));
    let clients = Arc::new(RwLock::new(Vec::<Client>::new()));
    let tasks = Arc::new(HashMap::<TaskID, JoinHandle<()>>::new());
    let tag_manager = Arc::new(RwLock::new(tag::TagManager::new()));
    let shelf_manager = Arc::new(RwLock::new(ShelfManager::new()));
    let workspaces = Arc::new(RwLock::new(HashMap::<
        workspace::WorkspaceId,
        workspace::Workspace,
    >::new()));
    let relay_responses = Arc::new(RwLock::new(HashMap::new()));
    let notify_queue = Arc::new(RwLock::new(VecDeque::new()));
    let id: NodeId = ep.node_id();
    let service = ServiceBuilder::new().service(RpcService {
        daemon_info: Arc::new(DaemonInfo::new(id, "".to_string())),
        peer_service: PeerService {
            peers: peers.clone(),
            clients: clients.clone(),
        },
        relay_responses: relay_responses.clone(),
        notify_queue: notify_queue.clone(),
        tasks: tasks.clone(),
        tag_manager: tag_manager.clone(),
        shelf_manager: shelf_manager.clone(),
        workspaces: workspaces.clone(),
    });
    loop {
        tokio::select! {
            Ok((stream, addr)) = listener.accept() => {
                let service = service.clone();
                let stream: Arc<Mutex<TcpStream>> = Arc::new(Mutex::new(stream));
                let client = Client {
                    hash: 0,
                    addr,
                    stream: stream.clone()
                };
                clients.write().await.push(client);
                tokio::spawn(async move {
                    handle_client(stream.clone(), addr, service).await;
                });
            },
            conn = ep.accept() => {
                let conn = conn.unwrap().await?;
                peers.write().await.insert(conn.remote_node_id().unwrap(), conn.clone());
                let (mut send, mut recv) = conn.accept_bi().await?;
                let msg = recv.read_to_end(100).await?;
            }
        }
    }
}

async fn handle_client(
    mut socket: Arc<Mutex<TcpStream>>,
    addr: SocketAddr,
    mut service: RpcService,
) {
    let mut header = vec![0; HEADER_SIZE];
    let mut socket = socket.lock().await;

    loop {
        let bytes_read = match socket.read_exact(&mut header).await {
            Ok(n) if n == 0 => {
                println!("{}", n);
                break;
            }
            Ok(n) => n,
            Err(e) => {
                println!("{:?}", e);
                9
            }
        };
        let msg_type: u8 = u8::from_le_bytes([header[0]]);
        let msg_code: u8 = u8::from_le_bytes([header[1]]);
        println!("Message Type: {}", msg_type);
        println!("Message Code: {}", msg_code);
        let size: u64 = u64::from_le_bytes(header[2..HEADER_SIZE].try_into().unwrap());
        println!("size: {}", size);
        let mut buffer = vec![0u8; size as usize];
        let bytes_read = match socket.read_exact(&mut buffer).await {
            Ok(n) if n == 0 => 0,
            Ok(n) => n,
            Err(e) => {
                println!("{:?}", e);
                0
            }
        };
        println!("read all");

        match msg_type.try_into() {
            Ok(MessageType::Request) => {
                println!("Request message type received");
                generate_request_match!(
                    msg_code,
                    buffer,
                    service,
                    socket,
                    (RequestCode::CreateTag, CreateTag),
                    (RequestCode::CreateWorkspace, CreateWorkspace),
                    (RequestCode::EditWorkspace, EditWorkspace),
                    (RequestCode::GetWorkspaces, GetWorkspaces),
                    (RequestCode::GetShelves, GetShelves),
                    (RequestCode::EditTag, EditTag),
                    (RequestCode::DeleteWorkspace, DeleteWorkspace),
                    (RequestCode::AddShelf, AddShelf),
                    (RequestCode::EditShelf, EditShelf),
                    (RequestCode::RemoveShelf, RemoveShelf),
                    (RequestCode::AttachTag, AttachTag),
                    (RequestCode::DetachTag, DetachTag),
                );
            }
            Ok(MessageType::Response) => {
                println!("Response message type received");
                match msg_code.try_into() {
                    Ok(RequestCode::CreateTag) => {
                        let res = CreateTagResponse::decode(&*buffer).unwrap();
                        println!("CreateTag Response received");
                        println!("\tTag ID: {:?}", res.tag_id);
                    }
                    Ok(RequestCode::CreateWorkspace) => {
                        let res = CreateWorkspaceResponse::decode(&*buffer).unwrap();
                        println!("CreateWorkspace Response received");
                        println!("\tWorkspace ID: {:?}", res.workspace_id);
                    }
                    Ok(RequestCode::EditWorkspace) => {
                        let _ = EditWorkspaceResponse::decode(&*buffer).unwrap();
                        println!("EditWorkspace Response received");
                    }
                    Ok(RequestCode::DeleteWorkspace) => {
                        let _ = DeleteWorkspaceResponse::decode(&*buffer).unwrap();
                        println!("DeleteWorkspace Response received");
                    }
                    Ok(RequestCode::GetWorkspaces) => {
                        let res = GetWorkspacesResponse::decode(&*buffer).unwrap();
                        println!("GetWorkspaces Response received");
                        println!("\tWorkspaces:");
                        for workspace in &res.workspaces {
                            println!("\t({:?}): {:?}", workspace.workspace_id, workspace.name);
                        }
                    }
                    Ok(RequestCode::GetShelves) => {
                        let res = GetShelvesResponse::decode(&*buffer).unwrap();
                        println!("GetShelves Response received");
                        println!("\tShelves:");
                        for shelf in &res.shelves {
                            println!("\t({:?}): {:?}", shelf.shelf_id, shelf.name);
                            println!("\t\t{:?}", shelf.path);
                        }
                    }
                    Ok(RequestCode::EditTag) => {
                        let _ = EditTagResponse::decode(&*buffer).unwrap();
                        println!("EditTag Response received");
                    }
                    Ok(RequestCode::AddShelf) => {
                        let res = AddShelfResponse::decode(&*buffer).unwrap();
                        println!("AddShelf Response received");
                        println!("\tShelf ID: {:?}", res.shelf_id);
                    }
                    Ok(RequestCode::RemoveShelf) => {
                        let _ = RemoveShelfResponse::decode(&*buffer).unwrap();
                        println!("RemoveShelf Response received");
                    }
                    Ok(RequestCode::AttachTag) => {
                        let _ = AttachTagResponse::decode(&*buffer).unwrap();
                        println!("AttachTag Response received");
                    }
                    Ok(RequestCode::DetachTag) => {
                        let _ = DetachTagResponse::decode(&*buffer).unwrap();
                        println!("DetachTag Response received");
                    }
                    Ok(RequestCode::DeleteTag) => {
                        let _ = DeleteTagResponse::decode(&*buffer).unwrap();
                        println!("DeleteTag Response received");
                    }
                    Ok(RequestCode::EditShelf) => {
                        let _ = EditShelfResponse::decode(&*buffer).unwrap();
                        println!("EditShelf Response received");
                    }
                    Ok(RequestCode::StripTag) => {
                        let _ = rpc::StripTagResponse::decode(&*buffer).unwrap();
                        println!("StripTag Response received");
                    }
                    Ok(_) => {
                        todo!();
                    }
                    Err(_) => {
                        println!("Unknown response code: {}", msg_code);
                        break;
                    }
                }
            }
            Ok(MessageType::Data) => {
                println!("Data message type received");
                // Handle data messages here
            }
            Ok(MessageType::Notification) => {
                println!("Notification message type received");
                // Handle notification messages here
            }
            Ok(MessageType::Sync) => {
                println!("Sync message type received");
                // Handle sync messages here
            }
            Err(_) => {
                println!("Unknown message type: {}", msg_type);
                break;
            }
        }
    }
    println!("Client disconnected: {}", addr);
}

fn get_secret_key() -> SecretKey {
    let mut rng = rand::rngs::OsRng;
    iroh_base::SecretKey::generate(&mut rng)
}
