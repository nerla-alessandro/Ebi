#![allow(dead_code)]
use crate::rpc::*;
use crate::services::peer::{Client, PeerService};
use crate::services::rpc::{DaemonInfo, RpcService, TaskID};
use anyhow::Result;
use iroh::{endpoint::Connection, Endpoint, NodeId, SecretKey};
use iroh::endpoint::{SendStream, RecvStream};
use prost::Message;
use rpc::MessageType;
use shelf::shelf::ShelfManager;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt, AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, TcpStream, tcp::{OwnedReadHalf, OwnedWriteHalf}};
use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinHandle;
use tower::{Service, ServiceBuilder};
use tokio::sync::{mpsc, watch, mpsc::Sender};

mod query;
mod rpc;
mod services;
mod shelf;
mod tag;
mod workspace;

const HEADER_SIZE: usize = 10; //[!] Move to Constant file 
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

macro_rules! handle_response {
    ($responses:expr, $code:path, $response:expr) => {
        let res = $response.clone();
        $responses.write().await.insert($response.metadata.unwrap().request_id, ($code, Box::new(res)));
    };
}

#[tokio::main]
async fn main() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:3000").await.unwrap();
    let ep = loop { // Generates a SK/PK pair until an unused one is found 
        let sec_key = get_secret_key();
        let ep = Endpoint::builder()
        .discovery_n0()
        .secret_key(sec_key)
        .alpns(vec![ALPN.to_vec()])
        .bind()
        .await;
        if let Ok(ep) = ep {
            break ep
        }
    };
    println!("{:?}", ep.node_addr().await?.node_id.as_bytes());
    println!("{:?}", ep.node_addr().await?.node_id);
    let peers = Arc::new(RwLock::new(HashMap::<NodeId, Sender<Vec<u8>>>::new()));
    let clients = Arc::new(RwLock::new(Vec::<Client>::new()));
    let tasks = Arc::new(HashMap::<TaskID, JoinHandle<()>>::new());
    let tag_manager = Arc::new(RwLock::new(tag::TagManager::new()));
    let shelf_manager = Arc::new(RwLock::new(ShelfManager::new()));
    let workspaces = Arc::new(RwLock::new(HashMap::<
        workspace::WorkspaceId,
        workspace::Workspace,
    >::new()));
    let responses = Arc::new(RwLock::new(HashMap::new()));
    let notify_queue = Arc::new(RwLock::new(VecDeque::new()));
    let id: NodeId = ep.node_id();
    let (tx, mut rx) = watch::channel::<u64>(64); 
    //[/] The Peer service subscribes to the ResponseHandler when a request is sent. 
    //[/] It is then notified when a response is received so it can acquire the read lock on the Response map.

    let service = ServiceBuilder::new().service(RpcService {
        daemon_info: Arc::new(DaemonInfo::new(id, "".to_string())),
        peer_service: PeerService {
            peers: peers.clone(),
            clients: clients.clone(),
        },
        responses: responses.clone(),
        notify_queue: notify_queue.clone(),
        tasks: tasks.clone(),
        tag_manager: tag_manager.clone(),
        shelf_manager: shelf_manager.clone(),
        workspaces: workspaces.clone(),
    });
    loop {
        tokio::select! {
            Ok((stream, addr)) = listener.accept() => {
                let mut service = service.clone();
                let (mut read, mut write) = stream.into_split();
                let client = Client {
                    hash: 0,
                    addr,
                    w_stream: w_stream.clone(),
                    r_stream: r_stream.clone()
                };
                clients.write().await.push(client);
                let responses = responses.clone();
                tokio::spawn(async move {
                    handle_client(&mut read, &mut write, & mut service, &responses).await;
                });
            },
            conn = ep.accept() => {
                let conn = conn.unwrap().await?;
                //[!] We need to check that the peer is authorized
                let mut service = service.clone();
                let responses = responses.clone();
                let (tx, mut rx) = mpsc::channel::<Vec<u8>>(64); 
                // [!] handle multi accept_bi?
                peers.write().await.insert(conn.remote_node_id().unwrap(), tx);
                tokio::spawn(async move {
                    loop {
                        tokio::select! {
                            Ok((mut write, mut read)) = conn.accept_bi() => {
                                let w_stream : Arc<Mutex<SendStream>> = Arc::new(Mutex::new(write));
                                let r_stream : Arc<Mutex<RecvStream>> = Arc::new(Mutex::new(read));
                                w_stream.clone();
                                r_stream.clone();
                                service.clone();
                                handle_client(&mut read, &mut write, &mut service, &responses).await;
                            }
                            Some(bytes) = rx.recv() => {
                                if let Ok((mut write, read)) = conn.open_bi().await {
                                    write.write_all(&bytes);
                                    handle_client(&mut read, &mut write, &mut service, &responses).await;
                                }
                            }
                        }
                    }
                });
            }
        }
    }
}

async fn handle_client<U: AsyncReadExt + Unpin, B: AsyncWriteExt + Unpin>(
    r_socket: &mut U,
    w_socket: &mut B,
    //addr: SocketAddr,
    service: & mut RpcService,
    responses: &Arc<RwLock<HashMap<u64, (RequestCode, Box<dyn prost::Message>)>>>
) {
    let mut header = vec![0; HEADER_SIZE];

    loop {
        let _bytes_read = match r_socket.read_exact(&mut header).await {
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
        let _bytes_read = match r_socket.read_exact(&mut buffer).await {
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
                    w_socket,
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
                        // Standard Response Handling
                        handle_response!(responses, RequestCode::CreateTag, res);
                    }
                    Ok(RequestCode::CreateWorkspace) => {
                        let res = CreateWorkspaceResponse::decode(&*buffer).unwrap();
                        println!("CreateWorkspace Response received");
                        println!("\tWorkspace ID: {:?}", res.workspace_id);
                        // Standard Response Handling
                        handle_response!(responses, RequestCode::CreateWorkspace, res);
                    }
                    Ok(RequestCode::EditWorkspace) => {
                        let res = EditWorkspaceResponse::decode(&*buffer).unwrap();
                        println!("EditWorkspace Response received");
                        // Standard Response Handling
                        handle_response!(responses, RequestCode::EditWorkspace, res);
                    }
                    Ok(RequestCode::DeleteWorkspace) => {
                        let res = DeleteWorkspaceResponse::decode(&*buffer).unwrap();
                        println!("DeleteWorkspace Response received");
                        // Standard Response Handling
                        handle_response!(responses, RequestCode::DeleteWorkspace, res);
                    }
                    Ok(RequestCode::GetWorkspaces) => {
                        let res = GetWorkspacesResponse::decode(&*buffer).unwrap();
                        println!("GetWorkspaces Response received");
                        println!("\tWorkspaces:");
                        for workspace in &res.workspaces {
                            println!("\t({:?}): {:?}", workspace.workspace_id, workspace.name);
                        }
                        // Standard Response Handling
                        handle_response!(responses, RequestCode::GetWorkspaces, res);
                    }
                    Ok(RequestCode::GetShelves) => {
                        let res = GetShelvesResponse::decode(&*buffer).unwrap();
                        println!("GetShelves Response received");
                        println!("\tShelves:");
                        for shelf in &res.shelves {
                            println!("\t({:?}): {:?}", shelf.shelf_id, shelf.name);
                            println!("\t\t{:?}", shelf.path);
                        }
                        // Standard Response Handling
                        handle_response!(responses, RequestCode::GetShelves, res);
                    }
                    Ok(RequestCode::EditTag) => {
                        let res = EditTagResponse::decode(&*buffer).unwrap();
                        println!("EditTag Response received");
                        // Standard Response Handling
                        handle_response!(responses, RequestCode::EditTag, res);
                    }
                    Ok(RequestCode::AddShelf) => {
                        let res = AddShelfResponse::decode(&*buffer).unwrap();
                        println!("AddShelf Response received");
                        println!("\tShelf ID: {:?}", res.shelf_id);
                        // Standard Response Handling
                        handle_response!(responses, RequestCode::AddShelf, res);
                    }
                    Ok(RequestCode::RemoveShelf) => {
                        let res = RemoveShelfResponse::decode(&*buffer).unwrap();
                        println!("RemoveShelf Response received");
                        // Standard Response Handling
                        handle_response!(responses, RequestCode::RemoveShelf, res);
                    }
                    Ok(RequestCode::AttachTag) => {
                        let res = AttachTagResponse::decode(&*buffer).unwrap();
                        println!("AttachTag Response received");
                        // Standard Response Handling
                        handle_response!(responses, RequestCode::AttachTag, res);
                    }
                    Ok(RequestCode::DetachTag) => {
                        let res = DetachTagResponse::decode(&*buffer).unwrap();
                        println!("DetachTag Response received");
                        // Standard Response Handling
                        handle_response!(responses, RequestCode::DetachTag, res);
                    }
                    Ok(RequestCode::DeleteTag) => {
                        let res = DeleteTagResponse::decode(&*buffer).unwrap();
                        println!("DeleteTag Response received");
                        // Standard Response Handling
                        handle_response!(responses, RequestCode::DeleteTag, res);
                    }
                    Ok(RequestCode::EditShelf) => {
                        let res = EditShelfResponse::decode(&*buffer).unwrap();
                        println!("EditShelf Response received");
                        // Standard Response Handling
                        handle_response!(responses, RequestCode::EditShelf, res);
                    }
                    Ok(RequestCode::StripTag) => {
                        let res = rpc::StripTagResponse::decode(&*buffer).unwrap();
                        println!("StripTag Response received");
                        // Standard Response Handling
                        handle_response!(responses, RequestCode::StripTag, res);
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
    //println!("Client disconnected: {}", addr);
}

fn get_secret_key() -> SecretKey {
    let mut rng = rand::rngs::OsRng;
    iroh_base::SecretKey::generate(&mut rng)
}
