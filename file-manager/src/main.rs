#![allow(dead_code)]
use shelf::shelf::Shelf;
use std::sync::Arc;
use iroh::{SecretKey, Endpoint,
    endpoint::Connection,
    NodeId,
};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use anyhow::Result;
use tokio::sync::{RwLock, Mutex};
use tower::{Service, ServiceBuilder};
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, AsyncBufReadExt, AsyncRead};
use std::collections::HashMap;
use crate::services::peer::{PeerService, Client};
use crate::services::rpc::{RpcService, TaskID};
use crate::rpc::{QueryRequest, RequestCode, EchoData};
use prost::Message;

use std::time::Instant;
use tokio::time::{sleep, Duration};

mod query;
mod shelf;
mod tag;
mod services;
mod workspace;
mod rpc;

const ALPN: &[u8] = b"ebi";

#[tokio::main]
async fn main() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:3000").await.unwrap();
    let sec_key = get_secret_key();
    let ep = Endpoint::builder()
        .discovery_n0()
        .secret_key(sec_key)
        .alpns(vec![ALPN.to_vec()])
        .bind()
        .await?;
    println!("{:?}", ep.node_addr().await?.node_id.as_bytes());
    println!("{:?}", ep.node_addr().await?.node_id);
    let peers = Arc::new(RwLock::new(HashMap::<NodeId, Connection>::new()));
    let clients = Arc::new(RwLock::new(Vec::<Client>::new()));
    let tasks = Arc::new(HashMap::<TaskID, JoinHandle<()>>::new());
    let service = ServiceBuilder::new().service(RpcService {  peer_service: PeerService { peers: peers.clone(), clients: clients.clone() }, tasks: tasks.clone() } );
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

async fn handle_client(mut socket: Arc<Mutex<TcpStream>>, addr: SocketAddr, mut service: RpcService) {
    let mut header = vec![0; 9];
    let mut socket = socket.lock().await;

    loop {
        let bytes_read = match socket.read_exact(&mut header).await {
            Ok(n) if n == 0 => {println!("{}", n); break},
            Ok(n) => n,
            Err(e) => {println!("{:?}", e); 9},
        };

        let req_type: u8 = u8::from_le_bytes([header[0]]);
        println!("req_type: {}", req_type);
        let size: u64 = u64::from_le_bytes(header[1..9].try_into().unwrap());
        println!("size: {}", size);
        let mut buffer = vec![0u8; size as usize];
        let bytes_read = match socket.read_exact(&mut buffer).await {
            Ok(n) if n == 0 => 0,
            Ok(n) => n,
            Err(e) => {println!("{:?}", e); 0},
        };
        println!("read all");
        let req_res = match req_type.try_into() {
            Ok(RequestCode::Query) => {
                let req = QueryRequest::decode(&*buffer).unwrap();
                if let Ok(response) = service.call(req).await {
                    let mut buf = Vec::new();
                    response.encode(&mut buf).unwrap();
                    let _ = socket.write_all(&buf).await;
                }
                Ok(())
            }
            Ok(RequestCode::Echo) => {

                let start = Instant::now();
                let req = EchoData::decode(&*buffer).unwrap();
                let elapsed = start.elapsed();
                println!("Total execution time for both tasks: {:?}", elapsed);
                if let Ok(response) = service.call(req).await {
                    let mut buf = vec![1];
                    buf[0] = 42;
                    response.encode_to_vec();
                    let _ = socket.write_all(&buf).await;
                    let vec = &response.encode_to_vec();
                    println!("vlen {}", vec.len());
                    socket.write_all(&vec).await.unwrap();
                }
                Ok(())
            }
            Err(_) => {
                println!("Unknown header {}", req_type);
                Err(())
            }
        };


    }
    println!("Client disconnected: {}", addr);
}

fn get_secret_key() -> SecretKey {
    let mut rng = rand::rngs::OsRng;
    iroh_base::SecretKey::generate(&mut rng)
}
