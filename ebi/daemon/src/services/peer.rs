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

use crate::services::rpc::RequestId;

const HEADER_SIZE: usize = 10; //[!] Move to Constant file 

pub enum PeerError {
    PeerNotFound,
    TimedOut,
    UnexpectedResponse,
    ConnectionClosed, // [!] investigate better
    Unknown,
}

#[derive(Clone)]
pub struct PeerService {
    pub peers: Arc<RwLock<HashMap<NodeId, Peer>>>,
    pub clients: Arc<RwLock<Vec<Client>>>,
    pub responses: Arc<RwLock<HashMap<RequestId, Response>>>,
}

pub struct Peer {
    pub id: NodeId,
    pub watcher: Receiver<Uuid>,
    pub sender: Sender<(Uuid, Vec<u8>)>,
}

pub struct Client {
    pub hash: u64,
    pub addr: SocketAddr,
    pub send: Sender<(Uuid, Vec<u8>)>,
    pub watcher: Receiver<Uuid>,
}

async fn wait_call(mut watcher: Receiver<Uuid>, request_uuid: Uuid) {
    let mut id = Uuid::now_v7();
    while id != request_uuid {
        // this returns an error only if sender in main is dropped
        watcher.changed().await.unwrap();
        id = *watcher.borrow_and_update();
    }
}
impl Service<(NodeId, Request)> for PeerService {
    type Response = Response;
    type Error = PeerError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: (NodeId, Request)) -> Self::Future {
        let peers = self.peers.clone();
        let responses = self.responses.clone();
        Box::pin(async move {
            let r_lock = peers.read().await;
            let r_peer = r_lock.get(&req.0).ok_or_else(|| PeerError::PeerNotFound)?;
            let sender = r_peer.sender.clone();
            let watcher = r_peer.watcher.clone();
            drop(r_lock);

            let mut payload = Vec::new();
            let request_uuid = Uuid::now_v7();
            let req = req.1.clone();
            req.metadata().as_mut().unwrap().request_uuid = request_uuid.as_bytes().to_vec();
            req.encode(&mut payload).unwrap();
            let mut buffer = vec![0; HEADER_SIZE];
            buffer[0] = MessageType::Request as u8;
            buffer[1] = req.request_code() as u8;
            let size = payload.len() as u64;
            buffer[2..HEADER_SIZE].copy_from_slice(&size.to_le_bytes());
            buffer.extend_from_slice(&payload);

            sender
                .send((request_uuid, buffer))
                .await
                .map_err(|_| PeerError::ConnectionClosed)?;

            tokio::select! {
                _ = sleep(Duration::from_secs(120)) => {
                    Err(PeerError::TimedOut)
                }
                _ = wait_call(watcher, request_uuid) => {
                    let responses = responses.read().await;
                    if let Some(res) = responses.get(&request_uuid) {
                        let res = res.clone();
                        res.try_into().map_err(|_| PeerError::UnexpectedResponse)
                    } else {
                        Err(PeerError::Unknown)
                    }
                }
            }
        })
    }
}
