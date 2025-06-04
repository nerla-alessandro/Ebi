use iroh::{
    NodeId,
};
use tokio::sync::mpsc::Sender;
use crate::rpc::*;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::net::TcpStream;
use tokio::net::{tcp::{OwnedReadHalf, OwnedWriteHalf}};
use tokio::sync::{Mutex, RwLock};
use tower::Service;
use prost::Message;

const HEADER_SIZE: usize = 10; //[!] Move to Constant file 

pub enum PeerError {
    PeerNotFound
}

#[derive(Clone)]
pub struct PeerService {
    pub peers: Arc<RwLock<HashMap<NodeId, Sender<Vec<u8>>>>>,
    pub clients: Arc<RwLock<Vec<Client>>>,
}

pub struct Client {
    pub hash: u64,
    pub addr: SocketAddr,
    pub r_stream: Arc<Mutex<OwnedReadHalf>>,
    pub w_stream: Arc<Mutex<OwnedWriteHalf>>,
}

impl PeerService {
}

impl Service<(NodeId, DeleteTag)> for PeerService {
    type Response = DeleteTagResponse;
    type Error = PeerError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: (NodeId, DeleteTag)) -> Self::Future {
        let peers = self.peers.clone();
        Box::pin(async move {
            // Open output Stream
            let conn = peers.read().await.get(&req.0).ok_or_else(|| PeerError::PeerNotFound)?;
            let out_stream = conn.open_uni().await?;
            // Seralize Message 
            let mut payload = Vec::new();
            req.1.encode(&mut payload).unwrap();
            let mut buffer = vec![0; HEADER_SIZE];
            buffer[0] = MessageType::Request as u8;
            buffer[1] = RequestCode::DeleteTag as u8;
            let size = payload.len() as u64;
            buffer[2..HEADER_SIZE].copy_from_slice(&size.to_le_bytes());
            buffer.extend_from_slice(&payload);
            // Write Message
            out_stream.write_all(&buffer);

            //Wait for req_id inside responses
            DeleteTagResponse ()
        })
    }
}
