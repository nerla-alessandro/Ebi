use tower::{Service};
use tokio::sync::{RwLock, Mutex};
use std::{sync::Arc, future::Future, pin::Pin, task::{Context, Poll}};
use iroh::{
    endpoint::Connection,
    protocol::{ProtocolHandler, Router},
    Endpoint, NodeId,
};
use std::net::SocketAddr;
use tokio::net::{TcpStream};
use std::collections::HashMap;


#[derive(Clone)]
pub struct PeerService {
    pub peers: Arc<RwLock<HashMap<NodeId, Connection>>>,
    pub clients: Arc<RwLock<Vec<Client>>>
}

pub struct Client {
    pub hash: u64,
    pub addr: SocketAddr,
    pub stream: Arc<Mutex<TcpStream>>
}

impl Service<String> for PeerService {

    type Response = ();
    type Error = ();
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: String) -> Self::Future {

        Box::pin(async move {
        });
        todo!()
    }
}
