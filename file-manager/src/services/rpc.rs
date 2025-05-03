use crate::rpc::{QueryRequest, QueryResponse, EchoData};
use crate::services::peer::PeerService;
use std::future::Future;
use tokio::task::JoinHandle;
use std::pin::Pin;
use std::task::{Context, Poll};
use tower::Service;
use std::collections::HashMap;
use std::sync::Arc;
use std::fs::File;
use std::io::Write;

#[derive(Clone)]
pub struct RpcService {
    pub peer_service: PeerService,
    pub tasks: Arc<HashMap<TaskID, JoinHandle<()>>>
}
pub type TaskID = u64;


impl Service<QueryRequest> for RpcService {
    type Response = QueryResponse;
    type Error = ();
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: QueryRequest) -> Self::Future  {
        todo!();
    }
}

impl Service<EchoData> for RpcService {
    type Response = EchoData;
    type Error = ();
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: EchoData) -> Self::Future  {
        Box::pin(async move {
            let data = &req.data[0];
            //let mut file = File::create("output.bin").unwrap();
            //file.write_all(&data).unwrap();
            let mut res: Vec<Vec<u8>> = Vec::new();
            res.push(data.clone());
            println!("sending");
            Ok(
                EchoData {
                    data: res
                }
            )
        })
    }
}
