use crate::query::{FileOrder, OrderedFileID, Query, RetrieveErr, RetrieveService};
use crate::services::cache::CacheService;
use crate::services::peer::PeerService;
use crate::workspace::WorkspaceId;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::task::JoinHandle;
use tower::Service;

type TaskID = u64;

#[derive(Clone)]
struct QueryService {
    retrieve_serv: Retrieve,
    peer: PeerService,
    tasks: Arc<HashMap<TaskID, JoinHandle<()>>>,
}

#[derive(Clone)]
struct Retrieve {
    cache: CacheService,
}

impl RetrieveService for Retrieve {
    async fn get_files<T: FileOrder>(
        &self,
        _uuid: (u64, u64),
    ) -> Result<BTreeSet<OrderedFileID<T>>, RetrieveErr> {
        todo!();
    }

    async fn get_all<T: FileOrder>(&self) -> Result<BTreeSet<OrderedFileID<T>>, RetrieveErr> {
        todo!();
    }
}

struct Request<'a, FileOrd: FileOrder> {
    query: String,
    ord: &'a FileOrd,
    workspace_id: WorkspaceId,
    partial: bool,
    client_id: u64,
}

impl<'b, FileOrd: Clone + FileOrder + Send> Service<Request<'b, FileOrd>> for QueryService
where
    OrderedFileID<FileOrd>: Ord,
    &'b FileOrd: Send,
{
    type Response = ();
    type Error = ();
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'b>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<'b, FileOrd>) -> Self::Future {
        let _ = Box::pin(async move {
            let query_str = req.query;
            let mut _query = Query::new(&query_str, req.ord.clone()).map_err(|_| ());
            // Ok until now
            //query.evaluate(self.retrieve_serv);

            let _peer_service = &self.peer.clone();
            let _join_handle: JoinHandle<()> = tokio::spawn(async move {
                //query.evaluate(self.retrieve_serv);
                //peer_service.call();
            });
        });
        todo!();
    }
}

impl<FileOrd: Clone + FileOrder> Service<(Query<FileOrd>, WorkspaceId)> for QueryService {
    type Response = ();
    type Error = ();
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _req: (Query<FileOrd>, WorkspaceId)) -> Self::Future {
        todo!()
    }
}
