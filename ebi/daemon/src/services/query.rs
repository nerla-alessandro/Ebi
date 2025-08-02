use crate::query::file_order::{FileOrder, OrderedFileSummary};
use crate::query::{Query, RetrieveErr, RetrieveService};
use crate::services::cache::CacheService;
use crate::services::peer::{PeerError, PeerService};
use crate::services::rpc::{try_get_workspace, uuid, DaemonInfo};
use crate::services::workspace::WorkspaceService;
use crate::shelf::shelf::{ShelfOwner, ShelfType};
use crate::tag::TagId;
use crate::workspace::WorkspaceId;
use bincode::{serde::borrow_decode_from_slice, serde::encode_to_vec};
use ebi_proto::rpc::{
    parse_code, ClientQuery, ClientQueryData, ClientQueryResponse, Data, DataCode, File, FileMetadata, PeerQuery, ReqMetadata, Request, ResMetadata, Response, ReturnCode
};
use iroh::NodeId;
use uuid::Uuid;
use core::error;
use std::collections::HashMap;
use std::collections::{BTreeSet, HashSet};
use std::fs::Metadata;
use std::result;
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::task::{JoinSet, JoinHandle};
use tower::Service;

type TaskID = u64;

#[derive(Clone)]
struct QueryService {
    retrieve_serv: Retrieve,
    peer_srv: PeerService,
    workspace_srv: WorkspaceService,
    daemon_info: Arc<DaemonInfo>
}

#[derive(Clone)]
struct Retrieve {
    cache: CacheService,
}

impl RetrieveService for Retrieve {
    async fn get_files(
        &self,
        _workspace_id: WorkspaceId,
        _tag_id: TagId,
    ) -> Result<BTreeSet<OrderedFileSummary>, RetrieveErr> {
        todo!();
    }

    async fn get_all(&self) -> Result<BTreeSet<OrderedFileSummary>, RetrieveErr> {
        todo!();
    }
}

impl Service<ClientQuery> for QueryService {
    type Response = ();
    type Error = ();
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: ClientQuery) -> Self::Future {
        let mut workspace_srv = self.workspace_srv.clone();
        let mut peer_srv = self.peer_srv.clone();
        let node_id = self.daemon_info.id.clone();
        let _ = Box::pin(async move {
            let query_str = req.query;

            //let ord = to_file_order(&req.file_ord.unwrap());
            let Ok(workspace) = try_get_workspace(&req.workspace_id, &mut workspace_srv).await
            else {
                return Err(ReturnCode::WorkspaceNotFound);
            };
            //let workspace_id = uuid(req.workspace_id.clone()).unwrap(); //[!] May be unnecessary 

            let Some(metadata) = req.metadata
            else {
                return Err(ReturnCode::MalformedRequest);
            };

            let file_order = FileOrder::try_from(req.file_ord.unwrap().order_by)?;
            let mut query = Query::new(&query_str, file_order, req.file_ord.unwrap().ascending)
                .map_err(|_| ReturnCode::InternalStateError)?;

            let shelves = &workspace.read().await.shelves;
            let mut nodes = HashSet::<NodeId>::new();
            for (_, shelf) in shelves {
                let shelf_r = shelf.read().await;
                match shelf_r.shelf_type {
                    ShelfType::Local(_) => {} //[?] Perform local queries after sending remote requests ?? 
                    ShelfType::Remote => {
                        if query.may_hold(shelf_r.get_tags()).await {
                            match shelf.read().await.shelf_owner {
                                ShelfOwner::Node(node_id) => {
                                    nodes.insert(node_id);
                                }
                                ShelfOwner::Sync(_) => {} // [!] Implement Sync logic
                            }
                        }
                    }
                }
            }
            
            let config = bincode::config::standard(); // [TODO] this should be set globally
            let mut serialized_query =
                encode_to_vec(&query, config).map_err(|_| ReturnCode::ParseError)?;

            let peer_query = Request::from(PeerQuery {
                query: serialized_query,
                workspace_id: req.workspace_id,
                metadata: req.metadata,
            });

            //[#]
        
            //[/] Return ClientQueryResponse in RpcService before asynchronously calling QueryService 

            let mut futures = JoinSet::<Result<Response, PeerError>>::new();
            for node_id in nodes {
                let peer_srv = peer_srv.clone();
                let peer_req = peer_query.clone();
                futures.spawn(async move {
                    peer_srv.call((node_id, peer_req)).await
                });
            }

            //[TODO] Perform local queries 

            if req.atomic {
                let mut files = Vec::<BTreeSet<OrderedFileSummary>>::new();
                let mut errors = Vec::<String>::new();
                while let Some(result) = futures.join_next().await {
                    if result.is_err(){
                        errors.push(format!("Join error: {:?}", result.unwrap_err()));
                        continue;
                    } else {
                        let result = result.unwrap();
                        if result.is_err() {
                            errors.push(format!("Peer error: {:?}", result.unwrap_err()));
                            continue;
                        } else {
                            let result = result.unwrap();
                            let metadata;
                            if let Some(meta) = result.metadata() {
                                metadata = meta;
                            } else {
                                errors.push("[?]: Malformed Response".to_string());
                                continue;
                            }
                            let node = metadata.source_id;
                            match result {
                                Response::PeerQueryResponse(res) => {                         
                                    match parse_code(metadata.return_code) {
                                        ReturnCode::Success => {
                                            //[?] Correct way to deserialize res.files into BTreeSet<OrderedFileSummary> ?? 
                                            match borrow_decode_from_slice::<BTreeSet<OrderedFileSummary>, _>(&res.files, config) {
                                                Ok((deserialized_files, _)) => {
                                                    files.push(deserialized_files);
                                                }
                                                Err(e) => {
                                                    errors.push(format!("[{:?}] Deserialization error: {:?}", node, e));
                                                }
                                            }
                                        }
                                        _ => {
                                            let error_str = match metadata.error_data {
                                                Some(data) => data.error_data.join("\n"),
                                                None => "Unknown error".to_string()
                                            };
                                            errors.push(format!("[{:?}] Deserialization error: {:?}", node, error_str));
                                        }
                                    }
                                }
                                res => {
                                    errors.push(format!("[{:?}]: Unexpected response type", node));
                                }
                            }
                        }
                    }
                }

                //[?] Is the tournament-style Merge-Sort approach the most efficient method ?? 
                //[/] BTreeSets are not guaranteed to be the same size 
                //[TODO] Time & Space Complexity analysis
                let file_vec: Vec<OrderedFileSummary>;
                if files.is_empty() {
                    file_vec = Vec::new();
                } else {
                    while files.len() > 1 {
                        let mut next_round = Vec::with_capacity((files.len() + 1) / 2);
                        let mut chunks = files.chunks_exact(2);
                        
                        if let Some(remainder) = chunks.remainder().first() {
                            next_round.push(remainder.clone());
                        }

                        for chunk in chunks.by_ref() {  //[TODO] Parallelise merge 
                            let a = &chunk[0];
                            let b = &chunk[1];
                            
                            // Merging the smaller set into the larger one is more efficient
                            let merged_tree = if a.len() <= b.len() {
                                b.union(a).cloned().collect()
                            } else {
                                a.union(b).cloned().collect()
                            };
                            next_round.push(merged_tree);
                        }
                        
                        files = next_round;
                    }
                    file_vec = Vec::from_iter(files[0].into_iter());
                }
                
                let client_node = NodeId::from_bytes(&req.metadata.unwrap().source_id)?;
                peer_srv.call((client_node, Data::ClientQueryData( ClientQueryData { //[!] ClientQueryData is the only Data-type RPC 
                    token: node_id.as_bytes().to_vec(),
                    return_code: ReturnCode::Success as u32, //[?] Should this always be success ?? 
                    files: file_vec.iter().map(|f| File { path: f.file_summary.path.to_string_lossy().to_string(), metadata: Some(FileMetadata {
                        size: f.file_summary.metadata.size,
                        readonly: f.file_summary.metadata.readonly,
                        modified: f.file_summary.metadata.modified,
                        accessed: f.file_summary.metadata.accessed,
                        created: f.file_summary.metadata.created,
                        unix: f.file_summary.metadata.unix,
                        windows: f.file_summary.metadata.windows
                    })}).collect()
                }))).await;
                
                //[TODO] Send data packet to Client 
            } else {
                todo!();
            }

            #[#]

            // Build PeerQuery request

            if req.atomic {
                for node_id in nodes {
                    match peer_srv.call((node_id, peer_query.clone())).await { //[!] Make parallel requests, wait for all if atomic 
                        Ok(res) => parse_code(res.metadata().unwrap().return_code),
                        Err(_) => ReturnCode::PeerServiceError, //[!] Generic error, expand with PeerService errors
                    };
                }
            }

            // Ok until now
            //query.evaluate(uuid(req.workspace_id).unwrap(), self.retrieve_serv).await.map_err(|_| ReturnCode::InternalStateError)?;

            let _join_handle: JoinHandle<()> = tokio::spawn(async move {
                //query.evaluate(self.retrieve_serv);
                //peer_service.call();
            });
            Ok(())
        });
        todo!();
    }
}
