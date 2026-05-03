Ebi's state crate.

# Objectives

This crate implements and defines state handling for a set of `Workspace`s. 

1. keeps track of locally modified states and globally synced states for each `Workspace`
2. generates related [operations](https://en.wikipedia.org/wiki/Conflict-free_replicated_data_type#Operation-based_CRDTs) to be shared to other peers for every state change
3. handles persistent storage via [redb](https://docs.rs/redb/latest/redb/)
4. exposes a [`tower::Service`] which serves as an interface to execute `Workspace` related operations

# Usage

Adding, removing or listing workspaces can be done directly on a `StateService`.

Single-workspace related operations are handled by `WorkspaceStateService`, which can be obtained by calling `fn workspace(&mut self, id: WorkspaceId)`.
```rust
# use std::path::PathBuf;
# use ebi_types::NodeId;
# use std::str::FromStr;
# const TEST_PKEY: &str = "ae58ff8833241ac82d6ff7611046ed67b5072d142c588d0063e942d9a75502b6";
# use ebi_state::service::StateService;
# async fn run() -> Result<(), ebi_proto::rpc::ReturnCode> {
# let node_id = NodeId::from_str(TEST_PKEY).unwrap();
let db_path = PathBuf::from("/my/path/to/persistent.db");
let mut state = StateService::new(&db_path).unwrap();
let workspace_name = String::from("example_name");
let workspace_desc = String::from("example_name");

let wk_id = state.create_workspace(workspace_name, workspace_desc).await?;
let mut workspace_scope = state.workspace(wk_id);


let shelf_path = PathBuf::from("/home/user");
workspace_scope.assign_shelf(shelf_path, node_id, false, None, None).await?;

state.remove_workspace(wk_id).await?;
# Ok(())
# }
```
