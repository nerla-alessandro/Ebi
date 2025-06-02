use std::convert::TryFrom;
macro_rules! impl_try_from {
    ($enum_name:ident, $( $variant:ident ),* $(,)?) => {
        impl TryFrom<u8> for $enum_name {
            type Error = ();

            fn try_from(v: u8) -> Result<Self, Self::Error> {
                match v {
                    $(
                        x if x == $enum_name::$variant as u8 => Ok($enum_name::$variant),
                    )*
                    _ => Err(()),
                }
            }
        }
    };
}
include!(concat!(env!("OUT_DIR"), "/ebi.rpc.rs"));

#[derive(Debug)]
pub enum MessageType {
    Request = 1,
    Response = 2,
    Data = 3,
    Notification = 4,
    Sync = 5,
}

#[derive(Debug)]
pub enum RequestCode {
    ClientQuery = 1,
    PeerQuery = 2,
    CreateWorkspace = 3,
    EditWorkspace = 4,
    DeleteWorkspace = 5,
    GetWorkspaces = 6,
    AddShelf = 7,
    EditShelf = 8,
    RemoveShelf = 9,
    GetShelves = 10,
    CreateTag = 11,
    EditTag = 12,
    DeleteTag = 13,
    AttachTag = 14,
    DetachTag = 15,
    StripTag = 16,
}

#[derive(Debug)]
pub enum DataCode {
    ClientQueryData = 1,
    PeerQueryData = 2,
}

#[derive(Debug)]
pub enum NotificationCode {
    Heartbeat = 1,
    Operation = 2,
    PeerConnected = 3,
}

#[derive(Debug)]
pub enum SyncCode {
    Sync = 1,
}
impl_try_from!(MessageType, Request, Response, Data, Notification, Sync);

impl_try_from!(
    RequestCode,
    ClientQuery,
    PeerQuery,
    CreateTag,
    EditWorkspace,
    DeleteWorkspace,
    GetWorkspaces,
    AddShelf,
    EditShelf,
    RemoveShelf,
    GetShelves,
    CreateTag,
    EditTag,
    DeleteTag,
    AttachTag,
    DetachTag,
    StripTag
);

impl_try_from!(DataCode, ClientQueryData, PeerQueryData);

impl_try_from!(NotificationCode, Heartbeat, Operation, PeerConnected);

impl_try_from!(ActionTarget, Workspace, Shelf, Tag);

impl_try_from!(ActionType, Create, Edit, Delete);

impl_try_from!(SyncCode, Sync);
