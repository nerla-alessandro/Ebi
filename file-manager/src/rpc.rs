use std::convert::TryFrom;

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

impl TryFrom<u8> for MessageType {
    type Error = ();

    fn try_from(v: u8) -> Result<Self, Self::Error> {
        match v {
            x if x == MessageType::Request as u8 => Ok(MessageType::Request),
            x if x == MessageType::Response as u8 => Ok(MessageType::Response),
            x if x == MessageType::Data as u8 => Ok(MessageType::Data),
            x if x == MessageType::Notification as u8 => Ok(MessageType::Notification),
            x if x == MessageType::Sync as u8 => Ok(MessageType::Sync),
            _ => Err(()),
        }
    }
}

impl TryFrom<u8> for RequestCode {
    type Error = ();

    fn try_from(v: u8) -> Result<Self, Self::Error> {
        match v {
            x if x == RequestCode::ClientQuery as u8 => Ok(RequestCode::ClientQuery),
            x if x == RequestCode::PeerQuery as u8 => Ok(RequestCode::PeerQuery),
            x if x == RequestCode::CreateWorkspace as u8 => Ok(RequestCode::CreateWorkspace),
            x if x == RequestCode::EditWorkspace as u8 => Ok(RequestCode::EditWorkspace),
            x if x == RequestCode::DeleteWorkspace as u8 => Ok(RequestCode::DeleteWorkspace),
            x if x == RequestCode::GetWorkspaces as u8 => Ok(RequestCode::GetWorkspaces),
            x if x == RequestCode::AddShelf as u8 => Ok(RequestCode::AddShelf),
            x if x == RequestCode::EditShelf as u8 => Ok(RequestCode::EditShelf),
            x if x == RequestCode::RemoveShelf as u8 => Ok(RequestCode::RemoveShelf),
            x if x == RequestCode::GetShelves as u8 => Ok(RequestCode::GetShelves),
            x if x == RequestCode::CreateTag as u8 => Ok(RequestCode::CreateTag),
            x if x == RequestCode::EditTag as u8 => Ok(RequestCode::EditTag),
            x if x == RequestCode::DeleteTag as u8 => Ok(RequestCode::DeleteTag),
            x if x == RequestCode::AttachTag as u8 => Ok(RequestCode::AttachTag),
            x if x == RequestCode::DetachTag as u8 => Ok(RequestCode::DetachTag),
            x if x == RequestCode::StripTag as u8 => Ok(RequestCode::StripTag),
            _ => Err(()),
        }
    }
}

impl TryFrom<u8> for DataCode {
    type Error = ();

    fn try_from(v: u8) -> Result<Self, Self::Error> {
        match v {
            x if x == DataCode::ClientQueryData as u8 => Ok(DataCode::ClientQueryData),
            x if x == DataCode::PeerQueryData as u8 => Ok(DataCode::PeerQueryData),
            _ => Err(()),
        }
    }
}

impl TryFrom<u8> for NotificationCode {
    type Error = ();

    fn try_from(v: u8) -> Result<Self, Self::Error> {
        match v {
            x if x == NotificationCode::Heartbeat as u8 => Ok(NotificationCode::Heartbeat),
            x if x == NotificationCode::Operation as u8 => Ok(NotificationCode::Operation),
            x if x == NotificationCode::PeerConnected as u8 => Ok(NotificationCode::PeerConnected),
            _ => Err(()),
        }
    }
}

impl TryFrom<u32> for ActionTarget {
    type Error = ();

    fn try_from(v: u32) -> Result<Self, Self::Error> {
        match v {
            x if x == ActionTarget::Workspace as u32 => Ok(ActionTarget::Workspace),
            x if x == ActionTarget::Shelf as u32 => Ok(ActionTarget::Shelf),
            x if x == ActionTarget::Tag as u32 => Ok(ActionTarget::Tag),
            _ => Err(()),
        }
    }
}

impl TryFrom<u8> for ActionType {
    type Error = ();

    fn try_from(v: u8) -> Result<Self, Self::Error> {
        match v {
            x if x == ActionType::Create as u8 => Ok(ActionType::Create),
            x if x == ActionType::Edit as u8 => Ok(ActionType::Edit),
            x if x == ActionType::Delete as u8 => Ok(ActionType::Delete),
            _ => Err(()),
        }
    }
}

impl TryFrom<u8> for SyncCode {
    type Error = ();

    fn try_from(v: u8) -> Result<Self, Self::Error> {
        match v {
            x if x == SyncCode::Sync as u8 => Ok(SyncCode::Sync),
            _ => Err(()),
        }
    }
}

tonic::include_proto!("ebi.rpc");
