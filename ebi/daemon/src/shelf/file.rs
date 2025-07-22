use crate::tag::TagRef;
use chrono::{DateTime, Utc};
use iroh::NodeId;
use std::cmp::Ordering;
use std::collections::BTreeSet;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
#[cfg(windows)]
use std::os::windows::fs::MetadataExt;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

#[derive(Debug, Clone)]
pub struct FileSummary {
    pub owner_node: Option<NodeId>, // Whether the file is local or remote
    pub path: PathBuf,
    pub metadata: FileMetadata,
    //[?] Icon/Preview ??
}

impl FileSummary {
    fn new(owner_node: Option<NodeId>, path: PathBuf, metadata: FileMetadata) -> Self {
        FileSummary {
            owner_node,
            path,
            metadata,
        }
    }
}

impl From<FileRef> for FileSummary {
    fn from(value: FileRef) -> Self {
        let file = value.file_ref.read().unwrap();
        FileSummary::new(
            None, //[!] Should be added in rpc.rs, as it is stored in DaemonInfo 
            file.path.clone(),
            file.metadata.clone(),
        )
    }
}

#[derive(Debug, Clone)]
pub struct FileRef {
    pub file_ref: Arc<RwLock<File>>,
}

#[derive(Debug)]
pub struct File {
    path: PathBuf,
    hash: u64,
    metadata: FileMetadata,
    tags: BTreeSet<TagRef>,
    dtags: BTreeSet<TagRef>,
}

#[derive(Debug, Clone)]
pub struct FileMetadata {
    pub size: u64,
    pub readonly: bool,
    pub modified: Option<DateTime<Utc>>,
    pub accessed: Option<DateTime<Utc>>,
    pub created: Option<DateTime<Utc>>,
    pub unix: Option<UnixMetadata>,
    pub windows: Option<WindowsMetadata>,
}

#[derive(Debug, Clone)]
pub struct UnixMetadata {
    permissions: u32,
    uid: u32,
    gid: u32,
}

#[derive(Debug, Clone)]
pub struct WindowsMetadata {
    attributes: u32,
}

impl FileMetadata {
    pub fn new(path: &PathBuf) -> Self {
        let meta = match std::fs::metadata(path) {
            Ok(meta) => meta,
            Err(_) => {
                return FileMetadata {
                    size: 0,
                    readonly: false,
                    modified: None,
                    accessed: None,
                    created: None,
                    unix: None,
                    windows: None,
                };
            }
        };

        FileMetadata {
            size: meta.len(),
            readonly: meta.permissions().readonly(),
            modified: meta.modified().ok().map(DateTime::<Utc>::from),
            accessed: meta.accessed().ok().map(DateTime::<Utc>::from),
            created: meta.created().ok().map(DateTime::<Utc>::from),

            #[cfg(unix)]
            unix: Some(UnixMetadata {
                permissions: meta.mode(),
                uid: meta.uid(),
                gid: meta.gid(),
            }),
            #[cfg(windows)]
            unix: None,

            #[cfg(windows)]
            windows: Some(WindowsMetadata {
                attributes: meta.file_attributes(),
            }),
            #[cfg(unix)]
            windows: None,
        }
    }
}

impl PartialEq for FileRef {
    fn eq(&self, other: &Self) -> bool {
        self.file_ref.read().unwrap().path == other.file_ref.read().unwrap().path
    }
}

impl Eq for FileRef {}

impl PartialOrd for FileRef {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(
            self.file_ref
                .read()
                .unwrap()
                .metadata
                .modified
                .cmp(&other.file_ref.read().unwrap().metadata.modified),
        )
    }
}

impl Ord for FileRef {
    fn cmp(&self, other: &Self) -> Ordering {
        self.file_ref
            .read()
            .unwrap()
            .metadata
            .modified
            .cmp(&other.file_ref.read().unwrap().metadata.modified)
    }
}

impl File {
    pub fn new(
        path: PathBuf,
        tags: BTreeSet<TagRef>,
        dtags: BTreeSet<TagRef>,
        metadata: FileMetadata,
    ) -> Self {
        File {
            path,
            hash: 0,
            metadata,
            tags,
            dtags,
        }
    }

    pub fn attach(&mut self, tag: TagRef) -> bool {
        self.tags.insert(tag)
    }

    pub fn detach(&mut self, tag: TagRef) -> bool {
        self.tags.remove(&tag)
    }

    pub fn attach_dtag(&mut self, tag: TagRef) -> bool {
        self.dtags.insert(tag)
    }

    pub fn detach_dtag(&mut self, tag: TagRef) -> bool {
        self.dtags.remove(&tag)
    }
}
