use crate::shelf::file::{File, FileMetadata, FileRef};
use crate::tag::TagRef;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::io;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::RwLock;

#[derive(Debug, Default)]
pub struct Node {
    pub files: HashMap<PathBuf, FileRef>,
    pub tags: HashMap<TagRef, BTreeSet<FileRef>>, // TODO: this should be changed to BTreeSet later
    pub dtags: HashSet<TagRef>,                   // directory level tags, to be applied down
    pub directories: HashMap<PathBuf, Node>, // untagged: set of untagged files, support structure
}

impl Node {
    pub fn new(path: PathBuf) -> Result<Self, io::Error> {
        let entries = std::fs::read_dir(&path)?
            .map(|res| res.map(|e| e.path()))
            .collect::<Result<Vec<_>, io::Error>>()?;
        let (dir_paths, file_paths): (Vec<_>, Vec<_>) =
            entries.into_iter().partition(|path| path.is_dir());
        let files = file_paths
            .into_iter()
            .map(|file_path| {
                (
                    file_path.clone(),
                    FileRef {
                        file_ref: Rc::new(RwLock::new(File::new(
                            file_path.clone(),
                            BTreeSet::new(),
                            BTreeSet::new(),
                            FileMetadata::new(&file_path),
                        ))),
                    },
                )
            })
            .collect::<HashMap<PathBuf, FileRef>>();
        let directories = dir_paths
            .into_iter()
            .map(|dir| {
                (
                    dir.strip_prefix(&path).unwrap().to_path_buf(),
                    Node::new(dir).unwrap(),
                )
            })
            .collect::<HashMap<PathBuf, Node>>();
        Ok(Node {
            files,
            tags: HashMap::new(),
            dtags: HashSet::new(),
            directories,
        })
    }

    pub fn attach_dtag(&mut self, tag: TagRef) -> bool {
        self.dtags.insert(tag.clone())
    }

    pub fn detach_dtag(&mut self, tag: TagRef) -> bool {
        self.dtags.remove(&tag)
    }

    pub fn attach(&mut self, tag: TagRef, file: FileRef) -> bool {
        let set = self.tags.entry(tag).or_insert_with(|| BTreeSet::new());
        set.insert(file.clone())
    }
}
