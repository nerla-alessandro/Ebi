use std::path::PathBuf;
use std::io;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::result::Result;

#[derive(Debug)]
pub struct Shelf {
    root: Node,
    // String = Workspace identifier + Global
    ownership: HashMap<Tag, HashSet<String>>
}


#[derive(Debug)]
struct Node {
    files: HashMap<PathBuf, &'static File>,
    tags: HashMap<Tag, &'static File>,
    directories: HashMap<PathBuf, &'static Node>
    // untagged: set of untagged files, support structure
}

#[derive(Debug, Eq, PartialOrd, PartialEq, Ord)]
pub struct Tag {
    name: String,
    // tags applied by the directory must be differentiated somehow
}

#[derive(Debug)]
struct Metadata {

}

#[derive(Debug)]
pub struct File {
    path: PathBuf,
    hash: u64,
    metadata: Metadata,
    tags: BTreeSet<&'static Tag>,
}

#[derive(Debug)]
pub struct Query {
}

// TODO: define appropriate errors, include I/O, etc.
pub enum QueryErr {
    SyntaxError,
    KeyError
}

// TODO: define appropriate errors
pub enum UpdateErr {
    PathNotFound,
}


impl File {
    pub fn new(path: PathBuf, tags: BTreeSet<&'static Tag>) -> Self {
        File { path, hash: 0, metadata: Metadata {}, tags }
    }

    pub fn add_tag(&mut self, tag: &'static Tag) -> bool {
        self.tags.insert(tag)
    }

    pub fn remove_tag(&mut self, tag: Tag) -> bool {
        self.tags.remove(&tag)
    }
}

impl Shelf {
    pub fn new(root: PathBuf) -> Result<Self, io::Error> {
        todo!();
    }

    pub async fn refresh(&self) -> Result<bool, io::Error> {
        todo!();
    }

    pub fn query(&self, query: Query) -> Result<Vec<&'static File>, QueryErr> {
        todo!();
    }

    pub fn insert(&self, file: PathBuf, tag: Tag) -> Result<bool, UpdateErr> {
        todo!();
    }

    pub fn delete(&self, file: Option<PathBuf>, tag: Tag) -> Result<bool, UpdateErr> {
        todo!();
    }
}
