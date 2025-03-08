use crate::query::{Query, QueryErr};
use crate::shelf::file::File;
use crate::shelf::node::Node;
use crate::tag::TagRef;
use std::io;
use std::path::PathBuf;
use std::result::Result;

// Shelf
#[derive(Debug)]
pub struct Shelf {
    root: Node,
    root_path: PathBuf,
    // String = Workspace identifier + Global
}

impl Shelf {
    pub fn new(path: PathBuf) -> Result<Self, io::Error> {
        Ok(Shelf {
            root: Node::new(path.clone())?,
            root_path: path,
        })
    }

    pub async fn refresh(&self) -> Result<bool, io::Error> {
        todo!();
    }

    pub fn query(&self, query: Query) -> Result<Vec<&'static File>, QueryErr> {
        todo!();
    }

    pub fn attach(&mut self, file: PathBuf, tag: TagRef) -> Result<bool, UpdateErr> {
        let stripped_path = file
            .strip_prefix(&self.root_path)
            .map_err(|_| UpdateErr::PathNotFound)?
            .parent();

        let mut node_v: Vec<(PathBuf, Node)> = Vec::new();
        // Take ownership
        let mut curr_node = std::mem::take(&mut self.root);

        // if stripped_path is none, file must be self.root
        if let Some(path) = stripped_path {
            for dir in path.components() {
                let dir: PathBuf = dir.as_os_str().into();
                // skip empty path (root dir)
                // this is needed because ancestors gives
                // see https://github.com/rust-lang/rust/issues/54927
                if dir.as_os_str().is_empty() {
                    continue;
                }

                let child = curr_node.directories.remove(&dir).unwrap();

                // Store the current node (ownership moved)
                node_v.push((dir.to_path_buf(), curr_node));

                // Move to the child node
                curr_node = child;
            }
        }

        let file = curr_node
            .files
            .get(&file)
            .ok_or_else(|| UpdateErr::FileNotFound)?;
        let file = file.clone();
        let res = file.file_ref.write().unwrap().add_tag(tag.clone());

        for (pbuf, mut node) in node_v.into_iter().rev() {
            if res {
                node.attach(tag.clone(), file.clone());
            }
            let child = std::mem::replace(&mut curr_node, node);
            curr_node.directories.insert(pbuf, child);
        }

        self.root = curr_node;
        Ok(res)
    }

    pub fn detach(&self, file: Option<PathBuf>, tag: TagRef) -> Result<bool, UpdateErr> {
        todo!();
    }
}

// TODO: define extensive errors
#[derive(Debug)]
pub enum UpdateErr {
    PathNotFound,
    FileNotFound,
}
