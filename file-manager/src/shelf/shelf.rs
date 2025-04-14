use crate::query::{Query, QueryErr};
use crate::shelf::file::File;
use crate::shelf::node::Node;
use crate::tag::{self, TagRef};
use std::collections::{BTreeSet, HashSet};
use std::io;
use std::path::PathBuf;
use std::result::Result;

use super::file::FileRef;

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

    pub async fn retrieve(&self, tag: TagRef) -> BTreeSet<FileRef> {
        let mut res = self
            .root
            .tags
            .get(&tag)
            .cloned()
            .unwrap_or_else(|| BTreeSet::<FileRef>::new());
        let mut dres = self
            .root
            .dtag_files
            .get(&tag)
            .cloned()
            .unwrap_or_else(|| BTreeSet::<FileRef>::new());
        res.append(&mut dres);
        res
    }

    pub async fn refresh(&self) -> Result<bool, io::Error> {
        todo!();
    }

    pub fn attach(&mut self, path: PathBuf, tag: TagRef) -> Result<bool, UpdateErr> {
        let stripped_path = path
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

                let child = curr_node
                    .directories
                    .remove(&dir)
                    .ok_or_else(|| UpdateErr::PathNotFound)?;

                // Store the current node (ownership moved)
                node_v.push((dir.to_path_buf(), curr_node));

                // Move to the child node
                curr_node = child;
            }
        }

        let file = curr_node
            .files
            .get(&path)
            .ok_or_else(|| UpdateErr::FileNotFound)?;
        let file = file.clone();
        let res = file.file_ref.write().unwrap().attach(tag.clone());

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

    pub fn detach(&mut self, path: Option<PathBuf>, tag: TagRef) -> Result<bool, UpdateErr> {
        match path {
            Some(path) => {
                let stripped_path = path
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

                        let child = curr_node
                            .directories
                            .remove(&dir)
                            .ok_or_else(|| UpdateErr::PathNotFound)?;

                        // Store the current node (ownership moved)
                        node_v.push((dir.to_path_buf(), curr_node));

                        // Move to the child node
                        curr_node = child;
                    }
                }

                let file = curr_node
                    .files
                    .get(&path)
                    .ok_or_else(|| UpdateErr::FileNotFound)?;
                let file = file.clone();
                let res = file.file_ref.write().unwrap().detach(tag.clone());
                for (pbuf, mut node) in node_v.into_iter().rev() {
                    if res {
                        node.detach(tag.clone(), Some(file.clone()));
                    }
                    let child = std::mem::replace(&mut curr_node, node);
                    curr_node.directories.insert(pbuf, child);
                }

                self.root = curr_node;
                Ok(res)
            }
            None => {
                fn recursive_detach(node: &mut Node, tag: TagRef) {
                    node.detach(tag.clone(), None);

                    for (_, node) in node.directories.iter_mut() {
                        recursive_detach(node, tag.clone());
                    }
                }

                let res = self.root.tags.get(&tag).is_some();

                // Detach tag from every single (tagged) file in the Shelf
                self.root.tags.get_mut(&tag).map(|set| {
                    set.iter().for_each(|file| {
                        file.file_ref.write().unwrap().detach(tag.clone());
                    });
                });
                // Delete the tag from every Node in the Shelf
                recursive_detach(&mut self.root, tag.clone());

                Ok(res)
            }
        }
    }

    pub fn attach_dtag(&mut self, path: PathBuf, dtag: TagRef) -> Result<bool, UpdateErr> {
        let dpath = path
            .strip_prefix(&self.root_path)
            .map_err(|_| UpdateErr::PathNotFound)?;

        let mut dtagged_parent = false;
        let mut curr_node = &mut self.root;
        for dir in dpath.components() {
            let dir: PathBuf = dir.as_os_str().into();
            let child = curr_node
                .directories
                .get_mut(&dir)
                .ok_or_else(|| UpdateErr::PathNotFound)?;
            if child.dtags.contains(&dtag) {
                dtagged_parent = true;
            }
            curr_node = child;
        }
        if dtagged_parent {
            return Ok(curr_node.attach_dtag(dtag.clone()));
        }

        let mut node_v: Vec<(PathBuf, Node)> = Vec::new();
        // Take ownership
        let mut curr_node = std::mem::take(&mut self.root);

        // if dpath is none, dir must be self.root
        for dir in dpath.components() {
            let dir: PathBuf = dir.as_os_str().into();
            let child = curr_node
                .directories
                .remove(&dir)
                .ok_or_else(|| UpdateErr::PathNotFound)?;
            // Store the current node (ownership moved)
            node_v.push((dir.to_path_buf(), curr_node));
            // Move to the child node
            curr_node = child;
        }

        let res = curr_node.attach_dtag(dtag.clone());

        fn recursive_attach(node: &mut Node, dtag: TagRef) -> BTreeSet<FileRef> {
            let mut files = node.files.values().cloned().collect::<BTreeSet<FileRef>>();
            let mut subdir_files = BTreeSet::new();
            for (_, subnode) in node.directories.iter_mut() {
                let mut sub_files = recursive_attach(subnode, dtag.clone());
                subdir_files.append(&mut sub_files);
            }
            files.append(&mut subdir_files);

            fn add_dtag_files(node: &mut Node, dtag: TagRef, files: BTreeSet<FileRef>) {
                let set = node
                    .dtag_files
                    .entry(dtag)
                    .or_insert_with(|| BTreeSet::new());
                files.iter().for_each(|f| {
                    set.insert(f.clone());
                });
            }

            add_dtag_files(node, dtag, files.clone());
            return files;
        }

        let files = recursive_attach(&mut curr_node, dtag.clone());

        for (pbuf, mut node) in node_v.into_iter().rev() {
            let set = node
                .dtag_files
                .entry(dtag.clone())
                .or_insert_with(|| BTreeSet::new());
            set.append(&mut files.clone());
            let child = std::mem::replace(&mut curr_node, node);
            curr_node.directories.insert(pbuf, child);
        }

        files.iter().for_each(|f| {
            f.file_ref.write().unwrap().attach_dtag(dtag.clone());
        });

        Ok(res)
    }

    pub fn detach_dtag(&mut self, path: PathBuf, dtag: TagRef) -> Result<bool, UpdateErr> {
        let dpath = path
            .strip_prefix(&self.root_path)
            .map_err(|_| UpdateErr::PathNotFound)?;

        let mut dtagged_parent = false;
        let mut curr_node = &mut self.root;
        for dir in dpath.components() {
            let dir: PathBuf = dir.as_os_str().into();
            let child = curr_node
                .directories
                .get_mut(&dir)
                .ok_or_else(|| UpdateErr::PathNotFound)?;
            if child.dtags.contains(&dtag) {
                dtagged_parent = true;
            }
            curr_node = child;
        }
        if dtagged_parent {
            return Ok(curr_node.detach_dtag(dtag.clone()));
        }

        let mut node_v: Vec<(PathBuf, Node)> = Vec::new();
        // Take ownership
        let mut curr_node = std::mem::take(&mut self.root);

        // if dpath is none, dir must be self.root
        for dir in dpath.components() {
            let dir: PathBuf = dir.as_os_str().into();
            let child = curr_node
                .directories
                .remove(&dir)
                .ok_or_else(|| UpdateErr::PathNotFound)?;
            // Store the current node (ownership moved)
            node_v.push((dir.to_path_buf(), curr_node));
            // Move to the child node
            curr_node = child;
        }

        let res = curr_node.detach_dtag(dtag.clone());

        fn recursive_detach(node: &mut Node, dtag: TagRef) -> BTreeSet<FileRef> {
            // Stop detaching the dtag when encountering a child node already dtagged with it
            if node.dtags.contains(&dtag) {
                return BTreeSet::new();
            }

            let mut files = node.files.values().cloned().collect::<BTreeSet<FileRef>>();
            let mut subdir_files = BTreeSet::new();
            for (_, subnode) in node.directories.iter_mut() {
                let mut sub_files = recursive_detach(subnode, dtag.clone());
                subdir_files.append(&mut sub_files);
            }
            files.append(&mut subdir_files);

            fn remove_dtag_files(node: &mut Node, dtag: TagRef, files: BTreeSet<FileRef>) {
                let set = node.dtag_files.get_mut(&dtag);
                match set {
                    Some(set) => {
                        files.iter().for_each(|f| {
                            set.remove(f);
                        });
                        if set.is_empty() {
                            node.dtag_files.remove(&dtag);
                        }
                    }
                    None => (), //[!] Critical Internal Error
                }
            }

            remove_dtag_files(node, dtag, files.clone());
            return files;
        }

        let files = recursive_detach(&mut curr_node, dtag.clone());

        for (pbuf, mut node) in node_v.into_iter().rev() {
            let set = node.dtag_files.get_mut(&dtag.clone());
            match set {
                Some(set) => {
                    files.iter().for_each(|f| {
                        set.remove(f);
                    });
                    let child = std::mem::replace(&mut curr_node, node);
                    curr_node.directories.insert(pbuf, child);
                }
                None => (), //[!] Internal Error
            }
        }

        files.iter().for_each(|f| {
            f.file_ref.write().unwrap().detach_dtag(dtag.clone());
        });

        Ok(res)
    }
}

// [TODO]: define extensive errors
#[derive(Debug)]
pub enum UpdateErr {
    PathNotFound,
    FileNotFound,
}
