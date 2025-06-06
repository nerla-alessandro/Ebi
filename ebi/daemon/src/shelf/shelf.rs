use crate::shelf::node::Node;
use crate::tag::TagRef;
use std::collections::{BTreeSet, HashMap};
use std::ffi::OsStr;
use std::io;
use std::path::PathBuf;
use std::result::Result;
use std::sync::Arc;
use tokio::sync::RwLock;

use super::file::FileRef;

pub struct ShelfInfo {
    pub id: u64,
    pub name: String,
    pub description: String,
    pub root_path: PathBuf,
    //summary: ShelfSummary
}

impl ShelfInfo {
    pub fn new(
        id: u64,
        name: Option<String>,
        description: Option<String>,
        root_path: String,
    ) -> Self {
        let root_path = PathBuf::from(root_path);
        let default_name = root_path
            .file_name()
            .unwrap_or_else(|| OsStr::new("Unnamed"))
            .to_string_lossy()
            .to_string();
        let default_description = "".to_string();
        let name = name.unwrap_or_else(|| default_name);
        let description = description.unwrap_or_else(|| default_description);
        ShelfInfo {
            id,
            name,
            description,
            root_path,
        }
    }
}

pub struct ShelfManager {
    pub shelves: HashMap<u64, Arc<RwLock<Shelf>>>,
    pub count: HashMap<u64, u64>,     // Workspaces per Shelf
    pub paths: HashMap<PathBuf, u64>, // Path to Shelf ID
}

impl ShelfManager {
    pub fn new() -> Self {
        ShelfManager {
            shelves: HashMap::new(),
            count: HashMap::new(),
            paths: HashMap::new(),
        }
    }

    pub fn add_shelf(&mut self, path: PathBuf) -> Result<u64, io::Error> {
        if self.paths.contains_key(&path) {
            let id = self.paths[&path];
            self.count.get_mut(&id).map(|c| *c += 1);
            return Ok(self.paths[&path]);
        }

        let id = loop {
            let id = rand::random::<u64>();
            if !self.shelves.contains_key(&id) {
                break id;
            }
        };
        let shelf = Shelf::new(path.clone())?;
        self.shelves.insert(id, Arc::new(RwLock::new(shelf)));
        self.count.insert(id, 1);
        self.paths.insert(path.clone(), id);
        return Ok(id);
    }

    pub async fn try_remove_shelf(&mut self, id: u64) -> bool {
        if self.shelves.contains_key(&id) {
            let workspace_count = self.count.get(&id).unwrap();
            if *workspace_count > 1 {
                self.count.get_mut(&id).map(|c| *c -= 1);
                return false;
            } else {
                let path = self.shelves[&id].read().await.root_path.clone();
                self.shelves.remove(&id);
                self.count.remove(&id);
                self.paths.remove(&path);
                return true;
            }
        }
        false
    }
}

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

    pub fn contains(&self, tag: TagRef) -> bool {
        self.root.tags.contains_key(&tag) || self.root.dtag_files.contains_key(&tag)
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

    pub fn detach(&mut self, path: PathBuf, tag: TagRef) -> Result<bool, UpdateErr> {
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
                    None => (), // Critical Internal Error
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
                None => (), // Internal Error
            }
        }

        files.iter().for_each(|f| {
            f.file_ref.write().unwrap().detach_dtag(dtag.clone());
        });

        Ok(res)
    }

    pub fn strip(&mut self, path: PathBuf, tag: TagRef) -> Result<(), UpdateErr> {
        let mut curr_node = &mut self.root;

        if !path.is_dir() {
            return Err(UpdateErr::PathNotDir);
        }

        for dir in path.components() {
            let dir: PathBuf = dir.as_os_str().into();
            let child = curr_node
                .directories
                .get_mut(&dir)
                .ok_or_else(|| UpdateErr::PathNotFound)?;
            curr_node = child;
        }

        fn recursive_remove(node: &mut Node, tag: TagRef) {
            node.dtags.remove(&tag);
            node.tags.remove(&tag);
            node.dtag_files.remove(&tag);
            node.files.values_mut().for_each(|file| {
                file.file_ref.write().unwrap().detach(tag.clone());
            });
            node.directories.values_mut().for_each(|child| {
                recursive_remove(child, tag.clone());
            });
        }

        recursive_remove(curr_node, tag);
        Ok(())
    }
}

#[derive(Debug)]
pub enum UpdateErr {
    PathNotFound,
    FileNotFound,
    PathNotDir,
}
