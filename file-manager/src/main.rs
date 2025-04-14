#![allow(dead_code)]

use crate::tag::{Tag, TagRef};
use shelf::shelf::Shelf;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

mod query;
mod shelf;
mod tag;

fn main() {
    let path = PathBuf::from(".\\A\\B\\C\\D\\E\\File.txt");
    let stripped_path = path.strip_prefix(".").unwrap().parent();

    println!("{:?}", stripped_path);

    println!("Components");
    // if stripped_path is none, file must be self.root
    if let Some(path) = stripped_path {
        for dir in path.components() {
            let dir: PathBuf = dir.as_os_str().into();
            println!("{:?}", dir);

            if dir.as_os_str().is_empty() {
                continue;
            }
        }
    }

    println!("Ancestors");
    // if stripped_path is none, file must be self.root
    if let Some(path) = stripped_path {
        for dir in path.ancestors().next().unwrap() {
            println!("{:?}", dir);

            if dir.is_empty() {
                continue;
            }
        }
    }
    // Test
    //Shelf::new(PathBuf::from("C:\\Users\\Alessandro\\Desktop\\Projects\\Tag-Based File Manager\\Automated Test Procedures\\Tests\\Workspaces\\Generated\\Directory"));
    //    let mut shelf = Shelf::new(PathBuf::from(
    //        "/home/yamabiko/Projects/ebi/Automated Test Procedures/Tests/Workspaces/Generated/Test/",
    //    ))
    //    .unwrap();
    //    let tag = Tag::default();
    //
    //    let tref = TagRef {
    //        tag_ref: Arc::new(RwLock::new(tag)),
    //    };
    //
    //    let file = PathBuf::from("/home/yamabiko/Projects/ebi/Automated Test Procedures/Tests/Workspaces/Generated/Test/4EZ3WpsO/hiLQFdlc.dat");
    //    if let Err(e) = shelf.attach(file, tref) {
    //        eprintln!("{:?}", e);
    //    }
}
