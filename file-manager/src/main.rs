use crate::tag::{Tag, TagRef};
use shelf::shelf::Shelf;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

mod query;
mod shelf;
mod tag;

fn main() {
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
