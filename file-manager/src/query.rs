use crate::shelf::file::{FileMetadata, FileRef};
use crate::tag::{TagManager, TagRef};
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::fmt::Binary;
use std::path::PathBuf;
use std::result;

#[derive(Debug)]
pub enum Order {
    Ascending,
    Descending,
}

pub trait FileOrder {}

#[derive(Debug)]
pub struct Name {
    pub order: Order,
}

impl FileOrder for Name {}

#[derive(Debug)]
pub struct Size {
    pub order: Order,
}

impl FileOrder for Size {}

#[derive(Debug)]
pub struct Modified {
    pub order: Order,
}

impl FileOrder for Modified {}

#[derive(Debug)]
pub struct Created {
    pub order: Order,
}

impl FileOrder for Created {}

#[derive(Debug)]
pub struct Accessed {
    pub order: Order,
}

impl FileOrder for Accessed {}

#[derive(Debug)]
pub struct Unordered;

impl FileOrder for Unordered {}

peg::parser! {
    grammar tag_query() for str {
        pub rule expression() -> Formula
            = precedence! {
                x:(@) _ "OR" _ y:@ { Formula::BinaryExpression((BinaryOp::OR), (Box::new(x)), (Box::new(y))) }
                --
                x:(@) _ "XOR" _ y:@ { Formula::BinaryExpression((BinaryOp::XOR), (Box::new(x)), (Box::new(y))) }
                --
                x:(@) _ "AND" _ y:@ { Formula::BinaryExpression((BinaryOp::AND), (Box::new(x)), (Box::new(y))) }
                --
                "NOT" _ x:@ { Formula::UnaryExpression((UnaryOp::NOT), (Box::new(x))) }
                --
                t:term() {
                    Formula::Proposition(Proposition { tag: TagManager::retrieve_tag(t) })
                }
                --
                "(" _ e:expression() _ ")" { e }
            }

        rule term() -> &'input str
            = "\"" t:$([^ '"']+) "\"" { t }

        rule _() = quiet!{[' ' | '\t' | '\n']*} // Ignore spaces, tabs, and newlines
    }
}

#[derive(Debug, Clone)]
enum Formula {
    Proposition(Proposition),
    BinaryExpression(BinaryOp, Box<Formula>, Box<Formula>),
    UnaryExpression(UnaryOp, Box<Formula>),
}

#[derive(Debug, Clone)]
enum BinaryOp {
    AND,
    OR,
    XOR,
}
#[derive(Debug, Clone)]
enum UnaryOp {
    NOT,
}

#[derive(Debug, Eq, PartialEq, Hash, Clone)]
struct Proposition {
    tag: Option<TagRef>,
}

#[derive(Debug, Clone)]
struct PeerID {
    id: String, //[!] Should be a UUID 
}

#[derive(Debug, Clone)]
pub struct OrderedFileID<FileOrder> {
    file_id: FileID,
    order: FileOrder,
}

impl PartialEq for OrderedFileID<Name> {
    fn eq(&self, other: &Self) -> bool {
        self.file_id.path.file_name() == other.file_id.path.file_name() //[!] path.file_name() is an Option (can, theoretically, be None)
    }
}

impl Eq for OrderedFileID<Name> {}

impl PartialOrd for OrderedFileID<Name> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(
            self.file_id
                .path
                .file_name()
                .unwrap()
                .cmp(other.file_id.path.file_name().unwrap()),
        )
    }
}

impl Ord for OrderedFileID<Name> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.file_id
            .path
            .file_name()
            .unwrap()
            .cmp(other.file_id.path.file_name().unwrap())
    }
}

impl PartialEq for OrderedFileID<Size> {
    fn eq(&self, other: &Self) -> bool {
        self.file_id.metadata.size == other.file_id.metadata.size
    }
}

impl Eq for OrderedFileID<Size> {}

impl PartialOrd for OrderedFileID<Size> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.file_id.metadata.size.cmp(&other.file_id.metadata.size))
    }
}

impl Ord for OrderedFileID<Size> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.file_id.metadata.size.cmp(&other.file_id.metadata.size)
    }
}

impl PartialEq for OrderedFileID<Modified> {
    fn eq(&self, other: &Self) -> bool {
        self.file_id.metadata.modified == other.file_id.metadata.modified
    }
}

impl Eq for OrderedFileID<Modified> {}

impl PartialOrd for OrderedFileID<Modified> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(
            self.file_id
                .metadata
                .modified
                .cmp(&other.file_id.metadata.modified),
        )
    }
}

impl Ord for OrderedFileID<Modified> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.file_id
            .metadata
            .modified
            .cmp(&other.file_id.metadata.modified)
    }
}

impl PartialEq for OrderedFileID<Created> {
    fn eq(&self, other: &Self) -> bool {
        self.file_id.metadata.created == other.file_id.metadata.created
    }
}

impl Eq for OrderedFileID<Created> {}

impl PartialOrd for OrderedFileID<Created> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(
            self.file_id
                .metadata
                .created
                .cmp(&other.file_id.metadata.created),
        )
    }
}

impl Ord for OrderedFileID<Created> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.file_id
            .metadata
            .created
            .cmp(&other.file_id.metadata.created)
    }
}

impl PartialEq for OrderedFileID<Accessed> {
    fn eq(&self, other: &Self) -> bool {
        self.file_id.metadata.accessed == other.file_id.metadata.accessed
    }
}

impl Eq for OrderedFileID<Accessed> {}

impl PartialOrd for OrderedFileID<Accessed> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(
            self.file_id
                .metadata
                .accessed
                .cmp(&other.file_id.metadata.accessed),
        )
    }
}

impl Ord for OrderedFileID<Accessed> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.file_id
            .metadata
            .accessed
            .cmp(&other.file_id.metadata.accessed)
    }
}

impl PartialEq for OrderedFileID<Unordered> {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for OrderedFileID<Unordered> {}

impl PartialOrd for OrderedFileID<Unordered> {
    fn partial_cmp(&self, _other: &Self) -> Option<std::cmp::Ordering> {
        Some(std::cmp::Ordering::Equal)
    }
}

impl Ord for OrderedFileID<Unordered> {
    fn cmp(&self, _other: &Self) -> std::cmp::Ordering {
        std::cmp::Ordering::Equal
    }
}

#[derive(Debug, Clone)]
struct FileID {
    root: Option<PeerID>, //[/] Whether the file is local or remote
    path: PathBuf,
    metadata: FileMetadata,
}

impl FileID {
    fn new(root: Option<PeerID>, path: PathBuf, metadata: FileMetadata) -> Self {
        FileID {
            root,
            path,
            metadata,
        }
    }
}

pub struct Query<T: FileOrder + Clone> {
    formula: Formula,
    order: T,
    result: Option<BTreeSet<OrderedFileID<T>>>
}

impl<T: FileOrder + Clone> Query<T> {
    pub fn new(query: &str, order: T) -> Result<Self, QueryErr> {
        let formula = tag_query::expression(query).or_else(|_err| Err(QueryErr::SyntaxError))?;
        Ok(Query {
            formula,
            order,
            result: None
        })
    }

    pub fn evaluate(&mut self) -> Result<BTreeSet<OrderedFileID<T>>, QueryErr>
    where
        OrderedFileID<T>: Ord,
    {
        self.simplify();
        Query::recursive_evaluate(self.formula.clone())
    }

    fn recursive_evaluate(formula: Formula) -> Result<BTreeSet<OrderedFileID<T>>, QueryErr>
    where
        OrderedFileID<T>: Ord,
    {
        match formula {
            Formula::BinaryExpression(BinaryOp::AND, x, y) => match (*x.clone(), *y.clone()) {
                (_, Formula::UnaryExpression(UnaryOp::NOT, b)) => {
                    let a = Query::recursive_evaluate(*x.clone())?;
                    let b = Query::recursive_evaluate(*b.clone())?;
                    let x: BTreeSet<OrderedFileID<T>> = a.difference(&b).cloned().collect();
                    Ok(x)
                }
                (Formula::UnaryExpression(UnaryOp::NOT, a), _) => {
                    let a = Query::recursive_evaluate(*a.clone())?;
                    let b = Query::recursive_evaluate(*y.clone())?;
                    let x: BTreeSet<OrderedFileID<T>> = b.difference(&a).cloned().collect();
                    Ok(x)
                }
                (a, b) => {
                    let a = Query::recursive_evaluate(a.clone())?;
                    let b = Query::recursive_evaluate(b.clone())?;
                    let x: BTreeSet<OrderedFileID<T>> = a.intersection(&b).cloned().collect();
                    Ok(x)
                }
            },
            Formula::BinaryExpression(BinaryOp::OR, x, y) => {
                let a = Query::recursive_evaluate(*x.clone())?;
                let b = Query::recursive_evaluate(*y.clone())?;
                let x: BTreeSet<OrderedFileID<T>> = a.union(&b).cloned().collect();
                Ok(x)
            }
            Formula::BinaryExpression(BinaryOp::XOR, x, y) => {
                let a = Query::recursive_evaluate(*x.clone())?;
                let b = Query::recursive_evaluate(*y.clone())?;
                let x: BTreeSet<OrderedFileID<T>> = a.symmetric_difference(&b).cloned().collect();
                Ok(x)
            }
            Formula::UnaryExpression(UnaryOp::NOT, x) => {
                let a = MockCacheService::get_all()?;
                let b = Query::recursive_evaluate(*x.clone())?;
                let x: BTreeSet<OrderedFileID<T>> = a.difference(&b).cloned().collect();
                Ok(x)
            }
            Formula::Proposition(p) => {
                if let Some(tag) = p.tag {
                    MockCacheService::get_files(tag.clone()).map_err(|_| QueryErr::KeyError)
                } else {
                    return Err(QueryErr::KeyError);
                }
            }
        }
    }

    fn simplify(&mut self) -> () {
        loop {
            let simplified_formula = Formula::recursive_simplify(self.formula.clone());
            self.formula = simplified_formula.0;
            if simplified_formula.1 {
                break;
            }
        }
    }
}

impl Formula {
    fn recursive_simplify(formula: Formula) -> (Formula, bool) {
        //[/] Further simplification is possible but NP-Hard 
        match formula {
            Formula::Proposition(_) => {
                return (formula, false);
            }
            Formula::BinaryExpression(BinaryOp::AND, x, y) => match *x.clone() {
                // De Morgan's Law (AND)
                Formula::UnaryExpression(UnaryOp::NOT, a) => match *y {
                    Formula::UnaryExpression(UnaryOp::NOT, b) => (
                        Formula::UnaryExpression(
                            UnaryOp::NOT,
                            Box::new(Formula::BinaryExpression(
                                BinaryOp::OR,
                                Box::new(Formula::recursive_simplify(*a).0),
                                Box::new(Formula::recursive_simplify(*b).0),
                            )),
                        ),
                        true,
                    ),
                    _ => {
                        let simplified_x = Formula::recursive_simplify(*x.clone());
                        let simplified_y = Formula::recursive_simplify(*y.clone());
                        (
                            Formula::BinaryExpression(
                                BinaryOp::AND,
                                Box::new(simplified_x.0),
                                Box::new(simplified_y.0),
                            ),
                            simplified_x.1 || simplified_y.1,
                        )
                    }
                },
                _ => {
                    let simplified_x = Formula::recursive_simplify(*x.clone());
                    let simplified_y = Formula::recursive_simplify(*y.clone());
                    (
                        Formula::BinaryExpression(
                            BinaryOp::AND,
                            Box::new(simplified_x.0),
                            Box::new(simplified_y.0),
                        ),
                        simplified_x.1 || simplified_y.1,
                    )
                }
            },
            Formula::BinaryExpression(BinaryOp::OR, x, y) => match *x.clone() {
                // De Morgan's Law (OR)
                Formula::UnaryExpression(UnaryOp::NOT, a) => match *y {
                    Formula::UnaryExpression(UnaryOp::NOT, b) => (
                        Formula::UnaryExpression(
                            UnaryOp::NOT,
                            Box::new(Formula::BinaryExpression(
                                BinaryOp::AND,
                                Box::new(Formula::recursive_simplify(*a).0),
                                Box::new(Formula::recursive_simplify(*b).0),
                            )),
                        ),
                        true,
                    ),
                    _ => {
                        let simplified_x = Formula::recursive_simplify(*x.clone());
                        let simplified_y = Formula::recursive_simplify(*y.clone());
                        (
                            Formula::BinaryExpression(
                                BinaryOp::OR,
                                Box::new(simplified_x.0),
                                Box::new(simplified_y.0),
                            ),
                            simplified_x.1 || simplified_y.1,
                        )
                    }
                },
                _ => {
                    let simplified_x = Formula::recursive_simplify(*x.clone());
                    let simplified_y = Formula::recursive_simplify(*y.clone());
                    (
                        Formula::BinaryExpression(
                            BinaryOp::OR,
                            Box::new(simplified_x.0),
                            Box::new(simplified_y.0),
                        ),
                        simplified_x.1 || simplified_y.1,
                    )
                }
            },
            Formula::BinaryExpression(BinaryOp::XOR, x, y) => match *x.clone() {
                // (NOT A) XOR (NOT B) ⊨ A XOR B
                Formula::UnaryExpression(UnaryOp::NOT, a) => match *y {
                    Formula::UnaryExpression(UnaryOp::NOT, b) => (
                        Formula::BinaryExpression(
                            BinaryOp::XOR,
                            Box::new(Formula::recursive_simplify(*a).0),
                            Box::new(Formula::recursive_simplify(*b).0),
                        ),
                        true,
                    ),
                    _ => {
                        let simplified_x = Formula::recursive_simplify(*x.clone());
                        let simplified_y = Formula::recursive_simplify(*y.clone());
                        (
                            Formula::BinaryExpression(
                                BinaryOp::XOR,
                                Box::new(simplified_x.0),
                                Box::new(simplified_y.0),
                            ),
                            simplified_x.1 || simplified_y.1,
                        )
                    }
                },
                _ => {
                    let simplified_x = Formula::recursive_simplify(*x.clone());
                    let simplified_y = Formula::recursive_simplify(*y.clone());
                    (
                        Formula::BinaryExpression(
                            BinaryOp::XOR,
                            Box::new(simplified_x.0),
                            Box::new(simplified_y.0),
                        ),
                        simplified_x.1 || simplified_y.1,
                    )
                }
            },
            Formula::UnaryExpression(UnaryOp::NOT, x) => match *x {
                // NOT (NOT A) ⊨ A
                Formula::UnaryExpression(UnaryOp::NOT, y) => {
                    (Formula::recursive_simplify(*y).0, true)
                }
                _ => {
                    let simplified_x = Formula::recursive_simplify(*x.clone());
                    (
                        Formula::UnaryExpression(UnaryOp::NOT, Box::new(simplified_x.0)),
                        simplified_x.1,
                    )
                }
            },
        }
    }
}

// TODO: define appropriate errors, include I/O, etc.
pub enum QueryErr {
    SyntaxError, // The Query is incorrectly formatted
    KeyError,    // The Query uses tags which do not exist
}

//[!] Placeholder for CacheService 

pub struct MockCacheService {}

impl MockCacheService {
    pub fn new() -> Self {
        MockCacheService {}
    }

    pub fn get_files<T: FileOrder>(tag: TagRef) -> Result<BTreeSet<OrderedFileID<T>>, QueryErr> {
        todo!();
    }

    pub fn get_all<T: FileOrder>() -> Result<BTreeSet<OrderedFileID<T>>, QueryErr> {
        todo!();
    }
}