use iroh::NodeId;

use crate::shelf::file::FileMetadata;
use crate::tag::{TagErr, TagId, TagManager};
use crate::workspace::WorkspaceId;
use std::collections::{BTreeSet, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

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
        pub rule expression() -> Result<Formula, QueryErr>
            = precedence! {
                x:(@) _ "OR" _ y:@ { Ok(Formula::BinaryExpression((BinaryOp::OR), (Box::new(x?)), (Box::new(y?)))) }
                --
                x:(@) _ "XOR" _ y:@ { Ok(Formula::BinaryExpression((BinaryOp::XOR), (Box::new(x?)), (Box::new(y?)))) }
                --
                x:(@) _ "AND" _ y:@ { Ok(Formula::BinaryExpression((BinaryOp::AND), (Box::new(x?)), (Box::new(y?)))) }
                --
                "NOT" _ x:@ { Ok(Formula::UnaryExpression((UnaryOp::NOT), (Box::new(x?)))) }
                --
                t:term() {
                    Ok(Formula::Proposition(Proposition { tag_id: t.parse::<TagId>().map_err(|_| QueryErr::ParseError)? }))
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

impl Formula {
    fn get_tags(&self) -> HashSet<TagId> {
        match self {
            Formula::BinaryExpression(_, x, y) => x
                .get_tags()
                .union(&y.get_tags())
                .cloned()
                .collect::<HashSet<TagId>>(),
            Formula::UnaryExpression(_, x) => x.get_tags(),
            Formula::Proposition(p) => HashSet::from([p.tag_id.clone()]),
        }
    }
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
    tag_id: TagId,
}

#[derive(Debug, Clone)]
pub struct OrderedFileSummary<FileOrder> {
    file_summary: FileSummary,
    order: FileOrder,
}

impl PartialEq for OrderedFileSummary<Name> {
    fn eq(&self, other: &Self) -> bool {
        self.file_summary.path.file_name() == other.file_summary.path.file_name() // path.file_name() is an Option (can, theoretically, be None)
    }
}

impl Eq for OrderedFileSummary<Name> {}

impl PartialOrd for OrderedFileSummary<Name> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(
            self.file_summary
                .path
                .file_name()
                .unwrap()
                .cmp(other.file_summary.path.file_name().unwrap()),
        )
    }
}

impl Ord for OrderedFileSummary<Name> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.file_summary
            .path
            .file_name()
            .unwrap()
            .cmp(other.file_summary.path.file_name().unwrap())
    }
}

impl PartialEq for OrderedFileSummary<Size> {
    fn eq(&self, other: &Self) -> bool {
        self.file_summary.metadata.size == other.file_summary.metadata.size
    }
}

impl Eq for OrderedFileSummary<Size> {}

impl PartialOrd for OrderedFileSummary<Size> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.file_summary.metadata.size.cmp(&other.file_summary.metadata.size))
    }
}

impl Ord for OrderedFileSummary<Size> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.file_summary.metadata.size.cmp(&other.file_summary.metadata.size)
    }
}

impl PartialEq for OrderedFileSummary<Modified> {
    fn eq(&self, other: &Self) -> bool {
        self.file_summary.metadata.modified == other.file_summary.metadata.modified
    }
}

impl Eq for OrderedFileSummary<Modified> {}

impl PartialOrd for OrderedFileSummary<Modified> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(
            self.file_summary
                .metadata
                .modified
                .cmp(&other.file_summary.metadata.modified),
        )
    }
}

impl Ord for OrderedFileSummary<Modified> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.file_summary
            .metadata
            .modified
            .cmp(&other.file_summary.metadata.modified)
    }
}

impl PartialEq for OrderedFileSummary<Created> {
    fn eq(&self, other: &Self) -> bool {
        self.file_summary.metadata.created == other.file_summary.metadata.created
    }
}

impl Eq for OrderedFileSummary<Created> {}

impl PartialOrd for OrderedFileSummary<Created> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(
            self.file_summary
                .metadata
                .created
                .cmp(&other.file_summary.metadata.created),
        )
    }
}

impl Ord for OrderedFileSummary<Created> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.file_summary
            .metadata
            .created
            .cmp(&other.file_summary.metadata.created)
    }
}

impl PartialEq for OrderedFileSummary<Accessed> {
    fn eq(&self, other: &Self) -> bool {
        self.file_summary.metadata.accessed == other.file_summary.metadata.accessed
    }
}

impl Eq for OrderedFileSummary<Accessed> {}

impl PartialOrd for OrderedFileSummary<Accessed> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(
            self.file_summary
                .metadata
                .accessed
                .cmp(&other.file_summary.metadata.accessed),
        )
    }
}

impl Ord for OrderedFileSummary<Accessed> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.file_summary
            .metadata
            .accessed
            .cmp(&other.file_summary.metadata.accessed)
    }
}

impl PartialEq for OrderedFileSummary<Unordered> {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for OrderedFileSummary<Unordered> {}

impl PartialOrd for OrderedFileSummary<Unordered> {
    fn partial_cmp(&self, _other: &Self) -> Option<std::cmp::Ordering> {
        Some(std::cmp::Ordering::Equal)
    }
}

impl Ord for OrderedFileSummary<Unordered> {
    fn cmp(&self, _other: &Self) -> std::cmp::Ordering {
        std::cmp::Ordering::Equal
    }
}

#[derive(Debug, Clone)]
struct FileSummary {
    root: Option<NodeId>, // Whether the file is local or remote
    path: PathBuf,
    metadata: FileMetadata,
    //[?] Icon/Preview ?? 
}

impl FileSummary {
    fn new(root: Option<NodeId>, path: PathBuf, metadata: FileMetadata) -> Self {
        FileSummary {
            root,
            path,
            metadata,
        }
    }
}

pub struct Query<T: FileOrder + Clone> {
    formula: Formula,
    order: T,
    result: Option<BTreeSet<OrderedFileSummary<T>>>,
}

impl<T: FileOrder + Clone> Query<T> {
    pub fn new(query: &str, order: T) -> Result<Self, QueryErr> {
        let formula = tag_query::expression(query).or_else(|_err| Err(QueryErr::SyntaxError))??;
        let mut query = Query {
            formula,
            order,
            result: None,
        };
        query.simplify();
        Ok(query)
    }

    pub fn get_tags(&self) -> HashSet<TagId> {
        self.formula.get_tags()
    }

    //[!] Should be executed inside the QueryService 
    pub fn validate<R>(
        &mut self,
        workspace_id: WorkspaceId,
        tag_manager: Arc<RwLock<TagManager>>,
    ) -> Result<(), TagErr> {
        let tag_set = self.formula.get_tags();
        tag_manager.read().unwrap().validate(tag_set, workspace_id)
    }

    pub async fn evaluate<R>(
        &mut self,
        workspace_id: WorkspaceId,
        ret_service: R,
    ) -> Result<BTreeSet<OrderedFileSummary<T>>, QueryErr>
    where
        OrderedFileSummary<T>: Ord,
        R: RetrieveService + Clone,
    {
        Query::recursive_evaluate(self.formula.clone(), workspace_id, ret_service.clone()).await
    }

    async fn recursive_evaluate<R>(
        formula: Formula,
        workspace_id: WorkspaceId,
        ret_service: R,
    ) -> Result<BTreeSet<OrderedFileSummary<T>>, QueryErr>
    where
        OrderedFileSummary<T>: Ord,
        R: RetrieveService + Clone,
    {
        match formula {
            Formula::BinaryExpression(BinaryOp::AND, x, y) => match (*x.clone(), *y.clone()) {
                (_, Formula::UnaryExpression(UnaryOp::NOT, b)) => {
                    let a =
                        Query::recursive_evaluate(*x.clone(), workspace_id, ret_service.clone())
                            .await?;
                    let b =
                        Query::recursive_evaluate(*b.clone(), workspace_id, ret_service.clone())
                            .await?;
                    let x: BTreeSet<OrderedFileSummary<T>> = a.difference(&b).cloned().collect();
                    Ok(x)
                }
                (Formula::UnaryExpression(UnaryOp::NOT, a), _) => {
                    let a =
                        Query::recursive_evaluate(*a.clone(), workspace_id, ret_service.clone())
                            .await?;
                    let b =
                        Query::recursive_evaluate(*y.clone(), workspace_id, ret_service.clone())
                            .await?;
                    let x: BTreeSet<OrderedFileSummary<T>> = b.difference(&a).cloned().collect();
                    Ok(x)
                }
                (a, b) => {
                    let a = Query::recursive_evaluate(a.clone(), workspace_id, ret_service.clone())
                        .await?;
                    let b = Query::recursive_evaluate(b.clone(), workspace_id, ret_service.clone())
                        .await?;
                    let x: BTreeSet<OrderedFileSummary<T>> = a.intersection(&b).cloned().collect();
                    Ok(x)
                }
            },
            Formula::BinaryExpression(BinaryOp::OR, x, y) => {
                let a = Query::recursive_evaluate(*x.clone(), workspace_id, ret_service.clone())
                    .await?;
                let b = Query::recursive_evaluate(*y.clone(), workspace_id, ret_service.clone())
                    .await?;
                let x: BTreeSet<OrderedFileSummary<T>> = a.union(&b).cloned().collect();
                Ok(x)
            }
            Formula::BinaryExpression(BinaryOp::XOR, x, y) => {
                let a = Query::recursive_evaluate(*x.clone(), workspace_id, ret_service.clone())
                    .await?;
                let b = Query::recursive_evaluate(*y.clone(), workspace_id, ret_service.clone())
                    .await?;
                let x: BTreeSet<OrderedFileSummary<T>> = a.symmetric_difference(&b).cloned().collect();
                Ok(x)
            }
            Formula::UnaryExpression(UnaryOp::NOT, x) => {
                let a = ret_service
                    .get_all()
                    .await
                    .map_err(|err| QueryErr::RuntimeError(err))?;
                let b = Query::recursive_evaluate(*x.clone(), workspace_id, ret_service).await?;
                let x: BTreeSet<OrderedFileSummary<T>> = a.difference(&b).cloned().collect();
                Ok(x)
            }
            Formula::Proposition(p) => ret_service
                .get_files(p.tag_id, workspace_id)
                .await
                .map_err(|err| QueryErr::RuntimeError(err)),
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
        // Further simplification is possible but NP-Hard
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
    SyntaxError,               // The Query is incorrectly formatted
    ParseError,                // A Tag_ID is not a valid UUID
    KeyError(TagErr),          // The Query uses tags which do not exist
    RuntimeError(RetrieveErr), // The Query could not be executed
}

//[!] Wrapper for a cacheservice.call() ?

pub enum RetrieveErr {
    CacheError,
}

pub trait RetrieveService {
    async fn get_files<T: FileOrder>(
        &self,
        _tag_id: TagId,
        _workspace_id: WorkspaceId,
    ) -> Result<BTreeSet<OrderedFileSummary<T>>, RetrieveErr> {
        todo!();
    }

    async fn get_all<T: FileOrder>(&self) -> Result<BTreeSet<OrderedFileSummary<T>>, RetrieveErr> {
        todo!();
    }
}
