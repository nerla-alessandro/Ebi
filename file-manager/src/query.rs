#[derive(Debug)]
pub struct Query {}

// TODO: define appropriate errors, include I/O, etc.
pub enum QueryErr {
    SyntaxError, // The Query is incorrectly formatted
    KeyError,    // The Query uses tags which do not exist in the Shelf
}
