pub mod assignment;
pub mod clause;
pub mod db;
pub mod pattern;

use crate::datom::Value;
use crate::query::clause::*;
use crate::storage::StorageError;
use std::collections::HashMap;
use std::rc::Rc;
use thiserror::Error;

#[derive(Debug, Default)]
pub struct Query {
    pub wher: Vec<Clause>,
}

impl Query {
    pub fn new() -> Self {
        Query { wher: Vec::new() }
    }

    pub fn wher(mut self, clause: Clause) -> Self {
        self.wher.push(clause);
        self
    }
}

#[derive(Debug)]
pub struct QueryResult {
    pub results: Vec<HashMap<Rc<str>, Value>>,
}

#[derive(Debug, Error)]
pub enum QueryError {
    #[error("error")]
    Error,
    #[error("storage error")]
    StorageError(#[from] StorageError),
}
