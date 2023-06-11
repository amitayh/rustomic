pub mod assignment;
pub mod clause;
pub mod db;
pub mod pattern;

use std::collections::HashMap;

use crate::datom::Value;
use crate::query::clause::*;
use crate::storage::StorageError;

#[derive(Debug)]
pub struct Query<'a> {
    pub wher: Vec<Clause<'a>>,
}

impl<'a> Query<'a> {
    pub fn new() -> Self {
        Query { wher: Vec::new() }
    }

    pub fn wher(mut self, clause: Clause<'a>) -> Self {
        self.wher.push(clause);
        self
    }
}

#[derive(Debug)]
pub struct QueryResult {
    pub results: Vec<HashMap<String, Value>>,
}

#[derive(Debug)]
pub enum QueryError {
    Error,
    StorageError(StorageError),
}
