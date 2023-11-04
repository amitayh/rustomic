pub mod assignment;
pub mod clause;
pub mod db;
pub mod pattern;

use crate::datom::Value;
use crate::query::clause::*;
use std::collections::HashMap;
use std::rc::Rc;
use thiserror::Error;

type PartialAssignment = HashMap<Rc<str>, Value>;
type Predicate = Box<dyn Fn(&PartialAssignment) -> bool>;

#[derive(Default)]
pub struct Query {
    pub wher: Vec<DataPattern>,
    pub predicates: Vec<Predicate>,
}

impl Query {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn wher(mut self, clause: DataPattern) -> Self {
        self.wher.push(clause);
        self
    }

    pub fn pred<P: Fn(&PartialAssignment) -> bool + 'static>(mut self, predicate: P) -> Self {
        self.predicates.push(Box::new(predicate));
        self
    }

    pub fn value_pred<P: Fn(&Value) -> bool + 'static>(
        self,
        variable: &'static str,
        predicate: P,
    ) -> Self {
        self.pred(move |assignment| {
            let value = assignment.get(variable);
            value.map_or(true, &predicate)
        })
    }

    pub fn test(&self, assignment: &PartialAssignment) -> bool {
        self.predicates
            .iter()
            .all(|predicate| predicate(assignment))
    }
}

#[derive(Debug)]
pub struct QueryResult {
    pub results: Vec<PartialAssignment>,
}

#[derive(Debug, Error)]
pub enum QueryError<S> {
    #[error("error")]
    Error,
    #[error("storage error")]
    StorageError(#[from] S),
    #[error("ident `{0}` not found")]
    IdentNotFound(Rc<str>),
}
