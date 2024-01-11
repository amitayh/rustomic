pub mod assignment;
pub mod clause;
pub mod db;
pub mod pattern;

use crate::datom::Value;
use crate::query::clause::*;
use crate::storage::attribute_resolver::ResolveError;
use std::collections::HashMap;
use std::rc::Rc;
use thiserror::Error;

type PartialAssignment = HashMap<Rc<str>, Value>;
type Predicate = Box<dyn Fn(&PartialAssignment) -> bool>;

pub trait Aggregator {}

struct Count;

impl Aggregator for Count {}

pub enum Find {
    Variable(Rc<str>),
    Aggregate(Box<dyn Aggregator>),
}

impl Find {
    pub fn variable(name: &str) -> Self {
        Self::Variable(Rc::from(name))
    }

    pub fn count() -> Self {
        Self::Aggregate(Box::new(Count))
    }
}

#[derive(Default)]
pub struct Query {
    pub find: Vec<Find>,
    pub wher: Vec<DataPattern>,
    pub predicates: Vec<Predicate>,
}

impl Query {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn find(mut self, find: Find) -> Self {
        self.find.push(find);
        self
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
    #[error("resolve error")]
    ResolveError(#[from] ResolveError<S>),
}
