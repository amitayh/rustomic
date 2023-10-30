pub mod assignment;
pub mod clause;
pub mod db;
pub mod pattern;

use crate::datom::Value;
use crate::query::clause::*;
use std::collections::HashMap;
use std::rc::Rc;
use thiserror::Error;

type Res = HashMap<Rc<str>, Value>;

pub struct VariablePredicate {
    variable: Rc<str>,
    predicate: Box<dyn Fn(&Value) -> bool>,
}

impl VariablePredicate {
    fn new<P: Fn(&Value) -> bool + 'static>(variable: &str, predicate: P) -> Self {
        Self {
            variable: Rc::from(variable),
            predicate: Box::new(predicate),
        }
    }

    fn test(&self, assignment: &Res) -> bool {
        let value = assignment.get(&self.variable);
        value.map_or(true, |v| (self.predicate)(v))
    }
}

#[derive(Default)]
pub struct Query {
    pub wher: Vec<Clause>,
    pub predicates: Vec<VariablePredicate>,
}

impl Query {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn wher(mut self, clause: Clause) -> Self {
        self.wher.push(clause);
        self
    }

    pub fn value_pred<P: Fn(&Value) -> bool + 'static>(
        mut self,
        variable: &str,
        predicate: P,
    ) -> Self {
        self.predicates
            .push(VariablePredicate::new(variable, predicate));
        self
    }

    pub fn test(&self, assignment: &Res) -> bool {
        self.predicates
            .iter()
            .all(|predicate| predicate.test(&assignment))
    }
}

#[derive(Debug)]
pub struct QueryResult {
    pub results: Vec<Res>,
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
