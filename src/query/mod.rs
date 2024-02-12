pub mod aggregation;
pub mod assignment;
pub mod clause;
pub mod database;
pub mod pattern;
pub mod resolver;

use crate::datom::Value;
use crate::query::aggregation::*;
use crate::query::clause::*;
use crate::storage::attribute_resolver::ResolveError;
use std::collections::HashMap;
use std::rc::Rc;
use std::u64;
use thiserror::Error;

pub type Assignment = HashMap<Rc<str>, Value>;
pub type Predicate = Rc<dyn Fn(&Assignment) -> bool>;
pub type Result<T, E> = std::result::Result<T, QueryError<E>>;
pub type AssignmentResult<E> = Result<Assignment, E>;
pub type QueryResult<E> = Result<Vec<Value>, E>;

#[derive(Default, Clone)]
pub struct Query {
    pub find: Vec<Find>,
    pub clauses: Vec<Clause>,
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

    pub fn with(mut self, clause: Clause) -> Self {
        self.clauses.push(clause);
        self
    }

    pub fn pred(mut self, predicate: impl Fn(&Assignment) -> bool + 'static) -> Self {
        self.predicates.push(Rc::new(predicate));
        self
    }

    pub fn value_pred(
        self,
        variable: &'static str,
        predicate: impl Fn(&Value) -> bool + 'static,
    ) -> Self {
        self.pred(move |assignment| {
            let value = assignment.get(variable);
            value.map_or(true, &predicate)
        })
    }
}

#[derive(Clone)]
pub enum Find {
    Variable(Rc<str>),
    Aggregate(AggregationFunction),
}

impl Find {
    pub fn variable(name: &str) -> Self {
        Self::Variable(Rc::from(name))
    }

    pub fn count() -> Self {
        Self::Aggregate(AggregationFunction::Count)
    }

    pub fn min(variable: &str) -> Self {
        Self::Aggregate(AggregationFunction::Min(Rc::from(variable)))
    }

    pub fn max(variable: &str) -> Self {
        Self::Aggregate(AggregationFunction::Max(Rc::from(variable)))
    }

    pub fn average(variable: &str) -> Self {
        Self::Aggregate(AggregationFunction::Average(Rc::from(variable)))
    }

    pub fn sum(variable: &str) -> Self {
        Self::Aggregate(AggregationFunction::Sum(Rc::from(variable)))
    }

    pub fn count_distinct(variable: &str) -> Self {
        Self::Aggregate(AggregationFunction::CountDistinct(Rc::from(variable)))
    }
}

#[derive(Debug, Error)]
pub enum QueryError<S> {
    #[error("error")]
    Error,
    #[error("storage error")]
    StorageError(#[from] S),
    #[error("resolve error")]
    ResolveError(#[from] ResolveError<S>),
    #[error("invalid variable {0} for find clause")]
    InvalidFindVariable(Rc<str>),
}
