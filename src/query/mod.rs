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
use std::fmt::Debug;
use std::sync::Arc;
use std::u64;
use thiserror::Error;

pub type Assignment = HashMap<Arc<str>, Value>;
pub type Result<T, E> = std::result::Result<T, QueryError<E>>;
pub type AssignmentResult<E> = Result<Assignment, E>;
pub type QueryResult<E> = Result<Vec<Value>, E>;

#[derive(Clone)]
pub struct Predicate(Arc<dyn Fn(&Assignment) -> bool>);

impl Predicate {
    fn test(&self, assignment: &Assignment) -> bool {
        self.0(assignment)
    }
}

impl Debug for Predicate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("<predicate>")
    }
}

impl PartialEq for Predicate {
    fn eq(&self, _: &Self) -> bool {
        false // TODO
    }
}

#[derive(Default, Clone, Debug, PartialEq)]
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

    pub fn r#where(mut self, clause: Clause) -> Self {
        self.clauses.push(clause);
        self
    }

    pub fn pred(mut self, predicate: impl Fn(&Assignment) -> bool + 'static) -> Self {
        self.predicates.push(Predicate(Arc::new(predicate)));
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

#[derive(Clone, Debug, PartialEq)]
pub enum Find {
    Variable(Arc<str>),
    Aggregate(AggregationFunction),
}

impl Find {
    pub fn variable(name: &str) -> Self {
        Self::Variable(Arc::from(name))
    }

    pub fn count() -> Self {
        Self::Aggregate(AggregationFunction::Count)
    }

    pub fn min(variable: &str) -> Self {
        Self::Aggregate(AggregationFunction::Min(Arc::from(variable)))
    }

    pub fn max(variable: &str) -> Self {
        Self::Aggregate(AggregationFunction::Max(Arc::from(variable)))
    }

    pub fn average(variable: &str) -> Self {
        Self::Aggregate(AggregationFunction::Average(Arc::from(variable)))
    }

    pub fn sum(variable: &str) -> Self {
        Self::Aggregate(AggregationFunction::Sum(Arc::from(variable)))
    }

    pub fn count_distinct(variable: &str) -> Self {
        Self::Aggregate(AggregationFunction::CountDistinct(Arc::from(variable)))
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
    InvalidFindVariable(Arc<str>),
}
