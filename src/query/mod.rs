pub mod aggregation;
pub mod aggregator;
pub mod assignment;
pub mod clause;
pub mod database;
pub mod pattern;
pub mod projector;
pub mod resolver;

use crate::datom::Value;
use crate::query::aggregation::*;
use crate::query::clause::*;
use crate::storage::attribute_resolver::ResolveError;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use thiserror::Error;

/// An assignment is a mapping between variables and values such that the clauses are satisfied.
pub type Assignment = HashMap<String, Value>;
pub type Result<T, E> = std::result::Result<T, QueryError<E>>;
pub type AssignmentResult<E> = Result<Assignment, E>;
pub type QueryResult<E> = Result<Vec<Value>, E>;

#[derive(Clone)]
pub struct Predicate(Arc<dyn Fn(&Assignment) -> bool + Send + Sync>);

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

#[derive(Default, Clone, Debug)]
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

    pub fn pred(mut self, predicate: impl Fn(&Assignment) -> bool + Send + Sync + 'static) -> Self {
        self.predicates.push(Predicate(Arc::new(predicate)));
        self
    }

    pub fn value_pred(
        self,
        variable: &'static str,
        predicate: impl Fn(&Value) -> bool + Send + Sync + 'static,
    ) -> Self {
        self.pred(move |assignment| {
            let value = assignment.get(variable);
            value.is_none_or(&predicate)
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum Find {
    Variable(String),
    Aggregate(AggregationFunction),
}

impl Find {
    pub fn variable(name: &str) -> Self {
        Self::Variable(name.to_string())
    }

    pub fn count() -> Self {
        Self::Aggregate(AggregationFunction::Count)
    }

    pub fn min(variable: &str) -> Self {
        Self::Aggregate(AggregationFunction::Min(variable.to_string()))
    }

    pub fn max(variable: &str) -> Self {
        Self::Aggregate(AggregationFunction::Max(variable.to_string()))
    }

    pub fn average(variable: &str) -> Self {
        Self::Aggregate(AggregationFunction::Average(variable.to_string()))
    }

    pub fn sum(variable: &str) -> Self {
        Self::Aggregate(AggregationFunction::Sum(variable.to_string()))
    }

    pub fn count_distinct(variable: &str) -> Self {
        Self::Aggregate(AggregationFunction::CountDistinct(variable.to_string()))
    }
}

#[derive(Debug, Error)]
pub enum QueryError<S> {
    #[error("storage error")]
    StorageError(#[from] S),
    #[error("resolve error")]
    ResolveError(#[from] ResolveError<S>),
    #[error("invalid variable {0} for find clause")]
    InvalidFindVariable(String),
}
