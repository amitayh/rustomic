pub mod assignment;
pub mod clause;
pub mod db;
pub mod pattern;
pub mod resolver;

use crate::datom::Value;
use crate::query::clause::*;
use crate::storage::attribute_resolver::ResolveError;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::u64;
use thiserror::Error;

pub type Assignment = HashMap<Rc<str>, Value>;
pub type Predicate = Rc<dyn Fn(&Assignment) -> bool>;
pub type Result<T, E> = std::result::Result<T, QueryError<E>>;
pub type AssignmentResult<E> = Result<Assignment, E>;
pub type QueryResult<E> = Result<Vec<Value>, E>;

enum AggregationState {
    Count(u64),
    Sum {
        variable: Rc<str>,
        sum: i64,
    },
    CountDistinct {
        variable: Rc<str>,
        seen: HashSet<Value>,
    },
}

impl AggregationState {
    fn count() -> Self {
        Self::Count(0)
    }

    fn sum(variable: Rc<str>) -> Self {
        Self::Sum { variable, sum: 0 }
    }

    fn count_distinct(variable: Rc<str>) -> Self {
        Self::CountDistinct {
            variable,
            seen: HashSet::new(),
        }
    }

    fn consume(&mut self, assignment: &Assignment) {
        match self {
            Self::Count(count) => *count += 1,
            Self::Sum { variable, sum } => {
                if let Some(Value::I64(value)) = assignment.get(variable) {
                    *sum += value;
                }
            }
            Self::CountDistinct { variable, seen } => {
                if let Some(value) = assignment.get(variable) {
                    if !seen.contains(value) {
                        seen.insert(value.clone());
                    }
                }
            }
        }
    }

    fn result(self) -> Value {
        match self {
            Self::Count(count) => Value::U64(count),
            Self::Sum { sum, .. } => Value::I64(sum),
            Self::CountDistinct { seen, .. } => Value::U64(seen.len() as u64),
        }
    }
}

#[derive(Clone)]
pub enum AggregationFunction {
    Count,
    Sum(Rc<str>),
    CountDistinct(Rc<str>),
}

impl AggregationFunction {
    fn empty_state(&self) -> AggregationState {
        match self {
            AggregationFunction::Count => AggregationState::count(),
            AggregationFunction::Sum(variable) => AggregationState::sum(Rc::clone(variable)),
            AggregationFunction::CountDistinct(variable) => {
                AggregationState::count_distinct(Rc::clone(variable))
            }
        }
    }
}

// ------------------------------------------------------------------------------------------------

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

    pub fn sum(variable: &str) -> Self {
        Self::Aggregate(AggregationFunction::Sum(Rc::from(variable)))
    }

    pub fn count_distinct(variable: &str) -> Self {
        Self::Aggregate(AggregationFunction::CountDistinct(Rc::from(variable)))
    }
}

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

    pub fn pred<P: Fn(&Assignment) -> bool + 'static>(mut self, predicate: P) -> Self {
        self.predicates.push(Rc::new(predicate));
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
