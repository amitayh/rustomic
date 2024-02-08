pub mod assignment;
pub mod clause;
pub mod db;
pub mod pattern;
pub mod projector;
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

// ------------------------------------------------------------------------------------------------

pub trait ToAggregator {
    fn to_aggregator(&self) -> Box<dyn Aggregator>;
}

pub trait Aggregator {
    fn consume(&mut self, assignment: &Assignment);
    fn result(&self) -> Value;
}

// ------------------------------------------------------------------------------------------------

struct Count;

impl ToAggregator for Count {
    fn to_aggregator(&self) -> Box<dyn Aggregator> {
        Box::new(CountAggregator::new())
    }
}

struct CountAggregator(u64);

impl CountAggregator {
    fn new() -> Self {
        Self(0)
    }
}

impl Aggregator for CountAggregator {
    fn consume(&mut self, _: &Assignment) {
        self.0 += 1;
    }

    fn result(&self) -> Value {
        Value::U64(self.0)
    }
}

// ------------------------------------------------------------------------------------------------

struct Sum(Rc<str>);

impl ToAggregator for Sum {
    fn to_aggregator(&self) -> Box<dyn Aggregator> {
        Box::new(SumAggregator::new(Rc::clone(&self.0)))
    }
}

struct SumAggregator {
    variable: Rc<str>,
    sum: i64,
}

impl SumAggregator {
    fn new(variable: Rc<str>) -> Self {
        Self { variable, sum: 0 }
    }
}

impl Aggregator for SumAggregator {
    fn consume(&mut self, assignment: &Assignment) {
        // TODO support U64
        if let Some(Value::I64(value)) = assignment.get(&self.variable) {
            self.sum += value;
        }
    }

    fn result(&self) -> Value {
        Value::I64(self.sum)
    }
}

// ------------------------------------------------------------------------------------------------

struct CountDistinct(Rc<str>);

impl ToAggregator for CountDistinct {
    fn to_aggregator(&self) -> Box<dyn Aggregator> {
        Box::new(CountDistinctAggregator::new(Rc::clone(&self.0)))
    }
}

struct CountDistinctAggregator {
    variable: Rc<str>,
    seen: HashSet<Value>,
}

impl CountDistinctAggregator {
    fn new(variable: Rc<str>) -> Self {
        Self {
            variable,
            seen: HashSet::new(),
        }
    }
}

impl Aggregator for CountDistinctAggregator {
    fn consume(&mut self, assignment: &Assignment) {
        if let Some(value) = assignment.get(&self.variable) {
            if !self.seen.contains(value) {
                self.seen.insert(value.clone());
            }
        }
    }

    fn result(&self) -> Value {
        Value::U64(self.seen.len() as u64)
    }
}

// ------------------------------------------------------------------------------------------------

#[derive(Clone)]
pub enum Find {
    Variable(Rc<str>),
    Aggregate(Rc<dyn ToAggregator>),
}

impl Find {
    pub fn variable(name: &str) -> Self {
        Self::Variable(Rc::from(name))
    }

    pub fn count() -> Self {
        Self::Aggregate(Rc::new(Count))
    }

    pub fn sum(variable: &str) -> Self {
        Self::Aggregate(Rc::new(Sum(Rc::from(variable))))
    }

    pub fn count_distinct(variable: &str) -> Self {
        Self::Aggregate(Rc::new(CountDistinct(Rc::from(variable))))
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
