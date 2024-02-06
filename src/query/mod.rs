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

type PartialAssignment = HashMap<Rc<str>, Value>;
type Predicate = Rc<dyn Fn(&PartialAssignment) -> bool>;

trait ToAggregator {
    fn to_aggregator(&self) -> Box<dyn Aggregator>;
}

pub trait Aggregator {
    fn init(&self) -> Value;
    fn consume(&mut self, key: &[Value], acc: &mut Value, assignment: &PartialAssignment);
}

// ------------------------------------------------------------------------------------------------

pub trait NewAggregator {
    fn consume(&mut self, assignment: &PartialAssignment);
    fn result(&self) -> Value;
}

struct NewCountAggregator(u64);

impl NewAggregator for NewCountAggregator {
    fn consume(&mut self, _: &PartialAssignment) {
        self.0 += 1;
    }

    fn result(&self) -> Value {
        Value::U64(self.0)
    }
}


struct NewSumAggregator {
    variable: Rc<str>,
    sum: i64,
}

impl NewAggregator for NewSumAggregator {
    fn consume(&mut self, assignment: &PartialAssignment) {
        // TODO support U64
        if let Some(Value::I64(value)) = assignment.get(&self.variable) {
            self.sum += value;
        }
    }

    fn result(&self) -> Value {
        Value::I64(self.sum)
    }
}

struct NewCountDistinct {
    variable: Rc<str>,
    seen: HashSet<Value>
}

impl NewAggregator for NewCountDistinct {
    fn consume(&mut self, assignment: &PartialAssignment) {
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

struct Count;

impl ToAggregator for Count {
    fn to_aggregator(&self) -> Box<dyn Aggregator> {
        Box::new(CountAggregator)
    }
}

struct CountAggregator;

impl Aggregator for CountAggregator {
    fn init(&self) -> Value {
        Value::U64(0)
    }

    fn consume(&mut self, _: &[Value], acc: &mut Value, _: &PartialAssignment) {
        if let Value::U64(count) = acc {
            *count += 1;
        }
    }
}

// TODO remove
impl Aggregator for Count {
    fn init(&self) -> Value {
        Value::U64(0)
    }

    fn consume(&mut self, _: &[Value], acc: &mut Value, _: &PartialAssignment) {
        if let Value::U64(count) = acc {
            *count += 1;
        }
    }
}

struct Sum(Rc<str>);

impl Aggregator for Sum {
    fn init(&self) -> Value {
        Value::I64(0)
    }

    fn consume(&mut self, _: &[Value], acc: &mut Value, assignment: &PartialAssignment) {
        if let Value::I64(sum) = acc {
            if let Some(Value::I64(value)) = assignment.get(&self.0) {
                *sum += value;
            }
        }
    }
}

struct CountDistinct(Rc<str>);

impl ToAggregator for CountDistinct {
    fn to_aggregator(&self) -> Box<dyn Aggregator> {
        Box::new(CountDistinctAggregator {
            variable: Rc::clone(&self.0),
            seen: HashMap::new(),
        })
    }
}

#[derive(Debug)]
struct CountDistinctAggregator {
    variable: Rc<str>,
    seen: HashMap<Vec<Value>, HashSet<Value>>,
}

impl Aggregator for CountDistinctAggregator {
    fn init(&self) -> Value {
        Value::U64(0)
    }

    fn consume(&mut self, key: &[Value], acc: &mut Value, assignment: &PartialAssignment) {
        if let Value::U64(count) = acc {
            if let Some(value) = assignment.get(&self.variable) {
                // TODO avoid key clone
                let seen_for_key = self.seen.entry(key.to_vec()).or_default();
                if seen_for_key.insert(value.clone()) {
                    *count += 1;
                }
            }
        }
    }
}

//#[derive(Clone)]
pub enum Find {
    Variable(Rc<str>),
    Aggregate(Box<dyn ToAggregator>),
}

impl Find {
    pub fn variable(name: &str) -> Self {
        Self::Variable(Rc::from(name))
    }

    pub fn count() -> Self {
        Self::Aggregate(Box::new(Count))
    }

    pub fn sum(variable: &str) -> Self {
        //Self::Aggregate(Box::new(Sum(Rc::from(variable))))
        Self::Aggregate(Box::new(Count))
    }

    pub fn count_distinct(variable: &str) -> Self {
        Self::Aggregate(Box::new(CountDistinct(Rc::from(variable))))
    }

    pub fn value(&self, assignment: &PartialAssignment) -> Option<Value> {
        match self {
            Find::Variable(variable) => assignment.get(variable).cloned(),
            _ => None,
        }
    }
}

#[derive(Default)]
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

    pub fn pred<P: Fn(&PartialAssignment) -> bool + 'static>(mut self, predicate: P) -> Self {
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

    pub fn find_variables(&self) -> impl Iterator<Item = &Rc<str>> {
        self.find.iter().filter_map(|find| match find {
            Find::Variable(variable) => Some(variable),
            _ => None,
        })
    }
}

#[derive(Debug)]
pub struct QueryResult {
    pub results: Vec<Vec<Value>>,
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
