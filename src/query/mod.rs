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
type Predicate = Rc<dyn Fn(&PartialAssignment) -> bool>;

pub trait Aggregator {
    fn init(&self) -> Value;
    fn consume(&self, acc: &mut Value, assignment: &PartialAssignment);
}

struct Count;

impl Aggregator for Count {
    fn init(&self) -> Value {
        Value::U64(0)
    }

    fn consume(&self, acc: &mut Value, _: &PartialAssignment) {
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

    fn consume(&self, acc: &mut Value, assignment: &PartialAssignment) {
        if let Value::I64(sum) = acc {
            if let Some(Value::I64(value)) = assignment.get(&self.0) {
                *sum += value;
            }
        }
    }
}

struct Distinct(Rc<str>);

impl Aggregator for Distinct {
    fn init(&self) -> Value {
        Value::U64(0)
    }

    fn consume(&self, _: &mut Value, _: &PartialAssignment) {}
}

#[derive(Clone)]
pub enum Find {
    Variable(Rc<str>),
    Aggregate(Rc<dyn Aggregator>),
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

    pub fn distinct(variable: &str) -> Self {
        Self::Aggregate(Rc::new(Distinct(Rc::from(variable))))
    }

    pub fn value(&self, assignment: &PartialAssignment) -> Option<Value> {
        match self {
            Find::Variable(variable) => assignment.get(variable).cloned(),
            _ => None,
        }
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

    /*
    fn find_variables(&self) -> impl Iterator<Item = &Rc<str>> {
        self.find.iter().filter_map(|find| match find {
            Find::Variable(variable) => Some(variable),
            _ => None,
        })
    }
    fn find_aggregations(&self) -> impl Iterator<Item = &Rc<dyn Aggregator>> {
        self.find.iter().filter_map(|find| match find {
            Find::Aggregate(aggregator) => Some(aggregator),
            _ => None,
        })
    }

    fn is_aggregated(&self) -> bool {
        self.find
            .iter()
            .any(|find| matches!(find, Find::Aggregate(_)))
    }
    */
}

#[derive(Debug)]
pub struct TempQueryResult(HashMap<Vec<Value>, Vec<Value>>);

impl TempQueryResult {
    /*
    pub fn from<T: IntoIterator<Item = Vec<Value>>>(query: Query, iter: T) -> Self {
        if query.is_aggregated() {
            let mut agg = HashMap::new();
            for assignment in iter {
                let key = key_of(&query, &assignment);
                let entry = agg.entry(key).or_insert_with(|| init(&query));
                for (index, aggregator) in query.find_aggregations().enumerate() {
                    if let Some(value) = entry.get_mut(index) {
                        aggregator.consume(value, &assignment);
                    }
                }
            }
            return Self(agg);
        }
        Self(HashMap::new())
    }
    */
}

/*
fn init(query: &Query) -> Vec<Value> {
    let mut result = Vec::with_capacity(query.find.len());
    for find in &query.find {
        if let Find::Aggregate(agg) = find {
            result.push(agg.init());
        }
    }
    result
}

fn key_of(query: &Query, assignment: &PartialAssignment) -> Vec<Value> {
    let mut key = Vec::with_capacity(query.find.len());
    for variable in query.find_variables() {
        if let Some(value) = assignment.get(variable) {
            key.push(value.clone());
        }
    }
    key
}
*/

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
