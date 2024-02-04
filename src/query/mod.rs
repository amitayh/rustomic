pub mod assignment;
pub mod clause;
pub mod db;
pub mod pattern;
pub mod resolver;

use crate::datom::Value;
use crate::query::clause::*;
use crate::storage::attribute_resolver::ResolveError;
use std::borrow::Borrow;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
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

#[derive(Debug)]
struct CountDistinct {
    variable: Rc<str>,
    seen: RefCell<HashSet<Value>>,
}

impl Aggregator for CountDistinct {
    fn init(&self) -> Value {
        Value::U64(0)
    }

    fn consume(&self, acc: &mut Value, assignment: &PartialAssignment) {
        {
            println!("---");
            dbg!(self.seen.borrow());
            dbg!(&acc);
            dbg!(&assignment);
            println!("---");
        }
        if let Value::I64(count) = acc {
            if let Some(value) = assignment.get(&self.variable) {
                if self.seen.borrow_mut().insert(value.clone()) {
                    *count += 1;
                }
            }
        }
    }
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

    pub fn count_distinct(variable: &str) -> Self {
        Self::Aggregate(Rc::new(CountDistinct {
            variable: Rc::from(variable),
            seen: RefCell::new(HashSet::new()),
        }))
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

    pub fn find_variables(&self) -> impl Iterator<Item = &Rc<str>> {
        self.find.iter().filter_map(|find| match find {
            Find::Variable(variable) => Some(variable),
            _ => None,
        })
    }

    fn is_aggregated(&self) -> bool {
        self.find
            .iter()
            .any(|find| matches!(find, Find::Aggregate(_)))
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
