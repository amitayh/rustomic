use std::collections::HashMap;
use std::collections::HashSet;
use std::rc::Rc;

use crate::datom::*;
use crate::query::pattern::*;
use crate::query::*;

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct Assignment2(HashMap<Rc<str>, Value>);

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct PartialAssignment {
    pub assigned: HashMap<Rc<str>, Value>,
    unassigned: HashSet<Rc<str>>,
}

pub enum Assignment3 {
    Complete(HashMap<Rc<str>, Value>),
    Partial {
        assigned: HashMap<Rc<str>, Value>,
        unassigned: HashSet<Rc<str>>,
    },
}

// TODO PartialAssignment / CompleteAssignment?
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct Assignment {
    pub assigned: HashMap<Rc<str>, Value>,
    unassigned: HashSet<Rc<str>>,
}

impl Assignment {
    pub fn new(variables: HashSet<Rc<str>>) -> Self {
        Self {
            assigned: HashMap::new(),
            unassigned: variables,
        }
    }

    /// ```
    /// use rustomic::query::*;
    /// use rustomic::query::assignment::*;
    /// use rustomic::query::clause::*;
    /// use rustomic::query::pattern::*;
    /// use rustomic::datom::*;
    ///
    /// let query = Query::new().wher(
    ///     Clause::new()
    ///         .with_entity(Pattern::variable("foo"))
    ///         .with_attribute(Pattern::variable("bar"))
    ///         .with_value(Pattern::variable("baz")),
    /// );
    /// let mut assignment = Assignment::from_query(&query);
    ///
    /// assignment.assign("foo", 1u64);
    /// assignment.assign("bar", 2u64);
    /// assignment.assign("baz", 3u64);
    ///
    /// assert_eq!(Value::U64(1), assignment.assigned["foo"]);
    /// assert_eq!(Value::U64(2), assignment.assigned["bar"]);
    /// assert_eq!(Value::U64(3), assignment.assigned["baz"]);
    /// ```
    pub fn from_query(query: &Query) -> Self {
        Self::new(
            query
                .wher
                .iter()
                .flat_map(|clause| clause.free_variables())
                .collect(),
        )
    }

    /// ```
    /// use std::rc::Rc;
    /// use std::collections::HashSet;
    /// use rustomic::query::assignment::*;
    ///
    /// let mut variables = HashSet::new();
    /// variables.insert(Rc::from("?foo"));
    /// let mut assignment = Assignment::new(variables);
    /// assert!(!assignment.is_complete());
    ///
    /// assignment.assign("?foo", 42u64);
    /// assert!(assignment.is_complete());
    /// ```
    pub fn is_complete(&self) -> bool {
        self.unassigned.is_empty()
    }

    /// ```
    /// use std::collections::HashSet;
    /// use std::rc::Rc;
    /// use rustomic::query::assignment::*;
    /// use rustomic::query::clause::*;
    /// use rustomic::query::pattern::*;
    /// use rustomic::datom::*;
    ///
    /// let mut variables = HashSet::new();
    /// variables.insert(Rc::from("?entity"));
    /// variables.insert(Rc::from("?attribute"));
    /// variables.insert(Rc::from("?value"));
    /// variables.insert(Rc::from("?tx"));
    /// let assignment = Assignment::new(variables);
    ///
    /// let clause = Clause::new()
    ///     .with_entity(Pattern::variable("?entity"))
    ///     .with_attribute(Pattern::variable("?attribute"))
    ///     .with_value(Pattern::variable("?value"))
    ///     .with_tx(Pattern::variable("?tx"));
    ///
    /// let entity = 1u64;
    /// let attribute = 2u64;
    /// let value = 3u64;
    /// let tx = 4u64;
    /// let datom = Datom::add(entity, attribute, value, tx);
    /// let updated = assignment.update_with(&clause, datom);
    ///
    /// assert_eq!(Value::U64(entity), updated.assigned["?entity"]);
    /// assert_eq!(Value::U64(attribute), updated.assigned["?attribute"]);
    /// assert_eq!(Value::U64(value), updated.assigned["?value"]);
    /// assert_eq!(Value::U64(tx), updated.assigned["?tx"]);
    /// ```
    pub fn update_with(&self, clause: &Clause, datom: Datom) -> Self {
        let mut assignment = self.clone();
        if let Some(entity_variable) = clause.entity.variable_name_ref() {
            assignment.assign(entity_variable, datom.entity);
        }
        if let Some(attribute_variable) = clause.attribute.variable_name_ref() {
            assignment.assign(attribute_variable, datom.attribute);
        }
        if let Some(value_variable) = clause.value.variable_name_ref() {
            assignment.assign(value_variable, datom.value);
        }
        if let Some(tx_variable) = clause.tx.variable_name_ref() {
            assignment.assign(tx_variable, datom.tx);
        }
        assignment
    }

    pub fn assigned_value<T>(&self, pattern: &Pattern<T>) -> Option<&Value> {
        pattern
            .variable_name_ref()
            .and_then(|variable| self.assigned.get(variable))
    }

    pub fn assign<V: Into<Value>>(&mut self, variable: &str, value: V) {
        if let Some(var) = self.unassigned.take(variable) {
            self.assigned.insert(var, value.into());
        }
    }
}
