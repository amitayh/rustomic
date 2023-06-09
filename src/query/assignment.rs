use std::collections::HashMap;
use std::collections::HashSet;

use crate::datom::*;
use crate::query::pattern::*;
use crate::query::*;

// TODO PartialAssignment / CompleteAssignment?
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct Assignment {
    pub assigned: HashMap<String, Value>,
    unassigned: HashSet<String>,
}

impl Assignment {
    pub fn new(variables: HashSet<String>) -> Self {
        Assignment {
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
    ///         .with_entity(EntityPattern::variable("foo"))
    ///         .with_attribute(AttributePattern::variable("bar"))
    ///         .with_value(ValuePattern::variable("baz")),
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
        Assignment::new(
            query
                .wher
                .iter()
                .flat_map(|clause| clause.free_variables())
                .map(String::from)
                .collect(),
        )
    }

    /// ```
    /// use std::collections::HashSet;
    /// use rustomic::query::assignment::*;
    ///
    /// let mut variables = HashSet::new();
    /// variables.insert(String::from("?foo"));
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
    /// use rustomic::query::assignment::*;
    /// use rustomic::query::clause::*;
    /// use rustomic::query::pattern::*;
    /// use rustomic::datom::*;
    ///
    /// let mut variables = HashSet::new();
    /// variables.insert(String::from("?entity"));
    /// variables.insert(String::from("?attribute"));
    /// variables.insert(String::from("?value"));
    /// let assignment = Assignment::new(variables);
    ///
    /// let clause = Clause::new()
    ///     .with_entity(EntityPattern::Variable("?entity"))
    ///     .with_attribute(AttributePattern::Variable("?attribute"))
    ///     .with_value(ValuePattern::Variable("?value"));
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
    /// ```
    pub fn update_with(&self, clause: &Clause, datom: Datom) -> Self {
        let mut assignment = self.clone();
        if let Some(entity_variable) = clause.entity.variable_name() {
            assignment.assign(entity_variable, datom.entity);
        }
        if let Some(attribute_variable) = clause.attribute.variable_name() {
            assignment.assign(attribute_variable, datom.attribute);
        }
        if let Some(value_variable) = clause.value.variable_name() {
            assignment.assign(value_variable, datom.value);
        }
        assignment
    }

    pub fn assigned_value<P: Pattern>(&self, pattern: &P) -> Option<&Value> {
        pattern
            .variable_name()
            .and_then(|variable| self.assigned.get(variable))
    }

    pub fn assign<V: Into<Value>>(&mut self, variable: &str, value: V) {
        if let Some(var) = self.unassigned.take(variable) {
            self.assigned.insert(var, value.into());
        }
    }
}
