use std::collections::HashMap;
use std::collections::HashSet;
use std::rc::Rc;

use crate::datom::*;
use crate::query::pattern::*;
use crate::query::*;

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct PartialAssignment {
    pub assigned: Assignment,
    unassigned: HashSet<Rc<str>>,
}

impl PartialAssignment {
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
    /// let query = Query::new().with(
    ///     Clause::new()
    ///         .with_entity(Pattern::variable("foo"))
    ///         .with_attribute(Pattern::variable("bar"))
    ///         .with_value(Pattern::variable("baz")),
    /// );
    /// let mut assignment = Assignment::from_query(&query);
    ///
    /// assignment.assign("foo", Value::U64(1));
    /// assignment.assign("bar", Value::U64(2));
    /// assignment.assign("baz", Value::U64(3));
    ///
    /// assert_eq!(Value::U64(1), assignment.assigned["foo"]);
    /// assert_eq!(Value::U64(2), assignment.assigned["bar"]);
    /// assert_eq!(Value::U64(3), assignment.assigned["baz"]);
    /// ```
    pub fn from_clauses(clauses: &[Clause]) -> Self {
        Self::new(
            clauses
                .iter()
                .flat_map(|clause| clause.free_variables())
                .collect(),
        )
    }

    /// ```
    /// use std::rc::Rc;
    /// use std::collections::HashSet;
    /// use rustomic::query::assignment::*;
    /// use rustomic::datom::*;
    ///
    /// let mut variables = HashSet::new();
    /// variables.insert(Rc::from("?foo"));
    /// let mut assignment = Assignment::new(variables);
    /// assert!(!assignment.is_complete());
    ///
    /// assignment.assign("?foo", Value::U64(42));
    /// assert!(assignment.is_complete());
    /// ```
    pub fn is_complete(&self) -> bool {
        self.unassigned.is_empty()
    }

    pub fn satisfies(&self, query: &Query) -> bool {
        query
            .predicates
            .iter()
            .all(|predicate| predicate(&self.assigned))
    }

    pub fn project(mut self, query: &Query) -> Option<Vec<Value>> {
        let mut result = Vec::with_capacity(query.find.len());
        for find in &query.find {
            if let Find::Variable(variable) = find {
                let value = self.assigned.remove(variable)?;
                result.push(value);
            }
        }
        Some(result)
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
    /// assert_eq!(Value::Ref(entity), updated.assigned["?entity"]);
    /// assert_eq!(Value::Ref(attribute), updated.assigned["?attribute"]);
    /// assert_eq!(Value::U64(value), updated.assigned["?value"]);
    /// assert_eq!(Value::Ref(tx), updated.assigned["?tx"]);
    /// ```
    pub fn update_with(&self, pattern: &Clause, datom: Datom) -> Self {
        let mut assignment = self.clone();
        if let Pattern::Variable(variable) = &pattern.entity {
            assignment.assign_ref(variable, datom.entity);
        }
        if let Pattern::Variable(variable) = &pattern.attribute {
            assignment.assign_ref(variable, datom.attribute);
        }
        if let Pattern::Variable(variable) = &pattern.value {
            assignment.assign(variable, datom.value);
        }
        if let Pattern::Variable(variable) = &pattern.tx {
            assignment.assign_ref(variable, datom.tx);
        }
        assignment
    }

    pub fn assign(&mut self, variable: &str, value: Value) {
        if let Some(var) = self.unassigned.take(variable) {
            self.assigned.insert(var, value);
        }
    }

    fn assign_ref(&mut self, variable: &str, entity: u64) {
        self.assign(variable, Value::Ref(entity));
    }
}
