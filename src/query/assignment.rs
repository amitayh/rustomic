use std::collections::HashMap;
use std::collections::HashSet;
use std::rc::Rc;

use crate::datom::*;
use crate::query::pattern::*;
use crate::query::*;

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct PartialAssignment {
    assigned: Assignment,
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
    /// let clauses = vec![
    ///     Clause::new()
    ///         .with_entity(Pattern::variable("foo"))
    ///         .with_attribute(Pattern::variable("bar"))
    ///         .with_value(Pattern::variable("baz")),
    /// ];
    /// let mut assignment = PartialAssignment::from_clauses(&clauses);
    ///
    /// assignment.assign("foo", Value::U64(1));
    /// assignment.assign("bar", Value::U64(2));
    /// assignment.assign("baz", Value::U64(3));
    ///
    /// assert_eq!(Some(&Value::U64(1)), assignment.get("foo"));
    /// assert_eq!(Some(&Value::U64(2)), assignment.get("bar"));
    /// assert_eq!(Some(&Value::U64(3)), assignment.get("baz"));
    /// ```
    pub fn from_clauses(clauses: &[Clause]) -> Self {
        Self::new(
            clauses
                .iter()
                .flat_map(|clause| clause.free_variables())
                .collect(),
        )
    }

    pub fn get(&self, variable: &str) -> Option<&Value> {
        self.assigned.get(variable)
    }

    pub fn get_ref(&self, variable: &str) -> Option<u64> {
        match self.get(variable) {
            Some(&Value::Ref(entity)) => Some(entity),
            _ => None,
        }
    }

    /// An assignment is considered "complete" when there are no more unassigned variables.
    ///
    /// ```
    /// use std::rc::Rc;
    /// use std::collections::HashSet;
    /// use rustomic::query::assignment::*;
    /// use rustomic::datom::*;
    ///
    /// let mut variables = HashSet::new();
    /// variables.insert(Rc::from("?foo"));
    /// let mut assignment = PartialAssignment::new(variables);
    /// assert!(!assignment.is_complete());
    ///
    /// assignment.assign("?foo", Value::U64(42));
    /// assert!(assignment.is_complete());
    /// ```
    pub fn is_complete(&self) -> bool {
        self.unassigned.is_empty()
    }

    pub fn complete(self) -> Assignment {
        self.assigned
    }

    /// Creates a new assignment where free variables from `clause` are assigned from `datom`.
    ///
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
    /// let assignment = PartialAssignment::new(variables);
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
    /// assert_eq!(Some(&Value::Ref(entity)), updated.get("?entity"));
    /// assert_eq!(Some(&Value::Ref(attribute)), updated.get("?attribute"));
    /// assert_eq!(Some(&Value::U64(value)), updated.get("?value"));
    /// assert_eq!(Some(&Value::Ref(tx)), updated.get("?tx"));
    /// ```
    pub fn update_with(&self, clause: &Clause, datom: Datom) -> Self {
        let mut assignment = self.clone();
        if let Pattern::Variable(variable) = &clause.entity {
            assignment.assign_ref(variable, datom.entity);
        }
        if let Pattern::Variable(variable) = &clause.attribute {
            assignment.assign_ref(variable, datom.attribute);
        }
        if let Pattern::Variable(variable) = &clause.value {
            assignment.assign(variable, datom.value);
        }
        if let Pattern::Variable(variable) = &clause.tx {
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
