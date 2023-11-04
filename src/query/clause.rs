use std::rc::Rc;

use crate::datom::*;
use crate::query::assignment::*;
use crate::query::pattern::*;

#[derive(Clone, Debug, Default)]
pub struct DataPattern {
    pub entity: Pattern<u64>,
    pub attribute: Pattern<AttributeIdentifier>,
    pub value: Pattern<Value>,
    pub tx: Pattern<u64>,
}

impl DataPattern {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_entity(mut self, entity: Pattern<u64>) -> Self {
        self.entity = entity;
        self
    }

    pub fn with_attribute(mut self, attribute: Pattern<AttributeIdentifier>) -> Self {
        self.attribute = attribute;
        self
    }

    pub fn with_value(mut self, value: Pattern<Value>) -> Self {
        self.value = value;
        self
    }

    pub fn with_tx(mut self, tx: Pattern<u64>) -> Self {
        self.tx = tx;
        self
    }

    /// ```
    /// use std::rc::Rc;
    /// use rustomic::query::clause::*;
    /// use rustomic::query::pattern::*;
    ///
    /// let clause = DataPattern::new()
    ///     .with_entity(Pattern::variable("foo"))
    ///     .with_attribute(Pattern::variable("bar"))
    ///     .with_value(Pattern::variable("baz"));
    ///
    /// let free_variables = clause.free_variables();
    /// assert_eq!(3, free_variables.len());
    /// assert!(free_variables.contains(&Rc::from("foo")));
    /// assert!(free_variables.contains(&Rc::from("bar")));
    /// assert!(free_variables.contains(&Rc::from("baz")));
    /// ```
    pub fn free_variables(&self) -> Vec<Rc<str>> {
        let mut variables = Vec::with_capacity(4);
        if let Pattern::Variable(variable) = &self.entity {
            variables.push(Rc::clone(variable));
        }
        if let Pattern::Variable(variable) = &self.attribute {
            variables.push(Rc::clone(variable));
        }
        if let Pattern::Variable(variable) = &self.value {
            variables.push(Rc::clone(variable));
        }
        if let Pattern::Variable(variable) = &self.tx {
            variables.push(Rc::clone(variable));
        }
        variables
    }

    /// ```
    /// use rustomic::query::*;
    /// use rustomic::query::assignment::*;
    /// use rustomic::query::clause::*;
    /// use rustomic::query::pattern::*;
    /// use rustomic::datom::*;
    ///
    /// let clause = DataPattern::new()
    ///     .with_entity(Pattern::variable("foo"))
    ///     .with_attribute(Pattern::variable("bar"))
    ///     .with_value(Pattern::variable("baz"))
    ///     .with_tx(Pattern::variable("qux"));
    ///
    /// let query = Query::new().wher(clause.clone());
    /// let mut assignment = Assignment::from_query(&query);
    /// assignment.assign("foo", 1u64);
    /// assignment.assign("bar", 2u64);
    /// assignment.assign("baz", 3u64);
    /// assignment.assign("qux", 4u64);
    ///
    /// let assigned = clause.bind(&assignment);
    ///
    /// assert_eq!(Pattern::Constant(1), assigned.entity);
    /// assert_eq!(Pattern::id(2), assigned.attribute);
    /// assert_eq!(Pattern::value(3u64), assigned.value);
    /// assert_eq!(Pattern::Constant(4), assigned.tx);
    /// ```
    pub fn bind(&self, assignment: &Assignment) -> Self {
        let mut clause = self.clone();
        if let Some(Value::U64(entity)) = assignment.assigned_value(&self.entity) {
            clause.entity = Pattern::Constant(*entity);
        }
        if let Some(Value::U64(attribute)) = assignment.assigned_value(&self.attribute) {
            clause.attribute = Pattern::id(*attribute);
        }
        if let Some(value) = assignment.assigned_value(&self.value) {
            clause.value = Pattern::Constant(value.clone());
        }
        if let Some(Value::U64(tx)) = assignment.assigned_value(&self.tx) {
            clause.tx = Pattern::Constant(*tx);
        }
        clause
    }
}
