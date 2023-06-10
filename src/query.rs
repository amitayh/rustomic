use std::collections::HashMap;
use std::collections::HashSet;
use std::ops::Bound;
use std::ops::RangeBounds;

use crate::datom::Datom;
use crate::datom::Value;
use crate::storage::StorageError;

trait Pattern {
    fn variable_name(&self) -> Option<&str>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EntityPattern<'a> {
    Variable(&'a str),
    Id(u64),
    Blank,
}

impl<'a> EntityPattern<'a> {
    pub fn variable(name: &'a str) -> Self {
        EntityPattern::Variable(name)
    }
}

impl<'a> Pattern for EntityPattern<'a> {
    fn variable_name(&self) -> Option<&str> {
        match *self {
            EntityPattern::Variable(variable) => Some(variable),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AttributePattern<'a> {
    Variable(&'a str),
    Ident(&'a str),
    Id(u64),
    Blank,
}

impl<'a> AttributePattern<'a> {
    pub fn variable(name: &str) -> AttributePattern {
        AttributePattern::Variable(name)
    }

    pub fn ident(name: &str) -> AttributePattern {
        AttributePattern::Ident(name)
    }
}

impl<'a> Pattern for AttributePattern<'a> {
    fn variable_name(&self) -> Option<&str> {
        match *self {
            AttributePattern::Variable(variable) => Some(variable),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ValuePattern<'a> {
    Variable(&'a str),
    Constant(Value),
    Range(Bound<&'a Value>, Bound<&'a Value>),
    Blank,
}

impl<'a> ValuePattern<'a> {
    pub fn variable(name: &'a str) -> Self {
        ValuePattern::Variable(name)
    }

    pub fn constant<V: Into<Value>>(value: V) -> Self {
        ValuePattern::Constant(value.into())
    }

    pub fn range<R: RangeBounds<Value>>(range: &'a R) -> Self {
        let start = range.start_bound();
        let end = range.end_bound();
        ValuePattern::Range(start, end)
    }
}

impl<'a> Pattern for ValuePattern<'a> {
    fn variable_name(&self) -> Option<&str> {
        match *self {
            ValuePattern::Variable(variable) => Some(variable),
            _ => None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Clause<'a> {
    pub entity: EntityPattern<'a>,
    pub attribute: AttributePattern<'a>,
    pub value: ValuePattern<'a>,
}

impl<'a> Clause<'a> {
    pub fn new() -> Self {
        Clause {
            entity: EntityPattern::Blank,
            attribute: AttributePattern::Blank,
            value: ValuePattern::Blank,
        }
    }

    pub fn with_entity(mut self, entity: EntityPattern<'a>) -> Self {
        self.entity = entity;
        self
    }

    pub fn with_attribute(mut self, attribute: AttributePattern<'a>) -> Self {
        self.attribute = attribute;
        self
    }

    pub fn with_value(mut self, value: ValuePattern<'a>) -> Self {
        self.value = value;
        self
    }

    /// ```
    /// use rustomic::query::*;
    ///
    /// let clause = Clause::new()
    ///     .with_entity(EntityPattern::variable("foo"))
    ///     .with_attribute(AttributePattern::variable("bar"))
    ///     .with_value(ValuePattern::variable("baz"));
    ///
    /// let free_variables = clause.free_variables();
    /// assert_eq!(3, free_variables.len());
    /// assert!(free_variables.contains(&"foo"));
    /// assert!(free_variables.contains(&"bar"));
    /// assert!(free_variables.contains(&"baz"));
    /// ```
    pub fn free_variables(&self) -> Vec<&str> {
        let mut variables = Vec::new();
        if let Some(variable) = self.entity.variable_name() {
            variables.push(variable);
        }
        if let Some(variable) = self.attribute.variable_name() {
            variables.push(variable);
        }
        if let Some(variable) = self.value.variable_name() {
            variables.push(variable);
        }
        variables
    }

    /// ```
    /// use rustomic::query::*;
    /// use rustomic::datom::*;
    ///
    /// let clause = Clause::new()
    ///     .with_entity(EntityPattern::variable("foo"))
    ///     .with_attribute(AttributePattern::variable("bar"))
    ///     .with_value(ValuePattern::variable("baz"));
    ///
    /// let query = Query::new().wher(clause.clone());
    /// let mut assignment = Assignment::from_query(&query);
    /// assignment.assign("foo", 1u64);
    /// assignment.assign("bar", 2u64);
    /// assignment.assign("baz", 3u64);
    ///
    /// let assigned = clause.assign(&assignment);
    ///
    /// assert_eq!(EntityPattern::Id(1), assigned.entity);
    /// assert_eq!(AttributePattern::Id(2), assigned.attribute);
    /// assert_eq!(ValuePattern::Constant(Value::U64(3)), assigned.value);
    /// ```
    pub fn assign(&self, assignment: &Assignment) -> Self {
        let mut clause = self.clone();
        if let Some(Value::U64(entity)) = assignment.assigned_value(&self.entity) {
            clause.entity = EntityPattern::Id(*entity);
        }
        if let Some(Value::U64(attribute)) = assignment.assigned_value(&self.attribute) {
            clause.attribute = AttributePattern::Id(*attribute);
        }
        if let Some(value) = assignment.assigned_value(&self.value) {
            clause.value = ValuePattern::Constant(value.clone());
        }
        clause
    }
}

#[derive(Debug)]
pub struct Query<'a> {
    pub wher: Vec<Clause<'a>>,
}

impl<'a> Query<'a> {
    pub fn new() -> Self {
        Query { wher: Vec::new() }
    }

    pub fn wher(mut self, clause: Clause<'a>) -> Self {
        self.wher.push(clause);
        self
    }
}

#[derive(Debug)]
pub struct QueryResult {
    pub results: Vec<HashMap<String, Value>>,
}

#[derive(Debug)]
pub enum QueryError {
    Error,
    StorageError(StorageError),
}

// TODO PartialAssignment / CompleteAssignment?
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct Assignment<'a> {
    pub assigned: HashMap<String, Value>,
    unassigned: HashSet<&'a str>,
}

impl<'a> Assignment<'a> {
    pub fn new(variables: HashSet<&'a str>) -> Self {
        Assignment {
            assigned: HashMap::new(),
            unassigned: variables,
        }
    }

    /// ```
    /// use rustomic::query::*;
    ///
    /// let query = Query::new().wher(
    ///     Clause::new()
    ///         .with_entity(EntityPattern::variable("foo"))
    ///         .with_attribute(AttributePattern::variable("bar"))
    ///         .with_value(ValuePattern::variable("baz")),
    /// );
    /// let assignment = Assignment::from_query(&query);
    /// ```
    pub fn from_query(query: &'a Query) -> Self {
        Assignment::new(
            query
                .wher
                .iter()
                .flat_map(|clause| clause.free_variables())
                .collect(),
        )
    }

    /// ```
    /// use std::collections::HashSet;
    /// use rustomic::query::*;
    ///
    /// let mut variables = HashSet::new();
    /// variables.insert("?foo");
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
    /// use rustomic::query::*;
    /// use rustomic::datom::*;
    ///
    /// let mut variables = HashSet::new();
    /// variables.insert("?entity");
    /// variables.insert("?attribute");
    /// variables.insert("?value");
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
            assignment.assign(value_variable, datom.value.clone());
        }
        assignment
    }

    fn assigned_value<P: Pattern>(&self, pattern: &P) -> Option<&Value> {
        pattern.variable_name().and_then(|variable| self.assigned.get(variable))
    }

    pub fn assign<V: Into<Value>>(&mut self, variable: &str, value: V) {
        if self.unassigned.remove(variable) {
            self.assigned.insert(String::from(variable), value.into());
        }
    }
}
