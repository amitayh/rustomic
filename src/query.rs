use std::collections::HashMap;
use std::collections::HashSet;
use std::ops::Bound;
use std::ops::RangeBounds;

use crate::datom::Datom;
use crate::datom::Value;
use crate::storage::StorageError;

trait Pattern {
    fn variable_name(&self) -> Option<&String>;

    fn assigned_value<'a>(&'a self, assignment: &'a Assignment) -> Option<&Value> {
        self.variable_name()
            .and_then(|variable| assignment.assigned.get(variable))
    }
}

#[derive(Clone, Debug)]
pub enum EntityPattern {
    Variable(String),
    Id(u64),
    Blank,
}

impl EntityPattern {
    pub fn variable(name: &str) -> Self {
        EntityPattern::Variable(String::from(name))
    }
}

impl Pattern for EntityPattern {
    fn variable_name(&self) -> Option<&String> {
        match self {
            EntityPattern::Variable(variable) => Some(&variable),
            _ => None,
        }
    }
}

#[derive(Clone, Debug)]
pub enum AttributePattern {
    Variable(String),
    Ident(String),
    Id(u64),
    Blank,
}

impl AttributePattern {
    pub fn variable(name: &str) -> AttributePattern {
        AttributePattern::Variable(String::from(name))
    }

    pub fn ident(name: &str) -> AttributePattern {
        AttributePattern::Ident(String::from(name))
    }
}

impl Pattern for AttributePattern {
    fn variable_name(&self) -> Option<&String> {
        match self {
            AttributePattern::Variable(variable) => Some(&variable),
            _ => None,
        }
    }
}

#[derive(Clone, Debug)]
pub enum ValuePattern<'a> {
    Variable(String),
    Constant(Value),
    Range(Bound<&'a Value>, Bound<&'a Value>),
    Blank,
}

impl<'a> ValuePattern<'a> {
    pub fn variable(name: &str) -> Self {
        ValuePattern::Variable(String::from(name))
    }

    pub fn constant<V: Into<Value>>(value: V) -> Self {
        ValuePattern::Constant(value.into())
    }

    pub fn range<R: RangeBounds<Value>>(range: &'a R) -> Self {
        ValuePattern::Range(range.start_bound(), range.end_bound())
    }
}

impl<'a> Pattern for ValuePattern<'a> {
    fn variable_name(&self) -> Option<&String> {
        match self {
            ValuePattern::Variable(variable) => Some(&variable),
            _ => None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Clause<'a> {
    pub entity: EntityPattern,
    pub attribute: AttributePattern,
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

    pub fn with_entity(mut self, entity: EntityPattern) -> Self {
        self.entity = entity;
        self
    }

    pub fn with_attribute(mut self, attribute: AttributePattern) -> Self {
        self.attribute = attribute;
        self
    }

    pub fn with_value(mut self, value: ValuePattern<'a>) -> Self {
        self.value = value;
        self
    }

    pub fn free_variables(&self) -> Vec<&String> {
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

    pub fn substitute(&mut self, assignment: &Assignment) {
        if let Some(Value::U64(entity)) = self.entity.assigned_value(assignment) {
            self.entity = EntityPattern::Id(*entity);
        }
        if let Some(Value::U64(attribute)) = self.attribute.assigned_value(assignment) {
            self.attribute = AttributePattern::Id(*attribute);
        }
        if let Some(value) = self.value.assigned_value(assignment) {
            self.value = ValuePattern::Constant(value.clone());
        }
    }
}

impl Datom {
    pub fn satisfies(&self, clause: &Clause) -> bool {
        if let EntityPattern::Id(entity) = clause.entity {
            if entity != self.entity {
                return false;
            }
        }
        if let AttributePattern::Id(attribute) = clause.attribute {
            if attribute != self.attribute {
                return false;
            }
        }
        if let ValuePattern::Constant(value) = &clause.value {
            if value != &self.value {
                return false;
            }
        }
        true
    }
}

pub struct Query<'a> {
    pub find: Vec<String>,
    pub wher: Vec<Clause<'a>>,
}

impl<'a> Query<'a> {
    pub fn new() -> Self {
        Query {
            find: Vec::new(),
            wher: Vec::new(),
        }
    }

    pub fn find(mut self, variable: &str) -> Self {
        self.find.push(String::from(variable));
        self
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
pub struct Assignment {
    pub assigned: HashMap<String, Value>,
    unassigned: HashSet<String>,
}

impl Assignment {
    pub fn empty(query: &Query) -> Self {
        Assignment {
            assigned: HashMap::new(),
            unassigned: query
                .wher
                .iter()
                .flat_map(|clause| clause.free_variables())
                .map(|variable| variable.clone())
                .collect(),
        }
    }

    pub fn is_complete(&self) -> bool {
        self.unassigned.is_empty()
    }

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

    fn assign<V: Into<Value>>(&mut self, variable: &String, value: V) {
        if self.unassigned.remove(variable) {
            self.assigned.insert(variable.clone(), value.into());
        }
    }
}
