use std::collections::HashMap;
use std::collections::HashSet;

use crate::datom;

#[derive(Clone, Debug)]
pub struct Variable(pub String);

impl Variable {
    pub fn new(name: &str) -> Variable {
        Variable(String::from(name))
    }
}

trait Pattern {
    fn variable_name(&self) -> Option<&String>;

    fn assigned_value<'a>(&'a self, assignment: &'a Assignment) -> Option<&datom::Value> {
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
pub enum ValuePattern {
    Variable(String),
    Constant(datom::Value),
    Blank,
}

impl ValuePattern {
    pub fn variable(name: &str) -> ValuePattern {
        ValuePattern::Variable(String::from(name))
    }

    pub fn constant<V: Into<datom::Value>>(value: V) -> ValuePattern {
        ValuePattern::Constant(value.into())
    }
}

impl Pattern for ValuePattern {
    fn variable_name(&self) -> Option<&String> {
        match self {
            ValuePattern::Variable(variable) => Some(&variable),
            _ => None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Clause {
    pub entity: EntityPattern,
    pub attribute: AttributePattern,
    pub value: ValuePattern,
}

impl Clause {
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
        if let Some(datom::Value::U64(entity)) = self.entity.assigned_value(assignment) {
            self.entity = EntityPattern::Id(*entity);
        }
        if let Some(datom::Value::U64(attribute)) = self.attribute.assigned_value(assignment) {
            self.attribute = AttributePattern::Id(*attribute);
        }
        if let Some(value) = self.value.assigned_value(assignment) {
            self.value = ValuePattern::Constant(value.clone());
        }
    }
}

impl datom::Datom {
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

pub struct Query {
    pub find: Vec<Variable>,
    pub wher: Vec<Clause>,
}

#[derive(Debug)]
pub struct QueryResult {
    pub results: Vec<HashMap<String, datom::Value>>,
}

// TODO PartialAssignment / CompleteAssignment?
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct Assignment {
    pub assigned: HashMap<String, datom::Value>,
    pub unassigned: HashSet<String>,
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

    pub fn assign<V: Into<datom::Value>>(&mut self, variable: &String, value: V) {
        if self.unassigned.contains(variable) {
            self.unassigned.remove(variable);
            self.assigned.insert(variable.clone(), value.into());
        }
    }

    pub fn update_with(&self, clause: &Clause, datom: &datom::Datom) -> Self {
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
}
