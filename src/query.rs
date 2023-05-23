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
    fn is_grounded(&self) -> bool;

    fn variable_name(&self) -> Option<String>;

    fn assigned_value<'a>(&'a self, assignment: &'a Assignment) -> Option<&datom::Value> {
        self.variable_name()
            .and_then(move |variable| assignment.assigned.get(&variable))
    }
}

#[derive(Clone, Debug)]
pub enum EntityPattern {
    Variable(Variable),
    Id(u64),
    Blank,
}

impl EntityPattern {
    pub fn variable(name: &str) -> Self {
        EntityPattern::Variable(Variable(String::from(name)))
    }
}

impl Pattern for EntityPattern {
    fn is_grounded(&self) -> bool {
        match self {
            EntityPattern::Id(_) => true,
            _ => false,
        }
    }

    fn variable_name(&self) -> Option<String> {
        match self {
            EntityPattern::Variable(variable) => Some(variable.0.clone()),
            _ => None,
        }
    }
}

#[test]
fn entity_pattern_is_grounded() {
    assert!(EntityPattern::Id(0).is_grounded());
    assert!(!EntityPattern::variable("foo").is_grounded());
    assert!(!EntityPattern::Blank.is_grounded());
}

#[derive(Clone, Debug)]
pub enum AttributePattern {
    Variable(Variable),
    Ident(String),
    Id(u64),
    Blank,
}

impl AttributePattern {
    pub fn variable(name: &str) -> AttributePattern {
        AttributePattern::Variable(Variable(String::from(name)))
    }

    pub fn ident(name: &str) -> AttributePattern {
        AttributePattern::Ident(String::from(name))
    }
}

impl Pattern for AttributePattern {
    fn is_grounded(&self) -> bool {
        match self {
            AttributePattern::Ident(_) | AttributePattern::Id(_) => true,
            _ => false,
        }
    }

    fn variable_name(&self) -> Option<String> {
        match self {
            AttributePattern::Variable(variable) => Some(variable.0.clone()),
            _ => None,
        }
    }
}

#[derive(Clone, Debug)]
pub enum ValuePattern {
    Variable(Variable),
    Constant(datom::Value),
    Blank,
}

impl ValuePattern {
    pub fn variable(name: &str) -> ValuePattern {
        ValuePattern::Variable(Variable(String::from(name)))
    }

    pub fn constant<V: Into<datom::Value>>(value: V) -> ValuePattern {
        ValuePattern::Constant(value.into())
    }
}

impl Pattern for ValuePattern {
    fn is_grounded(&self) -> bool {
        match self {
            ValuePattern::Constant(_) => true,
            _ => false,
        }
    }

    fn variable_name(&self) -> Option<String> {
        match self {
            ValuePattern::Variable(variable) => Some(variable.0.clone()),
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
    pub fn free_variables(&self) -> Vec<String> {
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

    pub fn num_grounded_terms(&self) -> usize {
        let entity = self.entity.is_grounded() as usize;
        let attribute = self.attribute.is_grounded() as usize;
        let value = self.value.is_grounded() as usize;
        entity + attribute + value
    }

    pub fn substitute(&self, assignment: &Assignment) -> Self {
        let mut clause = self.clone();
        if let Some(datom::Value::U64(entity)) = clause.entity.assigned_value(assignment) {
            clause.entity = EntityPattern::Id(*entity);
        }
        if let Some(datom::Value::U64(attribute)) = clause.attribute.assigned_value(assignment) {
            clause.attribute = AttributePattern::Id(*attribute);
        }
        if let Some(value) = clause.value.assigned_value(assignment) {
            clause.value = ValuePattern::Constant(value.clone());
        }
        clause
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
    pub results: Vec<Vec<datom::Value>>,
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
                .collect(),
        }
    }

    pub fn is_complete(&self) -> bool {
        self.unassigned.is_empty()
    }

    pub fn assign(&mut self, variable: &str, value: datom::Value) {
        let variable0 = String::from(variable);
        if self.unassigned.contains(&variable0) {
            self.unassigned.remove(&variable0);
            self.assigned.insert(variable0, value);
        }
    }
}
