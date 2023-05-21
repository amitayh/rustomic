use crate::datom;

#[derive(Clone, Debug)]
pub struct Variable(pub String);

impl Variable {
    pub fn new(name: &str) -> Variable {
        Variable(String::from(name))
    }
}

#[derive(Clone, Debug)]
pub enum EntityPattern {
    Variable(Variable),
    Id(u64),
    Blank,
}

impl EntityPattern {
    pub fn variable(name: &str) -> EntityPattern {
        EntityPattern::Variable(Variable(String::from(name)))
    }

    pub fn is_grounded(&self) -> bool {
        match self {
            EntityPattern::Id(_) => true,
            _ => false,
        }
    }
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

    pub fn is_grounded(&self) -> bool {
        match self {
            AttributePattern::Ident(_) | AttributePattern::Id(_) => true,
            _ => false,
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
    pub fn constant<V: Into<datom::Value>>(value: V) -> ValuePattern {
        ValuePattern::Constant(value.into())
    }

    pub fn is_grounded(&self) -> bool {
        match self {
            ValuePattern::Constant(_) => true,
            _ => false,
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
    pub fn num_grounded_terms(&self) -> usize {
        let entity = if self.entity.is_grounded() { 1 } else { 0 };
        let attribute = if self.attribute.is_grounded() { 1 } else { 0 };
        let value = if self.value.is_grounded() { 1 } else { 0 };
        entity + attribute + value
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
