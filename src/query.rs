use crate::datom;

pub struct Variable(pub String);

impl Variable {
    pub fn new(name: &str) -> Variable {
        Variable(String::from(name))
    }
}

pub enum DataPattern {
    Variable(String),
    Constant(datom::Value),
}

impl DataPattern {
    pub fn variable(name: &str) -> DataPattern {
        DataPattern::Variable(String::from(name))
    }

    pub fn constant<V: Into<datom::Value>>(value: V) -> DataPattern {
        DataPattern::Constant(value.into())
    }
}

pub struct Clause {
    pub entity: DataPattern,
    pub attribute: DataPattern,
    pub value: DataPattern,
}

pub struct Query {
    pub find: Vec<Variable>,
    pub wher: Vec<Clause>,
}

#[derive(Debug)]
pub struct QueryResult {
    pub results: Vec<Vec<datom::Value>>,
}
