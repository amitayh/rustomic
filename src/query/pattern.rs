use std::rc::Rc;

use crate::datom::Value;

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub enum Pattern<T> {
    Variable(Rc<str>),
    Constant(T),
    #[default]
    Blank,
}

impl<T> Pattern<T> {
    pub fn variable(name: &str) -> Self {
        Self::Variable(Rc::from(name))
    }

    pub fn variable_name(&self) -> Option<&str> {
        match self {
            Self::Variable(variable) => Some(variable),
            _ => None,
        }
    }
}

impl Pattern<AttributeIdentifier> {
    pub fn id(id: u64) -> Self {
        Self::Constant(AttributeIdentifier::Id(id))
    }

    pub fn ident(ident: &str) -> Self {
        Self::Constant(AttributeIdentifier::Ident(Rc::from(ident)))
    }
}

impl Pattern<Value> {
    pub fn value<V: Into<Value>>(value: V) -> Self {
        Self::Constant(value.into())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AttributeIdentifier {
    Ident(Rc<str>),
    Id(u64),
}
