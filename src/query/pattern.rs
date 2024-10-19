use std::sync::Arc;

use crate::datom::Value;

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub enum Pattern<T> {
    Variable(Arc<str>),
    Constant(T),
    #[default]
    Blank,
}

impl<T> Pattern<T> {
    pub fn variable(name: &str) -> Self {
        Self::Variable(Arc::from(name))
    }
}

impl Pattern<AttributeIdentifier> {
    pub fn id(id: u64) -> Self {
        Self::Constant(AttributeIdentifier::Id(id))
    }

    pub fn ident(ident: &str) -> Self {
        Self::Constant(AttributeIdentifier::Ident(Arc::from(ident)))
    }
}

impl Pattern<Value> {
    pub fn value(value: impl Into<Value>) -> Self {
        Self::Constant(value.into())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AttributeIdentifier {
    Ident(Arc<str>),
    Id(u64),
}
