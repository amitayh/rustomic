use rust_decimal::prelude::*;
use std::rc::Rc;

/// A datom is an immutable atomic fact that represents the addition or retraction of a relation
/// between an entity, an attribute, a value, and a transaction.
#[derive(Hash, Eq, PartialEq, Debug, Clone)]
pub struct Datom {
    pub entity: u64,
    pub attribute: u64,
    pub value: Value,
    pub tx: u64,
    pub op: Op,
}

impl Datom {
    pub fn add(entity: u64, attribute: u64, value: impl Into<Value>, tx: u64) -> Self {
        Self {
            entity,
            attribute,
            value: value.into(),
            tx,
            op: Op::Assert,
        }
    }

    pub fn retract(entity: u64, attribute: u64, value: impl Into<Value>, tx: u64) -> Self {
        Self {
            entity,
            attribute,
            value: value.into(),
            tx,
            op: Op::Retract,
        }
    }
}

#[derive(Hash, Eq, PartialEq, Debug, Clone, PartialOrd, Ord)]
pub enum Value {
    Nil,
    I64(i64),
    U64(u64),
    Decimal(Decimal),
    Str(Rc<str>),
    Ref(u64),
}

impl Value {
    pub fn str(str: &str) -> Self {
        Self::Str(Rc::from(str))
    }
}

impl From<i32> for Value {
    fn from(val: i32) -> Self {
        Self::I64(val.into())
    }
}

impl From<u32> for Value {
    fn from(val: u32) -> Self {
        Self::U64(val.into())
    }
}

impl From<i64> for Value {
    fn from(val: i64) -> Self {
        Self::I64(val)
    }
}

impl From<u64> for Value {
    fn from(val: u64) -> Self {
        Self::U64(val)
    }
}

impl From<Decimal> for Value {
    fn from(val: Decimal) -> Self {
        Self::Decimal(val)
    }
}

impl From<&str> for Value {
    fn from(val: &str) -> Self {
        Self::str(val)
    }
}

impl From<Rc<str>> for Value {
    fn from(val: Rc<str>) -> Self {
        Self::Str(val)
    }
}

#[derive(Hash, Eq, PartialEq, Debug, Clone, Copy)]
pub enum Op {
    Assert,
    Retract,
}
