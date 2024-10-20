use rust_decimal::prelude::*;
use std::rc::Rc;

use quickcheck::{Arbitrary, Gen};

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

impl Arbitrary for Datom {
    fn arbitrary(u: &mut Gen) -> Self {
        let entity = u64::arbitrary(u);
        let attribute = u64::arbitrary(u);
        let value = Value::arbitrary(u);
        let tx = u64::arbitrary(u);
        let op = Op::arbitrary(u);
        Self {
            entity,
            attribute,
            value,
            tx,
            op,
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

fn arbitrary_decimal(g: &mut Gen) -> Decimal {
    let mut arr = [0u8; 16];
    for x in &mut arr {
        *x = Arbitrary::arbitrary(g);
    }
    Decimal::deserialize(arr)
}

impl Arbitrary for Value {
    fn arbitrary(u: &mut Gen) -> Self {
        match u.choose(&[0, 1, 2, 3, 4, 5]) {
            Some(0) => Self::Nil,
            Some(1) => Self::I64(i64::arbitrary(u)),
            Some(2) => Self::U64(u64::arbitrary(u)),
            Some(3) => Self::Decimal(arbitrary_decimal(u)),
            Some(4) => Self::Str(String::arbitrary(u).into()),
            Some(5) => Self::Ref(u64::arbitrary(u)),
            _ => unreachable!(),
        }
    }
}

#[derive(Hash, Eq, PartialEq, Debug, Clone, Copy)]
pub enum Op {
    Assert,
    Retract,
}

impl Arbitrary for Op {
    fn arbitrary(u: &mut Gen) -> Self {
        if bool::arbitrary(u) {
            Self::Assert
        } else {
            Self::Retract
        }
    }
}
