use rust_decimal::prelude::*;
use std::rc::Rc;

use quickcheck::{Arbitrary, Gen};

#[derive(Hash, Eq, PartialEq, Debug, Clone, PartialOrd, Ord)]
pub enum Value {
    I64(i64),
    U64(u64),
    Decimal(Decimal),
    Str(Rc<str>),
}

impl Value {
    pub fn str(str: &str) -> Value {
        Value::Str(Rc::from(str))
    }

    /// ```
    /// use rustomic::datom::Value;
    ///
    /// let str_value = Value::Str(String::from("foo"));
    /// assert_eq!(None, str_value.as_u64());
    ///
    /// let u64_value = Value::U64(42);
    /// assert_eq!(Some(42), u64_value.as_u64());
    /// ```
    pub fn as_u64(&self) -> Option<u64> {
        match *self {
            Value::U64(value) => Some(value),
            _ => None,
        }
    }

    /// ```
    /// use rustomic::datom::Value;
    ///
    /// let u64_value = Value::U64(42);
    /// assert_eq!(None, u64_value.as_str());
    ///
    /// let str_value = Value::Str(String::from("foo"));
    /// assert_eq!(Some("foo"), str_value.as_str());
    /// ```
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::Str(value) => Some(value),
            _ => None,
        }
    }
}

impl From<i32> for Value {
    fn from(val: i32) -> Self {
        Value::I64(val.into())
    }
}

impl From<u32> for Value {
    fn from(val: u32) -> Self {
        Value::U64(val.into())
    }
}

impl From<i64> for Value {
    fn from(val: i64) -> Self {
        Value::I64(val)
    }
}

impl From<u64> for Value {
    fn from(val: u64) -> Self {
        Value::U64(val)
    }
}

impl From<Decimal> for Value {
    fn from(val: Decimal) -> Self {
        Value::Decimal(val)
    }
}

impl From<&str> for Value {
    fn from(val: &str) -> Self {
        Value::Str(Rc::from(val))
    }
}

#[derive(Hash, Eq, PartialEq, Debug, Clone, Copy)]
pub enum Op {
    Added,
    Retracted,
}

#[derive(Hash, Eq, PartialEq, Debug, Clone)]
pub struct Datom {
    pub entity: u64,
    pub attribute: u64,
    pub value: Value,
    pub tx: u64,
    pub op: Op,
}

impl Datom {
    pub fn add<V: Into<Value>>(entity: u64, attribute: u64, value: V, tx: u64) -> Datom {
        Datom {
            entity,
            attribute,
            value: value.into(),
            tx,
            op: Op::Added,
        }
    }

    pub fn retract<V: Into<Value>>(entity: u64, attribute: u64, value: V, tx: u64) -> Datom {
        Datom {
            entity,
            attribute,
            value: value.into(),
            tx,
            op: Op::Retracted,
        }
    }
}

impl Arbitrary for Value {
    fn arbitrary(u: &mut Gen) -> Self {
        match u.choose(&[0, 1, 2]) {
            Some(0) => Value::I64(i64::arbitrary(u)),
            Some(1) => Value::U64(u64::arbitrary(u)),
            Some(2) => Value::Str(String::arbitrary(u).into()),
            _ => panic!(),
        }
    }
}

impl Arbitrary for Op {
    fn arbitrary(u: &mut Gen) -> Self {
        if bool::arbitrary(u) {
            Op::Added
        } else {
            Op::Retracted
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
        Datom {
            entity,
            attribute,
            value,
            tx,
            op,
        }
    }
}
