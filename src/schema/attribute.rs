use std::rc::Rc;

use crate::datom::Value;
use crate::schema::*;
use crate::tx;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum ValueType {
    I64 = 1,
    U64 = 2,
    Decimal = 3,
    Str = 4,
    Ref = 5,
}

impl ValueType {
    /// ```
    /// use rustomic::schema::attribute::ValueType;
    ///
    /// let value_types = vec![
    ///     ValueType::I64,
    ///     ValueType::U64,
    ///     ValueType::Decimal,
    ///     ValueType::Str,
    ///     ValueType::Ref,
    /// ];
    /// for value_type in value_types {
    ///     assert_eq!(Some(value_type), ValueType::from(value_type as u64));
    /// }
    /// assert_eq!(None, ValueType::from(42));
    /// ```
    pub fn from(value: u64) -> Option<Self> {
        match value {
            1 => Some(Self::I64),
            2 => Some(Self::U64),
            3 => Some(Self::Decimal),
            4 => Some(Self::Str),
            5 => Some(Self::Ref),
            _ => None,
        }
    }
}

pub struct InvalidValue(u64);

impl TryFrom<u64> for ValueType {
    type Error = InvalidValue;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        ValueType::from(value).ok_or(InvalidValue(value))
    }
}

impl From<&Value> for ValueType {
    /// ```
    /// use std::rc::Rc;
    /// use rustomic::datom::Value;
    /// use rustomic::schema::attribute::ValueType;
    /// use rust_decimal::prelude::*;
    ///
    /// assert_eq!(Value::I64(42).value_type(), ValueType::I64);
    /// assert_eq!(Value::U64(42).value_type(), ValueType::U64);
    /// assert_eq!(Value::Decimal(42.into()).value_type(), ValueType::Decimal);
    /// assert_eq!(Value::str("foo").value_type(), ValueType::Str);
    /// assert_eq!(Value::Ref(42).value_type(), ValueType::Ref);
    /// assert_ne!(Value::U64(42).value_type(), ValueType::Str);
    /// ```
    fn from(value: &Value) -> Self {
        match value {
            Value::I64(_) => Self::I64,
            Value::U64(_) => Self::U64,
            Value::Decimal(_) => Self::Decimal,
            Value::Str(_) => Self::Str,
            Value::Ref(_) => Self::Ref,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Cardinality {
    One = 0,
    Many = 1,
}

impl TryFrom<u64> for Cardinality {
    type Error = InvalidValue;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        Cardinality::from(value).ok_or(InvalidValue(value))
    }
}

impl Cardinality {
    /// ```
    /// use rustomic::schema::attribute::Cardinality;
    ///
    /// assert_eq!(Some(Cardinality::One), Cardinality::from(0));
    /// assert_eq!(Some(Cardinality::Many), Cardinality::from(1));
    /// assert_eq!(None, Cardinality::from(42));
    /// ```
    pub fn from(value: u64) -> Option<Self> {
        match value {
            0 => Some(Self::One),
            1 => Some(Self::Many),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Attribute {
    pub id: u64,
    pub version: u64,
    pub definition: AttributeDefinition,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AttributeDefinition {
    pub ident: Rc<str>,
    pub value_type: ValueType,
    pub cardinality: Cardinality,
    pub doc: Option<Rc<str>>,
    pub unique: bool,
}

impl AttributeDefinition {
    pub fn new(ident: &str, value_type: ValueType) -> Self {
        AttributeDefinition {
            ident: Rc::from(ident),
            value_type,
            cardinality: Cardinality::One,
            doc: None,
            unique: false,
        }
    }

    pub fn with_doc(mut self, doc: &str) -> Self {
        self.doc = Some(Rc::from(doc));
        self
    }

    pub fn many(mut self) -> Self {
        self.cardinality = Cardinality::Many;
        self
    }

    pub fn unique(mut self) -> Self {
        self.unique = true;
        self
    }
}

impl From<AttributeDefinition> for tx::Operation {
    fn from(attribute: AttributeDefinition) -> Self {
        let mut operation = Self::on_new()
            .set(DB_ATTR_IDENT_IDENT, attribute.ident)
            .set(DB_ATTR_CARDINALITY_IDENT, attribute.cardinality as u64)
            .set(DB_ATTR_TYPE_IDENT, attribute.value_type as u64);
        if let Some(doc) = attribute.doc {
            operation = operation.set(DB_ATTR_DOC_IDENT, doc);
        }
        if attribute.unique {
            operation = operation.set(DB_ATTR_UNIQUE_IDENT, 1u64);
        }
        operation
    }
}
