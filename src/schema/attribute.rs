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
        match value {
            1 => Ok(Self::I64),
            2 => Ok(Self::U64),
            3 => Ok(Self::Decimal),
            4 => Ok(Self::Str),
            5 => Ok(Self::Ref),
            _ => Err(InvalidValue(value)),
        }
    }
}

impl Value {
    /// ```
    /// use std::rc::Rc;
    /// use rustomic::datom::Value;
    /// use rustomic::schema::attribute::ValueType;
    /// use rust_decimal::prelude::*;
    ///
    /// assert!(Value::I64(42).matches_type(ValueType::I64));
    /// assert!(Value::U64(42).matches_type(ValueType::U64));
    /// assert!(Value::Decimal(42.into()).matches_type(ValueType::Decimal));
    /// assert!(Value::str("foo").matches_type(ValueType::Str));
    /// assert!(Value::Ref(42).matches_type(ValueType::Ref));
    /// assert!(!Value::U64(42).matches_type(ValueType::Str));
    /// ```
    pub fn matches_type(&self, value_type: ValueType) -> bool {
        match self {
            Self::I64(_) => value_type == ValueType::I64,
            Self::U64(_) => value_type == ValueType::U64,
            Self::Decimal(_) => value_type == ValueType::Decimal,
            Self::Str(_) => value_type == ValueType::Str,
            Self::Ref(_) => value_type == ValueType::Ref,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Cardinality {
    One = 0,
    Many = 1,
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

#[derive(Debug)]
pub struct Attribute {
    pub ident: Rc<str>,
    pub value_type: ValueType,
    pub cardinality: Cardinality,
    pub doc: Option<Rc<str>>,
    pub unique: bool,
}

impl Attribute {
    pub fn new(ident: &str, value_type: ValueType) -> Self {
        Attribute {
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

impl From<Attribute> for tx::Operation {
    fn from(attribute: Attribute) -> Self {
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
