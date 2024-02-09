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

impl TryFrom<u64> for ValueType {
    type Error = InvalidTag;

    /// ```
    /// use rustomic::schema::attribute::*;
    ///
    /// let value_types = vec![
    ///     ValueType::I64,
    ///     ValueType::U64,
    ///     ValueType::Decimal,
    ///     ValueType::Str,
    ///     ValueType::Ref,
    /// ];
    /// for value_type in value_types {
    ///     assert_eq!(Ok(value_type), ValueType::try_from(value_type as u64));
    /// }
    /// assert_eq!(Err(InvalidTag(42)), ValueType::try_from(42));
    /// ```
    fn try_from(value: u64) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::I64),
            2 => Ok(Self::U64),
            3 => Ok(Self::Decimal),
            4 => Ok(Self::Str),
            5 => Ok(Self::Ref),
            x => Err(InvalidTag(x)),
        }
    }
}

impl From<&Value> for ValueType {
    /// ```
    /// use std::rc::Rc;
    /// use rustomic::datom::Value;
    /// use rustomic::schema::attribute::*;
    /// use rust_decimal::prelude::*;
    ///
    /// assert_eq!(ValueType::from(&Value::I64(42)), ValueType::I64);
    /// assert_eq!(ValueType::from(&Value::U64(42)), ValueType::U64);
    /// assert_eq!(ValueType::from(&Value::Decimal(42.into())), ValueType::Decimal);
    /// assert_eq!(ValueType::from(&Value::str("foo")), ValueType::Str);
    /// assert_eq!(ValueType::from(&Value::Ref(42)), ValueType::Ref);
    /// assert_ne!(ValueType::from(&Value::U64(42)), ValueType::Str);
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
    type Error = InvalidTag;

    /// ```
    /// use rustomic::schema::attribute::*;
    ///
    /// assert_eq!(Ok(Cardinality::One), Cardinality::try_from(0));
    /// assert_eq!(Ok(Cardinality::Many), Cardinality::try_from(1));
    /// assert_eq!(Err(InvalidTag(42)), Cardinality::try_from(42));
    /// ```
    fn try_from(value: u64) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::One),
            1 => Ok(Self::Many),
            x => Err(InvalidTag(x)),
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

impl From<AttributeDefinition> for tx::EntityOperation {
    fn from(attribute: AttributeDefinition) -> Self {
        let mut operation = Self::on_new()
            .set_value(DB_ATTR_IDENT_IDENT, attribute.ident)
            .set_value(DB_ATTR_CARDINALITY_IDENT, attribute.cardinality as u64)
            .set_value(DB_ATTR_TYPE_IDENT, attribute.value_type as u64);
        if let Some(doc) = attribute.doc {
            operation = operation.set_value(DB_ATTR_DOC_IDENT, doc);
        }
        if attribute.unique {
            operation = operation.set_value(DB_ATTR_UNIQUE_IDENT, 1u64);
        }
        operation
    }
}

#[derive(PartialEq, Debug, Clone, Copy)]
pub struct InvalidTag(pub u64);
