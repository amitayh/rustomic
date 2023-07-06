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
    pub fn from(value: u64) -> Option<ValueType> {
        match value {
            1 => Some(ValueType::I64),
            2 => Some(ValueType::U64),
            3 => Some(ValueType::Decimal),
            4 => Some(ValueType::Str),
            5 => Some(ValueType::Ref),
            _ => None,
        }
    }
}

impl Value {
    /// ```
    /// use rustomic::datom::Value;
    /// use rustomic::schema::attribute::ValueType;
    /// use rust_decimal::prelude::*;
    ///
    /// assert!(Value::I64(42).matches_type(ValueType::I64));
    /// assert!(Value::U64(42).matches_type(ValueType::U64));
    /// assert!(Value::U64(42).matches_type(ValueType::Ref));
    /// assert!(Value::Decimal(42.into()).matches_type(ValueType::Decimal));
    /// assert!(Value::Str(String::from("foo")).matches_type(ValueType::Str));
    /// assert!(!Value::U64(42).matches_type(ValueType::Str));
    /// ```
    pub fn matches_type(&self, value_type: ValueType) -> bool {
        match self {
            Value::I64(_) => value_type == ValueType::I64,
            Value::U64(_) => value_type == ValueType::U64 || value_type == ValueType::Ref,
            Value::Decimal(_) => value_type == ValueType::Decimal,
            Value::Str(_) => value_type == ValueType::Str,
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
    pub fn from(value: u64) -> Option<Cardinality> {
        match value {
            0 => Some(Cardinality::One),
            1 => Some(Cardinality::Many),
            _ => None,
        }
    }
}

#[derive(Debug)]
pub struct Attribute<'a> {
    pub ident: &'a str,
    pub value_type: ValueType,
    pub cardinality: Cardinality,
    pub doc: Option<&'a str>,
    pub unique: bool,
}

impl<'a> Attribute<'a> {
    pub fn new(ident: &'a str, value_type: ValueType) -> Self {
        Attribute {
            ident,
            value_type,
            cardinality: Cardinality::One,
            doc: None,
            unique: false,
        }
    }

    pub fn with_doc(mut self, doc: &'a str) -> Self {
        self.doc = Some(doc);
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

impl<'a> Into<tx::Operation> for Attribute<'a> {
    fn into(self) -> tx::Operation {
        let mut operation = tx::Operation::on_new()
            .set(DB_ATTR_IDENT_IDENT, self.ident)
            .set(DB_ATTR_CARDINALITY_IDENT, self.cardinality as u64)
            .set(DB_ATTR_TYPE_IDENT, self.value_type as u64);
        if let Some(doc) = self.doc {
            operation.set_mut(DB_ATTR_DOC_IDENT, doc);
        }
        if self.unique {
            operation.set_mut(DB_ATTR_UNIQUE_IDENT, 1u64);
        }
        operation
    }
}
