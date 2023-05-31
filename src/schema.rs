use crate::datom::Datom;
use crate::datom::Value;
use crate::tx;

pub const DB_ATTR_IDENT_IDENT: &str = "db/attr/ident";
pub const DB_ATTR_IDENT_ID: u64 = 1;

pub const DB_ATTR_CARDINALITY_IDENT: &str = "db/attr/cardinality";
pub const DB_ATTR_CARDINALITY_ID: u64 = 2;

pub const DB_ATTR_TYPE_IDENT: &str = "db/attr/type";
pub const DB_ATTR_TYPE_ID: u64 = 3;

pub const DB_ATTR_DOC_IDENT: &str = "db/attr/doc";
pub const DB_ATTR_DOC_ID: u64 = 4;

pub const DB_ATTR_UNIQUE_IDENT: &str = "db/attr/unique";
pub const DB_ATTR_UNIQUE_ID: u64 = 5;

pub const DB_TX_TIME_IDENT: &str = "db/tx/time";
pub const DB_TX_TIME_ID: u64 = 6;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum ValueType {
    U8 = 0,
    I32 = 1,
    U32 = 2,
    I64 = 3,
    U64 = 4,
    Str = 5,
    Ref = 6,
}

impl ValueType {
    /// ```
    /// use rustomic::schema::ValueType;
    ///
    /// let value_types = vec![
    ///     ValueType::U8,
    ///     ValueType::I32,
    ///     ValueType::U32,
    ///     ValueType::I64,
    ///     ValueType::U64,
    ///     ValueType::Str,
    ///     ValueType::Ref,
    /// ];
    /// for value_type in value_types {
    ///     assert_eq!(Some(value_type), ValueType::from(value_type as u8));
    /// }
    /// assert_eq!(None, ValueType::from(42));
    /// ```
    pub fn from(value: u8) -> Option<ValueType> {
        match value {
            0 => Some(ValueType::U8),
            1 => Some(ValueType::I32),
            2 => Some(ValueType::U32),
            3 => Some(ValueType::I64),
            4 => Some(ValueType::U64),
            5 => Some(ValueType::Str),
            6 => Some(ValueType::Ref),
            _ => None,
        }
    }
}

impl Value {
    /// ```
    /// use rustomic::datom::Value;
    /// use rustomic::schema::ValueType;
    ///
    /// assert!(Value::U8(42).matches_type(ValueType::U8));
    /// assert!(Value::I64(42).matches_type(ValueType::I64));
    /// assert!(Value::U64(42).matches_type(ValueType::U64));
    /// assert!(Value::U64(42).matches_type(ValueType::Ref));
    /// assert!(Value::Str(String::from("foo")).matches_type(ValueType::Str));
    /// assert!(!Value::U64(42).matches_type(ValueType::Str));
    /// ```
    pub fn matches_type(&self, value_type: ValueType) -> bool {
        match self {
            Value::U8(_) => value_type == ValueType::U8,
            Value::I32(_) => value_type == ValueType::I32,
            Value::U32(_) => value_type == ValueType::U32,
            Value::I64(_) => value_type == ValueType::I64,
            Value::U64(_) => value_type == ValueType::U64 || value_type == ValueType::Ref,
            Value::Str(_) => value_type == ValueType::Str,
        }
    }
}

pub enum Cardinality {
    One = 0,
    Many = 1,
}

pub struct Attribute {
    ident: String,
    value_type: ValueType,
    cardinality: Cardinality,
    doc: Option<String>,
    unique: bool,
}

impl Attribute {
    pub fn new(ident: &str, value_type: ValueType, cardinality: Cardinality) -> Self {
        Attribute {
            ident: String::from(ident),
            value_type,
            cardinality,
            doc: None,
            unique: false,
        }
    }

    pub fn with_doc(mut self, doc: &str) -> Self {
        self.doc = Some(String::from(doc));
        self
    }

    pub fn unique(mut self) -> Self {
        self.unique = true;
        self
    }

    pub fn build(self) -> tx::Operation {
        let mut operation = tx::Operation::on_new()
            .set(DB_ATTR_IDENT_IDENT, self.ident)
            .set(DB_ATTR_CARDINALITY_IDENT, self.cardinality as u8)
            .set(DB_ATTR_TYPE_IDENT, self.value_type as u8);
        if let Some(doc) = self.doc {
            operation.set_mut(DB_ATTR_DOC_IDENT, doc);
        }
        if self.unique {
            operation.set_mut(DB_ATTR_UNIQUE_IDENT, 1u8);
        }
        operation
    }
}

#[rustfmt::skip]
pub fn default_datoms() -> Vec<Datom> {
    let tx = 0u64;
    vec![
        // first transaction
        Datom::new(tx, DB_TX_TIME_ID, 0u64, tx),
        // "db/attr/ident" attribute
        Datom::new(DB_ATTR_IDENT_ID, DB_ATTR_IDENT_ID, DB_ATTR_IDENT_IDENT, tx),
        Datom::new(DB_ATTR_IDENT_ID, DB_ATTR_DOC_ID, "Human readable name of attribute", tx),
        Datom::new(DB_ATTR_IDENT_ID, DB_ATTR_TYPE_ID, ValueType::Str as u8, tx),
        Datom::new(DB_ATTR_IDENT_ID, DB_ATTR_CARDINALITY_ID, Cardinality::One as u8, tx),
        Datom::new(DB_ATTR_IDENT_ID, DB_ATTR_UNIQUE_ID, 1u8, tx),
        // "db/attr/doc" attribute
        Datom::new(DB_ATTR_DOC_ID, DB_ATTR_IDENT_ID, DB_ATTR_DOC_IDENT, tx),
        Datom::new(DB_ATTR_DOC_ID, DB_ATTR_DOC_ID, "Documentation of attribute", tx),
        Datom::new(DB_ATTR_DOC_ID, DB_ATTR_TYPE_ID, ValueType::Str as u8, tx),
        Datom::new(DB_ATTR_DOC_ID, DB_ATTR_CARDINALITY_ID, Cardinality::One as u8, tx),
        // "db/attr/type" attribute
        Datom::new(DB_ATTR_TYPE_ID, DB_ATTR_IDENT_ID, DB_ATTR_TYPE_IDENT, tx),
        Datom::new(DB_ATTR_TYPE_ID, DB_ATTR_DOC_ID, "Data type of attribute", tx),
        Datom::new(DB_ATTR_TYPE_ID, DB_ATTR_TYPE_ID, ValueType::U8 as u8, tx),
        Datom::new(DB_ATTR_TYPE_ID, DB_ATTR_CARDINALITY_ID, Cardinality::One as u8, tx),
        // "db/attr/cardinality" attribute
        Datom::new(DB_ATTR_CARDINALITY_ID, DB_ATTR_IDENT_ID, DB_ATTR_CARDINALITY_IDENT, tx),
        Datom::new(DB_ATTR_CARDINALITY_ID, DB_ATTR_DOC_ID, "Cardinality of attribyte", tx),
        Datom::new(DB_ATTR_CARDINALITY_ID, DB_ATTR_TYPE_ID, ValueType::U8 as u8, tx),
        Datom::new(DB_ATTR_CARDINALITY_ID, DB_ATTR_CARDINALITY_ID, Cardinality::One as u8, tx),
        // "db/attr/unique" attribute
        Datom::new(DB_ATTR_UNIQUE_ID, DB_ATTR_IDENT_ID, DB_ATTR_UNIQUE_IDENT, tx),
        Datom::new(DB_ATTR_UNIQUE_ID, DB_ATTR_DOC_ID, "Indicates this attribute is unique", tx),
        Datom::new(DB_ATTR_UNIQUE_ID, DB_ATTR_TYPE_ID, ValueType::U8 as u8, tx),
        Datom::new(DB_ATTR_UNIQUE_ID, DB_ATTR_CARDINALITY_ID, Cardinality::One as u8, tx),
        // "db/tx/time" attribute
        Datom::new(DB_TX_TIME_ID, DB_ATTR_IDENT_ID, DB_TX_TIME_IDENT, tx),
        Datom::new(DB_TX_TIME_ID, DB_ATTR_DOC_ID, "Transaction's wall clock time", tx),
        Datom::new(DB_TX_TIME_ID, DB_ATTR_TYPE_ID, ValueType::U64 as u8, tx),
        Datom::new(DB_TX_TIME_ID, DB_ATTR_CARDINALITY_ID, Cardinality::One as u8, tx),
    ]
}
