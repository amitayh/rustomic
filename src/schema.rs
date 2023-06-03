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
    I64 = 1,
    U64 = 2,
    // F64 = 3,
    Str = 4,
    Ref = 5,
}

impl ValueType {
    /// ```
    /// use rustomic::schema::ValueType;
    ///
    /// let value_types = vec![
    ///     ValueType::I64,
    ///     ValueType::U64,
    ///     // ValueType::F64,
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
            // 3 => Some(ValueType::F64),
            4 => Some(ValueType::Str),
            5 => Some(ValueType::Ref),
            _ => None,
        }
    }
}

impl Value {
    /// ```
    /// use rustomic::datom::Value;
    /// use rustomic::schema::ValueType;
    ///
    /// assert!(Value::I64(42).matches_type(ValueType::I64));
    /// assert!(Value::U64(42).matches_type(ValueType::U64));
    /// assert!(Value::U64(42).matches_type(ValueType::Ref));
    /// assert!(Value::Str(String::from("foo")).matches_type(ValueType::Str));
    /// assert!(!Value::U64(42).matches_type(ValueType::Str));
    /// ```
    pub fn matches_type(&self, value_type: ValueType) -> bool {
        match self {
            Value::I64(_) => value_type == ValueType::I64,
            Value::U64(_) => value_type == ValueType::U64 || value_type == ValueType::Ref,
            // Value::F64(_) => value_type == ValueType::F64,
            Value::Str(_) => value_type == ValueType::Str,
        }
    }
}

pub enum Cardinality {
    One = 0,
    Many = 1,
}

impl Cardinality {
    /// ```
    /// use rustomic::schema::Cardinality;
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

pub struct Attribute<'a> {
    ident: &'a str,
    value_type: ValueType,
    cardinality: Cardinality,
    doc: Option<&'a str>,
    unique: bool,
}

impl<'a> Attribute<'a> {
    pub fn new(ident: &'a str, value_type: ValueType, cardinality: Cardinality) -> Self {
        Attribute {
            ident,
            value_type,
            cardinality,
            doc: None,
            unique: false,
        }
    }

    pub fn with_doc(mut self, doc: &'a str) -> Self {
        self.doc = Some(doc);
        self
    }

    pub fn unique(mut self) -> Self {
        self.unique = true;
        self
    }

    pub fn build(self) -> tx::Operation {
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

#[rustfmt::skip]
pub fn default_datoms() -> Vec<Datom> {
    let tx = 0u64;
    vec![
        // first transaction
        Datom::new(tx, DB_TX_TIME_ID, 0u64, tx),
        // "db/attr/ident" attribute
        Datom::new(DB_ATTR_IDENT_ID, DB_ATTR_IDENT_ID, DB_ATTR_IDENT_IDENT, tx),
        Datom::new(DB_ATTR_IDENT_ID, DB_ATTR_DOC_ID, "Human readable name of attribute", tx),
        Datom::new(DB_ATTR_IDENT_ID, DB_ATTR_TYPE_ID, ValueType::Str as u64, tx),
        Datom::new(DB_ATTR_IDENT_ID, DB_ATTR_CARDINALITY_ID, Cardinality::One as u64, tx),
        Datom::new(DB_ATTR_IDENT_ID, DB_ATTR_UNIQUE_ID, 1u64, tx),
        // "db/attr/doc" attribute
        Datom::new(DB_ATTR_DOC_ID, DB_ATTR_IDENT_ID, DB_ATTR_DOC_IDENT, tx),
        Datom::new(DB_ATTR_DOC_ID, DB_ATTR_DOC_ID, "Documentation of attribute", tx),
        Datom::new(DB_ATTR_DOC_ID, DB_ATTR_TYPE_ID, ValueType::Str as u64, tx),
        Datom::new(DB_ATTR_DOC_ID, DB_ATTR_CARDINALITY_ID, Cardinality::One as u64, tx),
        // "db/attr/type" attribute
        Datom::new(DB_ATTR_TYPE_ID, DB_ATTR_IDENT_ID, DB_ATTR_TYPE_IDENT, tx),
        Datom::new(DB_ATTR_TYPE_ID, DB_ATTR_DOC_ID, "Data type of attribute", tx),
        Datom::new(DB_ATTR_TYPE_ID, DB_ATTR_TYPE_ID, ValueType::U64 as u64, tx),
        Datom::new(DB_ATTR_TYPE_ID, DB_ATTR_CARDINALITY_ID, Cardinality::One as u64, tx),
        // "db/attr/cardinality" attribute
        Datom::new(DB_ATTR_CARDINALITY_ID, DB_ATTR_IDENT_ID, DB_ATTR_CARDINALITY_IDENT, tx),
        Datom::new(DB_ATTR_CARDINALITY_ID, DB_ATTR_DOC_ID, "Cardinality of attribyte", tx),
        Datom::new(DB_ATTR_CARDINALITY_ID, DB_ATTR_TYPE_ID, ValueType::U64 as u64, tx),
        Datom::new(DB_ATTR_CARDINALITY_ID, DB_ATTR_CARDINALITY_ID, Cardinality::One as u64, tx),
        // "db/attr/unique" attribute
        Datom::new(DB_ATTR_UNIQUE_ID, DB_ATTR_IDENT_ID, DB_ATTR_UNIQUE_IDENT, tx),
        Datom::new(DB_ATTR_UNIQUE_ID, DB_ATTR_DOC_ID, "Indicates this attribute is unique", tx),
        Datom::new(DB_ATTR_UNIQUE_ID, DB_ATTR_TYPE_ID, ValueType::U64 as u64, tx),
        Datom::new(DB_ATTR_UNIQUE_ID, DB_ATTR_CARDINALITY_ID, Cardinality::One as u64, tx),
        // "db/tx/time" attribute
        Datom::new(DB_TX_TIME_ID, DB_ATTR_IDENT_ID, DB_TX_TIME_IDENT, tx),
        Datom::new(DB_TX_TIME_ID, DB_ATTR_DOC_ID, "Transaction's wall clock time", tx),
        Datom::new(DB_TX_TIME_ID, DB_ATTR_TYPE_ID, ValueType::U64 as u64, tx),
        Datom::new(DB_TX_TIME_ID, DB_ATTR_CARDINALITY_ID, Cardinality::One as u64, tx),
    ]
}
