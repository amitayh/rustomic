use crate::tx;

pub const DB_ATTR_IDENT: &str = "db/attr/ident";
pub const DB_ATTR_CARDINALITY: &str = "db/attr/cardinality";
pub const DB_ATTR_TYPE: &str = "db/attr/type";
pub const DB_ATTR_DOC: &str = "db/attr/doc";

pub const DB_TX_TIME: &str = "db/tx/time";

pub enum ValueType {
    U8 = 0,
    I32 = 1,
    U32 = 2,
    I64 = 3,
    U64 = 4,
    Str = 5,
    Ref = 6,
}

pub enum Cardinality {
    One = 0,
    Many = 1,
}

pub struct Attribute {
    pub ident: String,
    pub value_type: ValueType,
    pub cardinality: Cardinality,
    pub doc: Option<String>,
}

impl Attribute {
    pub fn new(ident: &str, value_type: ValueType, cardinality: Cardinality) -> tx::Operation {
        tx::Operation {
            entity: tx::Entity::New,
            attributes: vec![
                tx::AttributeValue::new(DB_ATTR_IDENT, ident),
                tx::AttributeValue::new(DB_ATTR_CARDINALITY, cardinality as u8),
                tx::AttributeValue::new(DB_ATTR_TYPE, value_type as u8),
            ],
        }
    }
}

impl Into<tx::Operation> for Attribute {
    fn into(self) -> tx::Operation {
        let mut attributes = vec![
            tx::AttributeValue::new(DB_ATTR_IDENT, self.ident),
            tx::AttributeValue::new(DB_ATTR_CARDINALITY, self.cardinality as u8),
            tx::AttributeValue::new(DB_ATTR_TYPE, self.value_type as u8),
        ];
        if let Some(doc) = self.doc {
            attributes.push(tx::AttributeValue::new(DB_ATTR_DOC, doc));
        }
        tx::Operation {
            entity: tx::Entity::New,
            attributes,
        }
    }
}
