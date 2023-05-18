use crate::tx;

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

impl Into<tx::Operation> for Attribute {
    fn into(self) -> tx::Operation {
        let mut attributes = vec![
            tx::AttributeValue::new("db/attr/ident", self.ident),
            tx::AttributeValue::new("db/attr/cardinality", self.cardinality as u8),
            tx::AttributeValue::new("db/attr/type", self.value_type as u8),
        ];
        if let Some(doc) = self.doc {
            attributes.push(tx::AttributeValue::new("db/attr/doc", doc));
        }
        tx::Operation {
            entity: tx::Entity::New,
            attributes,
        }
    }
}
