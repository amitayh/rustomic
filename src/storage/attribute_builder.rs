use crate::datom::*;
use crate::schema::attribute::*;
use crate::schema::*;

pub struct AttributeBuilder {
    id: u64,
    version: u64,
    ident: Option<String>,
    value_type: Option<ValueType>,
    cardinality: Option<Cardinality>,
    doc: Option<String>,
    unique: bool,
}

impl AttributeBuilder {
    pub fn new(id: u64) -> Self {
        Self {
            id,
            version: 0,
            ident: None,
            value_type: None,
            cardinality: None,
            doc: None,
            unique: false,
        }
    }

    pub fn consume(&mut self, datom: Datom) {
        self.version = self.version.max(datom.tx);
        match datom {
            Datom {
                attribute: DB_ATTR_IDENT_ID,
                value: Value::Str(ident),
                ..
            } => self.ident = Some(ident.to_string()),
            Datom {
                attribute: DB_ATTR_TYPE_ID,
                value: Value::U64(value_type),
                ..
            } => self.value_type = ValueType::try_from(value_type).ok(),
            Datom {
                attribute: DB_ATTR_CARDINALITY_ID,
                value: Value::U64(cardinality),
                ..
            } => self.cardinality = Cardinality::try_from(cardinality).ok(),
            Datom {
                attribute: DB_ATTR_DOC_ID,
                value: Value::Str(doc),
                ..
            } => self.doc = Some(doc.to_string()),
            Datom {
                attribute: DB_ATTR_UNIQUE_ID,
                value: Value::U64(1),
                ..
            } => self.unique = true,
            _ => (),
        }
    }

    pub fn build(self) -> Option<Attribute> {
        let ident = self.ident?;
        let value_type = self.value_type?;
        let cardinality = self.cardinality?;
        Some(Attribute {
            id: self.id,
            version: self.version,
            definition: AttributeDefinition {
                ident,
                value_type,
                cardinality,
                doc: self.doc,
                unique: self.unique,
            },
        })
    }
}
