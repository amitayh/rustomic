use std::{
    collections::{HashMap, HashSet},
    iter::Map,
};

mod datom;
mod tx;

// -----------------------------------------------------------------------------

trait Db {}

// -----------------------------------------------------------------------------

enum Cardinality {
    One,
    Many,
}

impl Cardinality {
    fn to_value(&self) -> u8 {
        match self {
            Cardinality::One => 0,
            Cardinality::Many => 1,
        }
    }
}

enum ValueType {
    Ref,
    Str,
}

impl ValueType {
    fn to_value(&self) -> u8 {
        match self {
            ValueType::Ref => 0,
            ValueType::Str => 1,
        }
    }
}

struct Attribute {
    ident: String,
    cardinality: Cardinality,
    value_type: ValueType,
    doc: Option<String>,
}

impl Attribute {
    fn to_operation(self) -> Operation {
        let mut attributes = vec![
            AttributeValue {
                attribute: String::from("db/attr/ident"),
                value: datom::Value::Str(self.ident),
            },
            AttributeValue {
                attribute: String::from("db/attr/cardinality"),
                value: datom::Value::U8(self.cardinality.to_value()),
            },
            AttributeValue {
                attribute: String::from("db/attr/type"),
                value: datom::Value::U8(self.value_type.to_value()),
            },
        ];
        if let Some(doc) = self.doc {
            attributes.push(AttributeValue {
                attribute: String::from("db/attr/doc"),
                value: datom::Value::Str(doc),
            });
        }
        Operation {
            entity: Entity::New,
            attributes,
        }
    }
}

// -----------------------------------------------------------------------------

struct TransctionResult {
    tx_data: Vec<datom::Datom>,
    temp_ids: HashMap<String, u64>,
}

struct AttributeValue {
    attribute: String,
    value: datom::Value,
}

enum Entity {
    New,            // Create a new entity and assign ID automatically.
    Id(u64),        // Update existing entity by ID.
    TempId(String), // Use a temp ID within transaction.
}

struct Operation {
    entity: Entity,
    attributes: Vec<AttributeValue>,
}

struct InMemoryDb {
    next_entity_id: u64,
    ident_to_entity_id: HashMap<String, u64>,
}

impl InMemoryDb {
    fn new() -> InMemoryDb {
        InMemoryDb {
            next_entity_id: 0,
            ident_to_entity_id: HashMap::new(),
        }
    }

    fn transact(&self, operations: Vec<Operation>) -> TransctionResult {
        TransctionResult {
            tx_data: vec![],
            temp_ids: HashMap::new(),
        }
    }
}

// -----------------------------------------------------------------------------

fn main() {
    let db = InMemoryDb::new();

    db.transact(vec![Attribute {
        ident: String::from("person/name"),
        cardinality: Cardinality::One,
        value_type: ValueType::Str,
        doc: Some(String::from("An artist's name")),
    }
    .to_operation()]);

    db.transact(vec![Operation {
        entity: Entity::New,
        attributes: vec![AttributeValue {
            attribute: String::from("person/name"),
            value: datom::Value::Str(String::from("John")),
        }],
    }]);

    println!("Hello, world!");
}
