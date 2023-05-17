use std::{
    collections::{HashMap, HashSet},
    iter::Map,
};

mod datom;
mod schema;
mod tx;

// -----------------------------------------------------------------------------

enum Cardinality {
    One,
    Many,
}

impl Into<u8> for Cardinality {
    fn into(self) -> u8 {
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

impl Into<u8> for ValueType {
    fn into(self) -> u8 {
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

impl Into<Operation> for Attribute {
    fn into(self) -> Operation {
        let mut attributes = vec![
            AttributeValue {
                attribute: String::from("db/attr/ident"),
                value: datom::Value::Str(self.ident),
            },
            AttributeValue {
                attribute: String::from("db/attr/cardinality"),
                value: datom::Value::U8(self.cardinality.into()),
            },
            AttributeValue {
                attribute: String::from("db/attr/type"),
                value: datom::Value::U8(self.value_type.into()),
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

// -----------------------------------------------------------------------------

impl InMemoryDb {
    fn new() -> InMemoryDb {
        let mut db = InMemoryDb {
            next_entity_id: 0,
            ident_to_entity_id: HashMap::new(),
        };
        db
    }

    fn insert_default_datoms(&mut self) {
        let datoms = schema::get_default_datoms();
    }

    fn query(&self, query: Query) -> QueryResult {
        QueryResult {}
    }

    fn transact(&mut self, operations: Vec<Operation>) -> TransctionResult {
        // validate attributes match value
        // validate cardinality
        let tx = self.create_tx_datom();
        let temp_ids = self.generate_temp_ids(&operations);
        let mut datoms: Vec<datom::Datom> = operations
            .iter()
            .flat_map(|operation| {
                if let Some(entity_id) = self.get_entity_id(&operation.entity, &temp_ids) {
                    self.get_datoms(tx.entity, entity_id, &operation.attributes, &temp_ids)
                } else {
                    vec![]
                }
            })
            .collect();
        datoms.push(tx);
        TransctionResult {
            tx_data: datoms,
            temp_ids,
        }
    }

    fn get_entity_id(&mut self, entity: &Entity, temp_ids: &HashMap<String, u64>) -> Option<u64> {
        match entity {
            Entity::New => Some(self.get_next_entity_id()),
            Entity::Id(id) => Some(*id),
            Entity::TempId(temp_id) => temp_ids.get(temp_id).copied(),
        }
    }

    fn create_tx_datom(&mut self) -> datom::Datom {
        let transaction_id = self.get_next_entity_id();
        datom::Datom {
            entity: transaction_id,
            attribute: *self.ident_to_entity_id.get("db/tx/time").unwrap(),
            value: datom::Value::U64(0),
            tx: transaction_id,
            op: datom::Op::Added,
        }
    }

    fn get_datoms(
        &self,
        transaction_id: u64,
        entity_id: u64,
        attributes: &Vec<AttributeValue>,
        temp_ids: &HashMap<String, u64>,
    ) -> Vec<datom::Datom> {
        attributes
            .iter()
            .map(|attribute| datom::Datom {
                entity: entity_id,
                attribute: *self.ident_to_entity_id.get(&attribute.attribute).unwrap(),
                value: attribute.value.clone(),
                tx: transaction_id,
                op: datom::Op::Added,
            })
            .collect()
    }

    fn generate_temp_ids(&mut self, operations: &Vec<Operation>) -> HashMap<String, u64> {
        operations
            .iter()
            .filter_map(|operation| {
                if let Entity::TempId(id) = &operation.entity {
                    Some((id.clone(), self.get_next_entity_id()))
                } else {
                    None
                }
            })
            .collect()
    }

    fn get_next_entity_id(&mut self) -> u64 {
        let entity_id = self.next_entity_id;
        self.next_entity_id += 1;
        entity_id
    }
}

// -----------------------------------------------------------------------------

struct Variable(String);

struct Clause {
    entity: u64,
    attribute: u64,
    value: u64,
}

struct Query {
    find: Vec<Variable>,
    wher: Vec<Clause>,
}

struct QueryResult {}

// -----------------------------------------------------------------------------

#[test]
fn create_entity_by_temp_id() {
    let mut db = InMemoryDb::new();

    // Create the schema
    db.transact(vec![
        Attribute {
            ident: String::from("artist/name"),
            cardinality: Cardinality::One,
            value_type: ValueType::Str,
            doc: Some(String::from("An artist's name")),
        }
        .into(),
        Attribute {
            ident: String::from("release/name"),
            cardinality: Cardinality::One,
            value_type: ValueType::Str,
            doc: Some(String::from("An release's name")),
        }
        .into(),
        Attribute {
            ident: String::from("release/artists"),
            cardinality: Cardinality::Many,
            value_type: ValueType::Ref,
            doc: Some(String::from("Artists of release")),
        }
        .into(),
    ]);

    // Insert data
    let tx_result = db.transact(vec![
        Operation {
            entity: Entity::TempId(String::from("john")),
            attributes: vec![AttributeValue {
                attribute: String::from("artist/name"),
                value: datom::Value::Str(String::from("John Lenon")),
            }],
        },
        Operation {
            entity: Entity::New,
            attributes: vec![AttributeValue {
                attribute: String::from("artist/name"),
                value: datom::Value::Str(String::from("Paul McCartney")),
            }],
        },
        Operation {
            entity: Entity::TempId(String::from("abbey-road")),
            attributes: vec![AttributeValue {
                attribute: String::from("release/name"),
                value: datom::Value::Str(String::from("Abbey Road")),
            }],
        },
        Operation {
            entity: Entity::TempId(String::from("abbey-road")),
            attributes: vec![AttributeValue {
                attribute: String::from("release/artists"),
                value: datom::Value::Str(String::from("john")),
            }],
        },
    ]);

    let john_id = tx_result.temp_ids.get(&String::from("john"));

    let query_result = db.query(Query {
        find: vec![Variable(String::from("release"))],
        wher: vec![
            // [?artist :artist/name ?artist-name]
            Clause {
                entity: 0,
                attribute: 0,
                value: 0,
            },
            // [?release :release/artists ?artist]
            Clause {
                entity: 0,
                attribute: 0,
                value: 0,
            },
            // [?release :release/name ?release-name]
            Clause {
                entity: 0,
                attribute: 0,
                value: 0,
            },
        ],
    });

    assert_eq!(4, 2 + 2);
}

// -----------------------------------------------------------------------------

fn main() {
    println!("Hello, world!");
}
