use std::collections::HashMap;

mod datom;
mod query;
mod schema;
mod tx;

struct InMemoryDb {
    next_entity_id: u64,
    ident_to_entity_id: HashMap<String, u64>,
    datoms: Vec<datom::Datom>,
}

impl InMemoryDb {
    fn new() -> InMemoryDb {
        let mut db = InMemoryDb {
            next_entity_id: 10,
            ident_to_entity_id: HashMap::new(),
            datoms: vec![
                // "db/attr/ident" attribute
                // TODO: unique?
                datom::Datom::new(1, 1, "db/attr/ident", 6),
                datom::Datom::new(1, 2, "Human readable name of attribute", 6),
                datom::Datom::new(1, 3, schema::ValueType::Str as u8, 6),
                datom::Datom::new(1, 4, schema::Cardinality::One as u8, 6),
                // "db/attr/doc" attribute
                datom::Datom::new(2, 1, "db/attr/doc", 6),
                datom::Datom::new(2, 2, "Documentation of attribute", 6),
                datom::Datom::new(2, 3, schema::ValueType::Str as u8, 6),
                datom::Datom::new(2, 4, schema::Cardinality::One as u8, 6),
                // "db/attr/type" attribute
                datom::Datom::new(3, 1, "db/attr/type", 6),
                datom::Datom::new(3, 2, "Data type of attribute", 6),
                datom::Datom::new(3, 3, schema::ValueType::U8 as u8, 6),
                datom::Datom::new(3, 4, schema::Cardinality::One as u8, 6),
                // "db/attr/cardinality" attribute
                datom::Datom::new(4, 1, "db/attr/cardinality", 6),
                datom::Datom::new(4, 2, "schema::Cardinality of attribyte", 6),
                datom::Datom::new(4, 3, schema::ValueType::U8 as u8, 6),
                datom::Datom::new(4, 4, schema::Cardinality::One as u8, 6),
                // "db/tx/time" attribute
                datom::Datom::new(5, 1, "db/tx/time", 6),
                datom::Datom::new(5, 2, "Transaction's wall clock time", 6),
                datom::Datom::new(5, 3, schema::ValueType::U64 as u8, 6),
                datom::Datom::new(5, 4, schema::Cardinality::One as u8, 6),
                // first transaction
                datom::Datom::new(6, 5, 0u64, 6),
            ],
        };
        db.ident_to_entity("db/attr/ident", 1);
        db.ident_to_entity("db/attr/doc", 2);
        db.ident_to_entity("db/attr/type", 3);
        db.ident_to_entity("db/attr/cardinality", 4);
        db.ident_to_entity("db/tx/time", 5);
        db
    }

    fn ident_to_entity(&mut self, ident: &str, entity: u64) {
        self.ident_to_entity_id.insert(String::from(ident), entity);
    }

    fn query(&self, query: query::Query) -> query::QueryResult {
        query::QueryResult {}
    }

    fn transact(&mut self, transaction: tx::Transaction) -> tx::TransctionResult {
        // validate attributes match value
        // validate cardinality
        let tx = self.create_tx_datom();
        let temp_ids = self.generate_temp_ids(&transaction.operations);
        let mut datoms: Vec<datom::Datom> = transaction
            .operations
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
        datoms.iter().for_each(|datom| {
            if let datom::Datom {
                entity,
                attribute: 1,
                value: datom::Value::Str(ident),
                tx: _,
                op: _,
            } = datom
            {
                self.ident_to_entity(ident, *entity);
            }
        });
        self.datoms.append(&mut datoms);
        tx::TransctionResult {
            tx_data: datoms,
            temp_ids,
        }
    }

    fn get_entity_id(
        &mut self,
        entity: &tx::Entity,
        temp_ids: &HashMap<String, u64>,
    ) -> Option<u64> {
        match entity {
            tx::Entity::New => Some(self.get_next_entity_id()),
            tx::Entity::Id(id) => Some(*id),
            tx::Entity::TempId(temp_id) => temp_ids.get(temp_id).copied(),
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
        attributes: &Vec<tx::AttributeValue>,
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

    fn generate_temp_ids(&mut self, operations: &Vec<tx::Operation>) -> HashMap<String, u64> {
        operations
            .iter()
            .filter_map(|operation| {
                if let tx::Entity::TempId(id) = &operation.entity {
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

#[test]
fn create_entity_by_temp_id() {
    let mut db = InMemoryDb::new();

    // Create the schema
    db.transact(tx::Transaction {
        operations: vec![
            schema::Attribute {
                ident: String::from("artist/name"),
                cardinality: schema::Cardinality::One,
                value_type: schema::ValueType::Str,
                doc: Some(String::from("An artist's name")),
            }
            .into(),
            schema::Attribute {
                ident: String::from("release/name"),
                cardinality: schema::Cardinality::One,
                value_type: schema::ValueType::Str,
                doc: Some(String::from("An release's name")),
            }
            .into(),
            schema::Attribute {
                ident: String::from("release/artists"),
                cardinality: schema::Cardinality::Many,
                value_type: schema::ValueType::Ref,
                doc: Some(String::from("Artists of release")),
            }
            .into(),
        ],
    });

    // Insert data
    let tx_result = db.transact(tx::Transaction {
        operations: vec![
            tx::Operation {
                entity: tx::Entity::TempId(String::from("john")),
                attributes: vec![tx::AttributeValue {
                    attribute: String::from("artist/name"),
                    value: datom::Value::Str(String::from("John Lenon")),
                }],
            },
            tx::Operation {
                entity: tx::Entity::New,
                attributes: vec![tx::AttributeValue {
                    attribute: String::from("artist/name"),
                    value: datom::Value::Str(String::from("Paul McCartney")),
                }],
            },
            tx::Operation {
                entity: tx::Entity::TempId(String::from("abbey-road")),
                attributes: vec![tx::AttributeValue {
                    attribute: String::from("release/name"),
                    value: datom::Value::Str(String::from("Abbey Road")),
                }],
            },
            tx::Operation {
                entity: tx::Entity::TempId(String::from("abbey-road")),
                attributes: vec![tx::AttributeValue {
                    attribute: String::from("release/artists"),
                    value: datom::Value::Str(String::from("john")),
                }],
            },
        ],
    });

    let john_id = tx_result.temp_ids.get(&String::from("john"));

    println!("@@@ {:?}", john_id);

    let query_result = db.query(query::Query {
        find: vec![query::Variable(String::from("release"))],
        wher: vec![
            // [?artist :artist/name ?artist-name]
            query::Clause {
                entity: 0,
                attribute: 0,
                value: 0,
            },
            // [?release :release/artists ?artist]
            query::Clause {
                entity: 0,
                attribute: 0,
                value: 0,
            },
            // [?release :release/name ?release-name]
            query::Clause {
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
