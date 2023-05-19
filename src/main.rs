mod datom;
mod db;
mod query;
mod schema;
mod tx;

fn extract_u64(result: &query::QueryResult) -> Option<&u64> {
    let foo = result.results.get(0)?.get(0)?;
    if let datom::Value::U64(id) = foo {
        return Some(id);
    }
    None
}

#[test]
fn create_entity_by_temp_id2() {
    let mut db = db::InMemoryDb::new();

    // Create the schema
    db.transact(tx::Transaction {
        operations: vec![schema::Attribute {
            ident: String::from("person/name"),
            cardinality: schema::Cardinality::One,
            value_type: schema::ValueType::Str,
            doc: Some(String::from("An person's name")),
        }
        .into()],
    });

    // Insert data
    let tx_result = db.transact(tx::Transaction {
        operations: vec![tx::Operation {
            entity: tx::Entity::TempId(String::from("john")),
            attributes: vec![tx::AttributeValue::new("person/name", "John Lenon")],
        }],
    });

    let john_id = tx_result.temp_ids.get(&String::from("john"));

    let query_result = db.query(query::Query {
        find: vec![query::Variable::new("john")],
        wher: vec![query::Clause {
            entity: query::DataPattern::variable("john"),
            attribute: query::DataPattern::constant("person/name"),
            value: query::DataPattern::constant("John Lenon"),
        }],
    });

    assert_eq!(john_id, extract_u64(&query_result));
}

#[test]
fn create_entity_by_temp_id() {
    let mut db = db::InMemoryDb::new();

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
                attributes: vec![tx::AttributeValue::new("artist/name", "John Lenon")],
            },
            tx::Operation {
                entity: tx::Entity::New,
                attributes: vec![tx::AttributeValue::new("artist/name", "Paul McCartney")],
            },
            tx::Operation {
                entity: tx::Entity::TempId(String::from("abbey-road")),
                attributes: vec![
                    tx::AttributeValue::new("release/name", "Abbey Road"),
                    tx::AttributeValue::new("release/artists", "john"),
                ],
            },
        ],
    });

    let john_id = tx_result.temp_ids.get(&String::from("john"));

    let query_result = db.query(query::Query {
        find: vec![query::Variable::new("release")],
        wher: vec![
            /*
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
            */
        ],
    });

    assert_eq!(4, 2 + 2);
}

// -----------------------------------------------------------------------------

fn main() {
    let mut db = db::InMemoryDb::new();

    // Create the schema
    db.transact(tx::Transaction {
        operations: vec![schema::Attribute {
            ident: String::from("person/name"),
            cardinality: schema::Cardinality::One,
            value_type: schema::ValueType::Str,
            doc: Some(String::from("An person's name")),
        }
        .into()],
    });

    // Insert data
    let tx_result = db.transact(tx::Transaction {
        operations: vec![tx::Operation {
            entity: tx::Entity::TempId(String::from("john")),
            attributes: vec![tx::AttributeValue::new("person/name", "John Lenon")],
        }],
    });

    let john_id = tx_result.temp_ids.get(&String::from("john"));

    let query_result = db.query(query::Query {
        find: vec![query::Variable::new("john")],
        wher: vec![query::Clause {
            entity: query::DataPattern::variable("john"),
            attribute: query::DataPattern::constant("person/name"),
            value: query::DataPattern::constant("John Lenon"),
        }],
    });

    println!("Hello, world! {:?}, {:?}", john_id.is_some(), query_result);
}
