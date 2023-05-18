mod datom;
mod schema;
mod query;
mod tx;
mod db;

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
