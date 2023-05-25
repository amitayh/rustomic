pub mod datom;
pub mod db;
pub mod query;
pub mod schema;
pub mod storage;
pub mod tx;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_entity_by_temp_id() {
        let mut db = db::InMemoryDb::new();

        // Create the schema
        let schema_result = db.transact(tx::Transaction {
            operations: vec![schema::attribute(
                "person/name",
                schema::ValueType::Str,
                schema::Cardinality::One,
                "A person's name",
            )],
        });
        assert!(schema_result.is_ok());

        // Insert data
        let tx_result = db.transact(tx::Transaction {
            operations: vec![tx::Operation {
                entity: tx::Entity::TempId(String::from("john")),
                attributes: vec![tx::AttributeValue::new("person/name", "Joe")],
            }],
        });
        assert!(tx_result.is_ok());

        let query_result = db.query(query::Query {
            find: vec![query::Variable::new("?john")],
            wher: vec![query::Clause {
                entity: query::EntityPattern::variable("?john"),
                attribute: query::AttributePattern::ident("person/name"),
                value: query::ValuePattern::constant("Joe"),
            }],
        });

        assert_eq!(
            tx_result.unwrap().temp_ids.get("john"),
            query_result
                .results
                .get(0)
                .and_then(|assignment| assignment.get("?john"))
                .and_then(|value| value.as_u64())
        );
    }

    #[test]
    fn reject_transaction_with_invalid_attribute_type() {
        let mut db = db::InMemoryDb::new();

        // Create the schema
        let schema_result = db.transact(tx::Transaction {
            operations: vec![schema::attribute(
                "person/name",
                schema::ValueType::Str,
                schema::Cardinality::One,
                "A person's name",
            )],
        });
        assert!(schema_result.is_ok());

        // This transaction should fail: "person/name" is of type `ValueType::Str`
        let tx_result = db.transact(tx::Transaction {
            operations: vec![tx::Operation {
                entity: tx::Entity::New,
                attributes: vec![tx::AttributeValue::new("person/name", 42)],
            }],
        });
        assert!(tx_result.is_err());
    }

    #[test]
    #[ignore]
    fn reference_temp_id_in_transaction() {
        let mut db = db::InMemoryDb::new();

        // Create the schema
        let schema_result = db.transact(tx::Transaction {
            operations: vec![
                schema::attribute(
                    "artist/name",
                    schema::ValueType::Str,
                    schema::Cardinality::One,
                    "An artist's name",
                ),
                schema::attribute(
                    "release/name",
                    schema::ValueType::Str,
                    schema::Cardinality::One,
                    "An release's name",
                ),
                schema::attribute(
                    "release/artists",
                    schema::ValueType::Ref,
                    schema::Cardinality::Many,
                    "Artists of release",
                ),
            ],
        });
        assert!(schema_result.is_ok());

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
        assert!(tx_result.is_ok());

        let query_result = db.query(query::Query {
            find: vec![query::Variable::new("?release-name")],
            wher: vec![
                query::Clause {
                    entity: query::EntityPattern::variable("?artist"),
                    attribute: query::AttributePattern::ident("artist/name"),
                    value: query::ValuePattern::constant("John Lenon"),
                },
                query::Clause {
                    entity: query::EntityPattern::variable("?release"),
                    attribute: query::AttributePattern::ident("release/artists"),
                    value: query::ValuePattern::variable("?artist"),
                },
                query::Clause {
                    entity: query::EntityPattern::variable("?release"),
                    attribute: query::AttributePattern::ident("release/name"),
                    value: query::ValuePattern::variable("?release-name"),
                },
            ],
        });

        assert_eq!(
            Some("Abbey Road"),
            query_result
                .results
                .get(0)
                .and_then(|assignment| assignment.get("?release-name"))
                .and_then(|value| value.as_str())
        );
    }
}
