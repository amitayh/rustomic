pub mod datom;
pub mod db;
pub mod query;
pub mod schema;
pub mod tx;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_entity_by_temp_id() {
        let mut db = db::InMemoryDb::new();

        // Create the schema
        db.transact(tx::Transaction {
            operations: vec![schema::Attribute::new(
                "person/name",
                schema::ValueType::Str,
                schema::Cardinality::One,
            )],
        });

        // Insert data
        let tx_result = db.transact(tx::Transaction {
            operations: vec![tx::Operation {
                entity: tx::Entity::TempId(String::from("john")),
                attributes: vec![tx::AttributeValue::new("person/name", "Joe")],
            }],
        });

        let john_id = tx_result.temp_ids.get("john");

        let query_result = db.query(query::Query {
            find: vec![query::Variable::new("?john")],
            wher: vec![query::Clause {
                entity: query::EntityPattern::variable("?john"),
                attribute: query::AttributePattern::ident("person/name"),
                value: query::ValuePattern::constant("Joe"),
            }],
        });

        assert_eq!(
            john_id,
            query_result
                .results
                .get(0)
                .and_then(|assignment| assignment.get("?john"))
                .and_then(|value| value.as_u64())
        );
    }

    #[test]
    fn reference_temp_id_in_transaction() {
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
        db.transact(tx::Transaction {
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
