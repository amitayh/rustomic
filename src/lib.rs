pub mod datom;
pub mod db;
pub mod query;
pub mod schema;
pub mod storage;
pub mod tx;

#[cfg(test)]
mod tests {
    use super::db::*;
    use super::query::*;
    use super::schema::*;
    use super::tx::*;

    #[test]
    fn create_entity_by_temp_id() {
        let mut db = InMemoryDb::new();

        // Create the schema
        let schema_result = db.transact(
            Transaction::new().with(
                Attribute::new("person/name", ValueType::Str, Cardinality::One)
                    .with_doc("A person's name")
                    .build(),
            ),
        );
        assert!(schema_result.is_ok());

        // Insert data
        let tx_result = db.transact(
            Transaction::new().with(Operation::on_temp_id("john").set("person/name", "Joe")),
        );
        assert!(tx_result.is_ok());

        let query_result = db.query(Query {
            find: vec![Variable::new("?john")],
            wher: vec![Clause {
                entity: EntityPattern::variable("?john"),
                attribute: AttributePattern::ident("person/name"),
                value: ValuePattern::constant("Joe"),
            }],
        });

        assert_eq!(
            tx_result.unwrap().temp_ids.get("john"),
            query_result.results[0]["?john"].as_u64()
        );
    }

    #[test]
    fn reject_transaction_with_invalid_attribute_type() {
        let mut db = InMemoryDb::new();

        // Create the schema
        let schema_result = db.transact(
            Transaction::new().with(
                Attribute::new("person/name", ValueType::Str, Cardinality::One)
                    .with_doc("A person's name")
                    .build(),
            ),
        );
        assert!(schema_result.is_ok());

        // This transaction should fail: "person/name" is of type `ValueType::Str`
        let tx_result =
            db.transact(Transaction::new().with(Operation::on_new().set("person/name", 42)));
        assert!(tx_result.is_err());
    }

    #[test]
    // #[ignore]
    fn reference_temp_id_in_transaction() {
        let mut db = InMemoryDb::new();

        // Create the schema
        let schema_result = db.transact(
            Transaction::new()
                .with(
                    Attribute::new("artist/name", ValueType::Str, Cardinality::One)
                        .with_doc("An artist's name")
                        .build(),
                )
                .with(
                    Attribute::new("release/name", ValueType::Str, Cardinality::One)
                        .with_doc("An release's name")
                        .build(),
                )
                .with(
                    Attribute::new("release/artists", ValueType::Ref, Cardinality::Many)
                        .with_doc("Artists of release")
                        .build(),
                ),
        );
        assert!(schema_result.is_ok());

        // Insert data
        let tx_result = db.transact(
            Transaction::new()
                .with(Operation::on_temp_id("john").set("artist/name", "John Lenon"))
                .with(Operation::on_new().set("artist/name", "Paul McCartney"))
                .with(
                    Operation::on_temp_id("abbey-road")
                        .set("release/name", "Abbey Road")
                        .set("release/artists", "john"),
                ),
        );
        assert!(tx_result.is_ok());

        let query_result = db.query(Query {
            find: vec![Variable::new("?release-name")],
            wher: vec![
                Clause {
                    entity: EntityPattern::variable("?artist"),
                    attribute: AttributePattern::ident("artist/name"),
                    value: ValuePattern::constant("John Lenon"),
                },
                Clause {
                    entity: EntityPattern::variable("?release"),
                    attribute: AttributePattern::ident("release/artists"),
                    value: ValuePattern::variable("?artist"),
                },
                Clause {
                    entity: EntityPattern::variable("?release"),
                    attribute: AttributePattern::ident("release/name"),
                    value: ValuePattern::variable("?release-name"),
                },
            ],
        });

        assert_eq!(
            Some("Abbey Road"),
            query_result.results[0]["?release-name"].as_str()
        );
    }
}
