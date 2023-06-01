pub mod clock;
pub mod datom;
pub mod db;
pub mod query;
pub mod schema;
pub mod storage;
pub mod tx;

#[cfg(test)]
mod tests {
    use std::ops::Bound;

    use crate::clock::MockClock;
    use crate::datom::Value;
    use crate::storage::InMemoryStorage;

    use super::db::*;
    use super::query::*;
    use super::schema::*;
    use super::tx::*;

    fn create_db() -> Db<InMemoryStorage, MockClock> {
        Db::new(InMemoryStorage::new(), MockClock::new())
    }

    #[test]
    fn create_entity_by_temp_id() {
        let mut db = create_db();

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
            Transaction::new().with(Operation::on_temp_id("joe").set("person/name", "Joe")),
        );
        assert!(tx_result.is_ok());

        let query_result = db.query(
            Query::new().find("?joe").wher(
                Clause::new()
                    .with_entity(EntityPattern::variable("?joe"))
                    .with_attribute(AttributePattern::ident("person/name"))
                    .with_value(ValuePattern::constant("Joe")),
            ),
        );

        assert!(query_result.is_ok());
        assert_eq!(
            tx_result.unwrap().temp_ids.get("joe"),
            query_result.unwrap().results[0]["?joe"].as_u64()
        );
    }

    #[test]
    fn reject_transaction_with_invalid_attribute_type() {
        let mut db = create_db();

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
    fn reference_temp_id_in_transaction() {
        let mut db = create_db();

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
                .with(Operation::on_temp_id("paul").set("artist/name", "Paul McCartney"))
                .with(
                    Operation::on_temp_id("abbey-road")
                        .set("release/name", "Abbey Road")
                        // "release/artists" has type `Ref`, should resolve temp IDs
                        .set("release/artists", "john")
                        .set("release/artists", "paul"),
                ),
        );
        assert!(tx_result.is_ok());

        let query_result = db.query(
            Query::new()
                .find("?release-name")
                .wher(
                    Clause::new()
                        .with_entity(EntityPattern::variable("?artist"))
                        .with_attribute(AttributePattern::ident("artist/name"))
                        .with_value(ValuePattern::constant("John Lenon")),
                )
                .wher(
                    Clause::new()
                        .with_entity(EntityPattern::variable("?release"))
                        .with_attribute(AttributePattern::ident("release/artists"))
                        .with_value(ValuePattern::variable("?artist")),
                )
                .wher(
                    Clause::new()
                        .with_entity(EntityPattern::variable("?release"))
                        .with_attribute(AttributePattern::ident("release/name"))
                        .with_value(ValuePattern::variable("?release-name")),
                ),
        );

        assert!(query_result.is_ok());
        assert_eq!(
            Some("Abbey Road"),
            query_result.unwrap().results[0]["?release-name"].as_str()
        );
    }

    #[test]
    fn support_range_queries() {
        let mut db = create_db();

        // Create the schema
        let schema_result = db.transact(
            Transaction::new()
                .with(Attribute::new("person/name", ValueType::Str, Cardinality::One).build())
                .with(Attribute::new("person/age", ValueType::I64, Cardinality::One).build()),
        );
        assert!(schema_result.is_ok());

        // Insert data
        let tx_result = db.transact(
            Transaction::new()
                .with(
                    Operation::on_new()
                        .set("person/name", "John")
                        .set("person/age", 33),
                )
                .with(
                    Operation::on_new()
                        .set("person/name", "Paul")
                        .set("person/age", 31),
                )
                .with(
                    Operation::on_new()
                        .set("person/name", "George")
                        .set("person/age", 30),
                )
                .with(
                    Operation::on_new()
                        .set("person/name", "Ringo")
                        .set("person/age", 32),
                ),
        );
        assert!(tx_result.is_ok());

        let query_result = db.query(
            Query::new()
                .find("?name")
                .wher(
                    Clause::new()
                        .with_entity(EntityPattern::variable("?person"))
                        .with_attribute(AttributePattern::ident("person/age"))
                        // .with_value(ValuePattern::range(32..)),
                        .with_value(ValuePattern::Range(
                            Bound::Included(&Value::I64(32)),
                            Bound::Unbounded,
                        )),
                )
                .wher(
                    Clause::new()
                        .with_entity(EntityPattern::variable("?person"))
                        .with_attribute(AttributePattern::ident("person/name"))
                        .with_value(ValuePattern::variable("?name")),
                ),
        );

        assert!(query_result.is_ok());
        let results = query_result.unwrap().results;
        let names: Vec<&str> = results
            .iter()
            .flat_map(|assignment| assignment["?name"].as_str().into_iter())
            .collect();

        assert_eq!(2, names.len());
        assert!(names.contains(&"John"));
        assert!(names.contains(&"Ringo"));
    }
}
