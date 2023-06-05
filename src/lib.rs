pub mod clock;
pub mod datom;
pub mod db;
pub mod query;
pub mod schema;
pub mod storage;
pub mod tx;

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::RwLock;

    use crate::clock::MockClock;
    use crate::storage::InMemoryStorage;

    use super::db::*;
    use super::query::*;
    use super::schema::*;
    use super::tx::*;

    fn create_db() -> (Transactor<InMemoryStorage, MockClock>, Db<InMemoryStorage>) {
        let storage = Arc::new(RwLock::new(InMemoryStorage::new()));
        let transactor = Transactor::new(storage.clone(), MockClock::new());
        let db = Db::new(storage);
        (transactor, db)
    }

    #[test]
    fn return_empty_result() {
        let (mut transacor, db) = create_db();

        // Create the schema
        let schema_result = transacor.transact(
            Transaction::new().with(
                Attribute::new("person/name", ValueType::Str, Cardinality::One)
                    .with_doc("A person's name")
                    .build(),
            ),
        );
        assert!(schema_result.is_ok());

        // Insert data
        let tx_result = transacor.transact(
            Transaction::new()
                .with(Operation::on_new().set("person/name", "Alice"))
                .with(Operation::on_new().set("person/name", "Bob")),
        );
        assert!(tx_result.is_ok());

        let query_result = db.query(
            Query::new().wher(
                Clause::new()
                    .with_entity(EntityPattern::variable("?name"))
                    .with_attribute(AttributePattern::ident("person/name"))
                    .with_value(ValuePattern::constant("Eve")),
            ),
        );

        assert!(query_result.is_ok());
        assert!(query_result.unwrap().results.is_empty());
    }

    /*
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
            Query::new().wher(
                Clause::new()
                    .with_entity(EntityPattern::variable("?joe"))
                    .with_attribute(AttributePattern::ident("person/name"))
                    .with_value(ValuePattern::constant("Joe")),
            ),
        );

        assert!(query_result.is_ok());
        let temp_ids = tx_result.unwrap().temp_ids;
        let joe_id = temp_ids.get("joe");
        assert!(joe_id.is_some());
        assert_eq!(joe_id, query_result.unwrap().results[0]["?joe"].as_u64());
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
                .with(Attribute::new("name", ValueType::Str, Cardinality::One).build())
                .with(Attribute::new("age", ValueType::I64, Cardinality::One).build()),
        );
        assert!(schema_result.is_ok());

        // Insert data
        let tx_result = db.transact(
            Transaction::new()
                .with(Operation::on_new().set("name", "John").set("age", 33))
                .with(Operation::on_new().set("name", "Paul").set("age", 31))
                .with(Operation::on_new().set("name", "George").set("age", 30))
                .with(Operation::on_new().set("name", "Ringo").set("age", 32)),
        );
        assert!(tx_result.is_ok());

        let query_result = db.query(
            Query::new()
                .wher(
                    Clause::new()
                        .with_entity(EntityPattern::variable("?person"))
                        .with_attribute(AttributePattern::ident("age"))
                        .with_value(ValuePattern::range(&(Value::I64(32)..))),
                )
                .wher(
                    Clause::new()
                        .with_entity(EntityPattern::variable("?person"))
                        .with_attribute(AttributePattern::ident("name"))
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

    #[test]
    fn return_latest_value_with_cardinality_one() {
        let mut db = create_db();

        // Create the schema
        let schema_result = db.transact(
            Transaction::new()
                .with(Attribute::new("name", ValueType::Str, Cardinality::One).build())
                .with(Attribute::new("likes", ValueType::Str, Cardinality::One).build()),
        );
        assert!(schema_result.is_ok());

        // Insert initial data
        let tx_result1 = db.transact(
            Transaction::new().with(
                Operation::on_temp_id("joe")
                    .set("name", "Joe")
                    .set("likes", "Pizza"),
            ),
        );
        assert!(tx_result1.is_ok());
        let joe_id = tx_result1.unwrap().temp_ids["joe"];

        // Update what Joe likes
        let tx_result2 = db.transact(
            Transaction::new().with(
                Operation::on_id(joe_id)
                    .set("name", "Joe")
                    .set("likes", "Ice cream"),
            ),
        );
        assert!(tx_result2.is_ok());

        let query_result = db.query(
            Query::new().wher(
                Clause::new()
                    .with_entity(EntityPattern::Id(joe_id))
                    .with_attribute(AttributePattern::ident("likes"))
                    .with_value(ValuePattern::variable("?likes")),
            ),
        );

        assert!(query_result.is_ok());
        let results = query_result.unwrap().results;
        let likes: HashSet<&str> = results
            .iter()
            .flat_map(|assignment| assignment["?likes"].as_str().into_iter())
            .collect();

        assert_eq!(1, likes.len());
        assert!(likes.contains("Ice cream"));
    }

    #[test]
    fn return_all_values_with_cardinality_many() {
        let mut db = create_db();

        // Create the schema
        let schema_result = db.transact(
            Transaction::new()
                .with(Attribute::new("name", ValueType::Str, Cardinality::One).build())
                .with(Attribute::new("likes", ValueType::Str, Cardinality::Many).build()),
        );
        assert!(schema_result.is_ok());

        // Insert initial data
        let tx_result1 = db.transact(
            Transaction::new().with(
                Operation::on_temp_id("joe")
                    .set("name", "Joe")
                    .set("likes", "Pizza"),
            ),
        );
        assert!(tx_result1.is_ok());
        let joe_id = tx_result1.unwrap().temp_ids["joe"];

        // Update what Joe likes
        let tx_result2 = db.transact(
            Transaction::new().with(
                Operation::on_id(joe_id)
                    .set("name", "Joe")
                    .set("likes", "Ice cream"),
            ),
        );
        assert!(tx_result2.is_ok());

        let query_result = db.query(
            Query::new().wher(
                Clause::new()
                    .with_entity(EntityPattern::Id(joe_id))
                    .with_attribute(AttributePattern::ident("likes"))
                    .with_value(ValuePattern::variable("?likes")),
            ),
        );

        assert!(query_result.is_ok());
        let results = query_result.unwrap().results;
        let likes: HashSet<&str> = results
            .iter()
            .flat_map(|assignment| assignment["?likes"].as_str().into_iter())
            .collect();

        assert_eq!(2, likes.len());
        assert!(likes.contains("Pizza"));
        assert!(likes.contains("Ice cream"));
    }
    */

    // TODO retract
}
