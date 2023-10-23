pub mod clock;
pub mod datom;
pub mod query;
pub mod schema;
pub mod storage;
pub mod tx;

#[cfg(test)]
mod tests {

    use std::collections::HashSet;
    use std::time::SystemTime;

    use crate::clock::Instant;
    use crate::schema::default::default_datoms;
    use crate::schema::DB_TX_TIME_ID;
    use crate::storage::memory2::InMemoryStorage;
    use crate::storage::WriteStorage;

    use super::datom::*;
    use super::query::clause::Clause;
    use super::query::db::*;
    use super::query::pattern::*;
    use super::query::*;
    use super::schema::attribute::*;

    use super::tx::transactor::*;
    use super::tx::*;

    fn now() -> Instant {
        Instant(
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .expect("time went backwards")
                .as_secs(),
        )
    }

    fn transact(
        transactor: &mut Transactor,
        storage: &mut InMemoryStorage,
        transaction: Transaction,
    ) -> TransctionResult {
        let result = transactor
            .transact(storage, now(), transaction)
            .expect("unable to transact");
        storage.save(&result.tx_data).expect("unable to save");
        result
    }

    fn create_db() -> (Transactor, InMemoryStorage) {
        let (mut transactor, mut storage) = create_empty_db();
        transact(&mut transactor, &mut storage, create_schema());
        (transactor, storage)
    }

    fn create_empty_db() -> (Transactor, InMemoryStorage) {
        let mut storage = InMemoryStorage::new();
        storage
            .save(&default_datoms())
            .expect("unable to save default datoms");
        (Transactor::new(), storage)
    }

    fn create_schema() -> Transaction {
        Transaction::new()
            .with(Attribute::new("movie/name", ValueType::Str))
            .with(Attribute::new("movie/year", ValueType::U64))
            .with(Attribute::new("movie/director", ValueType::Ref).many())
            .with(Attribute::new("movie/cast", ValueType::Ref).many())
            .with(Attribute::new("actor/name", ValueType::Str))
            .with(Attribute::new("person/name", ValueType::Str).with_doc("A person's name"))
            .with(Attribute::new("person/age", ValueType::I64).with_doc("A person's age"))
            .with(
                Attribute::new("person/likes", ValueType::Str)
                    .with_doc("Things a person likes")
                    .many(),
            )
            .with(Attribute::new("artist/name", ValueType::Str).with_doc("An artist's name"))
            .with(Attribute::new("release/name", ValueType::Str).with_doc("A release's name"))
            .with(
                Attribute::new("release/artists", ValueType::Ref)
                    .with_doc("Artists of release")
                    .many(),
            )
    }

    #[test]
    fn return_empty_result() {
        let (mut transactor, mut storage) = create_db();

        // Insert data
        let tx_result = transact(
            &mut transactor,
            &mut storage,
            Transaction::new()
                .with(Operation::on_new().set("person/name", "Alice"))
                .with(Operation::on_new().set("person/name", "Bob")),
        );

        let mut db = Db::new(tx_result.tx_id);
        let query_result = db.query(
            &storage,
            Query::new().wher(
                Clause::new()
                    .with_entity(EntityPattern::variable("?name"))
                    .with_attribute(AttributePattern::ident("person/name"))
                    .with_value(ValuePattern::constant(Value::str("Eve"))),
            ),
        );

        assert!(query_result.is_ok());
        assert!(query_result.unwrap().results.is_empty());
    }

    #[test]
    fn create_entity_by_temp_id() {
        let (mut transactor, mut storage) = create_db();

        // Insert data
        let TransctionResult {
            tx_id,
            tx_data: _,
            temp_ids,
        } = transact(
            &mut transactor,
            &mut storage,
            Transaction::new().with(Operation::on_temp_id("joe").set("person/name", "Joe")),
        );

        let mut db = Db::new(tx_id);
        let query_result = db.query(
            &storage,
            Query::new().wher(
                Clause::new()
                    .with_entity(EntityPattern::variable("?joe"))
                    .with_attribute(AttributePattern::ident("person/name"))
                    .with_value(ValuePattern::constant(Value::str("Joe"))),
            ),
        );

        assert!(query_result.is_ok());
        //dbg!(&storage);
        let joe_id = temp_ids.get("joe");
        assert!(joe_id.is_some());
        assert_eq!(
            joe_id.copied(),
            query_result.unwrap().results[0]["?joe"].as_u64()
        );
    }

    #[test]
    fn reject_transaction_with_invalid_attribute_type() {
        let (mut transactor, mut storage) = create_db();

        // This transaction should fail: "person/name" is of type `ValueType::Str`.
        let tx = Transaction::new().with(Operation::on_new().set("person/name", 42));
        let tx_result = transactor.transact(&mut storage, now(), tx);

        assert!(tx_result.is_err());
    }

    #[test]
    fn reject_transaction_with_duplicate_temp_ids() {
        let (mut transactor, mut storage) = create_db();

        // This transaction should fail: temp ID "duplicate" should only be used once.
        let tx = Transaction::new()
            .with(Operation::on_temp_id("duplicate").set("person/name", "Alice"))
            .with(Operation::on_temp_id("duplicate").set("person/name", "Bob"));
        let tx_result = transactor.transact(&mut storage, now(), tx);

        assert!(tx_result.is_err());
    }

    #[test]
    fn reference_temp_id_in_transaction() {
        let (mut transactor, mut storage) = create_db();

        // Insert data
        let tx_result = transact(
            &mut transactor,
            &mut storage,
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

        let mut db = Db::new(tx_result.tx_id);
        let query_result = db.query(
            &storage,
            Query::new()
                .wher(
                    Clause::new()
                        .with_entity(EntityPattern::variable("?artist"))
                        .with_attribute(AttributePattern::ident("artist/name"))
                        .with_value(ValuePattern::constant(Value::str("John Lenon"))),
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
    fn return_latest_value_with_cardinality_one() {
        let (mut transactor, mut storage) = create_empty_db();

        // Create the schema
        transact(
            &mut transactor,
            &mut storage,
            Transaction::new()
                .with(Attribute::new("person/name", ValueType::Str).with_doc("A person's name"))
                .with(
                    Attribute::new("person/likes", ValueType::Str)
                        .with_doc("Things a person likes"),
                ),
        );

        // Insert initial data
        let tx_result1 = transact(
            &mut transactor,
            &mut storage,
            Transaction::new().with(
                Operation::on_temp_id("joe")
                    .set("person/name", "Joe")
                    .set("person/likes", "Pizza"),
            ),
        );
        let joe_id = tx_result1.temp_ids["joe"];

        // Update what Joe likes
        let tx_result2 = transact(
            &mut transactor,
            &mut storage,
            Transaction::new().with(Operation::on_id(joe_id).set("person/likes", "Ice cream")),
        );

        let mut db = Db::new(tx_result2.tx_id);
        let query_result = db.query(
            &storage,
            Query::new().wher(
                Clause::new()
                    .with_entity(EntityPattern::Id(joe_id))
                    .with_attribute(AttributePattern::ident("person/likes"))
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
        let (mut transactor, mut storage) = create_db();

        // Insert initial data
        let tx_result1 = transact(
            &mut transactor,
            &mut storage,
            Transaction::new().with(
                Operation::on_temp_id("joe")
                    .set("person/name", "Joe")
                    .set("person/likes", "Pizza"),
            ),
        );
        let joe_id = tx_result1.temp_ids["joe"];

        // Update what Joe likes
        let tx_result2 = transact(
            &mut transactor,
            &mut storage,
            Transaction::new().with(Operation::on_id(joe_id).set("person/likes", "Ice cream")),
        );

        let mut db = Db::new(tx_result2.tx_id);
        let query_result = db.query(
            &storage,
            Query::new().wher(
                Clause::new()
                    .with_entity(EntityPattern::Id(joe_id))
                    .with_attribute(AttributePattern::ident("person/likes"))
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

    #[test]
    fn return_correct_value_for_database_snapshot() {
        let (mut transactor, mut storage) = create_db();

        // Insert initial data
        let TransctionResult {
            tx_id: first_tx_id,
            tx_data: _,
            temp_ids,
        } = transact(
            &mut transactor,
            &mut storage,
            Transaction::new().with(
                Operation::on_temp_id("joe")
                    .set("person/name", "Joe")
                    .set("person/likes", "Pizza"),
            ),
        );
        let joe_id = temp_ids["joe"];

        // Update what Joe likes
        transact(
            &mut transactor,
            &mut storage,
            Transaction::new().with(
                Operation::on_id(joe_id)
                    .set("person/name", "Joe")
                    .set("person/likes", "Ice cream"),
            ),
        );

        let mut db = Db::new(first_tx_id);
        let query_result = db.query(
            &storage,
            Query::new().wher(
                Clause::new()
                    .with_entity(EntityPattern::Id(joe_id))
                    .with_attribute(AttributePattern::ident("person/likes"))
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
        assert!(likes.contains("Pizza"));
    }

    #[test]
    fn search_for_tx_pattern() {
        let (mut transactor, mut storage) = create_db();

        // Insert initial data
        let tx_result = transact(
            &mut transactor,
            &mut storage,
            Transaction::new().with(Operation::on_new().set("person/name", "Joe")),
        );

        let mut db = Db::new(tx_result.tx_id);
        let query_result = db.query(
            &storage,
            Query::new()
                .wher(
                    Clause::new()
                        .with_entity(EntityPattern::Blank)
                        .with_attribute(AttributePattern::ident("person/name"))
                        .with_value(ValuePattern::constant(Value::str("Joe")))
                        .with_tx(TxPattern::variable("?tx")),
                )
                .wher(
                    Clause::new()
                        .with_entity(EntityPattern::variable("?tx"))
                        .with_attribute(AttributePattern::Id(DB_TX_TIME_ID))
                        .with_value(ValuePattern::variable("?tx_time")),
                ),
        );

        assert!(query_result.is_ok());
        let results = query_result.unwrap().results;
        assert_eq!(1, results.len());
        let result = &results[0];
        assert_eq!(Some(tx_result.tx_id), result["?tx"].as_u64());
        assert!(result["?tx_time"].as_u64().is_some_and(|time| time > 0));
    }

    /*
    #[test]
    fn support_range_queries() {
        let (mut transactor, storage) = create_db();

        // Insert data
        let tx_result = transactor.transact(
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

        let db = Db::new(storage, tx_result.unwrap().tx_id);
        let query_result = db.query(
            Query::new()
                .wher(
                    Clause::new()
                        .with_entity(EntityPattern::variable("?person"))
                        .with_attribute(AttributePattern::ident("person/age"))
                        .with_value(ValuePattern::range(&(Value::I64(32)..))),
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
    */

    // TODO retract
}
