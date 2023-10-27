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
    use crate::storage::memory::InMemoryStorage;
    use crate::storage::WriteStorage;

    use super::query::clause::*;
    use super::query::db::*;
    use super::query::pattern::*;
    use super::query::*;
    use super::schema::attribute::*;

    use super::tx::transactor::*;
    use super::tx::*;

    struct SUT {
        transactor: Transactor,
        storage: InMemoryStorage,
        last_tx: u64,
    }

    impl SUT {
        fn new() -> Self {
            let mut sut = Self::new_without_schema();
            sut.transact(create_schema());
            sut
        }

        fn new_without_schema() -> Self {
            let transactor = Transactor::new();
            let mut storage = InMemoryStorage::new();
            storage
                .save(&default_datoms())
                .expect("Unable to save default datoms");

            Self {
                transactor,
                storage,
                last_tx: 0,
            }
        }

        fn transact(&mut self, transaction: Transaction) -> TransctionResult {
            let result = self.try_transact(transaction).expect("Unable to transact");
            self.storage.save(&result.tx_data).expect("Unable to save");
            self.last_tx = result.tx_id;
            result
        }

        fn try_transact(&mut self, transaction: Transaction) -> Option<TransctionResult> {
            self.transactor
                .transact(&self.storage, now(), transaction)
                .ok()
        }

        fn query(&self, query: Query) -> QueryResult {
            self.query_at_snapshot(self.last_tx, query)
        }

        fn query_at_snapshot(&self, snapshot_tx: u64, query: Query) -> QueryResult {
            let mut db = Db::new(snapshot_tx);
            db.query(&self.storage, query).expect("Unable to query")
        }
    }

    fn now() -> Instant {
        Instant(
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .expect("Time went backwards")
                .as_secs(),
        )
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
        let mut sut = SUT::new();

        // Insert data
        sut.transact(
            Transaction::new()
                .with(Operation::on_new().set("person/name", "Alice"))
                .with(Operation::on_new().set("person/name", "Bob")),
        );

        let query_result = sut.query(
            Query::new().wher(
                Clause::new()
                    .with_entity(Pattern::variable("?name"))
                    .with_attribute(Pattern::ident("person/name"))
                    .with_value(Pattern::value("Eve")),
            ),
        );

        assert!(query_result.results.is_empty());
    }

    #[test]
    fn create_entity_by_temp_id() {
        let mut sut = SUT::new();

        // Insert data
        let result = sut.transact(
            Transaction::new().with(Operation::on_temp_id("joe").set("person/name", "Joe")),
        );

        let query_result = sut.query(
            Query::new().wher(
                Clause::new()
                    .with_entity(Pattern::variable("?joe"))
                    .with_attribute(Pattern::ident("person/name"))
                    .with_value(Pattern::value("Joe")),
            ),
        );

        let joe_id = result.temp_ids.get("joe");
        assert!(joe_id.is_some());
        assert_eq!(joe_id.copied(), query_result.results[0]["?joe"].as_u64());
    }

    #[test]
    fn reject_transaction_with_invalid_attribute_type() {
        let mut sut = SUT::new();

        // This transaction should fail: "person/name" is of type `ValueType::Str`.
        let tx = Transaction::new().with(Operation::on_new().set("person/name", 42));
        let tx_result = sut.try_transact(tx);

        assert!(tx_result.is_none());
    }

    #[test]
    fn reject_transaction_with_duplicate_temp_ids() {
        let mut sut = SUT::new();

        // This transaction should fail: temp ID "duplicate" should only be used once.
        let tx = Transaction::new()
            .with(Operation::on_temp_id("duplicate").set("person/name", "Alice"))
            .with(Operation::on_temp_id("duplicate").set("person/name", "Bob"));
        let tx_result = sut.try_transact(tx);

        assert!(tx_result.is_none());
    }

    #[test]
    fn reference_temp_id_in_transaction() {
        let mut sut = SUT::new();

        // Insert data
        sut.transact(
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

        let query_result = sut.query(
            Query::new()
                .wher(
                    Clause::new()
                        .with_entity(Pattern::variable("?artist"))
                        .with_attribute(Pattern::ident("artist/name"))
                        .with_value(Pattern::value("John Lenon")),
                )
                .wher(
                    Clause::new()
                        .with_entity(Pattern::variable("?release"))
                        .with_attribute(Pattern::ident("release/artists"))
                        .with_value(Pattern::variable("?artist")),
                )
                .wher(
                    Clause::new()
                        .with_entity(Pattern::variable("?release"))
                        .with_attribute(Pattern::ident("release/name"))
                        .with_value(Pattern::variable("?release-name")),
                ),
        );

        assert_eq!(
            Some("Abbey Road"),
            query_result.results[0]["?release-name"].as_str()
        );
    }

    #[test]
    fn return_latest_value_with_cardinality_one() {
        let mut sut = SUT::new_without_schema();

        // Create the schema
        sut.transact(
            Transaction::new()
                .with(Attribute::new("person/name", ValueType::Str).with_doc("A person's name"))
                .with(
                    Attribute::new("person/likes", ValueType::Str)
                        .with_doc("Things a person likes"),
                ),
        );

        // Insert initial data
        let tx_result = sut.transact(
            Transaction::new().with(
                Operation::on_temp_id("joe")
                    .set("person/name", "Joe")
                    .set("person/likes", "Pizza"),
            ),
        );
        let joe_id = tx_result.temp_ids["joe"];

        // Update what Joe likes
        sut.transact(
            Transaction::new().with(Operation::on_id(joe_id).set("person/likes", "Ice cream")),
        );

        let query_result = sut.query(
            Query::new().wher(
                Clause::new()
                    .with_entity(Pattern::Constant(joe_id))
                    .with_attribute(Pattern::ident("person/likes"))
                    .with_value(Pattern::variable("?likes")),
            ),
        );

        let likes: HashSet<&str> = query_result
            .results
            .iter()
            .flat_map(|assignment| assignment["?likes"].as_str().into_iter())
            .collect();

        assert_eq!(1, likes.len());
        assert!(likes.contains("Ice cream"));
    }

    #[test]
    fn return_all_values_with_cardinality_many() {
        let mut sut = SUT::new();

        // Insert initial data
        let tx_result = sut.transact(
            Transaction::new().with(
                Operation::on_temp_id("joe")
                    .set("person/name", "Joe")
                    .set("person/likes", "Pizza"),
            ),
        );
        let joe_id = tx_result.temp_ids["joe"];

        // Update what Joe likes
        sut.transact(
            Transaction::new().with(Operation::on_id(joe_id).set("person/likes", "Ice cream")),
        );

        let query_result = sut.query(
            Query::new().wher(
                Clause::new()
                    .with_entity(Pattern::Constant(joe_id))
                    .with_attribute(Pattern::ident("person/likes"))
                    .with_value(Pattern::variable("?likes")),
            ),
        );

        let likes: HashSet<&str> = query_result
            .results
            .iter()
            .flat_map(|assignment| assignment["?likes"].as_str().into_iter())
            .collect();

        assert_eq!(2, likes.len());
        assert!(likes.contains("Pizza"));
        assert!(likes.contains("Ice cream"));
    }

    #[test]
    fn return_correct_value_for_database_snapshot() {
        let mut sut = SUT::new();

        // Insert initial data
        let first_tx_result = sut.transact(
            Transaction::new().with(
                Operation::on_temp_id("joe")
                    .set("person/name", "Joe")
                    .set("person/likes", "Pizza"),
            ),
        );
        let joe_id = first_tx_result.temp_ids["joe"];

        // Update what Joe likes
        sut.transact(
            Transaction::new().with(
                Operation::on_id(joe_id)
                    .set("person/name", "Joe")
                    .set("person/likes", "Ice cream"),
            ),
        );

        let query_result = sut.query_at_snapshot(
            first_tx_result.tx_id,
            Query::new().wher(
                Clause::new()
                    .with_entity(Pattern::Constant(joe_id))
                    .with_attribute(Pattern::ident("person/likes"))
                    .with_value(Pattern::variable("?likes")),
            ),
        );

        let likes: HashSet<&str> = query_result
            .results
            .iter()
            .flat_map(|assignment| assignment["?likes"].as_str().into_iter())
            .collect();

        assert_eq!(1, likes.len());
        assert!(likes.contains("Pizza"));
    }

    #[test]
    fn search_for_tx_pattern() {
        let mut sut = SUT::new();

        // Insert initial data
        let tx_result =
            sut.transact(Transaction::new().with(Operation::on_new().set("person/name", "Joe")));

        let query_result = sut.query(
            Query::new()
                .wher(
                    Clause::new()
                        .with_entity(Pattern::Blank)
                        .with_attribute(Pattern::ident("person/name"))
                        .with_value(Pattern::value("Joe"))
                        .with_tx(Pattern::variable("?tx")),
                )
                .wher(
                    Clause::new()
                        .with_entity(Pattern::variable("?tx"))
                        .with_attribute(Pattern::id(DB_TX_TIME_ID))
                        .with_value(Pattern::variable("?tx_time")),
                ),
        );

        assert_eq!(1, query_result.results.len());
        let result = &query_result.results[0];
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
                        .with_entity(Pattern2::variable("?person"))
                        .with_attribute(Pattern2::ident("person/age"))
                        .with_value(ValuePattern::range(&(Value::I64(32)..))),
                )
                .wher(
                    Clause::new()
                        .with_entity(Pattern2::variable("?person"))
                        .with_attribute(Pattern2::ident("person/name"))
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
