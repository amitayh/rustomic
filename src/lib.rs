pub mod clock;
pub mod datom;
pub mod query;
pub mod schema;
pub mod storage;
pub mod tx;

#[cfg(test)]
mod tests {
    use googletest::prelude::*;
    use std::time::SystemTime;

    use crate::clock::Instant;
    use crate::schema::default::default_datoms;
    use crate::schema::DB_TX_TIME_ID;
    use crate::storage::memory::InMemoryStorage;
    use crate::storage::ReadStorage;
    use crate::storage::WriteStorage;

    use super::datom::*;
    use super::query::clause::*;
    use super::query::database::*;
    use super::query::pattern::*;
    use super::query::*;
    use super::schema::attribute::*;

    use super::tx::transactor::*;
    use super::tx::*;

    struct Sut<'a> {
        transactor: Transactor,
        storage: InMemoryStorage<'a>,
        last_tx: u64,
    }

    type StorageError<'a> = <InMemoryStorage<'a> as ReadStorage<'a>>::Error;

    impl<'a> Sut<'a> {
        fn new() -> Self {
            let transactor = Transactor::new();
            let mut storage = InMemoryStorage::new();
            storage
                .save(&default_datoms())
                .expect("Unable to save default datoms");

            let mut sut = Self {
                transactor,
                storage,
                last_tx: 0,
            };

            sut.transact(create_schema());
            sut
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

        fn query(&self, query: Query) -> Vec<Vec<Value>> {
            self.query_at_snapshot(self.last_tx, query)
        }

        fn query_at_snapshot(&self, snapshot_tx: u64, query: Query) -> Vec<Vec<Value>> {
            let mut db = Database::new(snapshot_tx);
            let results = db.query(&self.storage, query).expect("Unable to query");
            results.filter_map(|result| result.ok()).collect()
        }

        fn try_query(
            &self,
            query: Query,
        ) -> crate::query::Result<Vec<QueryResult<StorageError<'_>>>, StorageError<'_>> {
            let mut db = Database::new(self.last_tx);
            let result = db.query(&self.storage, query)?;
            Ok(result.collect())
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
            .with(AttributeDefinition::new("movie/name", ValueType::Str))
            .with(AttributeDefinition::new("movie/year", ValueType::U64))
            .with(AttributeDefinition::new("movie/director", ValueType::Ref).many())
            .with(AttributeDefinition::new("movie/cast", ValueType::Ref).many())
            .with(AttributeDefinition::new("actor/name", ValueType::Str))
            .with(
                AttributeDefinition::new("person/name", ValueType::Str).with_doc("A person's name"),
            )
            .with(
                AttributeDefinition::new("person/born", ValueType::I64)
                    .with_doc("A person's birth year"),
            )
            .with(
                AttributeDefinition::new("person/likes", ValueType::Str)
                    .with_doc("Things a person likes")
                    .many(),
            )
            .with(
                AttributeDefinition::new("person/email", ValueType::Str)
                    .with_doc("A person's email address. Unique across all people!")
                    .unique(),
            )
            .with(
                AttributeDefinition::new("artist/name", ValueType::Str)
                    .with_doc("An artist's name"),
            )
            .with(
                AttributeDefinition::new("release/name", ValueType::Str)
                    .with_doc("A release's name"),
            )
            .with(
                AttributeDefinition::new("release/artists", ValueType::Ref)
                    .with_doc("Artists of release")
                    .many(),
            )
    }

    fn create_beatles() -> Transaction {
        // [{:person/name "John" :person/born 1940}
        //  {:person/name "Paul" :person/born 1942}
        //  {:person/name "George" :person/born 1943}
        //  {:person/name "Ringo" :person/born 1940}]
        Transaction::new()
            .with(
                EntityOperation::on_new()
                    .assert("person/name", "John")
                    .assert("person/born", 1940),
            )
            .with(
                EntityOperation::on_new()
                    .assert("person/name", "Paul")
                    .assert("person/born", 1942),
            )
            .with(
                EntityOperation::on_new()
                    .assert("person/name", "George")
                    .assert("person/born", 1943),
            )
            .with(
                EntityOperation::on_new()
                    .assert("person/name", "Ringo")
                    .assert("person/born", 1940),
            )
    }

    #[test]
    fn return_empty_result() {
        let mut sut = Sut::new();

        // Insert data
        // [{:person/name "Alice}
        //  {:person/name "Bob"}]
        sut.transact(
            Transaction::new()
                .with(EntityOperation::on_new().assert("person/name", "Alice"))
                .with(EntityOperation::on_new().assert("person/name", "Bob")),
        );

        // [:find ?name
        //  :where [?name :person/name "Eve"]]
        let query_result = sut.query(
            Query::new().find(Find::variable("?name")).with(
                Clause::new()
                    .with_entity(Pattern::variable("?name"))
                    .with_attribute(Pattern::ident("person/name"))
                    .with_value(Pattern::value("Eve")),
            ),
        );

        assert!(query_result.is_empty());
    }

    #[test]
    fn create_entity_by_temp_id() {
        let mut sut = Sut::new();

        // Insert data
        // [{:db/id "joe"
        //   :person/name "Joe"}]
        let tx_result = sut.transact(
            Transaction::new()
                .with(EntityOperation::on_temp_id("joe").assert("person/name", "Joe")),
        );

        // [:find ?joe
        //  :where [?joe :person/name "Joe"]]
        let query_result = sut.query(
            Query::new().find(Find::variable("?joe")).with(
                Clause::new()
                    .with_entity(Pattern::variable("?joe"))
                    .with_attribute(Pattern::ident("person/name"))
                    .with_value(Pattern::value("Joe")),
            ),
        );

        let joe_id = tx_result.temp_ids.get("joe");
        assert!(joe_id.is_some());

        assert_that!(
            query_result,
            unordered_elements_are![elements_are![eq(Value::Ref(*joe_id.unwrap()))]]
        );
    }

    #[test]
    fn reject_transaction_with_invalid_attribute_type() {
        let mut sut = Sut::new();

        // This transaction should fail: "person/name" is of type `ValueType::Str`.
        let tx = Transaction::new().with(EntityOperation::on_new().assert("person/name", 42));
        let tx_result = sut.try_transact(tx);

        assert!(tx_result.is_none());
    }

    #[test]
    fn reject_transaction_with_duplicate_temp_ids() {
        let mut sut = Sut::new();

        // This transaction should fail: temp ID "duplicate" should only be used once.
        let tx = Transaction::new()
            .with(EntityOperation::on_temp_id("duplicate").assert("person/name", "Alice"))
            .with(EntityOperation::on_temp_id("duplicate").assert("person/name", "Bob"));
        let tx_result = sut.try_transact(tx);

        assert!(tx_result.is_none());
    }

    #[test]
    fn reference_temp_id_in_transaction() {
        let mut sut = Sut::new();

        // Insert data
        // [{:db/id "john"
        //   :artist/name "John Lenon"}
        //  {:db/id "paul"
        //   :artist/name "Paul McCartney"}
        //  {:db/id "abbey-road"
        //   :release/name "Abbey Road"
        //   :release/artists "john"
        //   :release/artists "paul"}]
        sut.transact(
            Transaction::new()
                .with(EntityOperation::on_temp_id("john").assert("artist/name", "John Lenon"))
                .with(EntityOperation::on_temp_id("paul").assert("artist/name", "Paul McCartney"))
                .with(
                    EntityOperation::on_temp_id("abbey-road")
                        .assert("release/name", "Abbey Road")
                        .set_reference("release/artists", "john")
                        .set_reference("release/artists", "paul"),
                ),
        );

        // [:find ?release-name
        //  :where [?artist :artist/name "John Lenon"]
        //         [?release :release/artist ?artist]
        //         [?release :release/name ?release-name]]
        let query_result = sut.query(
            Query::new()
                .find(Find::variable("?release-name"))
                .with(
                    Clause::new()
                        .with_entity(Pattern::variable("?artist"))
                        .with_attribute(Pattern::ident("artist/name"))
                        .with_value(Pattern::value("John Lenon")),
                )
                .with(
                    Clause::new()
                        .with_entity(Pattern::variable("?release"))
                        .with_attribute(Pattern::ident("release/artists"))
                        .with_value(Pattern::variable("?artist")),
                )
                .with(
                    Clause::new()
                        .with_entity(Pattern::variable("?release"))
                        .with_attribute(Pattern::ident("release/name"))
                        .with_value(Pattern::variable("?release-name")),
                ),
        );

        assert_that!(
            query_result,
            unordered_elements_are![elements_are![eq(Value::str("Abbey Road"))]]
        );
    }

    #[test]
    fn return_latest_value_with_cardinality_one() {
        let mut sut = Sut::new();

        // Insert initial data
        // [{:db/id "joe"
        //   :person/name "Joe"
        //   :person/email "foo@bar.com"}]
        let tx_result = sut.transact(
            Transaction::new().with(
                EntityOperation::on_temp_id("joe")
                    .assert("person/name", "Joe")
                    .assert("person/email", "foo@bar.com"),
            ),
        );
        let joe_id = tx_result.temp_ids["joe"];

        // Update Joe's email
        // [{:db/id joe_id
        //   :person/email "foo@baz.com"}]
        sut.transact(
            Transaction::new()
                .with(EntityOperation::on_id(joe_id).assert("person/email", "foo@baz.com")),
        );

        // [:find ?email
        //  :where [?joe_id :person/email ?email]]
        let query_result = sut.query(
            Query::new().find(Find::variable("?email")).with(
                Clause::new()
                    .with_entity(Pattern::Constant(joe_id))
                    .with_attribute(Pattern::ident("person/email"))
                    .with_value(Pattern::variable("?email")),
            ),
        );

        assert_that!(
            query_result,
            unordered_elements_are![elements_are![eq(Value::str("foo@baz.com"))]]
        );
    }

    #[test]
    fn return_all_values_with_cardinality_many() {
        let mut sut = Sut::new();

        // Insert initial data
        // [{:db/id "joe"
        //   :person/name "Joe"
        //   :person/likes "Pizza"}]
        let tx_result = sut.transact(
            Transaction::new().with(
                EntityOperation::on_temp_id("joe")
                    .assert("person/name", "Joe")
                    .assert("person/likes", "Pizza"),
            ),
        );
        let joe_id = tx_result.temp_ids["joe"];

        // Update what Joe likes
        // [{:db/id joe_id
        //   :person/likes "Ice cream"}]
        sut.transact(
            Transaction::new()
                .with(EntityOperation::on_id(joe_id).assert("person/likes", "Ice cream")),
        );

        // [:find ?likes
        //  :where [?joe_id :person/likes ?likes]]
        let query_result = sut.query(
            Query::new().find(Find::variable("?likes")).with(
                Clause::new()
                    .with_entity(Pattern::Constant(joe_id))
                    .with_attribute(Pattern::ident("person/likes"))
                    .with_value(Pattern::variable("?likes")),
            ),
        );

        assert_that!(
            query_result,
            unordered_elements_are![
                elements_are![eq(Value::str("Pizza"))],
                elements_are![eq(Value::str("Ice cream"))],
            ]
        );
    }

    #[test]
    fn return_correct_value_for_database_snapshot() {
        let mut sut = Sut::new();

        // Insert initial data
        // [{:db/id "joe"
        //   :person/name "Joe"
        //   :person/likes "Pizza"}]
        let first_tx_result = sut.transact(
            Transaction::new().with(
                EntityOperation::on_temp_id("joe")
                    .assert("person/name", "Joe")
                    .assert("person/likes", "Pizza"),
            ),
        );
        let joe_id = first_tx_result.temp_ids["joe"];

        // Update what Joe likes
        // [{:db/id joe_id
        //   :person/likes "Ice cream"}]
        sut.transact(
            Transaction::new().with(
                EntityOperation::on_id(joe_id)
                    .assert("person/name", "Joe")
                    .assert("person/likes", "Ice cream"),
            ),
        );

        // [:find ?likes
        //  :where [?joe_id :person/likes ?likes]]
        let query_result = sut.query_at_snapshot(
            first_tx_result.tx_id,
            Query::new().find(Find::variable("?likes")).with(
                Clause::new()
                    .with_entity(Pattern::Constant(joe_id))
                    .with_attribute(Pattern::ident("person/likes"))
                    .with_value(Pattern::variable("?likes")),
            ),
        );

        assert_that!(
            query_result,
            unordered_elements_are![elements_are![eq(Value::str("Pizza"))]]
        );
    }

    #[test]
    fn search_for_tx_pattern() {
        let mut sut = Sut::new();

        // Insert initial data
        let tx_result = sut.transact(
            Transaction::new().with(EntityOperation::on_new().assert("person/name", "Joe")),
        );

        // [:find ?tx ?tx_time
        //  :where [_ :person/name "Joe" ?tx]
        //         [?tx ?tx_time_id ?tx_time]]
        let query_result = sut.query(
            Query::new()
                .find(Find::variable("?tx"))
                .find(Find::variable("?tx_time"))
                .with(
                    Clause::new()
                        .with_attribute(Pattern::ident("person/name"))
                        .with_value(Pattern::value("Joe"))
                        .with_tx(Pattern::variable("?tx")),
                )
                .with(
                    Clause::new()
                        .with_entity(Pattern::variable("?tx"))
                        .with_attribute(Pattern::id(DB_TX_TIME_ID))
                        .with_value(Pattern::variable("?tx_time")),
                ),
        );

        assert_that!(
            query_result,
            unordered_elements_are![elements_are![
                eq(Value::Ref(tx_result.tx_id)),
                matches_pattern!(Value::U64(gt(0))),
            ]]
        );
    }

    #[test]
    fn restrict_result_by_tx() {
        let mut sut = Sut::new();

        // Insert initial data
        let tx_result = sut.transact(
            Transaction::new().with(EntityOperation::on_new().assert("person/name", "Joe")),
        );

        // Find all datoms belonging to transaction
        // [:find ?e ?a ?v
        //  :where [?e ?a ?v ?tx_id]]
        let query_result = sut.query(
            Query::new()
                .find(Find::variable("?e"))
                .find(Find::variable("?a"))
                .find(Find::variable("?v"))
                .with(
                    Clause::new()
                        .with_entity(Pattern::variable("?e"))
                        .with_attribute(Pattern::variable("?a"))
                        .with_value(Pattern::variable("?v"))
                        .with_tx(Pattern::Constant(tx_result.tx_id)),
                ),
        );

        assert_that!(
            query_result,
            unordered_elements_are![
                // person/name datom
                elements_are![anything(), anything(), eq(Value::str("Joe"))],
                // tx time datom
                elements_are![
                    eq(Value::Ref(tx_result.tx_id)),
                    eq(Value::Ref(DB_TX_TIME_ID)),
                    anything()
                ]
            ]
        );
    }

    #[test]
    fn aggregation_single_entity() {
        let mut sut = Sut::new();

        // Insert data
        // [{:person/new "John"}]
        sut.transact(
            Transaction::new().with(EntityOperation::on_new().assert("person/name", "John")),
        );

        // [:find (count)
        //  : where [?person :person/name]]
        let query_result = sut.query(
            Query::new().find(Find::count()).with(
                Clause::new()
                    .with_entity(Pattern::variable("?person"))
                    .with_attribute(Pattern::ident("person/name")),
            ),
        );

        assert_that!(
            query_result,
            unordered_elements_are![elements_are![eq(Value::U64(1))]]
        );
    }

    #[test]
    fn aggregation_with_key() {
        let mut sut = Sut::new();

        // Insert data
        sut.transact(create_beatles());

        // [:find ?born (count)
        //  :where [?person :person/born ?born]
        //         [?person :person/name ?name]]
        let query = Query::new()
            .find(Find::variable("?born"))
            .find(Find::count())
            .with(
                Clause::new()
                    .with_entity(Pattern::variable("?person"))
                    .with_attribute(Pattern::ident("person/born"))
                    .with_value(Pattern::variable("?born")),
            )
            .with(
                Clause::new()
                    .with_entity(Pattern::variable("?person"))
                    .with_attribute(Pattern::ident("person/name"))
                    .with_value(Pattern::variable("?name")),
            );

        let query_result = sut.query(query);

        assert_that!(
            query_result,
            unordered_elements_are![
                elements_are![eq(Value::I64(1940)), eq(Value::U64(2))], // John, Ringo
                elements_are![eq(Value::I64(1942)), eq(Value::U64(1))], // Paul
                elements_are![eq(Value::I64(1943)), eq(Value::U64(1))], // George
            ]
        );
    }

    #[test]
    fn aggregation_with_arbitrary_order() {
        let mut sut = Sut::new();

        // Insert data
        sut.transact(create_beatles());

        // [:find (sum ?born) ?born
        //  :where [?person :person/born ?born]
        //         [?person :person/name ?name]]
        let query = Query::new()
            .find(Find::sum("?born"))
            .find(Find::variable("?born"))
            .with(
                Clause::new()
                    .with_entity(Pattern::variable("?person"))
                    .with_attribute(Pattern::ident("person/born"))
                    .with_value(Pattern::variable("?born")),
            )
            .with(
                Clause::new()
                    .with_entity(Pattern::variable("?person"))
                    .with_attribute(Pattern::ident("person/name"))
                    .with_value(Pattern::variable("?name")),
            );

        let query_result = sut.query(query);

        assert_that!(
            query_result,
            unordered_elements_are![
                elements_are![eq(Value::I64(3880)), eq(Value::I64(1940))], // John, Ringo
                elements_are![eq(Value::I64(1942)), eq(Value::I64(1942))], // Paul
                elements_are![eq(Value::I64(1943)), eq(Value::I64(1943))], // George
            ]
        );
    }

    #[test]
    fn count_distinct_with_key() {
        let mut sut = Sut::new();

        // Insert data
        // [{:person/name "John"
        //   :person/likes ["Pizza" "Ice cream"]
        //   :person/born 1967}
        //  {:person/name "John"
        //   :person/likes ["Pizza" "Beer"]
        //   :person/born 1967}
        //  {:person/name "Mike"
        //   :person/likes "Pizza"
        //   :person/born 1967}
        //  {:person/name "James"
        //   :person/likes "Beer"
        //   :person/born 1963}]
        sut.transact(
            Transaction::new()
                .with(
                    EntityOperation::on_new()
                        .assert("person/name", "John")
                        .assert("person/likes", "Pizza")
                        .assert("person/likes", "Ice cream")
                        .assert("person/born", 1967),
                )
                .with(
                    EntityOperation::on_new()
                        .assert("person/name", "John")
                        .assert("person/likes", "Pizza")
                        .assert("person/likes", "Beer")
                        .assert("person/born", 1967),
                )
                .with(
                    EntityOperation::on_new()
                        .assert("person/name", "Mike")
                        .assert("person/likes", "Pizza")
                        .assert("person/born", 1967),
                )
                .with(
                    EntityOperation::on_new()
                        .assert("person/name", "James")
                        .assert("person/likes", "Beer")
                        .assert("person/born", 1963),
                ),
        );

        // [:find ?name (count-distinct ?likes)
        //  :where [?person :person/name ?name]
        //         [?person :person/likes ?likes]]
        let query = Query::new()
            .find(Find::variable("?name"))
            .find(Find::count_distinct("?likes"))
            .with(
                Clause::new()
                    .with_entity(Pattern::variable("?person"))
                    .with_attribute(Pattern::ident("person/name"))
                    .with_value(Pattern::variable("?name")),
            )
            .with(
                Clause::new()
                    .with_entity(Pattern::variable("?person"))
                    .with_attribute(Pattern::ident("person/likes"))
                    .with_value(Pattern::variable("?likes")),
            );

        let query_result = sut.query(query);

        assert_that!(
            query_result,
            unordered_elements_are![
                elements_are![eq(Value::str("John")), eq(Value::U64(3))],
                elements_are![eq(Value::str("Mike")), eq(Value::U64(1))],
                elements_are![eq(Value::str("James")), eq(Value::U64(1))],
            ]
        );
    }

    #[test]
    fn fail_query_when_requesting_invalid_identifiers() {
        let mut sut = Sut::new();

        // Insert data
        sut.transact(create_beatles());

        // [:find ?name
        //  :where [?person :person/born ?born]]
        let query_result = sut
            .try_query(
                Query::new().find(Find::variable("?name")).with(
                    Clause::new()
                        .with_entity(Pattern::variable("?person"))
                        .with_attribute(Pattern::ident("person/born"))
                        .with_value(Pattern::variable("?born")),
                ),
            )
            .expect("Unable to query");

        assert!(!query_result.is_empty());
        assert!(query_result
            .iter()
            .all(|result| matches!(result, Err(QueryError::InvalidFindVariable(_)))));
    }

    #[test]
    fn fail_aggregated_query_when_requesting_invalid_identifiers() {
        let mut sut = Sut::new();

        // Insert data
        sut.transact(create_beatles());

        // [:find ?name (count)
        //  :where [?person :person/born ?born]]
        let query_result = sut.try_query(
            Query::new()
                .find(Find::variable("?name"))
                .find(Find::count())
                .with(
                    Clause::new()
                        .with_entity(Pattern::variable("?person"))
                        .with_attribute(Pattern::ident("person/born"))
                        .with_value(Pattern::variable("?born")),
                ),
        );

        assert!(matches!(
            query_result,
            Err(QueryError::InvalidFindVariable(_))
        ));
    }

    #[test]
    fn support_query_predicates() {
        let mut sut = Sut::new();

        // Insert data
        sut.transact(create_beatles());

        // [:find ?name
        //  :where [?person :person/born ?born]
        //         [?person :person/name ?name]
        //         [(> ?born 1940)]]
        let query_result = sut.query(
            Query::new()
                .find(Find::variable("?name"))
                .with(
                    Clause::new()
                        .with_entity(Pattern::variable("?person"))
                        .with_attribute(Pattern::ident("person/born"))
                        .with_value(Pattern::variable("?born")),
                )
                .with(
                    Clause::new()
                        .with_entity(Pattern::variable("?person"))
                        .with_attribute(Pattern::ident("person/name"))
                        .with_value(Pattern::variable("?name")),
                )
                .value_pred("?born", |value| match value {
                    &Value::I64(born) => born > 1940,
                    _ => false,
                }),
        );

        assert_that!(
            query_result,
            unordered_elements_are![
                elements_are![eq(Value::str("Paul"))],
                elements_are![eq(Value::str("George"))],
            ]
        );
    }

    #[test]
    fn retract_facts() {
        let mut sut = Sut::new();

        // Insert data
        let tx_result = sut.transact(
            Transaction::new().with(
                EntityOperation::on_temp_id("joe")
                    .assert("person/name", "Joe")
                    .assert("person/likes", "Pizza"),
            ),
        );

        let joe_id = tx_result.temp_ids["joe"];
        // [:find ?likes
        //  :where [?joe_id :person/likes ?likes]]
        let query = Query::new().find(Find::variable("?likes")).with(
            Clause::new()
                .with_entity(Pattern::Constant(joe_id))
                .with_attribute(Pattern::ident("person/likes"))
                .with_value(Pattern::variable("?likes")),
        );

        assert_that!(
            sut.query(query.clone()),
            unordered_elements_are![elements_are![eq(Value::str("Pizza"))]]
        );

        // Retract
        sut.transact(
            Transaction::new()
                .with(EntityOperation::on_id(joe_id).retract("person/likes", "Pizza")),
        );

        assert_that!(sut.query(query), empty());
    }

    mod reject_a_transaction_with_duplicate_unique_value {
        use super::*;

        #[test]
        fn across_transactions() {
            let mut sut = Sut::new();

            sut.transact(
                Transaction::new().with(
                    EntityOperation::on_new()
                        .assert("person/name", "Alice")
                        .assert("person/email", "foo@bar.com"),
                ),
            );

            let tx_result = sut.try_transact(
                Transaction::new().with(
                    EntityOperation::on_new()
                        .assert("person/name", "Bob")
                        .assert("person/email", "foo@bar.com"),
                ),
            );

            assert!(tx_result.is_none());
        }

        #[test]
        fn within_a_transaction() {
            let mut sut = Sut::new();

            let tx_result = sut.try_transact(
                Transaction::new()
                    .with(
                        EntityOperation::on_new()
                            .assert("person/name", "Alice")
                            .assert("person/email", "foo@bar.com"),
                    )
                    .with(
                        EntityOperation::on_new()
                            .assert("person/name", "Bob")
                            .assert("person/email", "foo@bar.com"),
                    ),
            );

            assert!(tx_result.is_none());
        }
    }
}
