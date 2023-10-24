pub mod serde;

#[cfg(test)]
mod tests {
    use rustomic::datom::*;
    use rustomic::query::clause::*;
    use rustomic::query::pattern::*;
    use rustomic::storage::*;

    trait Storage {
        fn create() -> Self;
        fn save(&mut self, datoms: &[Datom]);
        fn find(&self, clause: &Clause) -> Vec<Datom>;
    }

    mod memory {
        use super::*;
        use rustomic::storage::memory::*;

        struct InMemory(InMemoryStorage);

        impl Storage for InMemory {
            fn create() -> Self {
                Self(InMemoryStorage::new())
            }

            fn save(&mut self, datoms: &[Datom]) {
                self.0.save(datoms).expect("should succeed")
            }

            fn find(&self, clause: &Clause) -> Vec<Datom> {
                self.0
                    .find(clause)
                    .map(|result| result.expect("should be valid"))
                    .collect()
            }
        }

        #[test]
        fn return_empty_result_if_no_datoms_match_search_criteria() {
            return_empty_result_if_no_datoms_match_search_criteria_impl::<InMemory>();
        }

        #[test]
        fn find_single_datom_by_entity_attribute_and_value() {
            find_single_datom_by_entity_attribute_and_value_impl::<InMemory>();
        }

        #[test]
        fn find_multiple_datoms_by_entity() {
            find_multiple_datoms_by_entity_impl::<InMemory>();
        }

        #[test]
        fn find_multiple_datoms_by_attribute_for_different_entity() {
            find_multiple_datoms_by_attribute_for_different_entity_impl::<InMemory>();
        }

        #[test]
        fn find_multiple_datoms_by_attribute_for_same_entity() {
            find_multiple_datoms_by_attribute_for_same_entity_impl::<InMemory>();
        }

        #[test]
        fn ignore_datoms_of_other_entities() {
            ignore_datoms_of_other_entities_impl::<InMemory>();
        }

        #[test]
        fn ignore_retracted_values() {
            ignore_retracted_values_impl::<InMemory>();
        }

        #[test]
        fn fetch_only_latest_value_for_attribute() {
            fetch_only_latest_value_for_attribute_impl::<InMemory>();
        }
    }

    mod disk {
        use super::*;
        use rocksdb::{Options, DB};
        use rustomic::storage::disk::*;
        use tempdir::TempDir;

        struct Disk(DiskStorage);

        impl Storage for Disk {
            fn create() -> Self {
                let dir = TempDir::new("rustomic").expect("Unable to create temp dir");
                let mut options = Options::default();
                options.create_if_missing(true);
                let db = DB::open(&options, dir).expect("Unable to open DB");
                Self(DiskStorage::new(db))
            }

            fn save(&mut self, datoms: &[Datom]) {
                self.0.save(datoms).expect("should succeed")
            }

            fn find(&self, clause: &Clause) -> Vec<Datom> {
                self.0
                    .find(clause)
                    .map(|result| result.expect("should be valid"))
                    .collect()
            }
        }

        #[test]
        fn return_empty_result_if_no_datoms_match_search_criteria() {
            return_empty_result_if_no_datoms_match_search_criteria_impl::<Disk>();
        }

        #[test]
        fn find_single_datom_by_entity_attribute_and_value() {
            find_single_datom_by_entity_attribute_and_value_impl::<Disk>();
        }

        #[test]
        fn find_multiple_datoms_by_entity() {
            find_multiple_datoms_by_entity_impl::<Disk>();
        }

        #[test]
        fn find_multiple_datoms_by_attribute_for_different_entity() {
            find_multiple_datoms_by_attribute_for_different_entity_impl::<Disk>();
        }

        #[test]
        fn find_multiple_datoms_by_attribute_for_same_entity() {
            find_multiple_datoms_by_attribute_for_same_entity_impl::<Disk>();
        }

        #[test]
        fn ignore_datoms_of_other_entities() {
            ignore_datoms_of_other_entities_impl::<Disk>();
        }

        #[test]
        fn ignore_retracted_values() {
            ignore_retracted_values_impl::<Disk>();
        }

        #[test]
        fn fetch_only_latest_value_for_attribute() {
            fetch_only_latest_value_for_attribute_impl::<Disk>();
        }
    }

    fn return_empty_result_if_no_datoms_match_search_criteria_impl<S: Storage>() {
        let storage = S::create();

        let entity = 100;
        let clause = Clause::new().with_entity(EntityPattern::Id(entity));
        let read_result = storage.find(&clause);

        assert!(read_result.is_empty());
    }

    fn find_single_datom_by_entity_attribute_and_value_impl<S: Storage>() {
        let mut storage = S::create();

        let entity = 100;
        let attribute = 101;
        let value = 102;
        let tx = 103;

        let datoms = vec![Datom::add(entity, attribute, value, tx)];
        storage.save(&datoms);

        let read_result = storage.find(
            &Clause::new()
                .with_entity(EntityPattern::Id(entity))
                .with_attribute(AttributePattern::Id(attribute))
                .with_value(ValuePattern::Constant(Value::U64(value))),
        );

        assert_eq!(datoms, read_result);
    }

    fn find_multiple_datoms_by_entity_impl<S: Storage>() {
        let mut storage = S::create();

        let entity = 100;
        let tx = 1000;
        let datoms = vec![
            Datom::add(entity, 101, 1u64, tx),
            Datom::add(entity, 102, 2u64, tx),
        ];
        storage.save(&datoms);

        let read_result = storage.find(&Clause::new().with_entity(EntityPattern::Id(entity)));

        assert_eq!(datoms, read_result);
    }

    fn find_multiple_datoms_by_attribute_for_different_entity_impl<S: Storage>() {
        let mut storage = S::create();

        let entity1 = 100;
        let entity2 = 101;
        let attribute1 = 102;
        let attribute2 = 103;
        let datoms = vec![
            Datom::add(entity1, attribute1, 1u64, 1000),
            Datom::retract(entity1, attribute1, 1u64, 1001),
            Datom::add(entity1, attribute1, 2u64, 1001),
            Datom::add(entity2, attribute1, 1u64, 1002),
            Datom::add(entity2, attribute2, 2u64, 1002),
            Datom::add(entity2, attribute2, 3u64, 1002),
        ];
        storage.save(&datoms);

        let read_result =
            storage.find(&Clause::new().with_attribute(AttributePattern::Id(attribute1)));

        let expected = vec![
            Datom::add(entity1, attribute1, 2u64, 1001),
            Datom::add(entity2, attribute1, 1u64, 1002),
        ];
        assert_eq!(2, read_result.len());
        assert!(expected.iter().all(|datom| read_result.contains(datom)));
    }

    fn find_multiple_datoms_by_attribute_for_same_entity_impl<S: Storage>() {
        let mut storage = S::create();

        let entity = 100;
        let attribute1 = 101;
        let attribute2 = 102;
        let attribute3 = 103;
        let datoms = vec![
            Datom::add(entity, attribute1, 1u64, 1000),
            Datom::add(entity, attribute2, 2u64, 1000),
            Datom::add(entity, attribute3, 3u64, 1001),
        ];
        storage.save(&datoms);

        let read_result = storage.find(&Clause::new().with_entity(EntityPattern::Id(entity)));

        assert_eq!(datoms, read_result);
    }

    fn ignore_datoms_of_other_entities_impl<S: Storage>() {
        let mut storage = S::create();

        let entity1 = 100;
        let entity2 = 101;
        let attribute = 102;
        let tx = 1000;
        let datoms = vec![
            Datom::add(entity1, attribute, 1u64, tx),
            Datom::add(entity2, attribute, 2u64, tx),
        ];
        storage.save(&datoms);

        let read_result = storage.find(&Clause::new().with_entity(EntityPattern::Id(entity1)));

        assert_eq!(datoms[0..1], read_result);
    }

    fn ignore_retracted_values_impl<S: Storage>() {
        let mut storage = S::create();

        let entity = 100;
        let attribute = 101;
        let datoms = vec![
            // Add value 1 in tx 1000
            Datom::add(entity, attribute, 1u64, 1000),
            // Retract value 1 in tx 1001
            Datom::retract(entity, attribute, 1u64, 1001),
        ];
        storage.save(&datoms);

        let read_result = storage.find(
            &Clause::new()
                .with_entity(EntityPattern::Id(entity))
                .with_attribute(AttributePattern::Id(attribute)),
        );

        assert!(read_result.is_empty());
    }

    fn fetch_only_latest_value_for_attribute_impl<S: Storage>() {
        let mut storage = S::create();

        let entity = 100;
        let attribute = 101;
        let datoms = vec![
            // Add value 1 in tx 1000
            Datom::add(entity, attribute, 1u64, 1000),
            // Replace value 1 with 2 in tx 1001
            Datom::retract(entity, attribute, 1u64, 1001),
            Datom::add(entity, attribute, 2u64, 1001),
        ];
        storage.save(&datoms);

        let read_result = storage.find(
            &Clause::new()
                .with_entity(EntityPattern::Id(entity))
                .with_attribute(AttributePattern::Id(attribute)),
        );

        assert_eq!(datoms[2..], read_result);
    }
}
