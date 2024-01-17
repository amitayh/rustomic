pub mod serde;

#[cfg(test)]
mod tests {
    use rustomic::datom::*;
    use rustomic::storage::*;

    trait TestStorage {
        fn create() -> Self;
        fn save(&mut self, datoms: &[Datom]);
        fn find(&self, restricts: Restricts) -> Vec<Datom>;
    }

    mod memory {
        use super::*;
        use rustomic::storage::memory::*;

        struct InMemory(InMemoryStorage);

        impl TestStorage for InMemory {
            fn create() -> Self {
                Self(InMemoryStorage::new())
            }

            fn save(&mut self, datoms: &[Datom]) {
                self.0.save(datoms).expect("Unable to save datoms")
            }

            fn find(&self, restricts: Restricts) -> Vec<Datom> {
                self.0
                    .find(restricts)
                    .map(|result| result.expect("Error while reading datom"))
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
        fn find_multiple_datoms_by_tx() {
            find_multiple_datoms_by_tx_impl::<InMemory>();
        }

        //#[test]
        //fn stack_safety_test() {
        //    stack_safety_test_impl::<InMemory>();
        //}

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

        struct Disk {
            path: TempDir,
        }

        impl TestStorage for Disk {
            fn create() -> Self {
                let path = TempDir::new("rustomic").expect("Unable to create temp dir");
                let mut storage = Self { path };
                storage.save(&[]); // Ensure DB is created
                storage
            }

            fn save(&mut self, datoms: &[Datom]) {
                let mut options = Options::default();
                options.create_if_missing(true);
                let db = DB::open(&options, &self.path).expect("Unable to open DB");
                DiskStorage::new(db)
                    .save(datoms)
                    .expect("Unable to save datoms")
            }

            fn find(&self, restricts: Restricts) -> Vec<Datom> {
                let db = DB::open_for_read_only(&Options::default(), &self.path, true)
                    .expect("Unable to open DB");

                DiskStorage::new(db)
                    .find(restricts)
                    .map(|result| result.expect("Error while reading datom"))
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
        fn find_multiple_datoms_by_tx() {
            find_multiple_datoms_by_tx_impl::<Disk>();
        }

        //#[test]
        //fn stack_safety_test() {
        //    stack_safety_test_impl::<Disk>();
        //}

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

    fn return_empty_result_if_no_datoms_match_search_criteria_impl<S: TestStorage>() {
        let storage = S::create();

        let entity = 100;
        let clause = Restricts::new(0).with_entity(entity);
        let read_result = storage.find(clause);

        assert!(read_result.is_empty());
    }

    fn find_single_datom_by_entity_attribute_and_value_impl<S: TestStorage>() {
        let mut storage = S::create();

        let entity = 100;
        let attribute = 101;
        let value = 102;
        let tx = 103;

        let datoms = vec![Datom::add(entity, attribute, value, tx)];
        storage.save(&datoms);

        let read_result = storage.find(
            Restricts::new(tx)
                .with_entity(entity)
                .with_attribute(attribute)
                .with_value(Value::U64(value)),
        );

        assert_eq!(datoms, read_result);
    }

    fn find_multiple_datoms_by_entity_impl<S: TestStorage>() {
        let mut storage = S::create();

        let entity = 100;
        let tx = 1000;
        let datoms = vec![
            Datom::add(entity, 101, 1u64, tx),
            Datom::add(entity, 102, 2u64, tx),
        ];
        storage.save(&datoms);

        let read_result = storage.find(Restricts::new(tx).with_entity(entity));

        assert_eq!(datoms, read_result);
    }

    fn find_multiple_datoms_by_tx_impl<S: TestStorage>() {
        let mut storage = S::create();

        let entity = 100;
        let tx = 1000;
        let datoms = vec![
            Datom::add(entity, 101, 1u64, tx - 1),
            Datom::add(entity, 102, 2u64, tx),
            Datom::add(entity, 103, 3u64, tx + 1),
        ];
        storage.save(&datoms);

        let read_result = storage.find(Restricts::new(u64::MAX).with_tx(tx));

        assert_eq!(datoms[1..2], read_result);
    }

    //fn stack_safety_test_impl<S: TestStorage>() {
    //    let mut storage = S::create();

    //    let entity = 100;
    //    let attribute = 101;
    //    let tx = 1000;
    //
    //    let size = 2_000_000;
    //    let mut datoms = Vec::with_capacity(size + 1);
    //    for i in 0..size {
    //        datoms.push(Datom::add(entity, attribute, i as u64, tx - 1));
    //    }
    //    datoms.push(Datom::add(entity, attribute, 0, tx));
    //
    //    storage.save(&datoms);

    //    let read_result = storage.find(Restricts::new(tx).with_tx(tx));

    //    assert_eq!(datoms[size..], read_result);
    //}

    fn find_multiple_datoms_by_attribute_for_different_entity_impl<S: TestStorage>() {
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

        let read_result = storage.find(Restricts::new(u64::MAX).with_attribute(attribute1));

        let expected = vec![
            Datom::add(entity1, attribute1, 2u64, 1001),
            Datom::add(entity2, attribute1, 1u64, 1002),
        ];
        assert_eq!(2, read_result.len());
        assert!(expected.iter().all(|datom| read_result.contains(datom)));
    }

    fn find_multiple_datoms_by_attribute_for_same_entity_impl<S: TestStorage>() {
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

        let read_result = storage.find(Restricts::new(u64::MAX).with_entity(entity));

        assert_eq!(datoms, read_result);
    }

    fn ignore_datoms_of_other_entities_impl<S: TestStorage>() {
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

        let read_result = storage.find(Restricts::new(u64::MAX).with_entity(entity1));

        assert_eq!(datoms[0..1], read_result);
    }

    fn ignore_retracted_values_impl<S: TestStorage>() {
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
            Restricts::new(u64::MAX)
                .with_entity(entity)
                .with_attribute(attribute),
        );

        assert!(read_result.is_empty());
    }

    fn fetch_only_latest_value_for_attribute_impl<S: TestStorage>() {
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
            Restricts::new(u64::MAX)
                .with_entity(entity)
                .with_attribute(attribute),
        );

        assert_eq!(datoms[2..], read_result);
    }
}
