pub mod disk;
pub mod serde;

#[cfg(test)]
mod tests {
    use rocksdb::{Options, DB};
    use rustomic::datom::*;
    use rustomic::query::clause::*;
    use rustomic::query::pattern::*;
    use rustomic::storage::disk::*;
    use rustomic::storage::memory::*;
    use rustomic::storage::*;
    use tempdir::TempDir;

    fn in_memory_storage() -> InMemoryStorage {
        InMemoryStorage::new()
    }
    fn disk_storage() -> DiskStorage {
        let dir = TempDir::new("rustomic").expect("Unable to create temp dir");
        let mut options = Options::default();
        options.create_if_missing(true);
        let db = DB::open(&options, dir).expect("Unable to open DB");
        DiskStorage::new(db)
    }

    fn return_empty_result_if_no_datoms_match_search_criteria_impl<'a, S: ReadStorage<'a>>(
        storage: &'a S,
    ) {
        let entity = 100;
        let clause = Clause::new().with_entity(EntityPattern::Id(entity));
        let read_result = storage.find(&clause);

        assert!(read_result.collect::<Vec<_>>().is_empty());
    }

    #[test]
    fn return_empty_result_if_no_datoms_match_search_criteria() {
        return_empty_result_if_no_datoms_match_search_criteria_impl(&in_memory_storage());
        return_empty_result_if_no_datoms_match_search_criteria_impl(&disk_storage());
    }

    fn _find_single_datom_by_entity_attribute_and_value<'a, S: ReadStorage<'a> + WriteStorage>(
        storage: &'a mut S,
    ) {
        let entity = 100;
        let attribute = 101;
        let value = 102;
        let tx = 103;

        let datoms = vec![Datom::add(entity, attribute, value, tx)];
        assert!(storage.save(&datoms).is_ok());

        let read_result = storage.find(
            &Clause::new()
                .with_entity(EntityPattern::Id(entity))
                .with_attribute(AttributePattern::Id(attribute))
                .with_value(ValuePattern::Constant(Value::U64(value))),
        );

        assert_eq!(
            datoms,
            read_result
                .map(|result| result.unwrap())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn find_single_datom_by_entity_attribute_and_value() {
        _find_single_datom_by_entity_attribute_and_value(&mut in_memory_storage());
        _find_single_datom_by_entity_attribute_and_value(&mut disk_storage());
    }

    fn find_multiple_datoms_by_entity_impl<'a, S: ReadStorage<'a> + WriteStorage>(
        storage: &'a mut S,
    ) {
        let entity = 100;
        let tx = 1000;
        let datoms = vec![
            Datom::add(entity, 101, 1u64, tx),
            Datom::add(entity, 102, 2u64, tx),
        ];
        assert!(storage.save(&datoms).is_ok());

        let read_result = storage.find(&Clause::new().with_entity(EntityPattern::Id(entity)));

        assert_eq!(
            datoms,
            read_result
                .map(|result| result.unwrap())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn find_multiple_datoms_by_entity() {
        find_multiple_datoms_by_entity_impl(&mut in_memory_storage());
        find_multiple_datoms_by_entity_impl(&mut disk_storage());
    }

    fn find_multiple_datoms_by_attribute_for_different_entity_impl<
        'a,
        S: ReadStorage<'a> + WriteStorage,
    >(
        storage: &'a mut S,
    ) {
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
        assert!(storage.save(&datoms).is_ok());

        let read_result =
            storage.find(&Clause::new().with_attribute(AttributePattern::Id(attribute1)));

        let read_result = read_result
            .map(|result| result.unwrap())
            .collect::<Vec<_>>();
        let expected = vec![
            Datom::add(entity1, attribute1, 2u64, 1001),
            Datom::add(entity2, attribute1, 1u64, 1002),
        ];
        assert_eq!(2, read_result.len());
        assert!(expected.iter().all(|datom| read_result.contains(datom)));
    }

    #[test]
    fn find_multiple_datoms_by_attribute_for_different_entity() {
        find_multiple_datoms_by_attribute_for_different_entity_impl(&mut in_memory_storage());
        find_multiple_datoms_by_attribute_for_different_entity_impl(&mut disk_storage());
    }

    fn find_multiple_datoms_by_attribute_for_same_entity_impl<
        'a,
        S: ReadStorage<'a> + WriteStorage,
    >(
        storage: &'a mut S,
    ) {
        let entity = 100;
        let attribute1 = 101;
        let attribute2 = 102;
        let attribute3 = 103;
        let datoms = vec![
            Datom::add(entity, attribute1, 1u64, 1000),
            Datom::add(entity, attribute2, 2u64, 1000),
            Datom::add(entity, attribute3, 3u64, 1001),
        ];
        assert!(storage.save(&datoms).is_ok());

        let read_result = storage.find(&Clause::new().with_entity(EntityPattern::Id(entity)));

        assert_eq!(
            datoms,
            read_result
                .map(|result| result.unwrap())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn find_multiple_datoms_by_attribute_for_same_entity() {
        find_multiple_datoms_by_attribute_for_same_entity_impl(&mut in_memory_storage());
        find_multiple_datoms_by_attribute_for_same_entity_impl(&mut disk_storage());
    }

    fn ignore_datoms_of_other_entities_impl<'a, S: ReadStorage<'a> + WriteStorage>(
        storage: &'a mut S,
    ) {
        let entity1 = 100;
        let entity2 = 101;
        let attribute = 102;
        let tx = 1000;
        let datoms = vec![
            Datom::add(entity1, attribute, 1u64, tx),
            Datom::add(entity2, attribute, 2u64, tx),
        ];
        assert!(storage.save(&datoms).is_ok());

        let read_result = storage.find(&Clause::new().with_entity(EntityPattern::Id(entity1)));

        assert_eq!(
            datoms[0..1],
            read_result
                .map(|result| result.unwrap())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn ignore_datoms_of_other_entities() {
        ignore_datoms_of_other_entities_impl(&mut in_memory_storage());
        ignore_datoms_of_other_entities_impl(&mut disk_storage());
    }

    fn ignore_retracted_values_impl<'a, S: ReadStorage<'a> + WriteStorage>(storage: &'a mut S) {
        let entity = 100;
        let attribute = 101;
        let datoms = vec![
            // Add value 1 in tx 1000
            Datom::add(entity, attribute, 1u64, 1000),
            // Retract value 1 in tx 1001
            Datom::retract(entity, attribute, 1u64, 1001),
        ];
        assert!(storage.save(&datoms).is_ok());

        let read_result = storage.find(
            &Clause::new()
                .with_entity(EntityPattern::Id(entity))
                .with_attribute(AttributePattern::Id(attribute)),
        );

        assert!(read_result.collect::<Vec<_>>().is_empty());
    }

    #[test]
    fn ignore_retracted_values() {
        ignore_retracted_values_impl(&mut in_memory_storage());
        ignore_retracted_values_impl(&mut disk_storage());
    }

    fn fetch_only_latest_value_for_attribute_impl<'a, S: ReadStorage<'a> + WriteStorage>(
        storage: &'a mut S,
    ) {
        let entity = 100;
        let attribute = 101;
        let datoms = vec![
            // Add value 1 in tx 1000
            Datom::add(entity, attribute, 1u64, 1000),
            // Replace value 1 with 2 in tx 1001
            Datom::retract(entity, attribute, 1u64, 1001),
            Datom::add(entity, attribute, 2u64, 1001),
        ];
        assert!(storage.save(&datoms).is_ok());

        let read_result = storage.find(
            &Clause::new()
                .with_entity(EntityPattern::Id(entity))
                .with_attribute(AttributePattern::Id(attribute)),
        );

        assert_eq!(
            datoms[2..],
            read_result
                .map(|result| result.unwrap())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn fetch_only_latest_value_for_attribute() {
        fetch_only_latest_value_for_attribute_impl(&mut in_memory_storage());
        fetch_only_latest_value_for_attribute_impl(&mut disk_storage());
    }
}
