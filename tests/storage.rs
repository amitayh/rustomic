extern crate rustomic;

use rustomic::datom::*;
use rustomic::query::*;
use rustomic::storage::*;

#[test]
fn read_datoms_by_entity_which_does_not_exist() {
    let storage = InMemoryStorage::new();

    let entity = 100;
    let tx = 101;

    let clause = Clause::new().with_entity(EntityPattern::Id(entity));
    let read_result = storage.find_datoms(&clause, tx);

    assert!(read_result.is_ok());
    assert!(read_result.unwrap().is_empty());
}

#[test]
fn read_datoms_by_entity() {
    let mut storage = InMemoryStorage::new();

    let entity = 100;
    let tx = 101;
    let attribute1 = 103;
    let attribute2 = 104;
    let datoms = vec![
        Datom::new(entity, attribute1, 1u64, tx),
        Datom::new(entity, attribute2, 2u64, tx),
    ];
    let save_result = storage.save(&datoms);
    assert!(save_result.is_ok());

    let clause = Clause::new().with_entity(EntityPattern::Id(entity));
    let read_result = storage.find_datoms(&clause, tx);
    assert!(read_result.is_ok());
    assert_eq!(datoms, read_result.unwrap());
}

#[test]
fn retract_values() {
    let mut storage = InMemoryStorage::new();

    let entity = 100;
    let attribute = 103;
    let datoms = vec![
        Datom::new(entity, attribute, 1u64, 1000),
        Datom::retract(entity, attribute, 1u64, 1001),
    ];
    let save_result = storage.save(&datoms);
    assert!(save_result.is_ok());

    let clause = Clause::new().with_entity(EntityPattern::Id(entity));
    let read_result = storage.find_datoms(&clause, 1001);
    assert!(read_result.is_ok());
    assert!(read_result.unwrap().is_empty());
}
