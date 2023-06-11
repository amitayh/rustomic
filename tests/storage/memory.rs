extern crate rustomic;

use rustomic::datom::*;
use rustomic::query::clause::*;
use rustomic::query::pattern::*;
use rustomic::storage::memory::*;
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
        Datom::add(entity, attribute1, 1u64, tx),
        Datom::add(entity, attribute2, 2u64, tx),
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
    let attribute = 101;
    let datoms = vec![
        // Add value 1 in tx 1000
        Datom::add(entity, attribute, 1u64, 1000),
        // Retract value 1 in tx 1001
        Datom::retract(entity, attribute, 1u64, 1001),
    ];
    let save_result = storage.save(&datoms);
    assert!(save_result.is_ok());

    let clause = Clause::new().with_entity(EntityPattern::Id(entity));
    let read_result = storage.find_datoms(&clause, 1001);
    assert!(read_result.is_ok());
    assert!(read_result.unwrap().is_empty());
}

#[test]
fn replace_values() {
    let mut storage = InMemoryStorage::new();

    let entity = 100;
    let attribute = 101;
    let datoms = vec![
        // Add value 1 in tx 1000
        Datom::add(entity, attribute, 1u64, 1000),
        // Replace value 1 with 2 in tx 1001
        Datom::retract(entity, attribute, 1u64, 1001),
        Datom::add(entity, attribute, 2u64, 1001),
    ];
    let save_result = storage.save(&datoms);
    assert!(save_result.is_ok());

    let clause = Clause::new().with_entity(EntityPattern::Id(entity));
    let read_result = storage.find_datoms(&clause, 1001);
    assert!(read_result.is_ok());

    let expected_result = vec![Datom::add(entity, attribute, 2u64, 1001)];
    assert_eq!(expected_result, read_result.unwrap());
}

#[test]
fn replace_values_avet() {
    let mut storage = InMemoryStorage::new();

    let entity = 100;
    let attribute = 101;
    let datoms = vec![
        // Add value 1 in tx 1000
        Datom::add(entity, attribute, 1u64, 1000),
        // Replace value 1 with 2 in tx 1001
        Datom::retract(entity, attribute, 1u64, 1001),
        Datom::add(entity, attribute, 2u64, 1001),
    ];
    let save_result = storage.save(&datoms);
    assert!(save_result.is_ok());

    // Force storage to use AVET index
    let clause1 = Clause::new()
        .with_attribute(AttributePattern::Id(attribute))
        .with_value(ValuePattern::constant(&Value::U64(1)));

    let read_result1 = storage.find_datoms(&clause1, 1001);
    assert!(read_result1.is_ok());
    assert!(read_result1.unwrap().is_empty());

    let clause2 = Clause::new()
        .with_attribute(AttributePattern::Id(attribute))
        .with_value(ValuePattern::constant(&Value::U64(2)));

    let read_result2 = storage.find_datoms(&clause2, 1001);
    assert!(read_result2.is_ok());
    
    let expected_result = vec![Datom::add(entity, attribute, 2u64, 1001)];
    assert_eq!(expected_result, read_result2.unwrap());
}

