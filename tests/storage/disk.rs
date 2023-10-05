extern crate rustomic;

use rocksdb::{Options, DB};
use rustomic::datom::*;
use rustomic::query::clause::*;
use rustomic::query::pattern::*;
use rustomic::storage::disk::*;
use tempdir::TempDir;

fn create_storage() -> DiskStorage {
    let dir = TempDir::new("rustomic").expect("Unable to create temp dir");
    let mut options = Options::default();
    options.create_if_missing(true);
    let db = DB::open(&options, dir).expect("Unable to open DB");
    DiskStorage::new(db)
}

#[test]
fn return_empty_result_if_no_datoms_match_search_criteria() {
    let storage = create_storage();

    let entity = 100;
    let clause = Clause::new().with_entity(EntityPattern::Id(entity));
    let read_result = storage.find_datoms(&clause);

    assert!(read_result.is_ok());
    assert!(read_result.unwrap().collect::<Vec<Datom>>().is_empty());
}

#[test]
fn find_single_datom_by_entity_attribute_and_value() {
    let mut storage = create_storage();

    let entity = 100;
    let attribute = 101;
    let value = 102;
    let tx = 103;

    let datoms = vec![Datom::add(entity, attribute, value, tx)];
    assert!(storage.save(&datoms).is_ok());

    let read_result = storage.find_datoms(
        &Clause::new()
            .with_entity(EntityPattern::Id(entity))
            .with_attribute(AttributePattern::Id(attribute))
            .with_value(ValuePattern::Constant(Value::U64(value))),
    );

    assert!(read_result.is_ok());
    assert_eq!(datoms, read_result.unwrap().collect::<Vec<Datom>>());
}

#[test]
fn find_multiple_datoms_by_entity() {
    let mut storage = create_storage();

    let entity = 100;
    let tx = 1000;
    let datoms = vec![
        Datom::add(entity, 101, 1u64, tx),
        Datom::add(entity, 102, 2u64, tx),
    ];
    assert!(storage.save(&datoms).is_ok());

    let read_result = storage.find_datoms(&Clause::new().with_entity(EntityPattern::Id(entity)));

    assert!(read_result.is_ok());
    assert_eq!(datoms, read_result.unwrap().collect::<Vec<Datom>>());
}

#[test]
fn find_multiple_datoms_by_attribute() {
    let mut storage = create_storage();

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

    let read_result = storage.find_datoms(
        &Clause::new()
            .with_attribute(AttributePattern::Id(attribute1)),
    );

    assert!(read_result.is_ok());

    let read_result = read_result.unwrap().collect::<Vec<Datom>>();
    let expected = vec![
        Datom::add(entity1, attribute1, 2u64, 1001),
        Datom::add(entity2, attribute1, 1u64, 1002),
    ];
    assert_eq!(2, read_result.len());
    assert!(expected.iter().all(|datom| read_result.contains(datom)));
}

#[test]
fn ignore_datoms_of_other_entities() {
    let mut storage = create_storage();

    let entity1 = 100;
    let entity2 = 101;
    let attribute = 102;
    let tx = 1000;
    let datoms = vec![
        Datom::add(entity1, attribute, 1u64, tx),
        Datom::add(entity2, attribute, 2u64, tx),
    ];
    assert!(storage.save(&datoms).is_ok());

    let read_result = storage.find_datoms(&Clause::new().with_entity(EntityPattern::Id(entity1)));

    assert!(read_result.is_ok());
    assert_eq!(datoms[0..1], read_result.unwrap().collect::<Vec<Datom>>());
}

#[test]
fn ignore_retracted_values() {
    let mut storage = create_storage();

    let entity = 100;
    let attribute = 101;
    let datoms = vec![
        // Add value 1 in tx 1000
        Datom::add(entity, attribute, 1u64, 1000),
        // Retract value 1 in tx 1001
        Datom::retract(entity, attribute, 1u64, 1001),
    ];
    assert!(storage.save(&datoms).is_ok());

    let read_result = storage.find_datoms(
        &Clause::new()
            .with_entity(EntityPattern::Id(entity))
            .with_attribute(AttributePattern::Id(attribute)),
    );

    assert!(read_result.is_ok());
    assert!(read_result.unwrap().collect::<Vec<Datom>>().is_empty());
}

#[test]
fn fetch_only_latest_value_for_attribute() {
    let mut storage = create_storage();

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

    let read_result = storage.find_datoms(
        &Clause::new()
            .with_entity(EntityPattern::Id(entity))
            .with_attribute(AttributePattern::Id(attribute)),
    );

    assert!(read_result.is_ok());
    assert_eq!(datoms[2..], read_result.unwrap().collect::<Vec<Datom>>());
}

/*

# EAVT
+---------+-------------+------+------+---------+
| e       | a           | v    | t    | op      |
+---------+-------------+------+------+---------+
| 100     | foo/baz     | 1    | 1001 | retract | <- seek [100 foo/baz 2]
| 100     | foo/baz     | 1    | 1000 | add     |
| 100     | foo/baz     | 2    | 1001 | add     | <- emit, next
| 101     | foo/bar     | 3    | 1002 | add     | <- end
| 101     | foo/bar     | 4    | 1002 | add     |
| 101     | foo/baz     | 1    | 1002 | add     |
| foo/bar | cardinality | many | 0    | add     |
+---------+-------------+------+------+---------+

[100 _ _]

# AEVT
+-------------+---------+------+------+---------+
| a           | e       | v    | t    | op      |
+-------------+---------+------+------+---------+
| cardinality | foo/bar | many | 0    | add     |
| foo/bar     | 101     | 3    | 1002 | add     |
| foo/bar     | 101     | 4    | 1002 | add     |
| foo/baz     | 100     | 1    | 1001 | retract | <- seek [foo/baz 100 2]
| foo/baz     | 100     | 1    | 1000 | add     |
| foo/baz     | 100     | 2    | 1001 | add     | <- emit, next
| foo/baz     | 101     | 1    | 1002 | add     | <- emit, next
+-------------+---------+------+------+---------+ <- end

[_ foo/baz _]

# AVET
+-------------+------+---------+------+---------+
| a           | v    | e       | t    | op      |
+-------------+------+---------+------+---------+
| cardinality | many | foo/bar | 0    | add     |
| foo/bar     | 3    | 101     | 1002 | add     |
| foo/bar     | 4    | 101     | 1002 | add     |
| foo/baz     | 1    | 100     | 1001 | retract | <- seek [foo/baz 1 101]
| foo/baz     | 1    | 100     | 1000 | add     |
| foo/baz     | 1    | 101     | 1002 | add     | <- emit, next
| foo/baz     | 2    | 100     | 1001 | add     | <- emit, next
+-------------+------+---------+------+---------+ <- end

[_ foo/baz _]

# ignore_datoms_of_other_entities

q: [101 foo/baz]
expected: [[101 foo/baz 1 1002]]

seek [101 foo/baz]
+---------+-------------+------+------+---------+
| e       | a           | v    | t    | op      |
+---------+-------------+------+------+---------+
| 100     | foo/baz     | 1    | 1001 | retract |
| 100     | foo/baz     | 1    | 1000 | add     |
| 100     | foo/baz     | 2    | 1001 | add     |
| 101     | foo/bar     | 3    | 1002 | add     |
| 101     | foo/bar     | 4    | 1002 | add     |
| 101     | foo/baz     | 1    | 1002 | add     | <- emit, next
| foo/bar | cardinality | many | 0    | add     | <- end
+---------+-------------+------+------+---------+

# ignore_retracted_values

# fetch_only_latest_value_for_attribute

q: [100 foo/baz]
expected: [[100 foo/baz 2 1001]]

seek [100 foo/baz]
+---------+-------------+------+------+---------+
| e       | a           | v    | t    | op      |
+---------+-------------+------+------+---------+
| 100     | foo/baz     | 1    | 1001 | retract | <- seek [100 foo/baz 2]
| 100     | foo/baz     | 1    | 1000 | add     |
| 100     | foo/baz     | 2    | 1001 | add     | <- emit, next
| 101     | foo/bar     | 3    | 1002 | add     | <- end
| 101     | foo/bar     | 4    | 1002 | add     |
| 101     | foo/baz     | 1    | 1002 | add     |
| foo/bar | cardinality | many | 0    | add     |
+---------+-------------+------+------+---------+

# fetch_only_latest_value_for_attribute

# find_multiple_datoms_by_entity

# find_multiple_datoms_by_entity

---------------------------------------------------------------------------------------------------

// ignore_datoms_of_other_entities
seek [100 "foo/bar"]
[100 "foo/bar" 1 1000 add]          -> emit
[101 "foo/bar" 2 1000 add]

// ignore_retracted_values
seek [100 "foo/bar"]
[100 "foo/bar" 1 1001 retract]      -> seek [100 "foo/bar" 2]
[100 "foo/bar" 1 1000 add]
-> done

// fetch_only_latest_value_for_attribute
seek [100 "foo/bar"]
[100 "foo/bar" 1 1001 retract]      -> seek [100 "foo/bar" 2]
[100 "foo/bar" 1 1000 add]
[100 "foo/bar" 2 1001 add]          -> emit

// fetch_only_latest_value_for_attribute
seek [100 "foo/bar"]
[100 "foo/bar" 1 1001 retract]      -> seek [100 "foo/bar" 2]
[100 "foo/bar" 1 1000 add]
[100 "foo/bar" 2 1001 add]          -> emit

seek [100 "foo/bar"]
[100 "foo/bar" 1 1001 add]          -> emit
[100 "foo/bar" 2 1001 retract]
[100 "foo/bar" 2 1000 add]

// find_multiple_datoms_by_entity
[seek 100]
[100 101 1 1000 add]                -> emit, seek [100 102]
[100 102 2 1000 add]                -> emit, seek [100 103]
-> done

// find_multiple_datoms_by_entity
[seek 100]
[100 101 1 1000 add]                -> emit, seek [100 102]
[100 102 1 1001 retract]            -> seek [100 102 2]
[100 102 2 1001 add]                -> emit, seek [100, 103]
[100 102 1 1000 add]
-> done

#[test]
fn read_datoms_by_entity() {
    let mut storage = create_storage();

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
    let read_result = storage.find_datoms(&clause);
    assert!(read_result.is_ok());
    assert_eq!(datoms, read_result.unwrap());
}

#[test]
fn retract_values() {
    let mut storage = create_storage();

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
    let read_result = storage.find_datoms(&clause);
    assert!(read_result.is_ok());
    assert!(read_result.unwrap().is_empty());
}

#[test]
fn replace_values() {
    let mut storage = create_storage();

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
    let read_result = storage.find_datoms(&clause);
    assert!(read_result.is_ok());

    let expected_result = vec![Datom::add(entity, attribute, 2u64, 1001)];
    assert_eq!(expected_result, read_result.unwrap());
}

#[test]
fn restrict_transaction() {
    let mut storage = create_storage();

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

    let clause = Clause::new()
        .with_entity(EntityPattern::Id(entity))
        .with_tx(TxPattern::range(..=1000));
    let read_result = storage.find(&clause);
    assert!(read_result.is_ok());

    let expected_result = vec![Datom::add(entity, attribute, 1u64, 1000)];
    assert_eq!(expected_result, read_result.unwrap());
}

#[test]
fn replace_values_avet() {
    let mut storage = DiskStorage::new();

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

    // 1 was retracted, should return empty result
    let clause1 = Clause::new()
        .with_attribute(AttributePattern::Id(attribute))
        .with_value(ValuePattern::constant(&Value::U64(1)));
    let read_result1 = storage.find_datoms(&clause1, 1001);
    assert!(read_result1.is_ok());
    assert!(read_result1.unwrap().is_empty());

    // 2 exists, should return in result
    let clause2 = Clause::new()
        .with_attribute(AttributePattern::Id(attribute))
        .with_value(ValuePattern::constant(&Value::U64(2)));
    let read_result2 = storage.find_datoms(&clause2, 1001);
    assert!(read_result2.is_ok());
    assert_eq!(
        vec![Datom::add(entity, attribute, 2u64, 1001)],
        read_result2.unwrap()
    );

    // Searching for range `1..`, only 2 should return
    let clause3 = Clause::new()
        .with_attribute(AttributePattern::Id(attribute))
        .with_value(ValuePattern::range(&(Value::U64(1)..)));
    let read_result3 = storage.find_datoms(&clause3, 1001);
    assert!(read_result3.is_ok());
    assert_eq!(
        vec![Datom::add(entity, attribute, 2u64, 1001)],
        read_result3.unwrap()
    );
}
*/
