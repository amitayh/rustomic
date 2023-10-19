use std::collections::btree_set::Range;
use std::collections::BTreeSet;
use std::fmt::Debug;

use crate::storage::*;

use thiserror::Error;

pub struct InMemoryStorage {
    // The EAVT index provides efficient access to everything about a given entity. Conceptually
    // this is very similar to row access style in a SQL database, except that entities can possess
    // arbitrary attributes rather than being limited to a predefined set of columns.
    //
    // The example below shows all of the facts about entity 42 grouped together:
    //
    //   +----+----------------+------------------------+------+--------+
    //   | E  | A              | V                      | Tx   | Op    |
    //   +----+----------------+------------------------+------+--------+
    //   | 41 | release/name   | "Abbey Road"           | 1100 | Added |
    // * | 42 | release/name   | "Magical Mystery Tour" | 1007 | Added |
    // * | 42 | release/year   | 1967                   | 1007 | Added |
    // * | 42 | release/artist | "The Beatles"          | 1007 | Added |
    //   | 43 | release/name   | "Let It Be"            | 1234 | Added |
    //   +----+----------------+------------------------+------+--------+
    //
    // EAVT is also useful in master or detail lookups, since the references to detail entities are
    // just ordinary versus alongside the scalar attributes of the master entity. Better still,
    // Datomic assigns entity ids so that when master and detail records are created in the same
    // transaction, they are colocated in EAVT.

    // The AEVT index provides efficient access to all values for a given attribute, comparable to
    // the traditional column access style. In the table below, notice how all release/name
    // attributes are grouped together. This allows Datomic to efficiently query for all values of
    // the release/name attribute, because they reside next to one another in this index.
    //
    //   +----------------+----+------------------------+------+--------+
    //   | A              | E  | V                      | Tx   | Op    |
    //   +----------------+----+------------------------+------+--------+
    //   | release/artist | 42 | "The Beatles"          | 1007 | Added |
    // * | release/name   | 41 | "Abbey Road"           | 1100 | Added |
    // * | release/name   | 42 | "Magical Mystery Tour" | 1007 | Added |
    // * | release/name   | 43 | "Let It Be"            | 1234 | Added |
    //   | release/year   | 42 | 1967                   | 1007 | Added |
    //   +----------------+----+------------------------+------+--------+

    // The AVET index provides efficient access to particular combinations of attribute and value.
    // The example below shows a portion of the AVET index allowing lookup by release/names.
    //
    // The AVET index is more expensive to maintain than other indexes, and as such it is the only
    // index that is not enabled by default. To maintain AVET for an attribute, specify db/index
    // true (or some value for db/unique) when installing or altering the attribute.
    //
    //   +----------------+------------------------+----+------+--------+
    //   | A              | V                      | E  | Tx   | Op    |
    //   +----------------+------------------------+----+------+--------+
    //   | release/name   | "Abbey Road"           | 41 | 1100 | Added |
    // * | release/name   | "Let It Be"            | 43 | 1234 | Added |
    // * | release/name   | "Let It Be"            | 55 | 2367 | Added |
    //   | release/name   | "Magical Mystery Tour" | 42 | 1007 | Added |
    //   | release/year   | 1967                   | 42 | 1007 | Added |
    //   | release/year   | 1984                   | 55 | 2367 | Added |
    //   +----------------+------------------------+----+------+--------+
    index: BTreeSet<Vec<u8>>,
}

impl Debug for InMemoryStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut list = f.debug_list();
        for datom_bytes in &self.index {
            let datom = serde::datom::deserialize(datom_bytes).or(Err(std::fmt::Error))?;
            match datom_bytes.first() {
                Some(&serde::index::TAG_EAVT) => {
                    list.entry(&format!(
                        "EAVT {{ e: {:?}, a: {:?}, v: {:?}, t: {:?}, op: {:?} }}",
                        datom.entity, datom.attribute, datom.value, datom.tx, datom.op
                    ));
                }
                Some(&serde::index::TAG_AEVT) => {
                    list.entry(&format!(
                        "AEVT {{ a: {:?}, e: {:?}, v: {:?}, t: {:?}, op: {:?} }}",
                        datom.attribute, datom.entity, datom.value, datom.tx, datom.op
                    ));
                }
                Some(&serde::index::TAG_AVET) => {
                    list.entry(&format!(
                        "AVET {{ a: {:?}, v: {:?}, e: {:?}, t: {:?}, op: {:?} }}",
                        datom.attribute, datom.value, datom.entity, datom.tx, datom.op
                    ));
                }
                _ => (),
            }
        }
        list.finish()
    }
}

impl InMemoryStorage {
    pub fn new() -> Self {
        Self {
            index: BTreeSet::new(),
        }
    }
}

#[derive(Debug, Error)]
#[error("error")]
pub enum InMemoryStorageError {
    #[error("read error")]
    ReadError(#[from] serde::ReadError),
}

//-------------------------------------------------------------------------------------------------

impl WriteStorage for InMemoryStorage {
    type Error = InMemoryStorageError;

    fn save(&mut self, datoms: &[Datom]) -> Result<(), Self::Error> {
        // TODO reserve capacity ahead of insertion?
        for datom in datoms {
            self.index.insert(serde::datom::serialize::eavt(datom));
            self.index.insert(serde::datom::serialize::aevt(datom));
            self.index.insert(serde::datom::serialize::avet(datom));
        }
        Ok(())
    }
}

//-------------------------------------------------------------------------------------------------

impl<'a> ReadStorage<'a> for InMemoryStorage {
    type Error = InMemoryStorageError;

    type Iter = InMemoryStorageIter<'a>;

    fn find(&'a self, clause: &Clause) -> Self::Iter {
        InMemoryStorageIter::new(&self.index, clause)
    }
}

pub struct InMemoryStorageIter<'a> {
    index: &'a BTreeSet<Vec<u8>>,
    range: Range<'a, Vec<u8>>,
    end: Vec<u8>,
}

impl<'a> InMemoryStorageIter<'a> {
    fn new(index: &'a BTreeSet<Vec<u8>>, clause: &Clause) -> Self {
        let start = serde::index::key(clause);
        let end = serde::index::next_prefix(&start);
        Self {
            index: &index,
            range: index.range(start..),
            end,
        }
    }
}

impl<'a> Iterator for InMemoryStorageIter<'a> {
    type Item = Result<Datom, InMemoryStorageError>;

    fn next(&mut self) -> Option<Self::Item> {
        let datom_bytes = self.range.next()?;
        if datom_bytes >= &self.end {
            return None;
        }
        match serde::datom::deserialize(datom_bytes) {
            Ok(datom) if datom.op == Op::Retracted => {
                let seek_key_size = serde::index::seek_key_size(&datom);
                let seek_key = serde::index::next_prefix(&datom_bytes[..seek_key_size]);
                self.range = self.index.range(seek_key..);
                self.next()
            }
            Ok(datom) => Some(Ok(datom)),
            Err(err) => Some(Err(InMemoryStorageError::ReadError(err))),
        }
    }
}

//-------------------------------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use crate::datom::*;
    use crate::query::clause::*;
    use crate::query::pattern::*;
    use crate::storage::memory2::*;

    fn create_storage() -> InMemoryStorage {
        InMemoryStorage::new()
    }

    #[test]
    fn return_empty_result_if_no_datoms_match_search_criteria() {
        let storage = create_storage();

        let entity = 100;
        let clause = Clause::new().with_entity(EntityPattern::Id(entity));
        let read_result = storage.find(&clause);

        assert!(read_result.collect::<Vec<_>>().is_empty());
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
    fn find_multiple_datoms_by_entity() {
        let mut storage = create_storage();

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
    fn find_multiple_datoms_by_attribute_for_different_entity() {
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

    //#[test]
    //fn find_multiple_datoms_by_attribute_for_same_entity() {
    //    let mut storage = create_storage();

    //    let entity = 100;
    //    let attribute1 = 101;
    //    let attribute2 = 102;
    //    let attribute3 = 103;
    //    let datoms = vec![
    //        Datom::add(entity, attribute1, 1u64, 1000),
    //        Datom::add(entity, attribute2, 2u64, 1000),
    //        Datom::add(entity, attribute3, 3u64, 1001),
    //    ];
    //    assert!(storage.save(&datoms).is_ok());

    //    let read_result =
    //        storage.find(&Clause::new().with_entity(EntityPattern::Id(entity)));

    //    assert_eq!(
    //        datoms,
    //        read_result
    //            .map(|result| result.unwrap())
    //            .collect::<Vec<_>>());
    //}

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

        let read_result = storage.find(&Clause::new().with_entity(EntityPattern::Id(entity1)));

        assert_eq!(
            datoms[0..1],
            read_result
                .map(|result| result.unwrap())
                .collect::<Vec<_>>()
        );
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

        let read_result = storage.find(
            &Clause::new()
                .with_entity(EntityPattern::Id(entity))
                .with_attribute(AttributePattern::Id(attribute)),
        );

        assert!(read_result.collect::<Vec<_>>().is_empty());
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
}
