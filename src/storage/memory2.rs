use std::collections::BTreeSet;

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

impl InMemoryStorage {
    pub fn new() -> Self {
        Self {
            index: BTreeSet::new(),
        }
    }
}

#[derive(Debug, Error)]
#[error("error")]
pub struct InMemoryStorageError;

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

fn satisfies(clause: &Clause, datom: &Datom) -> bool {
    let mut result = true;
    if let EntityPattern::Id(entity) = clause.entity {
        result &= datom.entity == entity;
    }
    if let AttributePattern::Id(attribute) = clause.attribute {
        result &= datom.attribute == attribute;
    }
    if let ValuePattern::Constant(value) = &clause.value {
        result &= &datom.value == value;
    }
    result
}

impl<'a> ReadStorage<'a> for InMemoryStorage {
    type Error = InMemoryStorageError;

    type Iter = InMemoryStorageIter;

    fn find(&'a self, clause: &Clause) -> Result<Self::Iter, Self::Error> {
        //self.index.iter()
        //    .map(|bytes| serde::datom::deserialize(bytes).unwrap())
        //    .filter(|datom| satisfies(clause, datom));

        let start = serde::index::key(clause);
        let end = serde::index::next_prefix(&start).unwrap(); // TODO
        let datoms = self
            .index
            .range(start..end)
            .map(|bytes| serde::datom::deserialize(bytes).unwrap())
            .filter(|datom| datom.op == Op::Added);

        todo!()
    }
}

pub struct InMemoryStorageIter;

impl InMemoryStorageIter {
    fn new(clause: &Clause) -> Self {
        Self
    }
}

impl Iterator for InMemoryStorageIter {
    type Item = Datom;

    fn next(&mut self) -> Option<Self::Item> {
        None
    }
}
