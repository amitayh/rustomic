pub mod attribute_resolver;
pub mod disk;
pub mod memory;
pub mod memory2;
pub mod serde;

use crate::datom::*;
use crate::query::clause::Clause;

pub trait ReadStorage<'a> {
    type Error: std::error::Error;
    type Iter: Iterator<Item = Result<Datom, Self::Error>>;

    fn find(&'a self, clause: &Clause) -> Self::Iter;
}

pub trait WriteStorage {
    type Error: std::error::Error;

    fn save(&mut self, datoms: &[Datom]) -> Result<(), Self::Error>;
}
