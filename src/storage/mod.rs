pub mod attribute_builder;
pub mod attribute_resolver;
pub mod disk;
mod iter;
pub mod memory;
pub mod restricts;
pub mod serde;

use crate::datom::*;
use crate::storage::restricts::*;

pub trait ReadStorage<'a> {
    type Error: std::error::Error;
    type Iter: Iterator<Item = Result<Datom, Self::Error>>;

    /// Returns an iterator that yields all *non-retracted* datoms that match the restircts.
    /// Iterator might fail with `Self::Error` during iteration.
    /// Ordering of datoms is not guaranteed.
    fn find(&'a self, restricts: Restricts) -> Self::Iter;

    fn latest_entity_id(&self) -> Result<u64, Self::Error>;
}

pub trait WriteStorage {
    type Error: std::error::Error;

    fn save(&mut self, datoms: &[Datom]) -> Result<(), Self::Error>;
}
