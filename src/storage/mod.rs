pub mod disk;
pub mod memory;
pub mod serde;

use crate::datom::*;
use crate::query::clause::Clause;
use thiserror::Error;

// TODO: create structs?
type EntityId = u64;
type AttributeId = u64;
type TransactionId = u64;

pub trait WriteStorage {
    type Error: std::error::Error;
    fn save(&mut self, datoms: &[Datom]) -> Result<(), Self::Error>;
}

pub trait ReadStorage<'a> {
    type Error: std::error::Error;
    type Iter: Iterator<Item = Datom>;
    fn find(&'a self, clause: &Clause) -> Result<Self::Iter, Self::Error>;
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("ident `{0}` not found")]
    IdentNotFound(String),
}
