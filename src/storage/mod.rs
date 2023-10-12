pub mod disk;
pub mod memory;
pub mod serde;

use crate::datom::*;
use crate::query::clause::Clause;
use thiserror::Error;

pub trait ReadStorage {
    type ReadError: std::error::Error;
    type Iter: Iterator<Item = Datom>;

    fn find(&self, clause: &Clause) -> Result<Self::Iter, Self::ReadError>;
}

pub trait WriteStorage {
    type WriteError: std::error::Error;

    // TODO: rename to `save` after previous is deprecated
    fn save(&mut self, datoms: &[Datom]) -> Result<(), Self::WriteError>;
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("ident `{0}` not found")]
    IdentNotFound(String),
}
