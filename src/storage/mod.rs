pub mod attribute_resolver;
pub mod disk;
pub mod memory;
pub mod serde;

use crate::datom::*;
use crate::query::clause::Clause;
use thiserror::Error;

pub trait ReadStorage {
    type Error: std::error::Error;
    type Iter: Iterator<Item = Datom>;

    fn find(&self, clause: &Clause) -> Result<Self::Iter, Self::Error>;
}

pub trait WriteStorage {
    type Error: std::error::Error;

    // TODO: rename to `save` after previous is deprecated
    fn save(&mut self, datoms: &[Datom]) -> Result<(), Self::Error>;
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("ident `{0}` not found")]
    IdentNotFound(String),
}
