use std::collections::HashMap;
use std::sync::{Arc, RwLock, RwLockReadGuard};

use crate::datom::Value;
use crate::query::*;
use crate::storage::Storage;

pub struct Db<S: Storage> {
    storage: Arc<RwLock<S>>,
    tx: u64,
}

impl<S: Storage> Db<S> {
    pub fn new(storage: Arc<RwLock<S>>, tx: u64) -> Self {
        Db { storage, tx }
    }

    pub fn query(&self, query: Query) -> Result<QueryResult, QueryError> {
        let mut results = Vec::new();
        let assignment = Assignment::from_query(&query);
        let storage = self.storage.read().map_err(|_| QueryError::Error)?;
        self.resolve(&storage, &query.wher, assignment, &mut results)?;
        Ok(QueryResult { results })
    }

    fn resolve(
        &self,
        storage: &RwLockReadGuard<S>,
        clauses: &[Clause],
        assignment: Assignment,
        results: &mut Vec<HashMap<String, Value>>,
    ) -> Result<(), QueryError> {
        if assignment.is_complete() {
            results.push(assignment.assigned);
            return Ok(());
        }
        if let [clause, rest @ ..] = clauses {
            let assigned_clause = clause.assign(&assignment);
            let datoms = storage
                .find_datoms(&assigned_clause, self.tx)
                .map_err(|err| QueryError::StorageError(err))?;

            // TODO can this be parallelized?
            for datom in datoms {
                self.resolve(
                    storage,
                    rest,
                    assignment.update_with(&assigned_clause, datom),
                    results,
                )?;
            }
        }
        Ok(())
    }
}
