use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::datom::Value;
use crate::query::*;
use crate::storage::Storage;

pub struct Db<S: Storage> {
    storage: Arc<RwLock<S>>,
}

impl<S: Storage> Db<S> {
    pub fn new(storage: Arc<RwLock<S>>) -> Self {
        Db { storage }
    }

    pub fn query(&self, query: Query) -> Result<QueryResult, QueryError> {
        let mut results = Vec::new();
        let assignment = Assignment::from_query(&query);
        self.resolve(&query.wher, assignment, &mut results)?;
        Ok(QueryResult { results })
    }

    fn resolve(
        &self,
        clauses: &[Clause],
        assignment: Assignment,
        results: &mut Vec<HashMap<String, Value>>,
    ) -> Result<usize, QueryError> {
        if assignment.is_complete() {
            results.push(assignment.assigned);
            return Ok(1);
        }
        let mut assignments = 0;
        if let [clause, rest @ ..] = clauses {
            let assigned_clause = clause.assign(&assignment);
            let storage = self.storage.read().map_err(|_| QueryError::Error)?;
            let datoms = storage
                .find_datoms(&assigned_clause)
                .map_err(|err| QueryError::StorageError(err))?;

            // TODO can this be parallelized?
            for datom in datoms {
                assignments += self.resolve(
                    rest,
                    assignment.update_with(&assigned_clause, datom),
                    results,
                )?;
            }
        }
        Ok(assignments)
    }
}
