use std::collections::HashMap;

use crate::datom::Value;
use crate::query::*;
use crate::storage::Storage;

pub struct Db<'a, S: Storage> {
    storage: &'a S,
}

impl<'a, S: Storage> Db<'a, S> {
    pub fn new(storage: &'a S) -> Self {
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
            let datoms = self
                .storage
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
