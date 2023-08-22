use std::collections::HashMap;
use std::sync::{Arc, RwLock, RwLockReadGuard};

use crate::datom::Value;
use crate::query::assignment::*;
use crate::query::clause::*;
use crate::query::*;
use crate::storage::Storage;

use super::pattern::TxPattern;

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
        let tx_pattern = TxPattern::range(..=self.tx);
        if let [clause, rest @ ..] = clauses {
            let mut assigned_clause = clause.assign(&assignment); // .with_tx(tx_pattern);
            if assigned_clause.tx == TxPattern::Blank {
                assigned_clause.with_tx2(tx_pattern);
            }
            let datoms = storage.find_datoms(&assigned_clause, self.tx)?;

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
