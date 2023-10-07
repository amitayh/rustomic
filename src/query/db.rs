use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;

use crate::datom::Value;
use crate::query::assignment::*;
use crate::query::clause::*;
use crate::query::*;
use crate::storage::ReadStorage;

use super::pattern::TxPattern;

pub struct Db<'a, S: ReadStorage<'a>> {
    storage: Arc<S>,
    tx: u64,
    _marker: PhantomData<&'a S>,
}

impl<'a, S: ReadStorage<'a>> Db<'a, S> {
    pub fn new(storage: Arc<S>, tx: u64) -> Self {
        Db { storage, tx, _marker: PhantomData }
    }

    pub fn query(&'a self, query: Query) -> Result<QueryResult, QueryError> {
        let mut results = Vec::new();
        let assignment = Assignment::from_query(&query);
        self.resolve(&query.wher, assignment, &mut results)?;
        Ok(QueryResult { results })
    }

    fn resolve(
        &'a self,
        clauses: &[Clause],
        assignment: Assignment,
        results: &mut Vec<HashMap<Rc<str>, Value>>,
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
            let datoms = self.storage.find(&assigned_clause).unwrap(); // TODO

            // TODO can this be parallelized?
            for datom in datoms {
                self.resolve(
                    rest,
                    assignment.update_with(&assigned_clause, datom),
                    results,
                )?;
            }
        }
        Ok(())
    }
}
