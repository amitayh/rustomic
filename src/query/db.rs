use std::collections::HashMap;

use crate::datom::Value;
use crate::query::assignment::*;
use crate::query::clause::*;
use crate::query::*;
use crate::storage::attribute_resolver::*;
use crate::storage::*;

use super::pattern::AttributePattern;
use super::pattern::TxPattern;

pub struct Db {
    tx: u64,
    attribute_resolver: CachingAttributeResolver,
}

impl Db {
    pub fn new(tx: u64) -> Self {
        Self {
            tx,
            attribute_resolver: CachingAttributeResolver::new(),
        }
    }

    pub fn query<'a, S: ReadStorage<'a>>(
        &mut self,
        storage: &'a S,
        query: Query,
    ) -> Result<QueryResult, QueryError<S::Error>> {
        let mut results = Vec::new();
        let query = self.resolve_idents(storage, query)?;
        let assignment = Assignment::from_query(&query);
        self.resolve(storage, &query.wher, assignment, &mut results)?;
        Ok(QueryResult { results })
    }

    fn resolve_idents<'a, S: ReadStorage<'a>>(
        &mut self,
        storage: &'a S,
        query: Query,
    ) -> Result<Query, QueryError<S::Error>> {
        let mut wher = Vec::with_capacity(query.wher.len());
        for mut clause in query.wher {
            if let AttributePattern::Ident(ident) = &clause.attribute {
                let attribute = self.attribute_resolver.resolve_ident(storage, &ident)?;
                let attribute =
                    attribute.ok_or_else(|| QueryError::IdentNotFound(Rc::clone(ident)))?;
                clause.attribute = AttributePattern::Id(attribute.id);
            }
            wher.push(clause);
        }
        Ok(Query { wher })
    }

    fn resolve<'a, S: ReadStorage<'a>>(
        &self,
        storage: &'a S,
        clauses: &[Clause],
        assignment: Assignment,
        results: &mut Vec<HashMap<Rc<str>, Value>>,
    ) -> Result<(), QueryError<S::Error>> {
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
            // TODO can this be parallelized?
            for datom in storage.find(&assigned_clause) {
                self.resolve(
                    storage,
                    rest,
                    assignment.update_with(&assigned_clause, datom?),
                    results,
                )?;
            }
        }
        Ok(())
    }
}
