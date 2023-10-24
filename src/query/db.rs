use std::collections::HashMap;

use crate::datom::Value;
use crate::query::assignment::*;
use crate::query::clause::*;
use crate::query::*;
use crate::storage::attribute_resolver::*;
use crate::storage::*;

use super::pattern::AttributePattern;

pub struct Db {
    tx: u64,
    attribute_resolver: AttributeResolver,
}

impl Db {
    pub fn new(tx: u64) -> Self {
        Self {
            tx,
            attribute_resolver: AttributeResolver::new(),
        }
    }

    pub fn query<'a, S: ReadStorage<'a>>(
        &mut self,
        storage: &'a S,
        mut query: Query,
    ) -> Result<QueryResult, QueryError<S::Error>> {
        let mut results = Vec::new();
        self.resolve_idents(storage, &mut query)?;
        let assignment = Assignment::from_query(&query);
        self.resolve(storage, &query.wher, assignment, &mut results)?;
        Ok(QueryResult { results })
    }

    fn resolve_idents<'a, S: ReadStorage<'a>>(
        &mut self,
        storage: &'a S,
        query: &mut Query,
    ) -> Result<(), QueryError<S::Error>> {
        for clause in &mut query.wher {
            if let AttributePattern::Ident(ident) = &clause.attribute {
                let attribute = self.attribute_resolver.resolve_ident(storage, ident)?;
                let attribute =
                    attribute.ok_or_else(|| QueryError::IdentNotFound(Rc::clone(ident)))?;
                clause.attribute = AttributePattern::Id(attribute.id);
            }
        }
        Ok(())
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
        if let [clause, rest @ ..] = clauses {
            let assigned_clause = clause.assign(&assignment);
            // TODO: optimize filtering in storage layer?
            let datoms = storage
                .find(&assigned_clause)
                .filter(|datom| datom.as_ref().map_or(false, |datom| datom.tx <= self.tx));

            // TODO can this be parallelized?
            for datom in datoms {
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
