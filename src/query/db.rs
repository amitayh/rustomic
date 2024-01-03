use std::collections::HashMap;

use crate::datom::Value;
use crate::query::assignment::*;
use crate::query::clause::*;
use crate::query::*;
use crate::storage::attribute_resolver::*;
use crate::storage::*;

use super::pattern::AttributeIdentifier;
use super::pattern::Pattern;

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
        self.resolve(storage, &query, &query.wher, &assignment, &mut results)?;
        Ok(QueryResult { results })
    }

    fn resolve_idents<'a, S: ReadStorage<'a>>(
        &mut self,
        storage: &'a S,
        query: &mut Query,
    ) -> Result<(), QueryError<S::Error>> {
        for clause in &mut query.wher {
            if let Pattern::Constant(AttributeIdentifier::Ident(ident)) = &clause.attribute {
                let attribute = self
                    .attribute_resolver
                    .resolve(storage, ident, self.tx)?
                    .ok_or_else(|| QueryError::IdentNotFound(Rc::clone(ident)))?;

                clause.attribute = Pattern::id(attribute.id);
            }
        }
        Ok(())
    }

    fn resolve<'a, S: ReadStorage<'a>>(
        &self,
        storage: &'a S,
        query: &Query,
        patterns: &[DataPattern],
        assignment: &Assignment,
        results: &mut Vec<HashMap<Rc<str>, Value>>,
    ) -> Result<(), QueryError<S::Error>> {
        if assignment.is_complete() {
            results.push(assignment.to_owned().assigned);
            return Ok(());
        }
        if let [pattern, rest @ ..] = patterns {
            let datoms = storage.find(restricts(pattern, &assignment.assigned, self.tx));

            // TODO can this be parallelized?
            for datom in datoms {
                let assignment = assignment.update_with(pattern, datom?);
                if query.test(&assignment.assigned) {
                    self.resolve(storage, query, rest, &assignment, results)?;
                }
            }
        }
        Ok(())
    }
}

fn restricts(pattern: &DataPattern, assignment: &HashMap<Rc<str>, Value>, tx: u64) -> Restricts {
    let mut restricts = Restricts::new(tx);
    restricts.entity = match pattern.entity {
        Pattern::Constant(entity) => Some(entity),
        Pattern::Variable(ref variable) => match assignment.get(variable) {
            Some(&Value::Ref(entity)) => Some(entity),
            _ => None,
        },
        _ => None,
    };
    restricts.attribute = match pattern.attribute {
        Pattern::Constant(AttributeIdentifier::Id(attribute)) => Some(attribute),
        Pattern::Variable(ref variable) => match assignment.get(variable) {
            Some(&Value::Ref(entity)) => Some(entity),
            _ => None,
        },
        _ => None,
    };
    restricts.value = match pattern.value {
        Pattern::Constant(ref value) => Some(value.clone()),
        Pattern::Variable(ref variable) => assignment.get(variable).cloned(),
        _ => None,
    };
    restricts.tx = match pattern.tx {
        Pattern::Constant(tx) => Some(tx),
        Pattern::Variable(ref variable) => match assignment.get(variable) {
            Some(&Value::Ref(tx)) => Some(tx),
            _ => None,
        },
        _ => None,
    };
    restricts
}
