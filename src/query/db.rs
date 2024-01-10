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
    ) -> Result<impl Iterator<Item = PartialAssignment> + 'a, QueryError<S::Error>> {
        self.resolve_idents(storage, &mut query)?;
        Ok(DbIterator::new(storage, query, self.tx))
    }

    fn resolve_idents<'a, S: ReadStorage<'a>>(
        &mut self,
        storage: &'a S,
        query: &mut Query,
    ) -> Result<(), QueryError<S::Error>> {
        for clause in &mut query.wher {
            if let Pattern::Constant(AttributeIdentifier::Ident(ident)) = &clause.attribute {
                let attribute =
                    self.attribute_resolver
                        .resolve(storage, Rc::clone(ident), self.tx)?;

                clause.attribute = Pattern::id(attribute.id);
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
struct StackState {
    pattern: usize,
    assignment: Assignment,
}

struct DbIterator<'a, S: ReadStorage<'a>> {
    storage: &'a S,
    stack: Vec<StackState>,
    complete: Vec<PartialAssignment>,
    query: Query,
    tx: u64,
}

impl<'a, S: ReadStorage<'a>> DbIterator<'a, S> {
    fn new(storage: &'a S, query: Query, tx: u64) -> Self {
        let assignment = Assignment::from_query(&query);
        DbIterator {
            storage,
            stack: vec![StackState {
                pattern: 0,
                assignment,
            }],
            complete: Vec::new(),
            query,
            tx,
        }
    }
}

impl<'a, S: ReadStorage<'a>> Iterator for DbIterator<'a, S> {
    type Item = PartialAssignment;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.complete.is_empty() {
            return self.complete.pop();
        }
        if let Some(state) = self.stack.pop() {
            if let Some(pattern) = self.query.wher.get(state.pattern) {
                let restricts = restricts(pattern, &state.assignment.assigned, self.tx);
                for datom in self.storage.find(restricts) {
                    let assignment = state.assignment.update_with(pattern, datom.unwrap());
                    if !self.query.test(&assignment.assigned) {
                        continue;
                    }
                    if assignment.is_complete() {
                        self.complete.push(assignment.assigned);
                    } else {
                        self.stack.push(StackState {
                            pattern: state.pattern + 1,
                            assignment,
                        });
                    }
                }
                return self.next();
            }
        }
        None
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
