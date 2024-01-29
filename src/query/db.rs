use crate::datom::Datom;
use crate::query::assignment::*;
use crate::query::*;
use crate::storage::attribute_resolver::*;
use crate::storage::*;

use super::pattern::AttributeIdentifier;
use super::pattern::Pattern;

pub struct Db {
    basis_tx: u64,
    attribute_resolver: AttributeResolver,
}

impl Db {
    pub fn new(basis_tx: u64) -> Self {
        Self {
            basis_tx,
            attribute_resolver: AttributeResolver::new(),
        }
    }

    pub fn query<'a, S: ReadStorage<'a>>(
        &mut self,
        storage: &'a S,
        mut query: Query,
    ) -> Result<
        impl Iterator<Item = Result<PartialAssignment, QueryError<S::Error>>> + 'a,
        QueryError<S::Error>,
    > {
        self.resolve_idents(storage, &mut query)?;
        Ok(DbIterator::new(storage, query, self.basis_tx))
    }

    /// Resolves attribute idents. Mutates input `query` such that clauses with
    /// `AttributeIdentifier::Ident` will be replaced with `AttributeIdentifier::Id`.
    fn resolve_idents<'a, S: ReadStorage<'a>>(
        &mut self,
        storage: &'a S,
        query: &mut Query,
    ) -> Result<(), QueryError<S::Error>> {
        for clause in &mut query.wher {
            if let Pattern::Constant(AttributeIdentifier::Ident(ident)) = &clause.attribute {
                let attribute =
                    self.attribute_resolver
                        .resolve(storage, Rc::clone(ident), self.basis_tx)?;

                clause.attribute = Pattern::id(attribute.id);
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
struct Frame {
    pattern_index: usize,
    assignment: Assignment,
}

struct DbIterator<'a, S: ReadStorage<'a>> {
    storage: &'a S,
    inner: S::Iter,
    frame: Frame,
    stack: Vec<Frame>,
    //stacks: NonEmptyVec<Frame>,
    query: Query,
    basis_tx: u64,
}

struct NonEmptyVec<T> {
    head: T,
    tail: Vec<T>
}

impl<T> NonEmptyVec<T> {
    fn new(head: T) -> Self {
        Self { head, tail: Vec::new() }
    }

    fn push(&mut self, item: T) {
        self.tail.push(item);
    }
}

impl<'a, S: ReadStorage<'a>> DbIterator<'a, S> {
    fn new(storage: &'a S, query: Query, basis_tx: u64) -> Self {
        let assignment = Assignment::from_query(&query);
        let pattern = query.wher.get(0).unwrap();
        let restricts = Restricts::from(pattern, &assignment.assigned, basis_tx);
        let inner = storage.find(restricts);
        DbIterator {
            storage,
            inner,
            frame: Frame {
                pattern_index: 0,
                assignment,
            },
            stack: Vec::new(),
            //stack: NonEmptyVec::new(Frame {
            //    pattern_index: 0,
            //    assignment,
            //}),
            query,
            basis_tx,
        }
    }
}

impl<'a, S: ReadStorage<'a>> Iterator for DbIterator<'a, S> {
    type Item = Result<PartialAssignment, QueryError<S::Error>>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(datom) = self.inner.next() {
            let pattern = self.query.wher.get(self.frame.pattern_index)?;
            let assignment = self.frame.assignment.update_with(pattern, datom.unwrap());
            if !assignment.satisfies(&self.query) {
                return self.next();
            }
            if assignment.is_complete() {
                return Some(Ok(assignment.assigned));
            }
            self.stack.push(Frame { pattern_index: self.frame.pattern_index + 1, assignment });
            return self.next();
        } else {
            // Inner iterator is exhausted, try next stack frame
            self.frame = self.stack.pop()?;
            let pattern = self.query.wher.get(self.frame.pattern_index)?;
            let restricts = Restricts::from(pattern, &self.frame.assignment.assigned, self.basis_tx);
            self.inner = self.storage.find(restricts);
            return self.next();
        }

        // ----------------------------------------------------------------------------------------

        /*
        if let Some(result) = self.complete.pop() {
            return Some(Ok(result));
        }

        let Frame {
            pattern_index,
            assignment,
        } = self.stack.pop()?;
        let pattern = self.query.wher.get(pattern_index)?;
        let restricts = Restricts::from(pattern, &assignment.assigned, self.basis_tx);
        for datom in self.storage.find(restricts) {
            match datom {
                Ok(datom) => {
                    let assignment = assignment.update_with(pattern, datom);
                    if !assignment.satisfies(&self.query) {
                        continue;
                    } else if assignment.is_complete() {
                        self.complete.push(assignment.assigned);
                    } else {
                        self.stack.push(Frame {
                            pattern_index: pattern_index + 1,
                            assignment,
                        });
                    }
                }
                Err(err) => return Some(Err(QueryError::StorageError(err))),
            }
        }
        self.next()
        */
    }
}
