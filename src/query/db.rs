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
        impl Iterator<Item = Result<Vec<Value>, QueryError<S::Error>>> + 'a,
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
        for clause in &mut query.clauses {
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
    clause_index: usize,
    assignment: Assignment,
}

impl Frame {
    fn next(&self, assignment: Assignment) -> Self {
        Self {
            clause_index: self.clause_index + 1,
            assignment,
        }
    }
}

struct DbIterator<'a, S: ReadStorage<'a>> {
    storage: &'a S,
    query: Query,
    frame: Frame,
    stack: Vec<Frame>,
    iterator: S::Iter,
    basis_tx: u64,
}

impl<'a, S: ReadStorage<'a>> DbIterator<'a, S> {
    fn new(storage: &'a S, query: Query, basis_tx: u64) -> Self {
        let frame = Frame {
            clause_index: 0,
            assignment: Assignment::from_query(&query),
        };
        let iterator = Self::iterator(storage, &frame, &query, basis_tx);
        DbIterator {
            storage,
            query,
            frame,
            stack: Vec::new(),
            iterator,
            basis_tx,
        }
    }

    fn process(&mut self, datom: Datom) -> Option<<Self as Iterator>::Item> {
        let clause = self.query.clauses.get(self.frame.clause_index)?;
        let assignment = self.frame.assignment.update_with(clause, datom);
        if !assignment.satisfies(&self.query) {
            return self.next();
        }
        if assignment.is_complete() {
            let projection = assignment.project(&self.query)?;
            return Some(Ok(projection));
        }
        self.stack.push(self.frame.next(assignment));
        self.next()
    }

    // Inner iterator is exhausted, try next stack frame
    fn next_frame(&mut self) -> Option<<Self as Iterator>::Item> {
        self.frame = self.stack.pop()?;
        self.iterator = Self::iterator(self.storage, &self.frame, &self.query, self.basis_tx);
        self.next()
    }

    fn iterator(storage: &'a S, frame: &Frame, query: &Query, basis_tx: u64) -> S::Iter {
        let clause = query.clauses.get(frame.clause_index);
        let restricts = Restricts::from(
            clause.unwrap_or(&Clause::default()),
            &frame.assignment.assigned,
            basis_tx,
        );
        storage.find(restricts)
    }
}

impl<'a, S: ReadStorage<'a>> Iterator for DbIterator<'a, S> {
    type Item = Result<Vec<Value>, QueryError<S::Error>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.iterator.next() {
            Some(Err(err)) => Some(Err(QueryError::StorageError(err))),
            Some(Ok(datom)) => self.process(datom),
            None => self.next_frame(),
        }
    }
}
