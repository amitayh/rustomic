use crate::datom::Datom;
use crate::query::assignment::*;
use crate::query::*;
use crate::storage::*;

pub struct Resolver<'a, S: ReadStorage<'a>> {
    storage: &'a S,
    query: Query,
    frame: Frame,
    stack: Vec<Frame>,
    iterator: S::Iter,
    basis_tx: u64,
}

impl<'a, S: ReadStorage<'a>> Resolver<'a, S> {
    pub fn new(storage: &'a S, query: Query, basis_tx: u64) -> Self {
        let frame = Frame::first(Assignment::from_query(&query));
        let iterator = Self::iterator(storage, &frame, &query, basis_tx);
        Resolver {
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
            return Some(Ok(assignment.assigned));
        }
        self.stack.push(self.frame.next(assignment));
        self.next()
    }

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

impl<'a, S: ReadStorage<'a>> Iterator for Resolver<'a, S> {
    type Item = Result<HashMap<Rc<str>, Value>, QueryError<S::Error>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.iterator.next() {
            Some(Err(err)) => Some(Err(QueryError::StorageError(err))),
            Some(Ok(datom)) => self.process(datom),
            None =>
                // Inner iterator is exhausted, try next stack frame
                self.next_frame(),
        }
    }
}

struct Frame {
    clause_index: usize,
    assignment: Assignment,
}

impl Frame {
    fn first(assignment: Assignment) -> Self {
        Self {
            clause_index: 0,
            assignment,
        }
    }

    fn next(&self, assignment: Assignment) -> Self {
        Self {
            clause_index: self.clause_index + 1,
            assignment,
        }
    }
}
