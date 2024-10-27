use crate::datom::Datom;
use crate::query::assignment::*;
use crate::query::*;
use crate::storage::restricts::*;
use crate::storage::*;

/// The resolver is an iterator over the resolved assignments which satisfy the clauses. It uses a
/// backtracking algorithm which builds up the assignment using matching datoms from storage until
/// the assignment is complete (no more unassigned variables remaining). This algorithm is
/// recursive by nature, but in order to implement the iterative `Iterator` trait it uses a stack
/// with frames that contain current assignment state and remaining clauses to satisfy.
pub struct Resolver<'a, S: ReadStorage<'a>> {
    storage: &'a S,
    clauses: Vec<Clause>,
    predicates: Vec<Predicate>,
    frame: Frame,
    stack: Vec<Frame>,
    iterator: S::Iter,
    basis_tx: u64,
}

impl<'a, S: ReadStorage<'a>> Resolver<'a, S> {
    pub fn new(
        storage: &'a S,
        clauses: Vec<Clause>,
        predicates: Vec<Predicate>,
        basis_tx: u64,
    ) -> Self {
        let frame = Frame::first(PartialAssignment::from_clauses(&clauses));
        let iterator = Self::iterator(storage, &frame, &clauses, basis_tx);
        Resolver {
            storage,
            clauses,
            predicates,
            frame,
            stack: Vec::new(),
            iterator,
            basis_tx,
        }
    }

    fn process(&mut self, datom: Datom) -> Option<<Self as Iterator>::Item> {
        let clause = self.clauses.get(self.frame.clause_index)?;
        let assignment = self.frame.assignment.update_with(clause, datom);
        if !assignment.satisfies(&self.predicates) {
            return self.next();
        }
        if assignment.is_complete() {
            return Some(Ok(assignment.complete()));
        }
        self.stack.push(self.frame.next(assignment));
        self.next()
    }

    fn next_frame(&mut self) -> Option<<Self as Iterator>::Item> {
        self.frame = self.stack.pop()?;
        self.iterator = Self::iterator(self.storage, &self.frame, &self.clauses, self.basis_tx);
        self.next()
    }

    fn iterator(storage: &'a S, frame: &Frame, clauses: &[Clause], basis_tx: u64) -> S::Iter {
        let clause = clauses
            .get(frame.clause_index)
            .expect("clause index should be valid");
        let restricts = Restricts::from(clause, &frame.assignment, basis_tx);
        storage.find(restricts)
    }
}

impl<'a, S: ReadStorage<'a>> Iterator for Resolver<'a, S> {
    type Item = AssignmentResult<S::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.iterator.next() {
            Some(Err(err)) => Some(Err(QueryError::StorageError(err))),
            Some(Ok(datom)) => self.process(datom),
            None => self.next_frame(), // Inner iterator exhausted, try next stack frame
        }
    }
}

struct Frame {
    clause_index: usize,
    assignment: PartialAssignment,
}

impl Frame {
    fn first(assignment: PartialAssignment) -> Self {
        Self {
            clause_index: 0,
            assignment,
        }
    }

    fn next(&self, assignment: PartialAssignment) -> Self {
        Self {
            clause_index: self.clause_index + 1,
            assignment,
        }
    }
}
