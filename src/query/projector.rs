use crate::query::*;

pub struct Projector<I> {
    iterator: I,
    find: Vec<Find>,
}

impl<I> Projector<I> {
    pub fn new(iterator: I, find: Vec<Find>) -> Self {
        Self { iterator, find }
    }

    fn project<E>(&self, mut assignment: HashMap<Rc<str>, Value>) -> QueryResult<E> {
        let mut result = Vec::with_capacity(self.find.len());
        for find in &self.find {
            if let Find::Variable(variable) = find {
                match assignment.remove(variable) {
                    Some(value) => result.push(value),
                    None => return Err(QueryError::InvalidFindVariable(Rc::clone(variable))),
                }
            }
        }
        Ok(result)
    }
}

impl<E, I: Iterator<Item = AssignmentResult<E>>> Iterator for Projector<I> {
    type Item = QueryResult<E>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.iterator.next() {
            Some(Ok(assignment)) => Some(self.project(assignment)),
            Some(Err(err)) => Some(Err(err)),
            None => None,
        }
    }
}
