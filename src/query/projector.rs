use crate::query::*;

pub struct Projector<R> {
    finds: Vec<Find>,
    results: R,
}

impl<R> Projector<R> {
    pub fn new(finds: Vec<Find>, results: R) -> Self {
        Self { finds, results }
    }

    fn project<E>(&self, mut assignment: Assignment) -> QueryResult<E> {
        let mut result = Vec::with_capacity(self.finds.len());
        for find in &self.finds {
            if let Find::Variable(variable) = find {
                match assignment.remove(variable) {
                    Some(value) => result.push(value),
                    None => return Err(QueryError::InvalidFindVariable(variable.clone())),
                }
            }
        }
        Ok(result)
    }
}

impl<E, R: Iterator<Item = AssignmentResult<E>>> Iterator for Projector<R> {
    type Item = QueryResult<E>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.results.next()? {
            Ok(assignment) => Some(self.project(assignment)),
            Err(err) => Some(Err(err)),
        }
    }
}
