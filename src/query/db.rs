use std::marker::PhantomData;

use crate::datom::Datom;
use crate::query::assignment::*;
use crate::query::*;
use crate::storage::attribute_resolver::*;
use crate::storage::*;

use super::Aggregator;
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
        let iterator = Resolver::new(storage, query.clone(), self.basis_tx);
        Aggregator2::new(iterator, &query)
        //Ok(Projector::new(iterator, query))
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

// ------------------------------------------------------------------------------------------------

struct Aggregator2<'a, S: ReadStorage<'a>> {
    aggregated: std::collections::hash_map::IntoIter<Vec<Value>, Vec<Value>>,
    marker: PhantomData<&'a S>,
}

impl<'a, S: ReadStorage<'a>> Aggregator2<'a, S> {
    fn new(resolver: Resolver<'a, S>, query: &Query) -> Result<Self, QueryError<S::Error>> {
        let mut aggregated = HashMap::new();
        for assignment in resolver {
            let ok = assignment?;
            let key = Self::key_of(query, &ok);
            let entry = aggregated.entry(key).or_insert_with(|| Self::init(query));
                for (index, aggregator) in query.find_aggregations().enumerate() {
                    if let Some(value) = entry.get_mut(index) {
                        aggregator.consume(value, &ok);
                    }
                }

        }
        Ok(Self { aggregated: aggregated.into_iter(), marker: PhantomData })
    }

    fn key_of(query: &Query, assignment: &PartialAssignment) -> Vec<Value> {
        let mut key = Vec::with_capacity(query.find.len());
        for variable in query.find_variables() {
            if let Some(value) = assignment.get(variable) {
                key.push(value.clone());
            }
        }
        key
    }

    fn init(query: &Query) -> Vec<Value> {
        let mut result = Vec::with_capacity(query.find.len());
        for find in &query.find {
            if let Find::Aggregate(agg) = find {
                result.push(agg.init());
            }
        }
        result
    }
}

impl<'a, S: ReadStorage<'a>> Iterator for Aggregator2<'a, S> {
    type Item = Result<Vec<Value>, QueryError<S::Error>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.aggregated.next() {
            Some((key, value)) => {
                let mut result = Vec::with_capacity(key.len() + value.len());
                result.extend(key);
                result.extend(value);
                Some(Ok(result))
            }
            None => None,
        }
    }
}

// ------------------------------------------------------------------------------------------------

struct Projector<'a, S: ReadStorage<'a>> {
    resolver: Resolver<'a, S>,
    query: Query,
}

impl<'a, S: ReadStorage<'a>> Projector<'a, S> {
    fn new(resolver: Resolver<'a, S>, query: Query) -> Self {
        Self { resolver, query }
    }

    fn project(&self, mut assignment: HashMap<Rc<str>, Value>) -> Option<<Self as Iterator>::Item> {
        let mut result = Vec::with_capacity(self.query.find.len());
        for find in &self.query.find {
            if let Find::Variable(variable) = find {
                let value = assignment.remove(variable)?;
                result.push(value);
            }
        }
        Some(Ok(result))
    }
}

impl<'a, S: ReadStorage<'a>> Iterator for Projector<'a, S> {
    type Item = Result<Vec<Value>, QueryError<S::Error>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.resolver.next() {
            Some(Err(err)) => Some(Err(err)),
            Some(Ok(assignment)) => self.project(assignment),
            None => None,
        }
    }
}

// ------------------------------------------------------------------------------------------------

#[derive(Debug)]
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

struct Resolver<'a, S: ReadStorage<'a>> {
    storage: &'a S,
    query: Query,
    frame: Frame,
    stack: Vec<Frame>,
    iterator: S::Iter,
    basis_tx: u64,
}

impl<'a, S: ReadStorage<'a>> Resolver<'a, S> {
    fn new(storage: &'a S, query: Query, basis_tx: u64) -> Self {
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
            //let projection = assignment.project(&self.query)?;
            return Some(Ok(assignment.assigned));
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

impl<'a, S: ReadStorage<'a>> Iterator for Resolver<'a, S> {
    type Item = Result<HashMap<Rc<str>, Value>, QueryError<S::Error>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.iterator.next() {
            Some(Err(err)) => Some(Err(QueryError::StorageError(err))),
            Some(Ok(datom)) => self.process(datom),
            None => self.next_frame(),
        }
    }
}
