use std::marker::PhantomData;

use crate::query::pattern::AttributeIdentifier;
use crate::query::pattern::Pattern;
use crate::query::resolver::Resolver;
use crate::query::*;
use crate::storage::attribute_resolver::*;
use crate::storage::*;

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
        Lala::new(iterator, query)
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

enum Lala<'a, S: ReadStorage<'a>> {
    Aggregate(Aggregator2<'a, S>),
    Project(Projector<'a, S>),
}

impl<'a, S: ReadStorage<'a>> Lala<'a, S> {
    fn new(resolver: Resolver<'a, S>, query: Query) -> Result<Self, QueryError<S::Error>> {
        if query.is_aggregated() {
            Ok(Self::Aggregate(Aggregator2::new(resolver, &query)?))
        } else {
            Ok(Self::Project(Projector::new(resolver, query)))
        }
    }
}

impl<'a, S: ReadStorage<'a>> Iterator for Lala<'a, S> {
    type Item = Result<Vec<Value>, QueryError<S::Error>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Aggregate(aggregator) => aggregator.next(),
            Self::Project(projector) => projector.next(),
        }
    }
}

// ------------------------------------------------------------------------------------------------

struct Aggregator2<'a, S: ReadStorage<'a>> {
    aggregated: std::collections::hash_map::IntoValues<Vec<Value>, Vec<Value>>,
    marker: PhantomData<&'a S>,
}

impl<'a, S: ReadStorage<'a>> Aggregator2<'a, S> {
    fn new(resolver: Resolver<'a, S>, query: &Query) -> Result<Self, QueryError<S::Error>> {
        let mut aggregated = HashMap::new();
        for assignment in resolver {
            let assignment = assignment?;
            let key = Self::key_of(query, &assignment);
            let entry = aggregated.entry(key).or_insert_with(|| Self::init(query, &assignment));
            for (index, find) in query.find.iter().enumerate() {
                if let Find::Aggregate(agg) = find {
                    if let Some(value) = entry.get_mut(index) {
                        agg.consume(value, &assignment);
                    }
                }
            }
        }
        Ok(Self {
            aggregated: aggregated.into_values(),
            marker: PhantomData,
        })
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

    fn init(query: &Query, assignment: &HashMap<Rc<str>, Value>) -> Vec<Value> {
        let mut result = Vec::with_capacity(query.find.len());
        for find in &query.find {
            let value = match find {
                Find::Variable(variable) => assignment[variable].clone(),
                Find::Aggregate(agg) => agg.init(),
            };
            result.push(value);
        }
        result
    }
}

impl<'a, S: ReadStorage<'a>> Iterator for Aggregator2<'a, S> {
    type Item = Result<Vec<Value>, QueryError<S::Error>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.aggregated.next() {
            Some(value) => Some(Ok(value)),
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
            Some(Ok(assignment)) => self.project(assignment),
            Some(Err(err)) => Some(Err(err)),
            None => None,
        }
    }
}

// ------------------------------------------------------------------------------------------------
