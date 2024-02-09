use crate::query::pattern::AttributeIdentifier;
use crate::query::pattern::Pattern;
use crate::query::resolver::Resolver;
use crate::query::*;
use crate::storage::attribute_resolver::*;
use crate::storage::*;
use either::*;

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
    ) -> Result<impl Iterator<Item = QueryResult<S::Error>>, S::Error> {
        self.resolve_idents(storage, &mut query)?;
        let Query {
            find,
            clauses,
            predicates,
        } = query;
        let resolved = Resolver::new(storage, clauses, self.basis_tx);
        let filtered = resolved.filter(move |result| match result {
            Ok(assignment) => predicates.iter().all(|predicate| predicate(assignment)),
            Err(_) => true,
        });
        if is_aggregated(&find) {
            aggregate(find, filtered).map(Left)
        } else {
            Ok(Right(filtered.map(move |result| match result {
                Ok(assignment) => project(&find, assignment),
                Err(err) => Err(err),
            })))
        }
    }

    /// Resolves attribute idents. Mutates input `query` such that clauses with
    /// `AttributeIdentifier::Ident` will be replaced with `AttributeIdentifier::Id`.
    fn resolve_idents<'a, S: ReadStorage<'a>>(
        &mut self,
        storage: &'a S,
        query: &mut Query,
    ) -> Result<(), S::Error> {
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

fn is_aggregated(find: &[Find]) -> bool {
    find.iter().any(|f| matches!(f, Find::Aggregate(_)))
}

fn project<E>(find: &[Find], mut assignment: Assignment) -> QueryResult<E> {
    let mut result = Vec::with_capacity(find.len());
    for f in find {
        if let Find::Variable(variable) = f {
            match assignment.remove(variable) {
                Some(value) => result.push(value),
                None => return Err(QueryError::InvalidFindVariable(Rc::clone(variable))),
            }
        }
    }
    Ok(result)
}

fn aggregate<E>(
    find: Vec<Find>,
    results: impl Iterator<Item = AssignmentResult<E>>,
) -> Result<impl Iterator<Item = QueryResult<E>>, E> {
    // TODO concurrent aggregation?
    let mut aggregated = HashMap::new();

    let indices = Indices::new(&find);
    let (variables, aggregates) = partition(find.into_iter(), |f| match f {
        Find::Variable(variale) => Left(variale),
        Find::Aggregate(aggregate) => Right(aggregate),
    });

    for result in results {
        let assignment = result?;
        let key = key_of(&variables, &assignment);
        let entry = aggregated.entry(key).or_insert_with(|| init(&aggregates));
        for agg in entry {
            agg.consume(&assignment);
        }
    }

    Ok(aggregated
        .into_iter()
        .map(move |(key, value)| Ok(indices.arrange(key, value))))
}

struct Indices {
    variables: Vec<usize>,
    aggregates: Vec<usize>,
}

impl Indices {
    fn new(find: &[Find]) -> Self {
        let (variables, aggregates) = partition(find.iter().enumerate(), |(index, f)| match f {
            Find::Variable(_) => Left(index),
            Find::Aggregate(_) => Right(index),
        });
        Self {
            variables,
            aggregates,
        }
    }

    fn arrange(&self, key: Vec<Value>, value: Vec<Box<dyn Aggregator>>) -> Vec<Value> {
        let mut result = vec![Value::U64(0); key.len() + value.len()];
        for (index, value) in self.variables.iter().zip(key.into_iter()) {
            result[*index] = value;
        }
        for (index, agg) in self.aggregates.iter().zip(value.into_iter()) {
            result[*index] = agg.result();
        }
        result
    }
}

fn key_of(variables: &[Rc<str>], assignment: &Assignment) -> Vec<Value> {
    variables
        .iter()
        .map(|variable| assignment[variable].clone())
        .collect()
}

fn init(aggregates: &[Rc<dyn ToAggregator>]) -> Vec<Box<dyn Aggregator>> {
    aggregates
        .iter()
        .map(|aggregate| aggregate.to_aggregator())
        .collect()
}

fn partition<T, A, B>(
    vec: impl ExactSizeIterator<Item = T>,
    f: impl Fn(T) -> Either<A, B>,
) -> (Vec<A>, Vec<B>) {
    let mut _as = Vec::with_capacity(vec.len());
    let mut _bs = Vec::with_capacity(vec.len());
    for x in vec {
        match f(x) {
            Either::Left(a) => _as.push(a),
            Either::Right(b) => _bs.push(b),
        }
    }
    (_as, _bs)
}
