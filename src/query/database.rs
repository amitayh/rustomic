use crate::query::pattern::AttributeIdentifier;
use crate::query::pattern::Pattern;
use crate::query::projector::Projector;
use crate::query::resolver::Resolver;
use crate::query::*;
use crate::storage::attribute_resolver::*;
use crate::storage::*;
use either::*;

pub struct Database {
    basis_tx: u64,
}

impl Database {
    pub fn new(basis_tx: u64) -> Self {
        Self { basis_tx }
    }

    pub async fn query<'a, S: ReadStorage<'a>>(
        &self,
        storage: &'a S,
        resolver: &AttributeResolver,
        mut query: Query,
    ) -> Result<impl Iterator<Item = QueryResult<S::Error>>, S::Error> {
        self.resolve_idents(storage, resolver, &mut query).await?;
        let Query {
            find,
            clauses,
            predicates,
        } = query;
        let resolved = Resolver::new(storage, clauses, predicates, self.basis_tx);
        if find.iter().any(|find| matches!(find, Find::Aggregate(_))) {
            let aggregated = aggregator::aggregate(find, resolved)?;
            Ok(Left(aggregated))
        } else {
            Ok(Right(Projector::new(find, resolved)))
        }
    }

    /// Resolves attribute idents. Mutates input `query` such that clauses with
    /// `AttributeIdentifier::Ident` will be replaced with `AttributeIdentifier::Id`.
    async fn resolve_idents<'a, S: ReadStorage<'a>>(
        &self,
        storage: &'a S,
        resolver: &AttributeResolver,
        query: &mut Query,
    ) -> Result<(), S::Error> {
        for clause in &mut query.clauses {
            if let Pattern::Constant(AttributeIdentifier::Ident(ident)) = &clause.attribute {
                let attribute = resolver.resolve(storage, ident, self.basis_tx).await?;
                clause.attribute = Pattern::id(attribute.id);
            }
        }
        Ok(())
    }
}
