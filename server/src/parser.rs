use std::sync::Arc;

use crate::edn::{Edn, Name};
use rustomic::datom::Value;
use rustomic::query::clause::*;
use rustomic::query::pattern::*;
use rustomic::query::{Find, Query};

enum State {
    Begin,
    Find,
    Where,
}

pub fn parse(input: &str) -> Result<Query, String> {
    let edn = Edn::try_from(input)?;
    let Edn::Vector(parts) = edn else {
        return Err("Invalid".to_string());
    };
    let mut query = Query::new();
    let mut state = State::Begin;
    for part in parts {
        match state {
            State::Begin => {
                if part == Edn::Keyword(Name::from("find")) {
                    state = State::Find;
                } else {
                    return Err("Invalid".to_string());
                }
            }
            State::Find => {
                if let Edn::Symbol(name) = part {
                    query = query.find(Find::Variable(name.name));
                } else if part == Edn::Keyword(Name::from("where")) {
                    state = State::Where;
                } else {
                    return Err("Invalid".to_string());
                }
            }
            State::Where => {
                if let Edn::Vector(parts) = part {
                    let clause = parse_clause(parts)?;
                    query = query.r#where(clause);
                } else {
                    return Err("Invalid".to_string());
                }
            }
        }
    }

    Ok(query)
}

#[derive(Debug)]
pub struct Unsupported(Edn);

impl TryFrom<Edn> for Value {
    type Error = Unsupported;

    fn try_from(value: Edn) -> Result<Self, Self::Error> {
        match value {
            Edn::Nil => Ok(Self::Nil),
            Edn::Integer(value) => Ok(Self::I64(value)),
            Edn::String(value) => Ok(Self::Str(value)),
            _ => Err(Unsupported(value)),
        }
    }
}

fn parse_clause(patterns: Vec<Edn>) -> Result<Clause, String> {
    let entity = match patterns.get(0) {
        Some(Edn::Symbol(name)) => {
            let name: String = name.into();
            Pattern::Variable(Arc::from(name))
        }
        Some(Edn::Integer(id)) => Pattern::Constant(*id as u64),
        // TODO: handle failures
        _ => Pattern::Blank,
    };
    let attribute = match patterns.get(1) {
        Some(Edn::Symbol(name)) => {
            let name: String = name.into();
            Pattern::Variable(Arc::from(name))
        }
        Some(Edn::Keyword(name)) => {
            let name: String = name.into();
            Pattern::Constant(AttributeIdentifier::Ident(Arc::from(name)))
        }
        Some(Edn::Integer(id)) => Pattern::Constant(AttributeIdentifier::Id(*id as u64)),
        // TODO: handle failures
        _ => Pattern::Blank,
    };
    let value = match patterns.get(2) {
        Some(Edn::Symbol(name)) => {
            let name: String = name.into();
            Pattern::Variable(Arc::from(name))
        }
        // TODO: remove clone
        Some(edn) => Pattern::Constant(edn.clone().try_into().unwrap()),
        // TODO: handle failures
        _ => Pattern::Blank,
    };
    Ok(Clause {
        entity,
        attribute,
        value,
        tx: Pattern::Blank,
    })
}

#[cfg(test)]
mod tests {
    use rustomic::query::Find;

    use super::*;

    #[test]
    fn test_empty_query() {
        let result = parse("");

        assert!(result.is_err());
    }

    #[test]
    fn parse_a_single_find_clause() {
        let query = "[:find ?foo]";

        assert_eq!(parse(query), Ok(Query::new().find(Find::variable("?foo"))));
    }

    #[test]
    fn parse_multiple_find_clauses() {
        let query = "[:find ?foo ?bar]";

        assert_eq!(
            parse(query),
            Ok(Query::new()
                .find(Find::variable("?foo"))
                .find(Find::variable("?bar")))
        );
    }

    #[test]
    fn parse_where_clauses() {
        let query = r#"[:find ?release-name
                        :where [?artist :artist/name "John Lenon"]
                               [?release :release/artists ?artist]
                               [?release :release/name ?release-name]]"#;

        assert_eq!(
            parse(query),
            Ok(Query::new()
                .find(Find::variable("?release-name"))
                .r#where(
                    Clause::new()
                        .with_entity(Pattern::variable("?artist"))
                        .with_attribute(Pattern::ident("artist/name"))
                        .with_value(Pattern::value("John Lenon")),
                )
                .r#where(
                    Clause::new()
                        .with_entity(Pattern::variable("?release"))
                        .with_attribute(Pattern::ident("release/artists"))
                        .with_value(Pattern::variable("?artist")),
                )
                .r#where(
                    Clause::new()
                        .with_entity(Pattern::variable("?release"))
                        .with_attribute(Pattern::ident("release/name"))
                        .with_value(Pattern::variable("?release-name")),
                ))
        );
    }
}
