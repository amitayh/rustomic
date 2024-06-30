use std::{
    collections::{HashMap, HashSet},
    rc::Rc,
};

use rustomic::query::{Find, Query};

use nom::{
    bytes::complete::{is_a, tag, take_while},
    character::{
        complete::{char, multispace0},
        is_alphabetic, is_alphanumeric,
    },
    combinator::opt,
    multi::many0,
    sequence::{delimited, preceded},
    IResult,
};

// ------------------------------------------------------------------------------------------------

mod edn {
    use std::collections::{BTreeMap, BTreeSet};

    use nom::{
        branch::alt,
        bytes::complete::{is_not, take_until, take_while, take_while1},
        character::complete::{alpha0, anychar, multispace1},
        combinator::not,
        multi::{many1, separated_list0},
        sequence::separated_pair,
        Parser,
    };

    use super::*;

    #[derive(PartialEq, Debug, Clone, PartialOrd, Eq, Ord)]
    struct Name {
        namespace: Option<Rc<str>>,
        name: Rc<str>,
    }

    impl Name {
        fn from(name: &str) -> Self {
            Self {
                namespace: None,
                name: Rc::from(name),
            }
        }

        fn namespaced(namespace: &str, name: &str) -> Self {
            Self {
                namespace: Some(Rc::from(namespace)),
                name: Rc::from(name),
            }
        }
    }

    #[derive(PartialEq, Debug, Clone, PartialOrd, Eq, Ord)]
    enum Edn {
        Nil,
        True,
        False,
        String(Rc<str>),
        // Char(char),
        Symbol(Name),
        Keyword(Name),
        // Integer(i32),
        // Float(f32),
        List(Vec<Edn>),
        Vector(Vec<Edn>),
        Map(BTreeMap<Edn, Edn>),
        Set(BTreeSet<Edn>),
    }

    impl TryFrom<&str> for Edn {
        type Error = String; // nom::Err<nom::error::Error<str>>;

        fn try_from(input: &str) -> Result<Self, Self::Error> {
            parse_edn(input)
                .map(|(_, result)| result)
                .map_err(|err| err.to_string())
        }
    }

    fn parse_nil(input: &str) -> IResult<&str, Edn> {
        tag("nil").map(|_| Edn::Nil).parse(input)
    }

    fn parse_true(input: &str) -> IResult<&str, Edn> {
        tag("true").map(|_| Edn::True).parse(input)
    }

    fn parse_false(input: &str) -> IResult<&str, Edn> {
        tag("false").map(|_| Edn::False).parse(input)
    }

    fn parse_string(input: &str) -> IResult<&str, Edn> {
        delimited(char('"'), is_not("\""), char('"'))
            .map(|str| Edn::String(Rc::from(str)))
            .parse(input)
    }

    fn parse_name_part(input: &str) -> IResult<&str, &str> {
        take_while1(|c: char| c.is_ascii_alphanumeric() || ".*+!-_?$%&=<>".contains(c))(input)
    }

    fn parse_name(input: &str) -> IResult<&str, Name> {
        let (input, first) = parse_name_part(input)?;
        let (input, second) = opt(preceded(char('/'), parse_name_part))(input)?;
        let name = match second {
            Some(second) => Name::namespaced(first, second),
            None => Name::from(first),
        };
        Ok((input, name))
    }

    fn parse_symbol(input: &str) -> IResult<&str, Edn> {
        parse_name.map(Edn::Symbol).parse(input)
    }

    fn parse_keyword(input: &str) -> IResult<&str, Edn> {
        preceded(char(':'), parse_name)
            .map(Edn::Keyword)
            .parse(input)
    }

    fn parse_vector(input: &str) -> IResult<&str, Edn> {
        delimited(
            char('['),
            separated_list0(multispace1, parse_edn),
            char(']'),
        )
        .map(Edn::Vector)
        .parse(input)
    }

    fn parse_list(input: &str) -> IResult<&str, Edn> {
        delimited(
            char('('),
            separated_list0(multispace1, parse_edn),
            char(')'),
        )
        .map(Edn::List)
        .parse(input)
    }

    fn parse_map(input: &str) -> IResult<&str, Edn> {
        delimited(
            char('{'),
            separated_list0(
                multispace1,
                separated_pair(parse_edn, multispace1, parse_edn),
            ),
            char('}'),
        )
        .map(|entries| Edn::Map(entries.into_iter().collect()))
        .parse(input)
    }

    fn parse_set(input: &str) -> IResult<&str, Edn> {
        delimited(
            tag("#{"),
            separated_list0(multispace1, parse_edn),
            char('}'),
        )
        .map(|elements| Edn::Set(elements.into_iter().collect()))
        .parse(input)
    }

    fn parse_edn(input: &str) -> IResult<&str, Edn> {
        alt((
            parse_nil,
            parse_true,
            parse_false,
            parse_string,
            parse_vector,
            parse_list,
            parse_map,
            parse_set,
            parse_keyword,
            parse_symbol,
        ))(input)
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn test_nil() {
            let result = Edn::try_from("nil");

            assert_eq!(result, Ok(Edn::Nil));
        }

        #[test]
        fn test_true() {
            let result = Edn::try_from("true");

            assert_eq!(result, Ok(Edn::True));
        }

        #[test]
        fn test_false() {
            let result = Edn::try_from("false");

            assert_eq!(result, Ok(Edn::False));
        }

        #[test]
        fn test_string() {
            let result = Edn::try_from(r#""hello world""#);

            assert_eq!(result, Ok(Edn::String(Rc::from("hello world"))));
        }

        #[test]
        fn test_symbol_without_namespace() {
            let result = Edn::try_from("hello-world");

            assert_eq!(result, Ok(Edn::Symbol(Name::from("hello-world"))));
        }

        #[test]
        fn test_symbol_with_namespace() {
            let result = Edn::try_from("hello/world");

            assert_eq!(result, Ok(Edn::Symbol(Name::namespaced("hello", "world"))));
        }

        #[test]
        fn test_keyword_with_namespace() {
            let result = Edn::try_from(":hello/world");

            assert_eq!(result, Ok(Edn::Keyword(Name::namespaced("hello", "world"))));
        }

        #[test]
        fn test_empty_vector() {
            let result = Edn::try_from("[]");

            assert_eq!(result, Ok(Edn::Vector(Vec::new())));
        }

        #[test]
        fn test_non_empty_vector() {
            let result = Edn::try_from("[foo bar]");

            assert_eq!(
                result,
                Ok(Edn::Vector(vec![
                    Edn::Symbol(Name::from("foo")),
                    Edn::Symbol(Name::from("bar"))
                ]))
            );
        }

        #[test]
        fn test_nested_vector() {
            let result = Edn::try_from("[foo [bar]]");

            assert_eq!(
                result,
                Ok(Edn::Vector(vec![
                    Edn::Symbol(Name::from("foo")),
                    Edn::Vector(vec![Edn::Symbol(Name::from("bar"))])
                ]))
            );
        }

        #[test]
        fn test_list() {
            let result = Edn::try_from("(foo)");

            assert_eq!(result, Ok(Edn::List(vec![Edn::Symbol(Name::from("foo"))])));
        }

        #[test]
        fn test_map() {
            let result = Edn::try_from("{:foo bar}");

            assert_eq!(
                result,
                Ok(Edn::Map(BTreeMap::from([(
                    Edn::Keyword(Name::from("foo")),
                    Edn::Symbol(Name::from("bar"))
                )])))
            );
        }

        #[test]
        fn test_set() {
            let result = Edn::try_from("#{foo bar}");

            assert_eq!(
                result,
                Ok(Edn::Set(BTreeSet::from([
                    Edn::Symbol(Name::from("foo")),
                    Edn::Symbol(Name::from("bar"))
                ])))
            );
        }
    }
}

// ------------------------------------------------------------------------------------------------

fn whitespace(input: &str) -> IResult<&str, &str> {
    take_while(|c: char| c.is_whitespace() || c == ',')(input)
}

fn symbol(input: &str) -> IResult<&str, &str> {
    take_while(|c: char| is_alphabetic(c.try_into().unwrap()))(input) // TODO
}

fn keyword(input: &str) -> IResult<&str, &str> {
    preceded(char(':'), symbol)(input)
}

fn variable(input: &str) -> IResult<&str, &str> {
    preceded(char('?'), symbol)(input)
}

fn parse_find(input: &str) -> IResult<&str, Vec<Find>> {
    let (input, _) = tag(":find")(input)?;
    let (input, _) = whitespace(input)?;
    let (input, variables) = many0(variable)(input)?;
    let find = variables
        .iter()
        .map(|variable| Find::variable(variable))
        .collect();

    Ok((input, find))
}

pub fn parse(input: &str) -> IResult<&str, Query> {
    let (input, find) = delimited(char('['), parse_find, char(']'))(input)?;
    Ok((
        input,
        Query {
            find,
            clauses: Vec::new(),
            predicates: Vec::new(),
        },
    ))
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

        assert_eq!(
            parse(query),
            Ok(("", Query::new().find(Find::variable("?foo"))))
        );
    }
}
