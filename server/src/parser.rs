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
        bytes::{complete::take_until, streaming::tag},
        character::complete::{alpha0, anychar},
        combinator::not,
        Parser,
    };

    use super::*;

    #[derive(PartialEq, Debug, Clone)]
    enum Edn {
        Nil,
        True,
        False,
        String(Rc<str>),
        Char(char),
        Symbol {
            name: Rc<str>,
        },
        Keyword {
            namespace: Option<Rc<str>>,
            name: Rc<str>,
        },
        Integer(i32),
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
        let (input, _) = char('"')(input)?;
        let (input, str) = take_until("\"")(input)?;
        let (input, _) = char('"')(input)?;
        Ok((input, Edn::String(Rc::from(str))))
    }

    fn parse_edn(input: &str) -> IResult<&str, Edn> {
        alt((parse_nil, parse_true, parse_false, parse_string))(input)
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
