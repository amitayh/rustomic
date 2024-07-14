use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use nom::branch::*;
use nom::bytes::complete::*;
use nom::character::complete::*;
use nom::character::streaming::multispace1;
use nom::combinator::*;
use nom::multi::*;
use nom::number::complete::*;
use nom::sequence::*;
use nom::IResult;
use nom::Parser;

use edn::*;
use nom_supreme::ParserExt;
use ordered_float::OrderedFloat;
use rustomic::query::clause::*;
use rustomic::query::pattern::*;
use rustomic::query::{Find, Query};

#[derive(PartialEq, Debug, Clone, PartialOrd, Eq, Ord)]
pub struct Name {
    pub namespace: Option<Rc<str>>,
    pub name: Rc<str>,
}

impl Name {
    pub fn from(name: &str) -> Self {
        Self {
            namespace: None,
            name: Rc::from(name),
        }
    }

    pub fn namespaced(namespace: &str, name: &str) -> Self {
        Self {
            namespace: Some(Rc::from(namespace)),
            name: Rc::from(name),
        }
    }
}

impl Into<String> for Name {
    fn into(self) -> String {
        match self.namespace {
            Some(namespace) => format!("{}/{}", namespace, self.name),
            None => format!("{}", self.name),
        }
    }
}

mod edn {
    use super::*;

    #[derive(PartialEq, Debug, Clone, PartialOrd, Eq, Ord)]
    pub enum Edn {
        /// `nil` represents nil, null or nothing. It should be read as an object with similar
        /// meaning on the target platform.
        Nil,

        /// `true` and `false` should be mapped to booleans.
        ///
        /// If a platform has canonic values for true and false, it is a further semantic of
        /// booleans that all instances of `true` yield that (identical) value, and similarly for
        /// `false`.
        Boolean(bool),

        /// Strings are enclosed in `"double quotes"`. May span multiple lines. Standard C/Java
        /// escape characters `\t, \r, \n, \\ and \" are supported.
        String(Rc<str>),

        /// Symbols are used to represent identifiers, and should map to something other than
        /// strings, if possible.
        ///
        /// Symbols begin with a non-numeric character and can contain alphanumeric characters and
        /// `. * + ! - _ ? $ % & = < >`. If `-`, `+` or `.` are the first character, the second
        /// character (if any) must be non-numeric. Additionally, `: #` are allowed as constituent
        /// characters in symbols other than as the first character.
        ///
        /// `/` has special meaning in symbols. It can be used once only in the middle of a symbol
        /// to separate the _prefix_ (often a namespace) from the _name_, e.g. `my-namespace/foo`.
        /// `/` by itself is a legal symbol, but otherwise neither the _prefix_ nor the _name_ part
        /// can be empty when the symbol contains `/`.
        ///
        /// If a symbol has a _prefix_ and `/`, the following _name_ component should follow the
        /// first-character restrictions for symbols as a whole. This is to avoid ambiguity in
        /// reading contexts where prefixes might be presumed as implicitly included namespaces and
        /// elided thereafter.
        Symbol(Name),

        /// Keywords are identifiers that typically designate themselves. They are semantically
        /// akin to enumeration values. Keywords follow the rules of symbols, except they can (and
        /// must) begin with `:`, e.g. `:fred` or `:my/fred`. If the target platform does not have
        /// a keyword type distinct from a symbol type, the same type can be used without conflict,
        /// since the mandatory leading `:` of keywords is disallowed for symbols. Per the symbol
        /// rules above, :/ and :/anything are not legal keywords. A keyword cannot begin with ::
        ///
        /// If the target platform supports some notion of interning, it is a further semantic of
        /// keywords that all instances of the same keyword yield the identical object.
        Keyword(Name),

        /// Integers consist of the digits `0` - `9`, optionally prefixed by `-` to indicate a
        /// negative number, or (redundantly) by `+`. No integer other than 0 may begin with 0.
        /// 64-bit (signed integer) precision is expected. An integer can have the suffix `N` to
        /// indicate that arbitrary precision is desired. -0 is a valid integer not distinct from
        /// 0.
        ///
        /// ```
        ///   integer
        ///     int
        ///     int N
        ///   digit
        ///     0-9
        ///   int
        ///     digit
        ///     1-9 digits
        ///     + digit
        ///     + 1-9 digits
        ///     - digit
        ///     - 1-9 digits
        /// ```
        Integer(i64),

        /// 64-bit (double) precision is expected.
        ///
        /// ```
        ///   floating-point-number
        ///     int M
        ///     int frac
        ///     int exp
        ///     int frac exp
        ///   digit
        ///     0-9
        ///   int
        ///     digit
        ///     1-9 digits
        ///     + digit
        ///     + 1-9 digits
        ///     - digit
        ///     - 1-9 digits
        ///   frac
        ///     . digits
        ///   exp
        ///     ex digits
        ///   digits
        ///     digit
        ///     digit digits
        ///   ex
        ///     e
        ///     e+
        ///     e-
        ///     E
        ///     E+
        ///     E-
        /// ```
        ///
        /// In addition, a floating-point number may have the suffix `M` to indicate that exact
        /// precision is desired.
        Float(OrderedFloat<f64>),

        /// A list is a sequence of values. Lists are represented by zero or more elements enclosed
        /// in parentheses `()`. Note that lists can be heterogeneous.
        ///
        /// ```
        /// (a b 42)
        /// ```
        List(Vec<Edn>),

        /// A vector is a sequence of values that supports random access. Vectors are represented
        /// by zero or more elements enclosed in square brackets `[]`. Note that vectors can be
        /// heterogeneous.
        ///
        /// ```
        /// [a b 42]
        /// ```
        Vector(Vec<Edn>),

        /// A map is a collection of associations between keys and values. Maps are represented by
        /// zero or more key and value pairs enclosed in curly braces `{}`. Each key should appear
        /// at most once. No semantics should be associated with the order in which the pairs
        /// appear.
        ///
        /// ```
        /// {:a 1, "foo" :bar, [1 2 3] four}
        /// ```
        ///
        /// Note that keys and values can be elements of any type. The use of commas above is
        /// optional, as they are parsed as whitespace.
        Map(BTreeMap<Edn, Edn>),

        /// A set is a collection of unique values. Sets are represented by zero or more elements
        /// enclosed in curly braces preceded by `#` `#{}`. No semantics should be associated with
        /// the order in which the elements appear. Note that sets can be heterogeneous.
        ///
        /// ```
        /// #{a b [1 2 3]}
        /// ```
        Set(BTreeSet<Edn>),
    }

    impl From<f64> for Edn {
        fn from(number: f64) -> Self {
            if number.fract() == 0.0 {
                Edn::Integer(number as i64)
            } else {
                Edn::Float(OrderedFloat(number))
            }
        }
    }

    impl TryFrom<&str> for Edn {
        type Error = String; // nom::Err<nom::error::Error<str>>;

        fn try_from(input: &str) -> Result<Self, Self::Error> {
            match edn(input) {
                Ok(("", edn)) => Ok(edn),
                Ok((leftovers, _)) => Err(format!("Leftovers: {}", leftovers)),
                Err(err) => Err(err.to_string()),
            }
        }
    }

    fn ws(input: &str) -> IResult<&str, &str> {
        take_while1(|c: char| c.is_whitespace() || c == ',')(input)
    }

    fn edns(input: &str) -> IResult<&str, Vec<Edn>> {
        separated_list0(ws, edn)(input)
    }

    fn entries(input: &str) -> IResult<&str, Vec<(Edn, Edn)>> {
        separated_list0(ws, separated_pair(edn, ws, edn))(input)
    }

    fn edn(input: &str) -> IResult<&str, Edn> {
        alt((
            tag("nil").map(|_| Edn::Nil),
            tag("true").map(|_| Edn::Boolean(true)),
            tag("false").map(|_| Edn::Boolean(false)),
            double.map(<Edn as From<f64>>::from),
            delimited(char('"'), is_not("\""), char('"')).map(|str| Edn::String(Rc::from(str))),
            delimited(char('['), edns, char(']')).map(Edn::Vector),
            delimited(char('('), edns, char(')')).map(Edn::List),
            delimited(tag("#{"), edns, char('}')).map(|xs| Edn::Set(xs.into_iter().collect())),
            delimited(char('{'), entries, char('}')).map(|xs| Edn::Map(xs.into_iter().collect())),
            preceded(char(':'), name).map(Edn::Keyword),
            name.map(Edn::Symbol),
        ))(input)
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn test_invalid_edn() {
            let result = Edn::try_from("[foo");

            assert!(result.is_err())
        }

        #[test]
        fn test_no_leftovers() {
            let result = Edn::try_from("[foo] bar");

            assert!(result.is_err())
        }

        #[test]
        fn test_nil() {
            let result = Edn::try_from("nil");

            assert_eq!(result, Ok(Edn::Nil));
        }

        #[test]
        fn test_true() {
            let result = Edn::try_from("true");

            assert_eq!(result, Ok(Edn::Boolean(true)));
        }

        #[test]
        fn test_false() {
            let result = Edn::try_from("false");

            assert_eq!(result, Ok(Edn::Boolean(false)));
        }

        #[test]
        fn test_string() {
            let result = Edn::try_from(r#""hello world""#);

            assert_eq!(result, Ok(Edn::String(Rc::from("hello world"))));
        }

        #[test]
        #[ignore]
        fn test_string_escape() {
            let result = Edn::try_from(r#""hello \"world\"""#);

            assert_eq!(result, Ok(Edn::String(Rc::from(r#"hello "world""#))));
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
        fn test_integer() {
            let result = Edn::try_from("1234");

            assert_eq!(result, Ok(Edn::Integer(1234)));
        }

        #[test]
        fn test_float() {
            let result = Edn::try_from("12.34");

            assert_eq!(result, Ok(Edn::Float(OrderedFloat(12.34))));
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
        fn consider_commas_as_whitespace() {
            let result = Edn::try_from("[foo, bar]");

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
            let result = Edn::try_from("#{1 2 3}");

            assert_eq!(
                result,
                Ok(Edn::Set(BTreeSet::from([
                    Edn::Integer(1),
                    Edn::Integer(2),
                    Edn::Integer(3)
                ])))
            );
        }
    }
}

fn find(input: &str) -> IResult<&str, Vec<Find>> {
    let (input, _) = tag(":find")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, names) = separated_list1(multispace1, name)(input)?;
    let find = names
        .into_iter()
        .map(|name| Find::Variable(name.name))
        .collect();

    Ok((input, find))
}

fn clause(input: &str) -> IResult<&str, Clause> {
    todo!()
}

fn parse_where(input: &str) -> IResult<&str, Vec<Clause>> {
    let (input, _) = tag(":where")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, clauses) = separated_list1(multispace1, clause)(input)?;
    Ok((input, clauses))
}

pub fn parse(input: &str) -> Result<Query, String> {
    let (_, (find, clauses)) =
        delimited(char('['), find.and(parse_where), char(']'))(input).unwrap();
    Ok(Query {
        find,
        clauses,
        predicates: Vec::new(),
    })
}

enum State {
    Begin,
    Find,
    Where,
}

pub fn parse2(input: &str) -> Result<Query, String> {
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
                if let Edn::Vector(clauses) = part {
                    let clauses = parse_clauses(clauses)?;
                    for clause in clauses {
                        query = query.r#where(clause);
                    }
                } else {
                    return Err("Invalid".to_string());
                }
            }
        }
    }

    Ok(query)
}

fn parse_clauses(clauses: Vec<Edn>) -> Result<Vec<Clause>, String> {
    todo!()
}

fn name_part(input: &str) -> IResult<&str, &str> {
    take_while1(|c: char| c.is_ascii_alphanumeric() || ".*+!-_?$%&=<>".contains(c))(input)
}

fn name(input: &str) -> IResult<&str, Name> {
    let (input, first) = name_part(input)?;
    let (input, second) = opt(preceded(char('/'), name_part))(input)?;
    let name = match second {
        Some(second) => Name::namespaced(first, second),
        None => Name::from(first),
    };
    Ok((input, name))
}

#[cfg(test)]
mod tests {
    use rustomic::query::Find;

    use super::*;

    #[test]
    fn test_empty_query() {
        let result = parse2("");

        assert!(result.is_err());
    }

    #[test]
    fn parse_a_single_find_clause() {
        let query = "[:find ?foo]";

        assert_eq!(parse2(query), Ok(Query::new().find(Find::variable("?foo"))));
    }

    #[test]
    fn parse_multiple_find_clauses() {
        let query = "[:find ?foo ?bar]";

        assert_eq!(
            parse2(query),
            Ok(Query::new()
                .find(Find::variable("?foo"))
                .find(Find::variable("?bar")))
        );
    }

    #[test]
    fn parse_where_clauses() {
        let query = r#"[:find ?release-name
                        :where [?artist :artist/name "John Lenon"]
                               [?release :release/artist ?artist]
                               [?release :release/name ?release-name]]"#;

        assert_eq!(
            parse2(query),
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
