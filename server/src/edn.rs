use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Display;

use nom::branch::*;
use nom::bytes::complete::*;
use nom::character::complete::*;
use nom::combinator::*;
use nom::multi::*;
use nom::number::complete::*;
use nom::sequence::*;
use nom::IResult;
use nom::Parser;

use ordered_float::OrderedFloat;

#[derive(PartialEq, Debug, Clone, PartialOrd, Eq, Ord)]
pub struct Name {
    pub namespace: Option<String>,
    pub name: String,
}

impl Name {
    pub fn from(name: &str) -> Self {
        Self {
            namespace: None,
            name: name.to_string(),
        }
    }

    pub fn namespaced(namespace: &str, name: &str) -> Self {
        Self {
            namespace: Some(namespace.to_string()),
            name: name.to_string(),
        }
    }
}

impl Display for Name {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.namespace {
            Some(namespace) => write!(f, "{}/{}", namespace, self.name),
            None => write!(f, "{}", self.name),
        }
    }
}

impl Into<String> for &Name {
    fn into(self) -> String {
        format!("{}", self)
    }
}

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
    String(String),

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

impl Edn {
    fn string(str: &str) -> Self {
        Self::String(str.to_string())
    }
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

impl Display for Edn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Edn::Nil => write!(f, "nil"),
            Edn::Boolean(value) => write!(f, "{}", value),
            Edn::String(value) => write!(f, r#""{}""#, value),
            Edn::Integer(value) => write!(f, "{}", value),
            Edn::Float(value) => write!(f, "{}", value),
            Edn::Symbol(value) => write!(f, "{}", value),
            Edn::Vector(values) => {
                write!(f, "[")?;
                for (index, value) in values.iter().enumerate() {
                    if index > 0 {
                        write!(f, " ")?;
                    }
                    write!(f, "{}", value)?;
                }
                write!(f, "]")?;
                Ok(())
            }
            _ => Err(std::fmt::Error),
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

fn edn(input: &str) -> IResult<&str, Edn> {
    alt((
        tag("nil").map(|_| Edn::Nil),
        tag("true").map(|_| Edn::Boolean(true)),
        tag("false").map(|_| Edn::Boolean(false)),
        double.map(<Edn as From<f64>>::from),
        delimited(char('"'), is_not("\""), char('"')).map(Edn::string),
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

        assert_eq!(result, Ok(Edn::String("hello world".to_string())));
    }

    #[test]
    #[ignore]
    fn test_string_escape() {
        let result = Edn::try_from(r#""hello \"world\"""#);

        assert_eq!(result, Ok(Edn::String(r#"hello "world""#.to_string())));
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

    mod format {
        use super::*;

        #[test]
        fn test_nil() {
            assert_eq!(format!("{}", Edn::Nil), "nil");
        }

        #[test]
        fn test_boolean() {
            assert_eq!(format!("{}", Edn::Boolean(true)), "true");
            assert_eq!(format!("{}", Edn::Boolean(false)), "false");
        }

        #[test]
        fn test_string() {
            let edn = Edn::string("hello world");

            assert_eq!(format!("{}", edn), r#""hello world""#);
        }

        // TODO: string escaping

        #[test]
        fn test_number() {
            assert_eq!(format!("{}", Edn::Integer(1234)), "1234");
            assert_eq!(format!("{}", Edn::Float(OrderedFloat(12.34))), "12.34");
        }

        #[test]
        fn test_symbol() {
            let plain = Edn::Symbol(Name::from("foo"));
            let namespaced = Edn::Symbol(Name::namespaced("foo", "bar"));

            assert_eq!(format!("{}", plain), "foo");
            assert_eq!(format!("{}", namespaced), "foo/bar");
        }

        #[test]
        fn test_empty_vector() {
            assert_eq!(format!("{}", Edn::Vector(vec![])), "[]");
        }

        #[test]
        fn test_non_empty_vector() {
            let edn = Edn::Vector(vec![
                Edn::Symbol(Name::from("foo")),
                Edn::Symbol(Name::from("bar")),
            ]);

            assert_eq!(format!("{}", edn), "[foo bar]");
        }
    }
}
