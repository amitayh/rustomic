pub struct Variable(pub String);

pub struct Clause {
    pub entity: u64,
    pub attribute: u64,
    pub value: u64,
}

pub struct Query {
    pub find: Vec<Variable>,
    pub wher: Vec<Clause>,
}

pub struct QueryResult {}
