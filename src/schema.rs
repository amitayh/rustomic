use crate::datom;

enum ValueType {
    U8 = 0,
    I32 = 1,
    U32 = 2,
    I64 = 3,
    U64 = 4,
    Str = 5,
}

// Cardinality
// One: 0
// Many: 1

// Op
// Added: 0
// Retracted: 1

fn datom<V: Into<datom::Value>>(entity: u64, attribute: u64, value: V, tx: u64) -> datom::Datom {
    datom::Datom {
        entity,
        attribute,
        value: value.into(),
        tx,
        op: datom::Op::Added,
    }
}

impl Into<datom::Value> for &str {
    fn into(self) -> datom::Value {
        datom::Value::Str(String::from(self))
    }
}

impl Into<datom::Value> for u8 {
    fn into(self) -> datom::Value {
        datom::Value::U8(self)
    }
}

impl Into<datom::Value> for u64 {
    fn into(self) -> datom::Value {
        datom::Value::U64(self)
    }
}

pub fn get_default_datoms() -> Vec<datom::Datom> {
    vec![
        // "db/attr/ident" attribute
        datom(1, 1, "db/attr/ident", 6),
        datom(1, 2, "Human readable name of attribute", 6),
        datom(1, 3, ValueType::Str as u8, 6),
        datom(1, 4, 0u8, 6),
        // "db/attr/doc" attribute
        datom(2, 1, "db/attr/doc", 6),
        datom(2, 2, "Documentation of attribute", 6),
        datom(2, 3, ValueType::Str as u8, 6),
        datom(2, 4, 0u8, 6),
        // "db/attr/type" attribute
        datom(3, 1, "db/attr/type", 6),
        datom(3, 2, "Data type of attribute", 6),
        datom(3, 3, ValueType::U8 as u8, 6),
        datom(3, 4, 0u8, 6),
        // "db/attr/cardinality" attribute
        datom(4, 1, "db/attr/cardinality", 6),
        datom(4, 2, "Cardinality of attribyte", 6),
        datom(4, 3, ValueType::U8 as u8, 6),
        datom(4, 4, 0u8, 6),
        // "db/tx/time" attribute
        datom(5, 1, "db/tx/time", 6),
        datom(5, 2, "Transaction's wall clock time", 6),
        datom(5, 3, ValueType::U64 as u8, 6),
        datom(5, 4, 0u8, 6),
        // first transaction
        datom(6, 5, 0u64, 6),
    ]
}
