use std::collections::HashSet;

mod datom;
mod tx;

#[derive(Hash, Eq, PartialEq, Debug)]
enum Cardinality {
    One,
    Many,
}

#[derive(Hash, Eq, PartialEq, Debug)]
enum ValueType {
    Ref,
    Str,
}

#[derive(Hash, Eq, PartialEq, Debug)]
struct Attribute {
    ident: String,
    cardinality: Cardinality,
    value_type: ValueType,
    doc: Option<String>,
}

// fn to_datoms(attribute: Attribute) -> HashSet<Datom> {
//     let mut datoms = HashSet::new();
//     datoms.insert(Datom {
//         entity: 1, // Attribute entity ID
//         attribute: 2, // Ident
//         value: Value::Str(attribute.ident),
//         tx: 1,
//         op: Op::Added
//     });
//     datoms
// }

fn main() {
    let mut schema = HashSet::new();
    schema.insert(Attribute {
        ident: String::from("artist/name"),
        cardinality: Cardinality::One,
        value_type: ValueType::Str,
        doc: Some(String::from("An artist's name")),
    });
    schema.insert(Attribute {
        ident: String::from("artist/country"),
        cardinality: Cardinality::One,
        value_type: ValueType::Ref,
        doc: Some(String::from("An artist's country of residence")),
    });
    schema.insert(Attribute {
        ident: String::from("artist/release"),
        cardinality: Cardinality::Many,
        value_type: ValueType::Ref,
        doc: None,
    });

    let transaction = tx::Transaction {
        operations: vec![tx::Operation::Add {
            entity: tx::EntityIdentifier::Existing(1),
            attribute: 1,
            value: datom::Value::U32(42),
        }],
    };

    let mut db = HashSet::new();
    db.insert(datom::Datom {
        entity: 1,
        attribute: 2,
        value: datom::Value::I32(1),
        tx: 1,
        op: datom::Op::Added,
    });
    db.insert(datom::Datom {
        entity: 10,
        attribute: 2,
        value: datom::Value::I32(1),
        tx: 1,
        op: datom::Op::Added,
    });

    println!("Hello, world!");
}
