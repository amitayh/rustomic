extern crate rustomic;

use quickcheck::*;
use quickcheck_macros::quickcheck;
use rust_decimal::prelude::*;
use rustomic::datom::*;
use rustomic::storage::*;

#[quickcheck]
fn test_serialization(datom: ArbitraryDatom) {
    let ArbitraryDatom(datom) = datom;
    let eavt = serde::datom::serialize::eavt(&datom);
    let aevt = serde::datom::serialize::aevt(&datom);
    let avet = serde::datom::serialize::avet(&datom);

    assert_eq!(
        datom,
        serde::datom::deserialize(serde::Index::Eavt, &eavt).unwrap()
    );
    assert_eq!(
        datom,
        serde::datom::deserialize(serde::Index::Aevt, &aevt).unwrap()
    );
    assert_eq!(
        datom,
        serde::datom::deserialize(serde::Index::Avet, &avet).unwrap()
    );
}

#[derive(Debug, Clone)]
struct ArbitraryDatom(Datom);

impl Arbitrary for ArbitraryDatom {
    fn arbitrary(g: &mut Gen) -> Self {
        Self(Datom {
            entity: u64::arbitrary(g),
            attribute: u64::arbitrary(g),
            value: arbitrary_value(g),
            tx: u64::arbitrary(g),
            op: arbitrary_op(g),
        })
    }
}

fn arbitrary_value(g: &mut Gen) -> Value {
    match g.choose(&[0, 1, 2, 3, 4, 5]) {
        Some(0) => Value::Nil,
        Some(1) => Value::I64(i64::arbitrary(g)),
        Some(2) => Value::U64(u64::arbitrary(g)),
        Some(3) => Value::Decimal(arbitrary_decimal(g)),
        Some(4) => Value::Str(String::arbitrary(g).into()),
        Some(5) => Value::Ref(u64::arbitrary(g)),
        _ => unreachable!(),
    }
}

fn arbitrary_decimal(g: &mut Gen) -> Decimal {
    let mut arr = [0u8; 16];
    for x in &mut arr {
        *x = u8::arbitrary(g);
    }
    Decimal::deserialize(arr)
}

fn arbitrary_op(g: &mut Gen) -> Op {
    if bool::arbitrary(g) {
        Op::Assert
    } else {
        Op::Retract
    }
}
