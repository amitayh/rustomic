extern crate rustomic;

use quickcheck::*;
use quickcheck_macros::quickcheck;
use rust_decimal::prelude::*;
use rustomic::datom::*;
use rustomic::storage::*;

#[quickcheck]
fn test_eavt_serialization(datom: ArbitraryDatom) {
    let ArbitraryDatom(datom) = datom;
    let serialized = serde::datom::serialize::eavt(&datom);
    let deserialized = serde::datom::deserialize(serde::Index::Eavt, &serialized);

    assert!(deserialized.is_ok());
    assert_eq!(datom, deserialized.unwrap());
}

#[quickcheck]
fn test_aevt_serialization(datom: ArbitraryDatom) {
    let ArbitraryDatom(datom) = datom;
    let serialized = serde::datom::serialize::aevt(&datom);
    let deserialized = serde::datom::deserialize(serde::Index::Aevt, &serialized);

    assert!(deserialized.is_ok());
    assert_eq!(datom, deserialized.unwrap());
}

#[quickcheck]
fn test_avet_serialization(datom: ArbitraryDatom) {
    let ArbitraryDatom(datom) = datom;
    let serialized = serde::datom::serialize::avet(&datom);
    let deserialized = serde::datom::deserialize(serde::Index::Avet, &serialized);

    assert!(deserialized.is_ok());
    assert_eq!(datom, deserialized.unwrap());
}

#[derive(Debug, Clone)]
struct ArbitraryDatom(Datom);

impl Arbitrary for ArbitraryDatom {
    fn arbitrary(g: &mut Gen) -> Self {
        Self(Datom {
            entity: u64::arbitrary(g),
            attribute: u64::arbitrary(g),
            value: ArbitraryValue::arbitrary(g).0,
            tx: u64::arbitrary(g),
            op: arbitrary_op(g),
        })
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        let ArbitraryDatom(datom) = self.clone();
        Box::new(
            ArbitraryValue(datom.value)
                .shrink()
                .map(move |ArbitraryValue(value)| {
                    Self(Datom {
                        entity: datom.entity,
                        attribute: datom.attribute,
                        value,
                        tx: datom.tx,
                        op: datom.op,
                    })
                }),
        )
    }
}

#[derive(Debug, Clone)]
struct ArbitraryValue(Value);

impl Arbitrary for ArbitraryValue {
    fn arbitrary(g: &mut Gen) -> Self {
        Self(match g.choose(&[0, 1, 2, 3, 4, 5]) {
            Some(0) => Value::Nil,
            Some(1) => Value::I64(i64::arbitrary(g)),
            Some(2) => Value::U64(u64::arbitrary(g)),
            Some(3) => Value::Decimal(arbitrary_decimal(g)),
            Some(4) => Value::Str(String::arbitrary(g).into()),
            Some(5) => Value::Ref(u64::arbitrary(g)),
            _ => unreachable!(),
        })
    }

    fn shrink(&self) -> Box<dyn Iterator<Item = Self>> {
        if matches!(&self.0, Value::Nil) {
            return empty_shrinker();
        }

        let chain = single_shrinker(Self(Value::Nil)).chain(
            match &self.0 {
                Value::Nil | Value::Decimal(_) => empty_shrinker(),
                Value::I64(value) => Box::new(value.shrink().map(Value::I64)),
                Value::U64(value) => Box::new(value.shrink().map(Value::U64)),
                Value::Str(value) => Box::new(value.to_string().shrink().map(Value::Str)),
                Value::Ref(value) => Box::new(value.shrink().map(Value::Ref)),
            }
            .map(Self),
        );

        Box::new(chain)
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
