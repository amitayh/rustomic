extern crate rustomic;

use quickcheck_macros::quickcheck;
use rustomic::datom::*;
use rustomic::storage::*;

#[quickcheck]
fn test_serialization(datom: Datom) {
    let eavt = serde::datom::serialize::eavt(&datom);
    let aevt = serde::datom::serialize::aevt(&datom);
    let avet = serde::datom::serialize::avet(&datom);

    assert_eq!(datom, serde::datom::deserialize(serde::Index::Eavt, &eavt).unwrap());
    assert_eq!(datom, serde::datom::deserialize(serde::Index::Aevt, &aevt).unwrap());
    assert_eq!(datom, serde::datom::deserialize(serde::Index::Avet, &avet).unwrap());
}
