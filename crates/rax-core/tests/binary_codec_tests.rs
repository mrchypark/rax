#[test]
fn binary_codec_round_trip_primitives() {
    let mut enc = rax_core::codec::BinaryEncoder::new();
    enc.put_u8(7);
    enc.put_u32(42);
    enc.put_u64(9_999);
    enc.put_string("mv2s");
    enc.put_bytes(&[1, 2, 3]);

    let bytes = enc.finish();
    let mut dec = rax_core::codec::BinaryDecoder::new(&bytes);

    assert_eq!(dec.get_u8().unwrap(), 7);
    assert_eq!(dec.get_u32().unwrap(), 42);
    assert_eq!(dec.get_u64().unwrap(), 9_999);
    assert_eq!(dec.get_string().unwrap(), "mv2s");
    assert_eq!(dec.get_bytes().unwrap(), vec![1, 2, 3]);
}
