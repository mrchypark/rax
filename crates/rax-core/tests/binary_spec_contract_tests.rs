use rax_core::codec::{BinaryDecoder, BinaryEncoder};

#[test]
fn binary_codec_matches_frozen_spec_vectors() {
    let mut enc = BinaryEncoder::new();
    enc.put_u8(7);
    enc.put_u32(42);
    enc.put_u64(9_999);
    enc.put_string("mv2s");
    enc.put_bytes(&[1, 2, 3]);
    let got = enc.finish();

    let expected: Vec<u8> = vec![
        0x07, 0x2a, 0x00, 0x00, 0x00, 0x0f, 0x27, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00,
        0x00, 0x00, 0x6d, 0x76, 0x32, 0x73, 0x03, 0x00, 0x00, 0x00, 0x01, 0x02, 0x03,
    ];
    assert_eq!(got, expected);

    let mut dec = BinaryDecoder::new(&got);
    assert_eq!(dec.get_u8().unwrap(), 7);
    assert_eq!(dec.get_u32().unwrap(), 42);
    assert_eq!(dec.get_u64().unwrap(), 9_999);
    assert_eq!(dec.get_string().unwrap(), "mv2s");
    assert_eq!(dec.get_bytes().unwrap(), vec![1, 2, 3]);
}
