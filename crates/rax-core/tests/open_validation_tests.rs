use rax_core::format::{validate_open, FormatError, MV2SFooter, MV2SHeader};

#[test]
fn rejects_corrupt_footer_magic() {
    let header = MV2SHeader {
        generation: 1,
        toc_offset: 128,
    }
    .encode();
    let mut footer = MV2SFooter {
        generation: 1,
        toc_offset: 128,
    }
    .encode();
    footer[0] = b'X';

    let err = validate_open(&header, &header, &footer).unwrap_err();
    assert_eq!(err, FormatError::InvalidMagic);
}
