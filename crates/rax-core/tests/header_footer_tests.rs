use rax_core::format::{validate_open, MV2SFooter, MV2SHeader};

#[test]
fn selects_newer_valid_header_generation() {
    let older = MV2SHeader {
        generation: 1,
        toc_offset: 128,
    }
    .encode();
    let newer = MV2SHeader {
        generation: 2,
        toc_offset: 256,
    }
    .encode();
    let footer = MV2SFooter {
        generation: 2,
        toc_offset: 256,
    }
    .encode();

    let open = validate_open(&older, &newer, &footer).unwrap();
    assert_eq!(open.generation, 2);
    assert_eq!(open.toc_offset, 256);
}
