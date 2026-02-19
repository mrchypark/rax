pub mod constants;
pub mod footer;
pub mod header;
pub mod toc;

pub use constants::spec_version;
pub use footer::MV2SFooter;
pub use header::MV2SHeader;
pub use toc::MV2SToc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FormatError {
    InvalidLength,
    InvalidMagic,
    ChecksumMismatch,
    NoValidHeader,
    GenerationMismatch,
    TocOffsetMismatch,
    TocOffsetOutOfRange,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenState {
    pub generation: u64,
    pub toc_offset: u64,
}

pub fn checksum(bytes: &[u8]) -> u32 {
    bytes.iter().fold(0_u32, |acc, b| acc.wrapping_add(*b as u32))
}

pub fn validate_open(page_a: &[u8], page_b: &[u8], footer: &[u8]) -> Result<OpenState, FormatError> {
    let a = MV2SHeader::decode(page_a).ok();
    let b = MV2SHeader::decode(page_b).ok();

    let selected = match (a, b) {
        (Some(x), Some(y)) => {
            if x.generation >= y.generation {
                x
            } else {
                y
            }
        }
        (Some(x), None) => x,
        (None, Some(y)) => y,
        (None, None) => return Err(FormatError::NoValidHeader),
    };

    let foot = MV2SFooter::decode(footer)?;
    if foot.generation != selected.generation {
        return Err(FormatError::GenerationMismatch);
    }
    if foot.toc_offset != selected.toc_offset {
        return Err(FormatError::TocOffsetMismatch);
    }
    if selected.toc_offset < 32 {
        return Err(FormatError::TocOffsetOutOfRange);
    }

    Ok(OpenState {
        generation: selected.generation,
        toc_offset: selected.toc_offset,
    })
}
