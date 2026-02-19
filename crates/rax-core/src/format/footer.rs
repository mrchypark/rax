use crate::format::{checksum, FormatError};

const FOOTER_MAGIC: [u8; 4] = *b"MV2F";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MV2SFooter {
    pub generation: u64,
    pub toc_offset: u64,
}

impl MV2SFooter {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(24);
        out.extend_from_slice(&FOOTER_MAGIC);
        out.extend_from_slice(&self.generation.to_le_bytes());
        out.extend_from_slice(&self.toc_offset.to_le_bytes());
        let csum = checksum(&out);
        out.extend_from_slice(&csum.to_le_bytes());
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, FormatError> {
        if bytes.len() != 24 {
            return Err(FormatError::InvalidLength);
        }
        if bytes[0..4] != FOOTER_MAGIC {
            return Err(FormatError::InvalidMagic);
        }
        let expected = checksum(&bytes[..20]);
        let actual = u32::from_le_bytes(bytes[20..24].try_into().map_err(|_| FormatError::InvalidLength)?);
        if expected != actual {
            return Err(FormatError::ChecksumMismatch);
        }
        Ok(Self {
            generation: u64::from_le_bytes(bytes[4..12].try_into().map_err(|_| FormatError::InvalidLength)?),
            toc_offset: u64::from_le_bytes(bytes[12..20].try_into().map_err(|_| FormatError::InvalidLength)?),
        })
    }
}
