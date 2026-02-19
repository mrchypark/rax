use crate::format::{checksum, FormatError};

const TOC_MAGIC: [u8; 4] = *b"MV2T";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MV2SToc {
    pub frame_count: u64,
}

impl MV2SToc {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(16);
        out.extend_from_slice(&TOC_MAGIC);
        out.extend_from_slice(&self.frame_count.to_le_bytes());
        let csum = checksum(&out);
        out.extend_from_slice(&csum.to_le_bytes());
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, FormatError> {
        if bytes.len() != 16 {
            return Err(FormatError::InvalidLength);
        }
        if bytes[0..4] != TOC_MAGIC {
            return Err(FormatError::InvalidMagic);
        }
        let expected = checksum(&bytes[..12]);
        let actual = u32::from_le_bytes(bytes[12..16].try_into().map_err(|_| FormatError::InvalidLength)?);
        if expected != actual {
            return Err(FormatError::ChecksumMismatch);
        }
        Ok(Self {
            frame_count: u64::from_le_bytes(bytes[4..12].try_into().map_err(|_| FormatError::InvalidLength)?),
        })
    }
}
