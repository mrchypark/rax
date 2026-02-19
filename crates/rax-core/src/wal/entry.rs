use crate::codec::{BinaryDecodeError, BinaryDecoder, BinaryEncoder};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WALEntry {
    PutFrame { frame_id: u64, payload: Vec<u8> },
    DeleteFrame { frame_id: u64 },
}

impl WALEntry {
    pub fn encode(&self) -> Vec<u8> {
        let mut enc = BinaryEncoder::new();
        match self {
            Self::PutFrame { frame_id, payload } => {
                enc.put_u8(1);
                enc.put_u64(*frame_id);
                enc.put_bytes(payload);
            }
            Self::DeleteFrame { frame_id } => {
                enc.put_u8(2);
                enc.put_u64(*frame_id);
            }
        }
        enc.finish()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, BinaryDecodeError> {
        let mut dec = BinaryDecoder::new(bytes);
        let tag = dec.get_u8()?;
        let frame_id = dec.get_u64()?;
        match tag {
            1 => Ok(Self::PutFrame {
                frame_id,
                payload: dec.get_bytes()?,
            }),
            2 => Ok(Self::DeleteFrame { frame_id }),
            _ => Err(BinaryDecodeError::UnexpectedEof),
        }
    }
}
