use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BinaryDecodeError {
    UnexpectedEof,
    Utf8,
    LengthOverflow,
}

impl fmt::Display for BinaryDecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnexpectedEof => write!(f, "unexpected eof"),
            Self::Utf8 => write!(f, "invalid utf8"),
            Self::LengthOverflow => write!(f, "length overflow"),
        }
    }
}

impl std::error::Error for BinaryDecodeError {}

#[derive(Default)]
pub struct BinaryEncoder {
    out: Vec<u8>,
}

impl BinaryEncoder {
    pub fn new() -> Self {
        Self { out: Vec::new() }
    }

    pub fn put_u8(&mut self, v: u8) {
        self.out.push(v);
    }

    pub fn put_u32(&mut self, v: u32) {
        self.out.extend_from_slice(&v.to_le_bytes());
    }

    pub fn put_u64(&mut self, v: u64) {
        self.out.extend_from_slice(&v.to_le_bytes());
    }

    pub fn put_bytes(&mut self, b: &[u8]) {
        self.put_u32(b.len() as u32);
        self.out.extend_from_slice(b);
    }

    pub fn put_string(&mut self, s: &str) {
        self.put_bytes(s.as_bytes());
    }

    pub fn finish(self) -> Vec<u8> {
        self.out
    }
}

pub struct BinaryDecoder<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> BinaryDecoder<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, at: 0 }
    }

    pub fn get_u8(&mut self) -> Result<u8, BinaryDecodeError> {
        let b = self.take(1)?;
        Ok(b[0])
    }

    pub fn get_u32(&mut self) -> Result<u32, BinaryDecodeError> {
        let b = self.take(4)?;
        Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }

    pub fn get_u64(&mut self) -> Result<u64, BinaryDecodeError> {
        let b = self.take(8)?;
        Ok(u64::from_le_bytes([
            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        ]))
    }

    pub fn get_bytes(&mut self) -> Result<Vec<u8>, BinaryDecodeError> {
        let len = self.get_u32()? as usize;
        let b = self.take(len)?;
        Ok(b.to_vec())
    }

    pub fn get_string(&mut self) -> Result<String, BinaryDecodeError> {
        let bytes = self.get_bytes()?;
        String::from_utf8(bytes).map_err(|_| BinaryDecodeError::Utf8)
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], BinaryDecodeError> {
        let end = self
            .at
            .checked_add(len)
            .ok_or(BinaryDecodeError::LengthOverflow)?;
        if end > self.bytes.len() {
            return Err(BinaryDecodeError::UnexpectedEof);
        }
        let s = &self.bytes[self.at..end];
        self.at = end;
        Ok(s)
    }
}
