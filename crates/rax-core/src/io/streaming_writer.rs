use std::io;

use tokio::io::{AsyncRead, AsyncReadExt};

pub struct StreamingWriter {
    chunk_size: usize,
    max_chunk_seen: usize,
}

impl StreamingWriter {
    pub fn new(chunk_size: usize) -> Self {
        Self {
            chunk_size: chunk_size.max(1),
            max_chunk_seen: 0,
        }
    }

    pub async fn write_from<R: AsyncRead + Unpin>(&mut self, reader: &mut R) -> io::Result<Vec<u8>> {
        let mut out = Vec::new();
        let mut buf = vec![0u8; self.chunk_size];

        loop {
            let n = reader.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            self.max_chunk_seen = self.max_chunk_seen.max(n);
            out.extend_from_slice(&buf[..n]);
        }
        Ok(out)
    }

    pub fn max_chunk_seen(&self) -> usize {
        self.max_chunk_seen
    }
}
