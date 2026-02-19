use std::io;
use std::io::Cursor;

use tokio::io::AsyncReadExt;

pub struct StreamingReader {
    cursor: Cursor<Vec<u8>>,
}

impl StreamingReader {
    pub fn new(bytes: Vec<u8>) -> Self {
        Self {
            cursor: Cursor::new(bytes),
        }
    }

    pub async fn read_chunk(&mut self, chunk_size: usize) -> io::Result<Option<Vec<u8>>> {
        let mut buf = vec![0u8; chunk_size.max(1)];
        let n = self.cursor.read(&mut buf).await?;
        if n == 0 {
            return Ok(None);
        }
        buf.truncate(n);
        Ok(Some(buf))
    }
}
