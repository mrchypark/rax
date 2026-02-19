use tokio::io::AsyncWriteExt;

use rax_core::io::streaming_writer::StreamingWriter;

#[tokio::test]
async fn stream_large_payload_without_full_buffering() {
    let payload = vec![7u8; 64 * 1024];
    let (mut tx, mut rx) = tokio::io::duplex(1024);
    let cloned = payload.clone();

    tokio::spawn(async move {
        tx.write_all(&cloned).await.unwrap();
        tx.shutdown().await.unwrap();
    });

    let mut writer = StreamingWriter::new(4096);
    let out = writer.write_from(&mut rx).await.unwrap();

    assert_eq!(out, payload);
    assert!(writer.max_chunk_seen() <= 4096);
}
