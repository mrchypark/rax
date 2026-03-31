# Wax v1 Binary Format

Status: Draft  
Date: 2026-03-29  
Audience: Rust implementers building a compatible `.wax` reader/writer

## 1. Purpose

This document defines the byte-level binary contract for Wax v1 `.wax` files.

The goal is narrower than the broader rewrite spec:

- define exact on-disk layout
- define field ordering and primitive encoding rules
- define recovery and commit semantics that affect compatibility
- mark opaque regions that may be carried forward without native interpretation

This document does not define a Rust-native text lane or embedding backend.

## 2. Compatibility Target

Wax v1 compatibility means:

- existing Swift-generated `.wax` files can be opened
- committed payloads, WAL state, TOC, and manifests can be validated
- a Rust writer can emit files that the current Swift implementation can open, unless explicitly called out as feature-gated

Important boundary:

- the core file format is fully specifiable
- the `lex` segment payload is an opaque SQLite FTS5 database blob
- the `vec` segment may be either opaque native index bytes or portable flat vectors

## 3. Primary Upstream Sources

The primary upstream sources for this document are the official `WaxCore.docc` pages:

- [Documentation.md](https://github.com/christopherkarani/Wax/blob/main/Sources/WaxCore/WaxCore.docc/Documentation.md)
- [FileFormat.md](https://github.com/christopherkarani/Wax/blob/main/Sources/WaxCore/WaxCore.docc/Articles/FileFormat.md)
- [WALAndCrashRecovery.md](https://github.com/christopherkarani/Wax/blob/main/Sources/WaxCore/WaxCore.docc/Articles/WALAndCrashRecovery.md)
- [StructuredMemory.md](https://github.com/christopherkarani/Wax/blob/main/Sources/WaxCore/WaxCore.docc/Articles/StructuredMemory.md)
- [ConcurrencyModel.md](https://github.com/christopherkarani/Wax/blob/main/Sources/WaxCore/WaxCore.docc/Articles/ConcurrencyModel.md)

Swift implementation files are used as secondary evidence for byte order, field ordering, and payload details that are not fully enumerated in the prose docs.

## 4. Source Classification

Each rule in this document should be interpreted in one of three categories:

- `Documented`
  - directly stated in the upstream `WaxCore.docc` pages
- `Implementation-derived`
  - directly confirmed in the Swift source
- `Conservative inference`
  - not fully explicit upstream, but chosen to preserve interoperability safely

The most important split is:

- header/footer/overall file layout/WAL role are mostly `Documented`
- exact TOC field order, exact `FrameMeta` order, and exact WAL payload field order are `Implementation-derived`
- treating float payloads as little-endian `f32` is a `Conservative inference` backed by platform behavior and bulk-copy code paths

## 5. Endianness and Primitive Rules

### Endianness

All fixed-width numeric values are little-endian.

This includes:

- `UInt16`
- `UInt32`
- `UInt64`
- `Int64`
- `Float32` values embedded in vector payloads and WAL embedding payloads

The `Float32` rule is an implementation-derived compatibility requirement. The Swift source bulk-copies `[Float]` memory into bytes on little-endian Apple platforms, so a compatible Rust implementation must treat those payloads as little-endian IEEE-754 `f32`.

### Primitive Encodings

Primitive encodings used throughout the format:

- `UInt8`: 1 byte
- `UInt16`: 2 bytes, little-endian
- `UInt32`: 4 bytes, little-endian
- `UInt64`: 8 bytes, little-endian
- `Int64`: 8 bytes, little-endian two's complement

### Variable-Length Encodings

Strings:

- `UInt32 byte_len`
- `byte_len` bytes of UTF-8

Blobs:

- `UInt32 byte_len`
- `byte_len` raw bytes

Optionals:

- `UInt8 tag`
- `0` means absent
- `1` means present and followed by payload

Arrays:

- `UInt32 count`
- encoded elements in sequence

### Decoder Limits

Recommended compatibility limits from Wax v1:

- max string bytes: `16 * 1024 * 1024`
- max blob bytes: `256 * 1024 * 1024`
- max array count: `10_000_000`
- max embedding dimensions: `1_000_000`
- max TOC bytes: `64 * 1024 * 1024`

Normative status:

- integers and variable-length encodings: `Implementation-derived`
- little-endian float interpretation: `Conservative inference`

## 6. File Layout

The file layout is:

```text
0x00000000  Header Page A         4096 bytes
0x00001000  Header Page B         4096 bytes
0x00002000  WAL Ring Buffer       wal_size bytes
...         Data Region           variable
...         TOC                   variable
...         Footer                64 bytes
```

Constants:

- header page size: `4096`
- header region size: `8192`
- footer size: `64`
- WAL record header size: `48`
- default WAL size: `256 MiB`

Normative status:

- file region ordering and fixed sizes: `Documented`

## 7. Magic Values

Header magic:

- ASCII: `WAX1`
- bytes: `57 41 58 31`

Footer magic:

- ASCII: `WAX1FOOT`
- bytes: `57 41 58 31 46 4F 4F 54`

Replay snapshot magic inside header extension:

- ASCII: `WALSNAP1`
- bytes: `57 41 4C 53 4E 41 50 31`

Vector segment magic:

- ASCII: `MV2V`
- bytes: `4D 56 32 56`

Normative status:

- `WAX1` and `WAX1FOOT`: `Documented`
- `WALSNAP1` and `MV2V`: `Implementation-derived`

## 8. Header Page

Two header pages exist at fixed offsets `0x0000` and `0x1000`.

Open-time selection rule:

1. read both pages
2. reject any page with invalid magic, unsupported version, or checksum mismatch
3. choose the valid page with the highest `header_page_generation`
4. if only one page is valid, use it
5. if neither is valid, the file is corrupt

### Header Layout

| Offset | Size | Type | Field |
|---|---:|---|---|
| 0 | 4 | fixed bytes | magic = `WAX1` |
| 4 | 2 | `UInt16` | format_version |
| 6 | 1 | `UInt8` | spec_major |
| 7 | 1 | `UInt8` | spec_minor |
| 8 | 8 | `UInt64` | header_page_generation |
| 16 | 8 | `UInt64` | file_generation |
| 24 | 8 | `UInt64` | footer_offset |
| 32 | 8 | `UInt64` | wal_offset |
| 40 | 8 | `UInt64` | wal_size |
| 48 | 8 | `UInt64` | wal_write_pos |
| 56 | 8 | `UInt64` | wal_checkpoint_pos |
| 64 | 8 | `UInt64` | wal_committed_seq |
| 72 | 32 | fixed bytes | toc_checksum |
| 104 | 32 | fixed bytes | header_checksum |
| 136 | 72 | extension | optional replay snapshot |
| 208 | 3888 | zero/reserved | unused in v1 |

Header page size is always exactly `4096`.

### Version Rules

Wax v1 packs version as:

- `spec_major = 1`
- `spec_minor = 0`
- `format_version = (major << 8) | minor = 0x0100`

Validation rules:

- `spec_major/spec_minor` must match the packed `format_version`
- unsupported `format_version` must be rejected

### Header Checksum

The header checksum is SHA-256 over the entire 4096-byte page with the checksum field zeroed before hashing.

Procedure:

1. copy the header page bytes
2. overwrite bytes `[104..136)` with 32 zero bytes
3. compute SHA-256 of the full 4096-byte page
4. store the digest at `[104..136)`

### Replay Snapshot Extension

The replay snapshot occupies bytes `[136..208)`.

Layout:

| Offset | Size | Type | Field |
|---|---:|---|---|
| 136 | 8 | fixed bytes | magic = `WALSNAP1` |
| 144 | 8 | `UInt64` | snapshot.file_generation |
| 152 | 8 | `UInt64` | snapshot.wal_committed_seq |
| 160 | 8 | `UInt64` | snapshot.footer_offset |
| 168 | 8 | `UInt64` | snapshot.wal_write_pos |
| 176 | 8 | `UInt64` | snapshot.wal_checkpoint_pos |
| 184 | 8 | `UInt64` | snapshot.wal_pending_bytes |
| 192 | 8 | `UInt64` | snapshot.wal_last_sequence |
| 200 | 8 | `UInt64` | snapshot.flags |

Valid snapshot rule:

- magic must equal `WALSNAP1`
- flags bit 0 must be set

If either condition fails, the snapshot must be ignored.

Normative status:

- top-level header responsibilities and A/B selection intent: `Documented`
- exact offsets, checksum zeroing rule, replay snapshot field layout: `Implementation-derived`

## 9. Footer

Footer size is exactly `64` bytes.

### Footer Layout

| Offset | Size | Type | Field |
|---|---:|---|---|
| 0 | 8 | fixed bytes | magic = `WAX1FOOT` |
| 8 | 8 | `UInt64` | toc_len |
| 16 | 32 | fixed bytes | toc_hash |
| 48 | 8 | `UInt64` | generation |
| 56 | 8 | `UInt64` | wal_committed_seq |

### Footer Validation

To validate footer against TOC:

1. read `toc_len`
2. locate TOC as `footer_offset - toc_len`
3. read `toc_len` bytes
4. compute TOC checksum per section 8.4
5. assert:
   - computed checksum == footer `toc_hash`
   - computed checksum == TOC trailing `toc_checksum`

Normative status:

- footer existence, size, and TOC hash role: `Documented`
- exact field layout: `Implementation-derived`

## 10. TOC

TOC is a binary-encoded structure using the primitive rules above.

### TOC High-Level Order

Field order is exactly:

1. `toc_version: UInt64`
2. `frame_count: UInt32`
3. `frames[frame_count]`
4. `indexes`
5. `time_index?`
6. `memories_track tag = UInt8(0)`
7. `logic_mesh tag = UInt8(0)`
8. `sketch_track tag = UInt8(0)`
9. `segment_catalog`
10. `ticket_ref`
11. `memory_binding?`
12. `replay_manifest tag = UInt8(0)`
13. `enrichment_queue tag = UInt8(0)`
14. `merkle_root[32]`
15. `toc_checksum[32]`

### TOC Version

Current TOC version is `1`.

Any other version must be rejected by v1-compatible readers.

### Dense Frame Invariant

Frames are stored as a dense array.

Compatibility rule:

- the frame at array index `i` must have `frame.id == i`

Any violation is TOC corruption.

### TOC Checksum

The trailing 32-byte `toc_checksum` is computed as:

```text
SHA256(toc_body || zero32)
```

Where:

- `toc_body = toc_bytes[0 .. len-32)`
- `zero32` is 32 zero bytes

Validation rule:

- the trailing 32 bytes must equal the computed checksum
- the footer `toc_hash` must also equal the computed checksum

Normative status:

- TOC existence and high-level contents: `Documented`
- exact field order, optional tags, and zero-only future tags: `Implementation-derived`

## 11. FrameMeta Encoding

Each `FrameMeta` is encoded in this exact order.

| Order | Field | Encoding |
|---:|---|---|
| 1 | `id` | `UInt64` |
| 2 | `timestamp` | `Int64` |
| 3 | `anchor_ts` | optional `Int64` |
| 4 | `kind` | optional string |
| 5 | `track` | optional string |
| 6 | `payload_offset` | `UInt64` |
| 7 | `payload_length` | `UInt64` |
| 8 | `checksum` | fixed 32 bytes |
| 9 | `uri` | optional string |
| 10 | `title` | optional string |
| 11 | `canonical_encoding` | `UInt8` enum |
| 12 | `canonical_length` | optional `UInt64` |
| 13 | `stored_checksum` | optional fixed 32 bytes |
| 14 | `metadata` | optional `Metadata` |
| 15 | `search_text` | optional string |
| 16 | `tags` | array of `TagPair` |
| 17 | `labels` | array of string |
| 18 | `content_dates` | array of string |
| 19 | `role` | `UInt8` enum |
| 20 | `parent_id` | optional `UInt64` |
| 21 | `chunk_index` | optional `UInt32` |
| 22 | `chunk_count` | optional `UInt32` |
| 23 | `chunk_manifest` | optional blob |
| 24 | `status` | `UInt8` enum |
| 25 | `supersedes` | optional `UInt64` |
| 26 | `superseded_by` | optional `UInt64` |

### Frame Enums

`canonical_encoding`:

- `0 = plain`
- `1 = lzfse`
- `2 = lz4`
- `3 = deflate`

`role`:

- `0 = document`
- `1 = chunk`
- `2 = blob`
- `3 = system`

`status`:

- `0 = active`
- `1 = deleted`

### Frame Invariants

Compatibility checks:

- if `canonical_encoding != plain`, then `canonical_length` must be present
- if `payload_length > 0`, then `stored_checksum` must be present
- checksum fields must always be 32 bytes

### Metadata Encoding

`Metadata` encoding:

1. `UInt32 count`
2. entries sorted by key ascending
3. for each entry:
   - key string
   - value string

Duplicate keys are invalid on decode.

### TagPair Encoding

`TagPair` encoding:

1. key string
2. value string

Normative status:

- presence of frame metadata fields and invariants: `Documented`
- exact serialization order: `Implementation-derived`

## 12. Index Manifests

`indexes` is encoded as:

1. optional `LexIndexManifest`
2. optional `VecIndexManifest`
3. `clip_manifest` absent marker: `UInt8(0)`

Any non-zero clip manifest tag is invalid in v1.

### LexIndexManifest

Field order:

1. `doc_count: UInt64`
2. `bytes_offset: UInt64`
3. `bytes_length: UInt64`
4. `checksum[32]`
5. `version: UInt32`

Notes:

- `version == 1` in current Swift implementation
- payload bytes are opaque SQLite database bytes

### VecIndexManifest

Field order:

1. `vector_count: UInt64`
2. `dimension: UInt32`
3. `bytes_offset: UInt64`
4. `bytes_length: UInt64`
5. `checksum[32]`
6. `similarity: UInt8`

`similarity` enum:

- `0 = cosine`
- `1 = dot`
- `2 = l2`

Normative status:

- manifest concepts and meaning: `Documented`
- exact field order and optional encoding: `Implementation-derived`

## 13. TimeIndexManifest

Optional `time_index` is encoded as:

- tag `UInt8`
- if present:
  - `bytes_offset: UInt64`
  - `bytes_length: UInt64`
  - `entry_count: UInt64`
  - `checksum[32]`

Normative status:

- time index concept: `Documented`
- exact serialization layout: `Implementation-derived`

## 14. Segment Catalog

`segment_catalog` encoding:

1. `UInt32 count`
2. `count` entries

Entry field order:

1. `segment_id: UInt64`
2. `bytes_offset: UInt64`
3. `bytes_length: UInt64`
4. `checksum[32]`
5. `compression: UInt8`
6. `kind: UInt8`

`compression` enum:

- `0 = none`
- `1 = lzfse`
- `2 = lz4`
- `3 = deflate`

`kind` enum:

- `0 = lex`
- `1 = vec`
- `2 = time`
- `3 = custom`

Catalog invariants:

- entries must be sorted by `bytes_offset` ascending
- ties are invalid for decode
- segments must not overlap
- `bytes_offset + bytes_length` must not overflow `u64`

Normative status:

- existence of segment catalog: `Documented`
- exact entry encoding and ordering invariants: `Implementation-derived`

## 15. TicketRef

`ticket_ref` field order:

1. `issuer: string`
2. `seq_no: UInt64`
3. `expires_in_secs: UInt64`
4. `capacity_bytes: UInt64`
5. `verified: UInt8`

`verified` must be `0` or `1`.

The empty v1 default is:

- issuer = empty string
- seq_no = 0
- expires_in_secs = 0
- capacity_bytes = 0
- verified = 0

Normative status:

- ticket reference concept inside TOC: `Implementation-derived`

## 16. MemoryBinding

`memory_binding` is optional.

When present, field order is:

1. `embedding_provider: optional string`
2. `embedding_model: optional string`
3. `embedding_dimensions: optional UInt32`
4. `embedding_normalized: optional UInt8`

`embedding_normalized` convention:

- absent = unknown
- `0` = false
- `1` = true

Normative status:

- memory binding extension field: `Implementation-derived`

## 17. WAL

The WAL is a fixed-size circular ring buffer starting at `wal_offset`, normally `8192`.

### WAL Record Header

WAL record header is exactly `48` bytes.

| Offset | Size | Type | Field |
|---|---:|---|---|
| 0 | 8 | `UInt64` | sequence |
| 8 | 4 | `UInt32` | length |
| 12 | 4 | `UInt32` | flags |
| 16 | 32 | fixed bytes | payload_checksum |

Flags:

- bit 0 = `is_padding`

### WAL Record Types

Sentinel record:

- entire 48-byte header is zero

Padding record:

- `flags.is_padding = true`
- `length = skip_bytes`
- checksum = `SHA256(empty_bytes)`
- no payload bytes follow

Data record:

- `sequence != 0`
- `length > 0`
- payload follows immediately
- checksum = `SHA256(payload)`

### WAL Scan Rules

Scanning stops when any of the following is encountered:

- sentinel header
- `sequence == 0`
- sequence does not strictly increase
- invalid padding checksum
- payload length exceeds WAL bounds
- payload checksum mismatch

Compatibility distinction:

- checksum mismatch implies partial or torn write and should terminate scanning
- checksum-valid payload with opcode decode failure implies structural corruption

Normative status:

- WAL as fixed-size circular ring and crash-recovery role: `Documented`
- exact 48-byte header layout and scan stop conditions: `Implementation-derived`

## 18. WALEntry Payloads

The data payload of a WAL record is a `WALEntry`.

Common rule:

- first byte is `opcode: UInt8`

Opcodes:

- `0x01 = putFrame`
- `0x02 = deleteFrame`
- `0x03 = supersedeFrame`
- `0x04 = putEmbedding`

### putFrame Payload

Field order:

1. `opcode: UInt8 = 0x01`
2. `frame_id: UInt64`
3. `timestamp_ms: Int64`
4. `options: FrameMetaSubset`
5. `payload_offset: UInt64`
6. `payload_length: UInt64`
7. `canonical_encoding: UInt8`
8. `canonical_length: UInt64`
9. `canonical_checksum[32]`
10. `stored_checksum[32]`

Notes:

- `canonical_length` is not optional in WAL `putFrame`, even when later TOC `canonical_length` may be omitted for `plain`
- checksums are fixed 32-byte SHA-256 digests

### FrameMetaSubset Payload

`FrameMetaSubset` is encoded in this exact order:

1. `uri: optional string`
2. `title: optional string`
3. `kind: optional string`
4. `track: optional string`
5. `tags: array<TagPair>`
6. `labels: array<string>`
7. `content_dates: array<string>`
8. `role: optional UInt8`
9. `parent_id: optional UInt64`
10. `chunk_index: optional UInt32`
11. `chunk_count: optional UInt32`
12. `chunk_manifest: optional blob`
13. `status: optional UInt8`
14. `supersedes: optional UInt64`
15. `superseded_by: optional UInt64`
16. `search_text: optional string`
17. `metadata: optional Metadata`

Enum values:

- role uses the same mapping as `FrameMeta.role`
- status uses the same mapping as `FrameMeta.status`

### deleteFrame Payload

Field order:

1. `opcode: UInt8 = 0x02`
2. `frame_id: UInt64`

### supersedeFrame Payload

Field order:

1. `opcode: UInt8 = 0x03`
2. `superseded_id: UInt64`
3. `superseding_id: UInt64`

### putEmbedding Payload

Field order:

1. `opcode: UInt8 = 0x04`
2. `frame_id: UInt64`
3. `dimension: UInt32`
4. `vector: dimension * Float32 little-endian`

Compatibility rule:

- `vector` byte count must equal `dimension * 4`

Normative status:

- opcodes and mutation kinds: `Documented`
- exact payload order and float storage shape: `Implementation-derived`

## 19. Vector Segment (`MV2V`)

The committed vector segment is referenced by `VecIndexManifest`.

### Segment Header

Header size is `36` bytes.

| Offset | Size | Type | Field |
|---|---:|---|---|
| 0 | 4 | fixed bytes | magic = `MV2V` |
| 4 | 2 | `UInt16` | version |
| 6 | 1 | `UInt8` | encoding |
| 7 | 1 | `UInt8` | similarity |
| 8 | 4 | `UInt32` | dimension |
| 12 | 8 | `UInt64` | vector_count |
| 20 | 8 | `UInt64` | payload_length |
| 28 | 8 | fixed bytes | reserved = zero8 |

Rules:

- version must be `1`
- reserved bytes must be all zero

### Encodings

Encoding values:

- `1 = uSearch`
- `2 = metal`
- `3 = flat`

#### Encoding 1: uSearch

Payload is opaque native USearch serialization bytes.

Compatibility guidance:

- a v1-compatible core reader may treat this as opaque bytes
- native search support for this payload is optional

#### Encoding 2 or 3: flat-like vectors

Payload body is:

1. vector matrix bytes, length = `vector_count * dimension * 4`
2. `frame_id_bytes_len: UInt64`
3. `frame_ids: vector_count * UInt64`

Rules:

- `frame_id_bytes_len == vector_count * 8`
- total segment size must match exactly

Although Swift names encoding `2` as `metal`, both `2` and `3` decode through the same portable vector/frame-id payload shape in the current implementation.

Normative status:

- committed vector segment concept: `Documented`
- `MV2V` header and encoding values: `Implementation-derived`
- shared decode shape for encodings `2` and `3`: `Implementation-derived`

## 20. Lex Segment

The committed lexical segment is referenced by `LexIndexManifest`.

Payload is an opaque SQLite database blob.

Compatibility facts:

- current Swift writer serializes SQLite main database bytes
- schema includes FTS5 and structured memory tables
- manifest version is currently `1`

This document intentionally treats the lex payload as opaque. A compatible core writer can preserve existing bytes without understanding internal SQLite page layout.

Normative status:

- lex segment existence: `Documented`
- opaque SQLite blob treatment in this document: `Conservative inference`

## 21. Integrity Rules

### Checksums

Wax v1 uses SHA-256 for:

- header checksum
- WAL payload checksum
- TOC checksum
- footer TOC hash
- frame canonical checksum
- frame stored checksum
- segment manifest checksum

### Payload Checksums

Frame checksums mean:

- `checksum`: SHA-256 of canonical form
- `stored_checksum`: SHA-256 of stored payload bytes as written at `payload_offset`

For uncompressed payloads these may be equal. For compressed payloads they usually differ.

### Region Boundaries

Manifest and catalog validation must assert:

- segment offsets are at or above `wal_offset + wal_size`
- segment end is at or below `footer_offset`
- no segment range overflow
- no segment overlap

Normative status:

- checksum families and validation intent: `Documented`
- exact committed-region boundary checks: `Implementation-derived`

## 22. Open / Recovery Algorithm

Recommended compatible open algorithm:

```text
1. Read header A and B
2. Validate magic/version/checksum
3. Select valid header with highest header_page_generation
4. Read footer from header.footer_offset
5. Read TOC using footer.toc_len
6. Validate TOC checksum and footer.toc_hash
7. Build committed in-memory state from TOC
8. If replay snapshot is valid and matches committed generation, use it to seed WAL scan start
9. Scan WAL from checkpoint position
10. Apply pending mutations in sequence order
11. Reconstruct pending write state
```

Recovery policy details:

- corruption in committed header/TOC/footer is fatal
- corruption in trailing pending WAL region may terminate replay without invalidating committed state

Normative status:

- high-level open and replay flow: `Documented`
- exact recovery ordering and fatal/non-fatal split: `Implementation-derived`

## 23. Commit Algorithm

Recommended compatible commit algorithm:

```text
1. Refuse commit if pending embeddings exist but no staged vector index exists
2. Refuse commit if staged vector index is stale relative to latest pending embedding sequence
3. Apply pending WAL mutations into TOC state
4. Append staged lex bytes to data region if present
5. Append staged vec bytes to data region if present
6. Encode TOC with trailing checksum placeholder
7. Replace trailing checksum with computed TOC checksum
8. Append TOC
9. Append footer
10. fsync file
11. Update selected header page fields and replay snapshot
12. Write next header generation to the alternate A/B page
13. fsync file
14. Advance WAL checkpoint to write_pos
15. Clear pending/staged state
```

Observed Swift compatibility rules:

- vector index must be restaged before commit if new pending embeddings arrived after staging
- staged lex/vec indexes may be dropped if identical to committed bytes

Normative status:

- commit writes TOC + footer + header after applying WAL state: `Documented`
- staged-index freshness checks: `Implementation-derived`

## 24. Rust Implementation Checklist

Minimum compatible parser/writer behavior:

- parse both header pages and choose by generation
- implement header checksum exactly
- implement TOC checksum exactly
- validate dense frame ids
- validate segment ordering and overlap
- support all WAL opcodes
- decode `putEmbedding` vectors as little-endian `f32`
- read `MV2V` headers and distinguish opaque vs portable payloads
- preserve opaque lex/uSearch payloads byte-for-byte when rewriting unchanged state

## 25. Ambiguities and Inferences

The following points are implementation-derived rather than explicitly documented by the upstream prose docs:

- WAL embedding vectors are native `[Float]` bulk copies and therefore effectively little-endian `f32`
- `MV2V` encoding `2` and `3` currently decode through the same vector/frame-id payload shape
- `lex` payload compatibility is defined at the blob level, not at the SQLite page-schema level in this document

These choices should be treated as compatibility requirements unless proven otherwise by fixture testing against real Wax-generated files.

## 26. Suggested Test Fixtures

To lock compatibility down, build fixtures for:

- empty store
- store with both header pages valid and different generations
- store with compressed and uncompressed frames
- store with pending WAL records and no commit
- store with committed lex and vec manifests
- store with `MV2V` flat payload
- store with `MV2V` uSearch payload
- store with structured memory tables populated inside lex blob
- store with superseded and deleted frames
