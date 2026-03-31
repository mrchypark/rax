# Wax v2 Binary Format

Status: Draft  
Date: 2026-03-29  
Scope: proposed byte-level format for Wax v2, optimized for cold open and search p95

## 1. Purpose

This document defines the proposed on-disk binary format for Wax v2.

It is intentionally:

- performance-first
- single-file
- backend-independent at the container level
- not internally compatible with Wax v1

This is the first byte-level draft for v2. It fixes the invariants that should not drift later, while leaving backend-specific segment payloads versioned and replaceable.

## 2. Primary Design Inputs

This format is derived from the approved v2 architecture document:

- [2026-03-29-wax-v2-architecture.md](/Users/cypark/Documents/project/rax/docs/specs/2026-03-29-wax-v2-architecture.md)

The key architectural constraints carried into this spec are:

- cheap cold open
- immutable query-serving segments
- tiny commit journal, not a replay-heavy WAL
- zero-copy or near-zero-copy manifest loading
- independently versioned segment families
- Wax-owned top-level metadata, not backend-private file formats

## 3. Format Philosophy

Wax v2 separates three concerns:

1. publication protocol
   - superblocks
   - commit journal
   - generation switching
2. logical store snapshot
   - manifest
   - segment descriptors
3. backend-specific query data
   - doc segments
   - text segments
   - vector segments

The main rule is:

- Wax owns 1 and 2 completely
- Wax owns the shape of segment descriptors in 3
- backend internals may exist inside segment payloads, but only behind Wax-defined segment envelopes

## 4. Fixed Constants

These constants should be fixed in the first implementation unless benchmark data proves they are wrong.

- byte order: little-endian
- base page size: `4096`
- superblock size: `4096`
- superblock count: `2`
- journal region size: `65536` bytes
- file preamble size: `73728` bytes

Derived layout:

- `0x00000000` superblock A
- `0x00001000` superblock B
- `0x00002000` journal region
- `0x00012000` first appendable object boundary

Reasoning:

- page-sized superblocks simplify atomic pointer switching
- a fixed small journal bounds recovery cost
- the append region starts on a page boundary

## 5. Magic Values and Versioning

### 5.1 File Magic

Proposed top-level file magic:

- ASCII: `WAX2`
- bytes: `57 41 58 32`

### 5.2 Object Magic

Objects inside the appendable region use a shared Wax object envelope.

Proposed envelope magic:

- ASCII: `WXOB`
- bytes: `57 58 4F 42`

### 5.3 Journal Magic

Journal region magic:

- ASCII: `WXJR`
- bytes: `57 58 4A 52`

### 5.4 Versioning Model

Three independent version axes exist:

- file format version
- manifest schema version
- segment family version

This is a hard guardrail.

## 6. High-Level File Layout

```text
┌────────────────────────────────────────────────────────────┐
│ Superblock A                    4 KiB                     │
├────────────────────────────────────────────────────────────┤
│ Superblock B                    4 KiB                     │
├────────────────────────────────────────────────────────────┤
│ Commit Journal Region           64 KiB                    │
├────────────────────────────────────────────────────────────┤
│ Appendable Object Region        variable                  │
│  ├─ doc objects                                         │
│  ├─ txt objects                                         │
│  ├─ vec objects                                         │
│  ├─ manifest objects                                    │
│  └─ future objects                                      │
└────────────────────────────────────────────────────────────┘
```

The appendable object region is monotonic.

Objects are never updated in place.

Publication changes only the active manifest pointer.

## 7. Superblock

There are always two superblocks.

Open-time rule:

1. read both
2. validate magic/version/checksum
3. choose the valid block with the highest generation
4. if only one is valid, use it
5. if neither is valid, the file is corrupt

### 7.1 Superblock Layout

| Offset | Size | Type | Field |
|---|---:|---|---|
| 0 | 4 | fixed bytes | magic = `WAX2` |
| 4 | 2 | `UInt16` | file_format_major |
| 6 | 2 | `UInt16` | file_format_minor |
| 8 | 8 | `UInt64` | superblock_generation |
| 16 | 8 | `UInt64` | active_manifest_offset |
| 24 | 8 | `UInt64` | active_manifest_length |
| 32 | 8 | `UInt64` | active_manifest_generation |
| 40 | 8 | `UInt64` | journal_region_offset |
| 48 | 8 | `UInt64` | journal_region_length |
| 56 | 8 | `UInt64` | append_region_start |
| 64 | 8 | `UInt64` | file_logical_end |
| 72 | 8 | `UInt64` | feature_flags |
| 80 | 16 | bytes | writer_uuid |
| 96 | 32 | bytes | active_manifest_checksum |
| 128 | 32 | bytes | superblock_checksum |
| 160 | 3936 | reserved/zero | reserved |

Superblock size is exactly `4096`.

### 7.2 Required Invariants

- `journal_region_offset == 8192`
- `journal_region_length == 65536`
- `append_region_start == 73728`
- `active_manifest_offset >= append_region_start`
- `active_manifest_offset + active_manifest_length <= file_logical_end`
- reserved bytes must be zero for v2.0

### 7.3 Superblock Checksum

The checksum is SHA-256 over the full 4096-byte page with bytes `[128..160)` zeroed before hashing.

## 8. Journal Region

The journal is not a replay log for documents.

It exists only to make manifest publication crash-safe and bounded.

### 8.1 Journal Region Header

At offset `8192`, the journal begins with:

| Offset | Size | Type | Field |
|---|---:|---|---|
| 0 | 4 | fixed bytes | magic = `WXJR` |
| 4 | 2 | `UInt16` | journal_major |
| 6 | 2 | `UInt16` | journal_minor |
| 8 | 8 | `UInt64` | latest_entry_seq |
| 16 | 32 | bytes | region_checksum |
| 48 | 16 | bytes | reserved |
| 64 | ... | journal entries | entries |

### 8.2 Journal Entry Layout

Journal entries are fixed-header + payload.

Header:

| Offset | Size | Type | Field |
|---|---:|---|---|
| 0 | 8 | `UInt64` | entry_seq |
| 8 | 4 | `UInt32` | entry_type |
| 12 | 4 | `UInt32` | payload_length |
| 16 | 8 | `UInt64` | target_manifest_offset |
| 24 | 8 | `UInt64` | target_manifest_generation |
| 32 | 32 | bytes | payload_checksum |

Header size: `64`

### 8.3 Journal Entry Types

Initial journal entry types:

- `1 = prepare_manifest_publish`
- `2 = commit_manifest_publish`
- `3 = clear_completed_publish`

### 8.4 Recovery Contract

Recovery only needs to answer:

- was a manifest publish interrupted?
- if so, which complete manifest generation should be trusted?

The journal must never require replaying user documents or search index mutations.

### 8.5 Journal Invariants

- journal entries are ordered by `entry_seq`
- only the newest valid incomplete publish matters during recovery
- if both journal and superblock disagree, the highest fully valid manifest generation wins
- recovery time must remain bounded by journal size, not corpus size

## 9. Object Envelope

Every append-region object begins with a common Wax envelope.

### 9.1 Envelope Layout

| Offset | Size | Type | Field |
|---|---:|---|---|
| 0 | 4 | fixed bytes | magic = `WXOB` |
| 4 | 2 | `UInt16` | object_type |
| 6 | 2 | `UInt16` | object_version |
| 8 | 8 | `UInt64` | object_length |
| 16 | 8 | `UInt64` | logical_generation |
| 24 | 8 | `UInt64` | alignment |
| 32 | 32 | bytes | payload_checksum |
| 64 | payload | bytes | object payload |
| ... | padding | zero bytes | object alignment padding |

Envelope header size: `64`

### 9.2 Object Types

Initial object types:

- `1 = manifest`
- `2 = doc_segment`
- `3 = txt_segment`
- `4 = vec_segment`
- `5 = compaction_note`
- `6 = reserved_future`

### 9.3 Alignment Rule

- object payload start is immediately after the 64-byte header
- next object begins at the next multiple of `max(alignment, 4096)`
- large objects should use page alignment

## 10. Manifest

The manifest is the authoritative snapshot of the active store state.

It must be zero-copy or near-zero-copy readable.

### 10.1 Manifest Goals

- describe the currently active doc/txt/vec segment set
- allow open without scanning the whole file
- support snapshot isolation
- avoid per-open reconstruction of the whole corpus catalog

### 10.2 Manifest Layout

The manifest object payload is:

| Offset | Size | Type | Field |
|---|---:|---|---|
| 0 | 4 | fixed bytes | manifest_magic = `WXMF` |
| 4 | 2 | `UInt16` | manifest_major |
| 6 | 2 | `UInt16` | manifest_minor |
| 8 | 8 | `UInt64` | manifest_generation |
| 16 | 8 | `UInt64` | created_at_ms |
| 24 | 8 | `UInt64` | previous_manifest_offset |
| 32 | 8 | `UInt64` | previous_manifest_generation |
| 40 | 8 | `UInt64` | live_doc_count |
| 48 | 8 | `UInt64` | deleted_doc_count |
| 56 | 8 | `UInt64` | segment_count |
| 64 | 8 | `UInt64` | feature_flags |
| 72 | 8 | `UInt64` | reserved |
| 80 | 32 | bytes | manifest_contents_checksum |
| 112 | variable | bytes | segment descriptor table |

### 10.3 Segment Descriptor Table

Manifest segment descriptors are fixed-size entries so the table is easily mapped.

Descriptor size: `128` bytes

Descriptor layout:

| Offset | Size | Type | Field |
|---|---:|---|---|
| 0 | 2 | `UInt16` | family |
| 2 | 2 | `UInt16` | family_version |
| 4 | 4 | `UInt32` | flags |
| 8 | 8 | `UInt64` | object_offset |
| 16 | 8 | `UInt64` | object_length |
| 24 | 8 | `UInt64` | segment_generation |
| 32 | 8 | `UInt64` | doc_id_start |
| 40 | 8 | `UInt64` | doc_id_end_exclusive |
| 48 | 8 | `UInt64` | min_timestamp_ms |
| 56 | 8 | `UInt64` | max_timestamp_ms |
| 64 | 8 | `UInt64` | live_items |
| 72 | 8 | `UInt64` | tombstoned_items |
| 80 | 8 | `UInt64` | backend_id |
| 88 | 8 | `UInt64` | backend_aux |
| 96 | 32 | bytes | object_checksum |

Remaining bytes up to `128` are reserved and must be zero in v2.0.

### 10.4 Segment Family Values

Initial family values:

- `1 = doc`
- `2 = txt`
- `3 = vec`

### 10.5 Backend Identification

`backend_id` exists so the manifest can declare which runtime/backend family produced the segment.

Important rule:

- `backend_id` is descriptive metadata
- it does not own the surrounding Wax segment contract

Examples:

- doc-native-v1
- txt-library-a
- vec-rust-hnsw

The concrete numeric assignments may be defined in a registry appendix later.

### 10.6 Manifest Invariants

- segment descriptors must be sorted by `(family, object_offset, segment_generation)`
- each `object_offset` must point to a valid object envelope
- no two active descriptors may reference overlapping object ranges
- `doc_id_start <= doc_id_end_exclusive`
- doc id ranges may overlap across different families, but must be compatible within a snapshot
- reserved bytes must be zero

## 11. Doc Segment Family

Doc segments are Wax-owned and must not depend on the text or vector backend.

### 11.1 Purpose

Doc segments store:

- stable `doc_id`
- document timestamps
- deletion/tombstone state
- payload references
- lightweight metadata filter data
- preview pointers or inline preview bytes

### 11.2 Proposed Doc Segment Payload

Doc segment payload begins with:

| Offset | Size | Type | Field |
|---|---:|---|---|
| 0 | 4 | fixed bytes | `WXDG` |
| 4 | 2 | `UInt16` | doc_segment_major |
| 6 | 2 | `UInt16` | doc_segment_minor |
| 8 | 8 | `UInt64` | row_count |
| 16 | 8 | `UInt64` | payload_bytes_offset |
| 24 | 8 | `UInt64` | metadata_bytes_offset |
| 32 | 8 | `UInt64` | preview_bytes_offset |
| 40 | 8 | `UInt64` | row_table_offset |
| 48 | 32 | bytes | contents_checksum |
| 80 | variable | bytes | section directory + section bodies |

Each row in the row table should be fixed-size in v2.0 to keep open-time reads predictable.

Suggested row fields:

- `doc_id`
- `timestamp_ms`
- `flags`
- `payload_offset`
- `payload_length`
- `metadata_ref`
- `preview_ref`

### 11.3 Doc Segment Invariants

- rows sorted by `doc_id`
- no duplicate active rows for the same `doc_id` inside one segment
- payload references must stay within object bounds
- doc segments are directly readable without backend code

## 12. Text Segment Family

Text segments wrap a Rust-native library-backed text index, but with Wax-owned metadata.

### 12.1 Purpose

Text segments must support:

- term lookup
- postings retrieval
- BM25-style ranking
- doc filtering by `doc_id`
- optional snippets

### 12.2 Proposed Text Segment Payload

Text segment payload begins with:

| Offset | Size | Type | Field |
|---|---:|---|---|
| 0 | 4 | fixed bytes | `WXTG` |
| 4 | 2 | `UInt16` | txt_segment_major |
| 6 | 2 | `UInt16` | txt_segment_minor |
| 8 | 8 | `UInt64` | posting_count |
| 16 | 8 | `UInt64` | term_count |
| 24 | 8 | `UInt64` | indexed_doc_count |
| 32 | 8 | `UInt64` | lexicon_offset |
| 40 | 8 | `UInt64` | postings_offset |
| 48 | 8 | `UInt64` | stored_fields_offset |
| 56 | 8 | `UInt64` | backend_blob_offset |
| 64 | 8 | `UInt64` | backend_blob_length |
| 72 | 32 | bytes | contents_checksum |
| 104 | variable | bytes | Wax-owned sections + optional backend blob |

### 12.3 Text Segment Rules

- Wax-owned fields must remain interpretable without understanding the backend blob
- `backend_blob` is optional
- if present, `backend_blob` must be fully bounded by `backend_blob_offset` and `backend_blob_length`
- if the backend is later replaced, the segment family version may change without affecting `doc` and `vec`

## 13. Vector Segment Family

Vector segments wrap the persisted ANN data path.

### 13.1 Purpose

Vector segments must support:

- fast ANN query
- bounded open cost
- stable `doc_id` mapping
- future backend replacement

### 13.2 Proposed Vector Segment Payload

Vector segment payload begins with:

| Offset | Size | Type | Field |
|---|---:|---|---|
| 0 | 4 | fixed bytes | `WXVG` |
| 4 | 2 | `UInt16` | vec_segment_major |
| 6 | 2 | `UInt16` | vec_segment_minor |
| 8 | 8 | `UInt64` | vector_count |
| 16 | 4 | `UInt32` | dimension |
| 20 | 4 | `UInt32` | metric |
| 24 | 8 | `UInt64` | graph_offset |
| 32 | 8 | `UInt64` | vector_data_offset |
| 40 | 8 | `UInt64` | doc_id_map_offset |
| 48 | 8 | `UInt64` | backend_blob_offset |
| 56 | 8 | `UInt64` | backend_blob_length |
| 64 | 8 | `UInt64` | entry_point_doc_id |
| 72 | 32 | bytes | contents_checksum |
| 104 | variable | bytes | Wax-owned sections + optional backend blob |

### 13.3 Metric Values

Initial metric values:

- `1 = cosine`
- `2 = dot`
- `3 = l2`

### 13.4 Vector Segment Rules

- open must not require graph rebuild from raw vectors
- graph and mapping sections must be directly addressable from the segment
- if `backend_blob` exists, Wax-owned offsets still remain authoritative
- vector count and doc id map count must match

## 14. Checksums

Wax v2 uses SHA-256 in the first draft for:

- superblock checksum
- journal entry payload checksum
- object envelope payload checksum
- manifest checksum
- segment contents checksum

This may be revised later only if benchmark data justifies another checksum family.

## 15. Open Algorithm

Recommended v2 open path:

1. read superblock A and B
2. validate both
3. choose highest valid generation
4. read active manifest from chosen superblock
5. validate manifest checksum
6. validate segment descriptor table
7. memory-map or minimally load active objects
8. recover only incomplete manifest publication state from the tiny journal if needed
9. return a ready-to-query snapshot

Open must never:

- scan the whole append region
- rebuild text postings
- rebuild vector graph

## 16. Commit Algorithm

Recommended v2 commit path:

1. build new immutable objects off the active read path
2. append new object envelopes and payloads
3. append new manifest object
4. write `prepare_manifest_publish` journal entry
5. fsync appended bytes
6. write next-generation superblock pointing at the new manifest
7. fsync superblock
8. write `commit_manifest_publish` or clear entry
9. future readers switch to the new manifest generation

This keeps the hot read path immutable and bounded.

## 17. Hard Invariants

These invariants must be fixed in the first implementation to avoid future rewrites.

### 17.1 Publication Invariants

- exactly one manifest generation is active per selected superblock
- manifest publication is atomic at the snapshot level
- recovery cost is bounded by journal size

### 17.2 Container Invariants

- top-level format is Wax-owned
- segment families are independently versioned
- backend-private formats never define the whole store

### 17.3 Search Invariants

- open-time searchability does not depend on rebuild
- doc ids are Wax-owned and stable
- active readers are isolated from concurrent publication

### 17.4 Layout Invariants

- large objects are page-aligned
- all object ranges are checksum-protected
- reserved bytes are zero in v2.0

## 18. Risks

### 18.1 Manifest Drift

Risk:

- the manifest accumulates too much backend-specific detail and stops being stable

Mitigation:

- keep backend metadata descriptive, not authoritative

### 18.2 Segment Opaqueness

Risk:

- backend blobs grow until Wax no longer really owns segment semantics

Mitigation:

- require Wax-owned section offsets and checksums in every segment family

### 18.3 Journal Creep

Risk:

- the tiny journal becomes a replay log over time

Mitigation:

- constrain journal entry types to publication state only

## 19. Out of Scope for This Spec

This document intentionally does not finalize:

- the specific Rust-native text library
- the specific Rust HNSW implementation
- structured memory segment format
- advanced RAG or reranking payloads
- migration format from Wax v1

## 20. Recommended Follow-Up Docs

- `wax-v2-text-lane.md`
- `wax-v2-vector-lane.md`
- `wax-v2-benchmark-plan.md`
- `wax-v2-migration-plan.md`
