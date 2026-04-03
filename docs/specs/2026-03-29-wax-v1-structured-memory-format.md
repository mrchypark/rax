# Wax v1 Structured Memory Format

Status: Draft  
Date: 2026-03-29  
Audience: Rust implementers and designers working on Wax-compatible structured memory

## 1. Purpose

This document defines the structured memory model used by Wax v1 and explains how that model is persisted in the current implementation.

It is intentionally split into two layers:

- the conceptual model defined by `WaxCore`
- the concrete persistence binding used by Wax v1

That split matters because Wax v1 does not persist structured memory in a standalone `.wax` binary subformat. Instead, it stores structured memory inside the lexical search blob.

## 2. Primary Upstream Sources

Primary upstream documentation:

- [StructuredMemory.md](https://github.com/christopherkarani/Wax/blob/main/Sources/WaxCore/WaxCore.docc/Articles/StructuredMemory.md)
- [Documentation.md](https://github.com/christopherkarani/Wax/blob/main/Sources/WaxCore/WaxCore.docc/Documentation.md)
- [FileFormat.md](https://github.com/christopherkarani/Wax/blob/main/Sources/WaxCore/WaxCore.docc/Articles/FileFormat.md)

Secondary implementation sources:

- `Sources/WaxTextSearch/StructuredMemorySchema.swift`
- `Sources/WaxTextSearch/FTS5SearchEngine.swift`
- `Sources/WaxCore/StructuredMemory/*.swift`

## 3. Source Classification

This document uses three rule classes:

- `Documented`
  - directly stated in upstream `WaxCore.docc`
- `Implementation-derived`
  - directly confirmed in Swift source
- `Conservative inference`
  - chosen to preserve interoperability where the upstream contract is implicit

## 4. Core Model

Structured memory in Wax is a graph-like fact store with bitemporal semantics.

Core fact shape:

- subject
- predicate
- object

Every fact also carries:

- valid time range
- system time range
- provenance evidence

Normative status:

- RDF-like triple model and bitemporal semantics: `Documented`

## 5. Entity Model

### 5.1 EntityKey

`EntityKey` is an open-world string identifier for a named concept.

Examples from the upstream docs:

- `alice`
- `company:acme`

Entity properties:

- stable string key
- kind string
- zero or more aliases

Normative status:

- open-world entity key concept: `Documented`
- persistence as a unique string key: `Implementation-derived`

### 5.2 Alias Normalization

Aliases are normalized for lookup consistency.

The upstream docs state:

- aliases are NFKC-normalized
- aliases are case-folded

In v1 persistence this normalized alias is stored separately from the original alias text.

Normative status:

- normalization intent: `Documented`
- exact storage split into `alias` and `alias_norm`: `Implementation-derived`

## 6. Predicate Model

`PredicateKey` names a property or relationship.

Examples:

- `works_at`
- `founded_year`

In persistence, predicates are interned as unique string keys.

Normative status:

- predicate key concept: `Documented`
- unique predicate table: `Implementation-derived`

## 7. Fact Values

Wax v1 supports seven fact value kinds.

| Kind Code | Swift Case | Meaning |
|---:|---|---|
| 1 | `.string(String)` | text value |
| 2 | `.int(Int64)` | integer value |
| 3 | `.double(Double)` | finite floating-point value |
| 4 | `.bool(Bool)` | boolean value |
| 5 | `.data(Data)` | binary blob |
| 6 | `.timeMs(Int64)` | timestamp in milliseconds |
| 7 | `.entity(EntityKey)` | reference to another entity |

The upstream docs define the seven logical value types. The numeric codes come from the SQL schema.

Normative status:

- value kinds: `Documented`
- numeric kind mapping: `Implementation-derived`

## 8. Time Semantics

Every fact is evaluated against two half-open ranges:

- valid time: `[valid_from_ms, valid_to_ms)`
- system time: `[system_from_ms, system_to_ms)`

`nil` end values mean open-ended ranges.

A fact matches an `as-of` query only when:

- query valid time is inside the valid range
- query system time is inside the system range

Normative status:

- bitemporal model and half-open semantics: `Documented`
- exact SQL column layout: `Implementation-derived`

## 9. Evidence Provenance

Each asserted fact may carry one or more `StructuredEvidence` records.

Evidence fields:

- `sourceFrameId: UInt64`
- `chunkIndex: UInt32?`
- `spanUTF8: Range<Int>?`
- `extractorId: String`
- `extractorVersion: String`
- `confidence: Double?`
- `assertedAtMs: Int64`

Persistence stores UTF-8 span as:

- `span_start_utf8`
- `span_end_utf8`

and links the evidence either to a fact span or directly to a fact.

Normative status:

- provenance model: `Documented`
- exact SQL representation and dual foreign-key form: `Implementation-derived`

## 10. Deduplication and Versioning

### 10.1 Fact Deduplication

The upstream docs state that facts are deduplicated by a SHA-256 hash of:

- subject
- predicate
- object

The current implementation stores that digest as `fact_hash BLOB(32)` and enforces uniqueness.

### 10.2 Span Deduplication

The current implementation also deduplicates spans using `span_key_hash BLOB(32)`.

### 10.3 VersionRelation

Wax v1 supports four version relations:

| Code | Case | Meaning |
|---:|---|---|
| 0 | `sets` | independent assertion |
| 1 | `updates` | superseding update |
| 2 | `extends` | non-superseding extension |
| 3 | `retracts` | superseding retraction relation |

In implementation terms:

- `updates` and `retracts` are superseding relations
- `sets` and `extends` are not

Normative status:

- fact deduplication by `(subject, predicate, object)` hash: `Documented`
- span hash and version relation numeric mapping: `Implementation-derived`

## 11. Persistence Binding in Wax v1

This is the most important implementation fact.

Structured memory is not stored in a standalone `.wax`-native binary subformat in v1.

Instead:

- structured memory lives inside the lexical search database
- that lexical search database is serialized as the committed `lex` segment blob
- therefore structured memory persistence is coupled to the text lane

Practical consequence:

- if a reader cannot understand the `lex` blob, it cannot natively query v1 structured memory

Normative status:

- structured memory types are used by `WaxTextSearch` for storage/querying: `Documented`
- persistence coupling to SQLite lexical blob: `Implementation-derived`

## 12. v1 SQL Schema

The current implementation creates these tables.

### 12.1 `sm_entity`

Columns:

- `entity_id INTEGER PRIMARY KEY`
- `key TEXT NOT NULL UNIQUE`
- `kind TEXT NOT NULL DEFAULT ''`
- `created_at_ms INTEGER NOT NULL`

Meaning:

- canonical entity row

### 12.2 `sm_entity_alias`

Columns:

- `alias_id INTEGER PRIMARY KEY`
- `entity_id INTEGER NOT NULL`
- `alias TEXT NOT NULL`
- `alias_norm TEXT NOT NULL`
- `created_at_ms INTEGER NOT NULL`
- unique `(entity_id, alias_norm)`

Meaning:

- alternate names for an entity

### 12.3 `sm_predicate`

Columns:

- `predicate_id INTEGER PRIMARY KEY`
- `key TEXT NOT NULL UNIQUE`
- `created_at_ms INTEGER NOT NULL`

Meaning:

- canonical predicate row

### 12.4 `sm_fact`

Columns:

- `fact_id INTEGER PRIMARY KEY`
- `subject_entity_id INTEGER NOT NULL`
- `predicate_id INTEGER NOT NULL`
- object columns:
  - `object_kind`
  - `object_text`
  - `object_int`
  - `object_real`
  - `object_bool`
  - `object_blob`
  - `object_time_ms`
  - `object_entity_id`
- `version_relation INTEGER NOT NULL DEFAULT 0`
- `qualifiers_hash BLOB`
- `fact_hash BLOB NOT NULL`
- `created_at_ms INTEGER NOT NULL`

Important constraints:

- `fact_hash` length must be 32
- `qualifiers_hash`, if present, must be 32 bytes
- `version_relation IN (0,1,2,3)`
- exactly one object storage column family must be populated according to `object_kind`
- `fact_hash` is unique

### 12.5 `sm_fact_span`

Columns:

- `span_id INTEGER PRIMARY KEY`
- `fact_id INTEGER NOT NULL`
- `valid_from_ms INTEGER NOT NULL`
- `valid_to_ms INTEGER`
- `system_from_ms INTEGER NOT NULL`
- `system_to_ms INTEGER`
- `span_key_hash BLOB NOT NULL`
- `created_at_ms INTEGER NOT NULL`

Important constraints:

- `valid_to_ms IS NULL OR valid_to_ms > valid_from_ms`
- `system_to_ms IS NULL OR system_to_ms > system_from_ms`
- `span_key_hash` length must be 32
- `span_key_hash` is unique

### 12.6 `sm_evidence`

Columns:

- `evidence_id INTEGER PRIMARY KEY`
- `span_id INTEGER`
- `fact_id INTEGER`
- `source_frame_id INTEGER NOT NULL`
- `chunk_index INTEGER`
- `span_start_utf8 INTEGER`
- `span_end_utf8 INTEGER`
- `extractor_id TEXT NOT NULL`
- `extractor_version TEXT NOT NULL`
- `confidence REAL`
- `asserted_at_ms INTEGER NOT NULL`
- `created_at_ms INTEGER NOT NULL`

Important constraint:

- exactly one of `span_id` or `fact_id` must be non-null

Normative status:

- the existence of SQL persistence is `Implementation-derived`
- exact table names, columns, and checks are `Implementation-derived`

## 13. Indexes

The current implementation creates at least these indexes:

- `sm_entity_key_idx` on entity key
- `sm_entity_alias_norm_idx` on normalized alias
- `sm_predicate_key_idx` on predicate key
- `sm_fact_subject_pred_idx` on `(subject_entity_id, predicate_id)`
- `sm_fact_edge_out_idx` for entity-object edges
- `sm_fact_edge_in_idx` for reverse entity edges
- `sm_span_current_fact_idx` for current spans
- `sm_evidence_span_idx`
- `sm_evidence_fact_idx`
- `sm_evidence_frame_idx`

These indexes are performance artifacts, but they matter operationally for compatibility if the v1 SQLite path is preserved.

## 14. Query Semantics

The v1 query layer supports the following behaviors.

### 14.1 Entity Resolution

Entity lookup normal flow:

1. normalize alias
2. search `sm_entity_alias.alias_norm`
3. join back to canonical entity row

### 14.2 Fact Query

Fact query filters by:

- optional subject
- optional predicate
- system-time containment
- valid-time containment

and returns logically current or historical facts depending on the `as-of` point.

### 14.3 Evidence Frame Recovery

Structured memory can surface original Wax frames by following evidence rows back through `source_frame_id`.

This is what allows structured memory to participate in unified search.

Normative status:

- bitemporal query intent and provenance usage: `Documented`
- exact SQL execution path: `Implementation-derived`

## 15. Rust Compatibility Guidance

For a Rust rewrite, there are three realistic compatibility levels.

### Level A. Concept-only compatibility

Implement:

- entity/predicate/fact/evidence model
- bitemporal semantics
- hash-based deduplication

Do not implement:

- direct reading of v1 structured memory from existing `lex` blobs

Result:

- product semantics compatible
- file-level v1 structured memory interoperability not preserved

### Level B. Read compatibility

Implement:

- open existing SQLite `lex` blob
- query `sm_*` tables
- keep Rust-native model aligned with existing schema

Result:

- existing Wax files can be queried
- write path may still diverge

### Level C. Full v1 persistence compatibility

Implement:

- full read/write against the existing SQLite schema
- alias normalization parity
- fact/span hashing parity
- evidence persistence parity

Result:

- strongest interoperability
- ties structured memory to SQLite compatibility

## 16. Design Consequence for the Rust Rewrite

The key architectural consequence is:

structured memory and text lane are coupled in v1.

This means the decision between:

- `sqlite-compat`
- `native-text lane`

also decides the first implementation strategy for structured memory.

In practice:

- `sqlite-compat` preserves v1 structured memory most directly
- `native-text lane` requires a new structured memory persistence strategy or a migration path

## 17. Recommended Follow-Up Docs

The next documents that naturally follow this one are:

- `sqlite-compat-vs-native-text-lane.md`
  - decision document for text and structured persistence
- `structured-memory-hashing-and-normalization.md`
  - exact alias normalization and hash reproducibility spec
- `wax-v1-lex-blob-compat.md`
  - SQLite blob compatibility boundary and migration rules
