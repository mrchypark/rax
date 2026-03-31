# Wax v2 Dataset Pack Manifest Schema

Status: Draft  
Date: 2026-03-30  
Scope: canonical JSON manifest schema for benchmark dataset packs used by the Wax v2 harness

## 1. Purpose

This document defines the concrete manifest contract shared by:

- dataset packer
- benchmark runner
- artifact writer
- validator tools

The goal is to make a dataset pack self-describing.

The runner must be able to load one manifest and know:

- what corpus it is looking at
- which query sets are present
- what vector properties apply
- which dirty profile produced the pack
- whether the pack is safe to compare against another pack

This schema is the concrete companion to:

- [2026-03-29-wax-v2-dataset-spec.md](/Users/cypark/Documents/project/rax/docs/specs/2026-03-29-wax-v2-dataset-spec.md)
- [2026-03-29-wax-v2-benchmark-harness-spec.md](/Users/cypark/Documents/project/rax/docs/specs/2026-03-29-wax-v2-benchmark-harness-spec.md)
- [2026-03-30-wax-v2-benchmark-harness-implementation-plan.md](/Users/cypark/Documents/project/rax/docs/specs/2026-03-30-wax-v2-benchmark-harness-implementation-plan.md)

## 2. Format Overview

Each dataset pack must contain a top-level file:

- `manifest.json`

Encoding rules:

- UTF-8
- JSON object at the root
- stable key ordering when written by the packer
- RFC 3339 timestamps in UTC
- lowercase snake_case enum values unless noted otherwise

## 3. Top-Level Schema

Top-level object:

```json
{
  "schema_version": "wax_dataset_pack_v1",
  "generated_at": "2026-03-30T00:00:00Z",
  "generator": {
    "name": "wax-bench-packer",
    "version": "0.1.0"
  },
  "identity": {},
  "environment_constraints": {},
  "corpus": {},
  "text_profile": {},
  "metadata_profile": {},
  "vector_profile": {},
  "dirty_profile": {},
  "files": [],
  "query_sets": [],
  "checksums": {}
}
```

All top-level sections are required except where this document explicitly marks a field as nullable or optional.

## 4. Top-Level Fields

### 4.1 `schema_version`

Type:

- string

Required value for the first release:

- `wax_dataset_pack_v1`

Rule:

- incompatible manifest changes require a new schema version
- additive fields do not require a new schema version if older readers can ignore them safely

### 4.2 `generated_at`

Type:

- string timestamp

Rule:

- informational only
- must not participate in pack identity comparisons

### 4.3 `generator`

Type:

```json
{
  "name": "wax-bench-packer",
  "version": "0.1.0"
}
```

Required fields:

- `name`
- `version`

## 5. `identity` Object

```json
{
  "dataset_id": "knowledge-medium-clean-v1",
  "dataset_version": "v1",
  "dataset_family": "knowledge",
  "dataset_tier": "medium",
  "variant_id": "clean",
  "embedding_spec_id": "minilm-l6-384-f32-cosine",
  "embedding_model_version": "2026-03-15",
  "embedding_model_hash": "sha256:...",
  "corpus_checksum": "sha256:...",
  "query_checksum": "sha256:..."
}
```

Required fields:

- `dataset_id`
- `dataset_version`
- `dataset_family`
- `dataset_tier`
- `variant_id`
- `embedding_spec_id`
- `embedding_model_version`
- `embedding_model_hash`
- `corpus_checksum`
- `query_checksum`

Enum values:

- `dataset_family`: `knowledge`, `messaging`, `catalog`, or future additive values
- `dataset_tier`: `small`, `medium`, `large`
- `variant_id`: `clean`, `dirty_light`, `dirty_heavy`, or future additive values

Rules:

- `dataset_id` must be stable and human-readable
- `corpus_checksum` must reflect logical corpus payload, not only packed file bytes
- `query_checksum` must cover all query definitions in canonical order

## 6. `environment_constraints` Object

```json
{
  "min_ram_gb": 4,
  "recommended_ram_gb": 8,
  "notes": "large tier should not run on low-memory phones for fairness comparisons"
}
```

Required fields:

- `min_ram_gb`
- `recommended_ram_gb`

Optional fields:

- `notes`

Rules:

- this section is advisory for loaders and runners
- readers may warn when the host appears below the declared minimum

## 7. `corpus` Object

```json
{
  "doc_count": 50000,
  "vector_count": 50000,
  "total_text_bytes": 18300211,
  "avg_doc_length": 366.0,
  "median_doc_length": 301,
  "p95_doc_length": 1104,
  "max_doc_length": 8291,
  "languages": [
    {
      "code": "en",
      "ratio": 0.98
    }
  ]
}
```

Required fields:

- `doc_count`
- `vector_count`
- `total_text_bytes`
- `avg_doc_length`
- `median_doc_length`
- `p95_doc_length`
- `max_doc_length`
- `languages`

Rules:

- `doc_count` must be greater than zero
- `vector_count` may be zero for text-only packs
- `languages` must sum to approximately `1.0`

## 8. `text_profile` Object

```json
{
  "length_buckets": {
    "short_ratio": 0.42,
    "medium_ratio": 0.46,
    "long_ratio": 0.12
  },
  "tokenization_notes": "single-language english baseline corpus"
}
```

Required fields:

- `length_buckets`

Required `length_buckets` fields:

- `short_ratio`
- `medium_ratio`
- `long_ratio`

Optional fields:

- `tokenization_notes`

Rules:

- ratios should sum to approximately `1.0`

## 9. `metadata_profile` Object

```json
{
  "facets": [
    {
      "name": "workspace_id",
      "kind": "categorical_medium",
      "cardinality": 120,
      "null_ratio": 0.0
    },
    {
      "name": "tags",
      "kind": "collection",
      "cardinality": 400,
      "null_ratio": 0.08
    }
  ],
  "selectivity_exemplars": {
    "broad": "workspace_id = w1",
    "medium": "workspace_id = w8 AND is_archived = false",
    "narrow": "workspace_id = w8 AND priority = high AND created_at >= 2026-01-01",
    "zero_hit": "workspace_id = missing_workspace"
  }
}
```

Required fields:

- `facets`
- `selectivity_exemplars`

Each facet requires:

- `name`
- `kind`
- `cardinality`
- `null_ratio`

Allowed `kind` values:

- `categorical_low`
- `categorical_medium`
- `categorical_high`
- `boolean`
- `time`
- `numeric_range`
- `collection`

Rules:

- at least one `collection` facet is required for schema-conformant packs
- `null_ratio` must be between `0.0` and `1.0`

## 10. `vector_profile` Object

```json
{
  "enabled": true,
  "embedding_dimensions": 384,
  "embedding_dtype": "f32",
  "distance_metric": "cosine",
  "query_vectors": {
    "precomputed_available": true,
    "runtime_embedding_supported": true
  }
}
```

Required fields:

- `enabled`
- `embedding_dimensions`
- `embedding_dtype`
- `distance_metric`
- `query_vectors`

Allowed `embedding_dtype` values:

- `f32`
- `i8`
- `u8`

Allowed `distance_metric` values:

- `cosine`
- `dot`
- `l2`

Rules:

- when `enabled` is `false`, `vector_count` in `corpus` must be `0`
- when `enabled` is `false`, `embedding_dimensions` must still be present and set to `0`

## 11. `dirty_profile` Object

```json
{
  "profile": "dirty_light",
  "base_dataset_id": "knowledge-medium-clean-v1",
  "seed": 42,
  "delete_ratio": 0.08,
  "update_ratio": 0.05,
  "append_ratio": 0.12,
  "target_segment_count_range": [8, 14],
  "target_segment_topology": [
    {
      "tier": "large",
      "count": 1
    },
    {
      "tier": "medium",
      "count": 4
    },
    {
      "tier": "small",
      "count": 8
    }
  ],
  "target_tombstone_ratio": 0.09,
  "compaction_state": "pre_compaction"
}
```

Required fields:

- `profile`
- `seed`
- `delete_ratio`
- `update_ratio`
- `append_ratio`
- `target_segment_count_range`
- `target_segment_topology`
- `target_tombstone_ratio`
- `compaction_state`

Conditionally required:

- `base_dataset_id` for dirty variants

Allowed `compaction_state` values:

- `clean`
- `pre_compaction`
- `post_compaction`

Rules:

- `clean` packs should use zero ratios and `compaction_state = clean`
- dirty variants must declare the clean base they were derived from
- `target_segment_topology` must be structured data, never a free-form sentence

## 12. `files` Array

The manifest must enumerate all payload files in the pack.

Each entry:

```json
{
  "path": "docs.ndjson",
  "kind": "documents",
  "format": "ndjson",
  "record_count": 50000,
  "checksum": "sha256:..."
}
```

Required fields:

- `path`
- `kind`
- `format`
- `record_count`
- `checksum`

Allowed `kind` values:

- `documents`
- `metadata`
- `query_set`
- `ground_truth`
- `document_vectors`
- `query_vectors`
- `prebuilt_store`

Rules:

- paths must be relative to the dataset pack root
- a manifest may list multiple files of the same kind
- `prebuilt_store` is optional and must never be the only source of logical dataset identity

## 13. `query_sets` Array

Each query set object:

```json
{
  "query_set_id": "knowledge-medium-core-v1",
  "path": "queries/core.jsonl",
  "ground_truth_path": "queries/core-ground-truth.jsonl",
  "query_count": 160,
  "classes": [
    "keyword",
    "prefix",
    "fuzzy_keyword",
    "topical",
    "vector",
    "hybrid",
    "metadata_filtered",
    "no_hit",
    "high_recall"
  ],
  "difficulty_distribution": {
    "easy": 40,
    "medium": 80,
    "hard": 40
  }
}
```

Required fields:

- `query_set_id`
- `path`
- `ground_truth_path`
- `query_count`
- `classes`
- `difficulty_distribution`

Allowed `classes` values:

- `keyword`
- `prefix`
- `fuzzy_keyword`
- `topical`
- `vector`
- `hybrid`
- `metadata_filtered`
- `no_hit`
- `high_recall`

Rules:

- `query_set_id` values must be unique
- `path` must point to a file listed in `files`
- `ground_truth_path` must point to a file listed in `files`

## 14. `checksums` Object

```json
{
  "manifest_payload_checksum": "sha256:...",
  "logical_documents_checksum": "sha256:...",
  "logical_metadata_checksum": "sha256:...",
  "logical_query_definitions_checksum": "sha256:...",
  "logical_vector_payload_checksum": "sha256:...",
  "fairness_fingerprint": "sha256:..."
}
```

Required fields:

- `manifest_payload_checksum`
- `logical_documents_checksum`
- `logical_metadata_checksum`
- `logical_query_definitions_checksum`
- `fairness_fingerprint`

Conditionally required:

- `logical_vector_payload_checksum` when vector payload exists

Rule:

- checksum values should use the form `sha256:<hex>`
- file-local integrity stays in each `files[].checksum`
- top-level checksums are for logical equivalence and fairness comparison
- `fairness_fingerprint` should be derived from the canonical comparison fields in Section 17.3

## 15. Query Definition Schema

The manifest references query files, but the query object shape also needs to be fixed.

Each query definition should contain:

```json
{
  "query_id": "q-001",
  "query_class": "hybrid",
  "difficulty": "medium",
  "query_text": "rust async sqlite benchmark",
  "top_k": 10,
  "filter_spec": {
    "workspace_id": "w8",
    "tags_any": ["rust", "benchmark"]
  },
  "preview_expected": true,
  "embedding_available": true,
  "lane_eligibility": {
    "text": true,
    "vector": true,
    "hybrid": true
  }
}
```

Required fields:

- `query_id`
- `query_class`
- `difficulty`
- `query_text`
- `top_k`
- `filter_spec`
- `preview_expected`
- `embedding_available`
- `lane_eligibility`

Rules:

- `query_id` must be unique within the pack
- a query cannot mark `vector = true` if `embedding_available = false`

## 16. Validation Rules

Readers and packers must validate all of these:

- top-level required sections exist
- all required fields exist
- enum values are known
- `doc_count` matches document payload
- `query_count` matches query file contents
- all checksums match actual files
- all `path` values exist in the pack
- dirty profile rules match `variant_id`
- vector profile rules match corpus vector counts
- `fairness_fingerprint` matches the canonical fairness field set
- ground-truth files align with declared queries

Validation should return structured errors rather than a single opaque parse failure.

## 17. Compatibility Rules

### 17.1 Additive Fields

Readers should ignore unknown fields only if:

- `schema_version` is unchanged
- the reader can still validate all required fields

### 17.2 Breaking Changes

These require a new `schema_version`:

- changing enum semantics
- renaming required keys
- changing checksum format
- changing top-level object structure incompatibly

### 17.3 Stable Comparison Rule

Two packs are comparable only if all of these match:

- `schema_version`
- `dataset_family`
- `dataset_tier`
- `embedding_spec_id`
- `embedding_model_hash`
- `query_checksum`
- `fairness_fingerprint`

They may intentionally differ in:

- `variant_id`
- `dirty_profile`
- `generated_at`
- `generator.version`

## 18. Minimal Example

```json
{
  "schema_version": "wax_dataset_pack_v1",
  "generated_at": "2026-03-30T00:00:00Z",
  "generator": {
    "name": "wax-bench-packer",
    "version": "0.1.0"
  },
  "identity": {
    "dataset_id": "knowledge-small-clean-v1",
    "dataset_version": "v1",
    "dataset_family": "knowledge",
    "dataset_tier": "small",
    "variant_id": "clean",
    "embedding_spec_id": "minilm-l6-384-f32-cosine",
    "embedding_model_version": "2026-03-15",
    "embedding_model_hash": "sha256:model",
    "corpus_checksum": "sha256:corpus",
    "query_checksum": "sha256:queries"
  },
  "environment_constraints": {
    "min_ram_gb": 4,
    "recommended_ram_gb": 8
  },
  "corpus": {
    "doc_count": 1000,
    "vector_count": 1000,
    "total_text_bytes": 240000,
    "avg_doc_length": 240.0,
    "median_doc_length": 210,
    "p95_doc_length": 640,
    "max_doc_length": 2000,
    "languages": [
      {
        "code": "en",
        "ratio": 1.0
      }
    ]
  },
  "text_profile": {
    "length_buckets": {
      "short_ratio": 0.4,
      "medium_ratio": 0.5,
      "long_ratio": 0.1
    }
  },
  "metadata_profile": {
    "facets": [
      {
        "name": "workspace_id",
        "kind": "categorical_medium",
        "cardinality": 12,
        "null_ratio": 0.0
      },
      {
        "name": "tags",
        "kind": "collection",
        "cardinality": 80,
        "null_ratio": 0.1
      }
    ],
    "selectivity_exemplars": {
      "broad": "workspace_id = w1",
      "medium": "workspace_id = w3 AND is_archived = false",
      "narrow": "workspace_id = w3 AND tags_any = rust",
      "zero_hit": "workspace_id = missing"
    }
  },
  "vector_profile": {
    "enabled": true,
    "embedding_dimensions": 384,
    "embedding_dtype": "f32",
    "distance_metric": "cosine",
    "query_vectors": {
      "precomputed_available": true,
      "runtime_embedding_supported": true
    }
  },
  "dirty_profile": {
    "profile": "clean",
    "seed": 0,
    "delete_ratio": 0.0,
    "update_ratio": 0.0,
    "append_ratio": 0.0,
    "target_segment_count_range": [1, 1],
    "target_segment_topology": [
      {
        "tier": "large",
        "count": 1
      }
    ],
    "target_tombstone_ratio": 0.0,
    "compaction_state": "clean"
  },
  "files": [
    {
      "path": "docs.ndjson",
      "kind": "documents",
      "format": "ndjson",
      "record_count": 1000,
      "checksum": "sha256:docs"
    },
    {
      "path": "queries/core.jsonl",
      "kind": "query_set",
      "format": "jsonl",
      "record_count": 40,
      "checksum": "sha256:queryset"
    },
    {
      "path": "queries/core-ground-truth.jsonl",
      "kind": "ground_truth",
      "format": "jsonl",
      "record_count": 40,
      "checksum": "sha256:groundtruth"
    }
  ],
  "query_sets": [
    {
      "query_set_id": "knowledge-small-core-v1",
      "path": "queries/core.jsonl",
      "ground_truth_path": "queries/core-ground-truth.jsonl",
      "query_count": 40,
      "classes": [
        "keyword",
        "prefix",
        "fuzzy_keyword",
        "topical",
        "vector",
        "hybrid",
        "metadata_filtered",
        "no_hit",
        "high_recall"
      ],
      "difficulty_distribution": {
        "easy": 10,
        "medium": 20,
        "hard": 10
      }
    }
  ],
  "checksums": {
    "manifest_payload_checksum": "sha256:manifest",
    "logical_documents_checksum": "sha256:docs-logical",
    "logical_metadata_checksum": "sha256:meta-logical",
    "logical_query_definitions_checksum": "sha256:queries-logical",
    "logical_vector_payload_checksum": "sha256:vectors-logical",
    "fairness_fingerprint": "sha256:fairness"
  }
}
```
