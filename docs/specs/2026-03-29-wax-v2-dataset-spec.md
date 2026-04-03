# Wax v2 Dataset Spec

Status: Draft  
Date: 2026-03-29  
Scope: benchmark dataset contract for Wax v2 performance evaluation

## 1. Purpose

This document defines the benchmark datasets used by Wax v2 performance work.

Its job is to make benchmark results interpretable and reproducible.

Without a strict dataset contract, the team cannot tell whether a result changed because:

- the engine changed
- the corpus changed
- the query set changed
- the delete and compaction shape changed

This document is the dataset-facing companion to:

- [2026-03-29-wax-v2-benchmark-plan.md](./2026-03-29-wax-v2-benchmark-plan.md)
- [2026-03-29-wax-v2-ttfq-benchmark-plan.md](./2026-03-29-wax-v2-ttfq-benchmark-plan.md)
- [2026-03-29-wax-v2-benchmark-harness-spec.md](./2026-03-29-wax-v2-benchmark-harness-spec.md)
- [2026-03-29-wax-upstream-benchmark-analysis.md](./2026-03-29-wax-upstream-benchmark-analysis.md)

## 2. Design Goals

The dataset contract must support:

- repeatable cold-open and TTFQ measurements
- repeatable warm-search p95 measurements
- realistic metadata filtering behavior
- dirty-store behavior with tombstones and segment accumulation
- like-for-like comparison across revisions and platforms

It should not depend on:

- one vendor embedder
- one platform-specific tokenizer
- ad-hoc manually assembled corpora

## 3. Dataset Identity

Every benchmark dataset pack must have a stable identity.

### 3.1 Required Identity Fields

Each dataset pack must define:

- `dataset_id`
- `dataset_version`
- `dataset_family`
- `dataset_tier`
- `variant_id`
- `corpus_checksum`
- `query_checksum`
- `embedding_spec_id`

### 3.2 Naming Guidance

`dataset_id` should be human-readable and stable.

Recommended shape:

- `family-tier-variant-version`

Example:

- `knowledge-medium-clean-v1`

### 3.3 Versioning Rule

`dataset_version` must change whenever any of the following changes:

- document text
- metadata distribution
- query sets
- embedding dimensions
- dirty-store generation recipe

## 4. Dataset Families

Wax v2 should begin with a small number of well-understood dataset families.

### 4.1 Family A: Knowledge Corpus

Purpose:

- general-purpose product and documentation search

Expected traits:

- paragraph-heavy text
- medium metadata richness
- mixed lexical and semantic retrieval value

### 4.2 Family B: Messaging Or Notes Corpus

Purpose:

- short text and recency-heavy workloads

Expected traits:

- short documents
- high count
- weaker lexical structure
- stronger filter and recency sensitivity

### 4.3 Family C: Catalog Or Record Corpus

Purpose:

- filter-heavy structured search workloads

Expected traits:

- shorter text per item
- more metadata facets
- frequent no-hit and narrow-hit filter cases

The first usable benchmark suite does not need all three families on day one, but the spec should reserve them now to avoid later schema churn.

## 5. Dataset Tiers

Every family should support these size tiers.

### 5.1 Small Tier

Purpose:

- expose fixed overheads
- stress `container_open` and first-query constant costs

Suggested range:

- 1,000 to 5,000 docs

### 5.2 Medium Tier

Purpose:

- approximate realistic single-user or app-local stores

Suggested range:

- 50,000 to 100,000 docs

### 5.3 Large Tier

Purpose:

- expose segment-scaling, memory, and warm-tail issues

Suggested range:

- 500,000+ docs

## 6. Required Document Schema

Each logical document in a benchmark corpus must carry the same canonical fields, even if some are empty.

### 6.1 Required Fields

- `doc_id`
- `title`
- `body`
- `created_at`
- `updated_at`
- `language`
- `facet_map`
- `source_kind`
- `quality_label`

### 6.2 Optional Fields

- `summary`
- `tags`
- `author_id`
- `thread_id`
- `recency_bucket`
- `numeric_fields`

The benchmark runner may ignore some of these, but the dataset contract should keep them explicit.

## 7. Text Composition Rules

Each dataset tier should include a controlled mixture of text lengths.

### 7.1 Required Length Buckets

- short
- medium
- long

### 7.2 Required Measurements

Each manifest must record:

- `doc_count`
- `total_text_bytes`
- `avg_doc_length`
- `median_doc_length`
- `p95_doc_length`
- `max_doc_length`

### 7.3 Language Rule

The first benchmark generation should stay mostly single-language per dataset pack.

Reason:

- multilingual tokenization changes too many variables at once

Multilingual packs can be added later as distinct dataset families or variants.

## 8. Metadata And Filter Schema

Filter behavior is part of Wax v2 architecture risk, so metadata must be deliberate rather than decorative.

### 8.1 Required Facet Types

Every benchmark family should have a mixture of:

- low-cardinality categorical facets
- medium-cardinality categorical facets
- boolean facets
- time or recency facets
- one numeric range facet
- one collection or set-membership facet

### 8.2 Required Filter Selectivity Bands

Each query set must include filters that produce:

- broad matches
- medium selectivity
- narrow selectivity
- zero-hit cases

### 8.3 Required Manifest Fields

The manifest must record:

- facet names
- facet cardinalities
- null ratios
- selectivity exemplars used by query sets

## 9. Query Set Contract

Query sets are part of dataset identity and must be versioned with the corpus.

### 9.1 Required Query Classes

Every dataset pack must include:

- keyword queries
- prefix or typeahead queries
- fuzzy keyword queries
- multi-term topical queries
- vector semantic queries
- hybrid queries
- metadata-filtered queries
- no-hit queries
- broad high-recall queries

### 9.2 Required Difficulty Labels

Each query must carry one of:

- `easy`
- `medium`
- `hard`

Difficulty is benchmark metadata, not a promise of user relevance quality.

### 9.3 Required Query Fields

Each query definition must include:

- `query_id`
- `query_class`
- `difficulty`
- `query_text`
- `top_k`
- `filter_spec`
- `preview_expected`
- `embedding_available`

### 9.4 Lane Eligibility

Each query must declare whether it is valid for:

- text-only execution
- vector-only execution
- hybrid execution

This prevents the harness from inventing invalid workload combinations.

## 10. Embedding Rules

The benchmark dataset must separate embedding data from retrieval semantics cleanly.

### 10.1 Required Embedding Metadata

Each dataset pack must declare:

- `embedding_spec_id`
- `embedding_dimensions`
- `embedding_dtype`
- `embedding_distance_metric`
- whether query embeddings are precomputed

### 10.2 Canonical Vector Payload Rule

If document vectors are included in the dataset pack, they must be stored in a stable canonical format.

The pack must not require a backend-private ANN format to be considered valid input.

### 10.3 Query Embedding Variants

The dataset contract must support:

- precomputed query vectors
- runtime query embedding
- query sets with no vector path at all

This keeps search-engine measurement separable from embedder measurement.

## 11. Dirty Variant Generation

Dirty stores are required. They must not be improvised during one-off testing.

### 11.1 Required Dirty Dimensions

Every dirty variant recipe must specify:

- delete ratio
- update ratio
- append ratio
- target segment count range
- target segment topology
- target tombstone ratio
- compaction state

### 11.2 Required Dirty Profiles

At minimum, support:

- `clean`
- `dirty_light`
- `dirty_heavy`

### 11.3 Dirty Generation Reproducibility

Dirty variants must be generated from:

- a declared seed
- a declared mutation recipe
- a declared base dataset pack version

## 12. Packaging Format

The dataset packer may choose the physical representation, but the logical package must contain these pieces.

### 12.1 Required Pack Contents

- dataset manifest
- document payload
- metadata payload
- query sets
- vector payload if applicable
- dirty recipe or prebuilt dirty-store metadata

### 12.2 Manifest Requirements

The manifest must include:

- identity fields
- corpus summary fields
- query summary fields
- facet summary fields
- embedding summary fields
- dirty-variant summary fields

### 12.3 Checksum Rule

Every major pack component must have its own checksum.

This should include:

- document payload
- metadata payload
- query definitions
- vector payload

## 13. Reproducibility Rules

The dataset spec must make pack regeneration possible without hidden tribal knowledge.

### 13.1 Required Reproduction Inputs

- source corpus provenance
- normalization rules
- random seed
- filtering rules
- metadata synthesis rules if synthetic facets are used
- embedding generation spec

### 13.2 Forbidden Shortcuts

Do not allow:

- hand-edited query files with no version change
- manual dirty-store mutation outside the declared recipe
- mixing vectors from one embedder run with a manifest from another without version change

## 14. Validation Requirements

Every produced dataset pack should be validated before benchmark use.

### 14.1 Required Validation Checks

- document count matches manifest
- query count matches manifest
- all referenced filters are valid
- all vector dimensions are consistent
- dirty ratios are within tolerance
- no duplicate `doc_id`
- no duplicate `query_id`

### 14.2 Sanity Metrics

Each validation run should also report:

- top facet cardinalities
- doc-length histogram summary
- query-class counts
- difficulty distribution
- filter selectivity distribution

## 15. Initial Recommended Dataset Matrix

The first benchmark program should aim for:

1. one family that resembles knowledge or documentation search
2. one small clean tier
3. one medium clean tier
4. one medium dirty tier
5. one large clean tier

This is enough to test:

- fixed cost
- realistic TTFQ
- warm p95
- tombstone sensitivity
- scale behavior

## 16. Key Risks

### 16.1 Synthetic Overfitting

Risk:

- datasets look clean and repeatable but do not reflect real query and metadata pain

Mitigation:

- mix real corpus structure with declared synthetic augmentation only where needed

### 16.2 Query Leakage Into Tuning

Risk:

- one narrow query pack becomes the hidden optimization target

Mitigation:

- keep multiple query classes and difficulty bands in every tier

### 16.3 Dirty Variant Ambiguity

Risk:

- two teams use the same dirty label for materially different segment and tombstone shapes

Mitigation:

- make dirty variants recipe-defined and checksum-addressable

## 17. Immediate Next Dependency

This dataset spec should feed directly into:

- harness implementation planning
- artifact schema design
- baseline dataset creation

If implementation begins before dataset identity is frozen, benchmark outputs will be hard to compare across revisions.
