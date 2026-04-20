# Wax v2 Compaction Follow-up

Status: Approved follow-up design  
Date: 2026-04-19  
Scope: first concrete compaction design target for the current append-only Wax v2 publication model in `rax`

## Summary

The current write path can append immutable segment objects, publish a new manifest generation, and atomically switch the active superblock. It cannot yet compact older active segments, rewrite tombstone-heavy ranges, or publish a filtered replacement generation with explicit lineage.

This follow-up fixes that design gap without changing the current append-only publication rule:

1. compaction is a background rewrite that builds replacement immutable segments off the active read path
2. compaction publishes a new manifest generation atomically, just like any other commit
3. old segments are removed from the new manifest, not rewritten in place
4. physical byte reclamation is deferred; the first compaction target is logical active-set cleanup, not file shrink

## Current State

Today the implementation already has:

- immutable `WXOB` object publication
- alternate-superblock manifest switching
- manifest-visible `doc` segment publication
- tombstone flags in `wax-v2-docstore`
- live/tombstone counters in manifest segment descriptors
- snapshot-isolation reopen tests

Today it does not yet have:

- segment selection rules for compaction
- compaction lineage recording
- multi-segment replacement planning
- filter-aware liveness reconciliation across input segments
- physical space reclamation

## Design Goals

- keep snapshot isolation unchanged
- keep the write path append-only
- make compaction another manifest-generation publish, not a special mutation path
- preserve stable Wax-owned numeric doc ids
- allow family-specific rewrite rules for `doc`, `txt`, and `vec`
- defer file shrinking and byte reclamation until a later rewrite tool or checkpoint design

## Non-Goals

- in-place object mutation
- synchronous query-blocking compaction
- shrinking the `.wax` file in the first compaction implementation
- final public runtime API for compaction control
- final binary layout for `compaction_note`

## Compaction Model

### 1. Compaction Is Logical First

The first compaction implementation is logical compaction:

- select active input segments from manifest generation `N`
- build replacement segments off-path
- append replacement segment objects
- append compaction lineage metadata
- publish manifest generation `N+1` that references replacements and omits the compacted inputs

This reduces active segment count and dead-record pressure without needing immediate file truncation.

Physical reclamation is explicitly deferred.

That means:

- old compacted segment bytes may remain in the file after they leave the active manifest
- a later full-store rewrite, checkpoint, or vacuum tool may reclaim dead bytes
- logical compaction must still be useful before physical reclamation exists

This is a correction to any implicit assumption that “compaction” must immediately shrink the file.

### 2. Compaction Publication Uses The Existing Commit Model

Compaction must use the same commit semantics as ordinary publication:

1. choose a base manifest generation
2. build replacement objects off the active read path
3. append replacement segment objects
4. append optional compaction lineage object
5. append a new manifest object
6. atomically switch the active superblock to the new manifest generation

Crash and recovery expectations remain the same:

- if compaction crashes before manifest switch, old segments remain active
- if compaction crashes after manifest switch, the new manifest generation wins if it is fully valid
- old segments remain readable for older snapshots until those readers complete

## Segment Selection Rules

Compaction should begin with deterministic, descriptor-visible triggers.

### 1. Tombstone Pressure

Compaction candidates should be eligible when tombstone ratio becomes materially high.

Recommended initial trigger inputs:

- `tombstoned_items / (live_items + tombstoned_items)`
- family-specific dead-item thresholds

Initial direction:

- `doc` segments: compact when tombstone ratio is high enough to distort metadata-filtered reads or docstore fan-out
- `vec` segments: compact sooner when dead-node pressure risks query p95 degradation
- `txt` segments: compact when deleted-doc masking or many small overlapping segments make query fan-out expensive

### 2. Segment-Count Pressure

Compaction candidates should also be eligible when too many active segments in one family exist for the current workload tier.

This is especially important for:

- `vec` multi-segment fan-out
- `txt` postings merge overhead
- manifest growth across repeated generations

### 3. Base-Generation Stability

Compaction planning must target one concrete base manifest generation.

If the active manifest changes before publish, the compactor must not blindly publish stale assumptions.

Initial rule:

- if base generation changed and the selected input set is no longer identical, abandon or re-plan compaction

This keeps the current snapshot model coherent and avoids hidden lost-update behavior.

## Rewrite Rules By Segment Family

### 1. `doc` Family

For the first compaction implementation, `doc` is the authoritative liveness source.

Compaction must preserve:

- stable numeric doc ids
- latest surviving metadata for each live doc id
- tombstone precedence over older live rows

Initial merge rule:

- for a given `doc_id`, the newest row across selected input segments wins
- if the newest row is tombstoned, that doc must not appear as live in the replacement segment
- replacement segment descriptors must recompute `live_items`, `tombstoned_items`, timestamp range, and doc-id range from rewritten rows

### 2. `txt` And `vec` Families

`txt` and `vec` compaction must be driven by Wax-level liveness, not backend-private semantics.

Initial rule:

- only docs considered live by the chosen compaction snapshot may be copied into replacement `txt` or `vec` segments
- backend blobs may be rebuilt during compaction, but the logical query contract must not change

This means a future compactor may rewrite:

- postings and lexicon materialization for `txt`
- canonical vectors, previews, and ANN blobs for `vec`

without changing:

- stable doc ids
- manifest publication rules
- query-surface semantics

## Lineage Recording

The binary format already reserves object type `5 = compaction_note`.

The first compaction implementation should use compaction lineage metadata, but that metadata must not become authoritative for recovery.

Initial rule:

- the manifest remains authoritative
- compaction lineage is explanatory and diagnostic

The first `compaction_note` payload should describe:

- base manifest generation
- replacement manifest generation
- compacted input segment object offsets or generations
- replacement output segment object offsets or generations
- family
- compaction reason such as `tombstone_ratio`, `segment_count`, or `manual`

This lineage is for:

- debugging
- benchmark attribution
- later vacuum/rewrite tooling

It is not a substitute for manifest validation.

## Publication Expectations

Compaction must preserve these invariants:

- readers query against the manifest generation they started with
- no active object is mutated in place
- old compacted segments disappear only from newer manifests
- replacement segments are fully appended before manifest switch
- recovery does not need to inspect compacted-out bytes to reopen the store

## Recommended Implementation Order

1. add a compaction-note binary contract to `wax-v2-core`
2. add selection planning over active manifest descriptors
3. add `doc`-family rewrite first
4. publish replacement manifest generations using the existing superblock switch
5. add `txt` and `vec` family rewrites behind the same model
6. add later byte-reclamation or full-store rewrite tooling separately

## Open Risks

### 1. Logical Compaction Without Physical Reclamation

Risk:

- active reads improve, but file size grows indefinitely

Mitigation:

- treat logical compaction and physical reclamation as separate tracked deliverables
- add later rewrite/vacuum tooling explicitly instead of hand-waving shrink behavior

### 2. Stale Base-Generation Planning

Risk:

- compactor publishes replacements against an outdated snapshot

Mitigation:

- bind compaction plans to one base manifest generation
- require re-plan when selected inputs drift

### 3. Backend Drift

Risk:

- `txt` or `vec` compaction starts depending on backend-private invariants

Mitigation:

- keep Wax-owned liveness and manifest descriptors authoritative
- treat rebuilt backend blobs as outputs, not the source of truth

## Immediate Follow-up

This document closes the current roadmap item “add compaction design follow-up.”

The next implementation slice should return to executable migration work:

- move the benchmark runner toward the real core open path
- keep the benchmark harness green while shifting more runtime behavior into `wax-v2-*` crates
