# RAX Performance Benchmark & Profiling Checklist

Date: 2026-02-19  
Scope: `MV2S` streaming, incremental backup/restore (`object_store = 0.7.0`), core retrieval paths

## 1. Preflight

- [ ] Run from repo root: `/Users/cypark/Documents/project/rax`
- [ ] Use release profile for measurement:
  - `cargo test --release --workspace --no-fail-fast`
- [ ] Prepare output directory:
  - `mkdir -p artifacts/perf`
- [ ] Record environment once:
  - `rustc -Vv | tee artifacts/perf/env-rustc.txt`
  - `uname -a | tee artifacts/perf/env-uname.txt`

## 2. Measurement Rules

- [ ] Use at least 5 runs per scenario; separate cold/warm numbers.
- [ ] Capture wall time and peak RSS using macOS `time`:
  - `/usr/bin/time -lp <command>`
- [ ] Keep workload and machine state stable (same power mode, no large background jobs).

## 3. Critical Scenarios (must pass + measure)

### A. MV2S Streaming

- [ ] Functional gate:
  - `cargo test --release -p rax-core stream_large_payload_without_full_buffering -- --exact`
- [ ] Repeatable timing (5 runs):
  - `for i in $(seq 1 5); do /usr/bin/time -lp cargo test --release -p rax-core stream_large_payload_without_full_buffering -- --exact; done 2>&1 | tee artifacts/perf/mv2s-streaming.log`

### B. Incremental Backup

- [ ] Functional gate:
  - `cargo test --release -p rax-core incremental_backup_contains_only_changed_segments_and_wal_range -- --exact`
- [ ] Object-store compatibility gate:
  - `cargo test --release -p rax-core backup_exporter_writes_manifest_to_object_store_memory_backend -- --exact`
- [ ] Repeatable timing (5 runs):
  - `for i in $(seq 1 5); do /usr/bin/time -lp cargo test --release -p rax-core incremental_backup_contains_only_changed_segments_and_wal_range -- --exact; done 2>&1 | tee artifacts/perf/incremental-backup.log`

### C. Incremental Restore + E2E Recovery

- [ ] Functional gate:
  - `cargo test --release -p rax-core incremental_restore_applies_ordered_chain -- --exact`
- [ ] PITR gate:
  - `cargo test --release -p rax-core restore_pitr_stops_at_target_timestamp_and_produces_expected_state -- --exact`
- [ ] E2E gate:
  - `cargo test --release -p rax-compat-tests backup_restore_e2e_memory_backend_round_trip -- --exact`
- [ ] Repeatable timing (5 runs):
  - `for i in $(seq 1 5); do /usr/bin/time -lp cargo test --release -p rax-compat-tests backup_restore_e2e_memory_backend_round_trip -- --exact; done 2>&1 | tee artifacts/perf/restore-e2e.log`

### D. WAL + Retrieval Safety Nets

- [ ] WAL replay:
  - `cargo test --release -p rax-core wal_replay_recovers_pending_put -- --exact`
- [ ] WAL compaction:
  - `cargo test --release -p rax-core wal_compaction_removes_committed_records -- --exact`
- [ ] Hybrid retrieval determinism:
  - `cargo test --release -p rax-rag same_inputs_produce_identical_context_order -- --exact`

## 4. Profiling Checklist

### macOS CPU Hotspots

- [ ] Build with debuginfo for symbolized profiles:
  - `RUSTFLAGS="-C debuginfo=2" cargo test --release -p rax-core stream_large_payload_without_full_buffering -- --exact`
- [ ] Run Time Profiler in Instruments for:
  - Streaming path
  - Incremental backup path
  - Incremental restore path
- [ ] Save traces under `artifacts/perf/traces/` and annotate top 10 stacks.

### Allocation / Memory

- [ ] During each run, record `maximum resident set size` from `/usr/bin/time -lp`.
- [ ] Flag regressions if peak RSS increases by more than 15% versus previous baseline on same machine/workload.

## 5. Suggested SLO Gates (initial)

- [ ] No correctness regression in all gates above.
- [ ] `MV2S` streaming warm p95 wall time does not regress by more than 10% vs last baseline.
- [ ] Incremental backup output only includes changed segments and expected WAL range (already asserted by test).
- [ ] Restore e2e warm p95 wall time does not regress by more than 10% vs last baseline.
- [ ] Peak RSS for streaming and restore does not regress by more than 15% vs last baseline.

## 6. Reporting Template

Fill and commit with each performance run:

```
Date:
Commit:
Machine:
Rust:

Scenario: MV2S streaming
- p50:
- p95:
- peak RSS:
- notes:

Scenario: Incremental backup
- p50:
- p95:
- peak RSS:
- notes:

Scenario: Incremental restore e2e
- p50:
- p95:
- peak RSS:
- notes:

Decision: PASS / FAIL
```
