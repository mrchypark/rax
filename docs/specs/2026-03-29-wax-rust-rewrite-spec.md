# Wax Rust Rewrite Spec

Status: Draft  
Date: 2026-03-29  
Scope: `christopherkarani/Wax` 분석 기반의 cross-platform Rust 재작성 1차 스펙

## 1. 목표

이 문서는 Swift 기반 `Wax`를 Rust로 재작성할 때 유지해야 할 파일 포맷 규약과, 재설계가 필요한 ML/검색 계층을 분리해서 정의한다.

이번 스펙의 기본 가정은 다음과 같다.

- 기본 목표는 기존 `.wax` v1 파일과의 호환성 유지다.
- `mcp`, `node`, `python` 래퍼는 분석 대상에서 제외한다.
- 우선순위는 `온디스크 포맷`, `검색`, `임베딩`, `플랫폼 이식성`이다.
- "pure Rust"는 기본 방향으로 삼되, 기존 v1 의미론을 그대로 유지하기 위해 불가피한 예외가 있으면 문서에 명시한다.

## 2. 분석 범위와 근거

### 2.1 Primary Upstream Sources

이 문서에서 가장 우선하는 원문 기준은 `WaxCore.docc`다.

- [Documentation.md](https://github.com/christopherkarani/Wax/blob/main/Sources/WaxCore/WaxCore.docc/Documentation.md)
- [FileFormat.md](https://github.com/christopherkarani/Wax/blob/main/Sources/WaxCore/WaxCore.docc/Articles/FileFormat.md)
- [WALAndCrashRecovery.md](https://github.com/christopherkarani/Wax/blob/main/Sources/WaxCore/WaxCore.docc/Articles/WALAndCrashRecovery.md)
- [StructuredMemory.md](https://github.com/christopherkarani/Wax/blob/main/Sources/WaxCore/WaxCore.docc/Articles/StructuredMemory.md)
- [ConcurrencyModel.md](https://github.com/christopherkarani/Wax/blob/main/Sources/WaxCore/WaxCore.docc/Articles/ConcurrencyModel.md)

이 다섯 문서는 다음 역할로 해석한다.

- `Documentation.md`
  - `WaxCore`의 책임 범위와 public concept map
- `FileFormat.md`
  - `.wax` 레이아웃, header/footer/TOC/frame metadata의 상위 규약
- `WALAndCrashRecovery.md`
  - WAL 구조와 recovery semantics
- `StructuredMemory.md`
  - structured memory의 개념 모델과 저장 책임 위치
- `ConcurrencyModel.md`
  - runtime/concurrency 해석 보조 자료

### 2.2 Secondary Implementation Sources

byte-level 필드 순서, optional tag 인코딩, opaque payload 해석처럼 문서 원문에 완전히 적혀 있지 않은 부분은 Swift 구현 코드를 2차 근거로 사용했다.

분석에 사용한 구현 근거는 아래 파일들이다.

- `Sources/WaxCore/WaxCore.docc/Articles/FileFormat.md`
- `Sources/WaxCore/WaxCore.docc/Articles/WALAndCrashRecovery.md`
- `Sources/WaxCore/FileFormat/WaxHeaderPage.swift`
- `Sources/WaxCore/FileFormat/WaxFooter.swift`
- `Sources/WaxCore/FileFormat/WaxTOC.swift`
- `Sources/WaxCore/FileFormat/FrameMeta.swift`
- `Sources/WaxCore/FileFormat/IndexManifests.swift`
- `Sources/WaxCore/FileFormat/SegmentCatalog.swift`
- `Sources/WaxCore/WAL/WALRecord.swift`
- `Sources/WaxCore/WAL/WALEntryCodec.swift`
- `Sources/WaxCore/Wax.swift`
- `Sources/WaxTextSearch/FTS5Schema.swift`
- `Sources/WaxTextSearch/FTS5SearchEngine.swift`
- `Sources/WaxTextSearch/FTS5Serializer.swift`
- `Sources/WaxVectorSearch/VectorSerializer.swift`
- `Sources/WaxVectorSearch/LoadedVectorSearchEngine.swift`
- `Sources/WaxVectorSearch/USearchVectorEngine.swift`
- `Sources/WaxVectorSearch/AccelerateVectorEngine.swift`
- `Sources/WaxVectorSearchMiniLM/MiniLMEmbedder.swift`
- `Sources/Wax/Wax.docc/Articles/Architecture.md`
- `Sources/Wax/Wax.docc/Articles/UnifiedSearch.md`
- `Sources/Wax/Wax.docc/Articles/RAGPipeline.md`

### 2.3 해석 원칙

이 문서의 규약은 아래 우선순위를 따른다.

1. `WaxCore.docc` 원문에 직접 명시된 내용
2. Swift 구현에서 직접 확인되는 binary/layout behavior
3. 1과 2를 보존하기 위한 최소 추론

따라서 이후 본문에서:

- `문서 기준`은 upstream doc에서 직접 확인된 규약
- `구현 기준`은 Swift 소스에서 직접 확인된 규약
- `추론`은 호환 구현을 위해 채택한 보수적 해석

으로 읽으면 된다.

## 3. 현재 Wax의 핵심 구조

원본 Wax는 Apple 플랫폼 전용 최적화를 많이 포함하지만, 구조는 크게 4층으로 분해된다.

1. `WaxCore`
   - `.wax` 단일 파일 포맷
   - WAL, header/footer/TOC, frame payload 저장
   - checksum, compression, binary codec
2. `WaxTextSearch`
   - SQLite FTS5 기반 text lane
   - structured memory schema도 같은 SQLite blob에 저장
3. `WaxVectorSearch`
   - vector index serialization
   - USearch / Accelerate / MetalANNS 엔진 교체 가능
4. `Wax`
   - ingestion orchestration
   - hybrid search, query classification, RAG assembly

즉 Rust 재작성에서 가장 먼저 고정해야 할 것은 UI나 assistant integration이 아니라:

- `.wax` v1 binary contract
- commit / recovery semantics
- search lane semantics
- embedding provider abstraction

## 4. `.wax` v1 호환 스펙

### 4.1 파일 레이아웃

`.wax` v1 파일은 아래 순서를 갖는다.

1. Header Page A: `4096` bytes
2. Header Page B: `4096` bytes
3. WAL ring buffer: 기본 `256 MiB`, 생성 시 조정 가능
4. payload region: frame payloads + committed search segments
5. TOC
6. Footer: `64` bytes

Rust 구현은 이 순서와 오프셋 규약을 그대로 유지해야 한다.

### 4.2 공통 인코딩 규약

- 모든 primitive는 little-endian이다.
- string은 `UInt32 byte length + UTF-8 bytes`다.
- blob은 `UInt32 length + bytes`다.
- optional은 `tag byte (0/1) + payload`다.
- array는 `UInt32 count + element sequence`다.
- 주요 decoder limit:
  - max string bytes: `16 MiB`
  - max blob bytes: `256 MiB`
  - max array count: `10,000,000`
  - max TOC bytes: `64 MiB`

### 4.3 Header

Header page는 dual-page A/B 전략을 사용한다.

- magic: `WAX1`
- format version: packed `UInt16`, 현재 `1.0`
- `headerPageGeneration`이 더 큰 쪽을 우선 선택
- checksum이 맞지 않으면 폐기

Rust 구현 필수 필드:

- formatVersion
- specMajor/specMinor
- headerPageGeneration
- fileGeneration
- footerOffset
- walOffset
- walSize
- walWritePos
- walCheckpointPos
- walCommittedSeq
- tocChecksum
- headerChecksum
- optional replay snapshot (`WALSNAP1`)

### 4.4 Footer

Footer는 고정 64-byte 구조다.

- magic: `WAX1FOOT`
- tocLen
- tocHash
- generation
- walCommittedSeq

Footer의 `tocHash`는 TOC의 마지막 32-byte checksum과 동일해야 한다.

### 4.5 TOC

TOC v1은 아래를 포함한다.

- `tocVersion`
- dense `frames` array
- `indexes`
- optional `timeIndex`
- `segmentCatalog`
- `ticketRef`
- optional `memoryBinding`
- `merkleRoot`
- `tocChecksum`

v1 decoder는 아래 future field를 허용하지 않는다.

- memories_track
- logic_mesh
- sketch_track
- replay_manifest
- enrichment_queue

즉 Rust 쪽도 v1 compatibility mode에서는 이 필드들을 0으로 유지해야 한다.

### 4.6 FrameMeta

frame는 dense `UInt64 id`를 사용하며 TOC 배열 index와 동일하다.

필수 호환 필드:

- id
- timestamp
- anchorTs?
- kind?
- track?
- payloadOffset
- payloadLength
- checksum
- uri?
- title?
- canonicalEncoding
- canonicalLength?
- storedChecksum?
- metadata?
- searchText?
- tags
- labels
- contentDates
- role
- parentId?
- chunkIndex?
- chunkCount?
- chunkManifest?
- status
- supersedes?
- supersededBy?

유지해야 하는 invariant:

- compressed payload이면 `canonicalLength`가 반드시 존재
- `payloadLength > 0`이면 `storedChecksum`이 반드시 존재
- frame ids는 dense해야 함

### 4.7 Compression

v1 payload encoding:

- `plain`
- `lzfse`
- `lz4`
- `deflate`

Rust 재작성 권고:

- cross-platform 기본 writer는 `lz4` 또는 `deflate`
- `lzfse`는 read compatibility 우선
- strict portable mode에서는 `lzfse` write를 비활성화

### 4.8 WAL

WAL은 fixed-size circular ring buffer다.

record header는 48 bytes:

- sequence: `UInt64`
- payload length: `UInt32`
- flags: `UInt32`
- checksum: `32 bytes`

opcode:

- `0x01` `putFrame`
- `0x02` `deleteFrame`
- `0x03` `supersedeFrame`
- `0x04` `putEmbedding`

핵심 동작:

- commit 전 변경은 WAL에만 존재
- commit 시 pending mutation을 TOC/segment에 반영
- header checkpoint/writePos를 갱신
- crash recovery는 selected header + replay snapshot + WAL scan으로 수행

Rust 구현 필수 요구:

- checksum mismatch record는 partial write로 간주하고 scan 중단
- checksum이 맞지만 opcode decode가 실패하면 structural corruption으로 처리
- pending mutation scan과 state scan을 분리
- sequence는 단조 증가해야 함

## 5. 검색/ML 동작 분석

### 5.1 Text lane

현재 text lane은 SQLite FTS5 blob을 `lex` segment로 저장한다.

구조:

- FTS virtual table: `frames_fts`
- mapping table: `frame_mapping(frame_id, rowid_ref)`
- application_id: `WAXT`
- user_version: `3`
- structured memory tables도 같은 SQLite DB 안에 존재

의미론:

- primary FTS query 실행
- 필요 시 OR-expanded fallback query 실행
- `bm25(frames_fts)` 결과를 Wax score로 변환
- snippet 생성

중요한 결론:

- v1 lexical persistence는 "텍스트 색인 결과"가 아니라 "SQLite 데이터베이스 blob" 자체다.
- 따라서 strict pure Rust만으로 기존 v1 lex segment를 read/write 호환하는 것은 현실적으로 어렵다.

### 5.2 Vector lane

vector segment는 `MV2V` header를 가진다.

encoding:

- `1`: uSearch
- `2`: metal
- `3`: flat

segment header 필드:

- magic `MV2V`
- version
- encoding
- similarity
- dimension
- vectorCount
- payloadLength
- reserved[8]

현재 엔진별 특성:

- `USearchVectorEngine`
  - HNSW-like native index serialization 사용
- `AccelerateVectorEngine`
  - flat vector payload 사용
- `Metal`/`MetalANNS`
  - GPU 최적화, 하지만 commit 시 portable flat-like payload로 재구성 가능

Rust 구현 결론:

- reader는 `uSearch`와 `flat` 모두 읽어야 함
- writer의 canonical encoding은 `flat`으로 고정하는 것이 가장 이식성이 높음
- `uSearch` write는 optional compatibility feature로 분리 가능

### 5.3 Embedding providers

현재 내장 provider는 CoreML 기반:

- `MiniLM all-MiniLM-L6-v2` 384 dims
- `Snowflake Arctic Embed Small`

공통 추상화:

- `EmbeddingProvider`
- optional `BatchEmbeddingProvider`
- optional `QueryAwareEmbeddingProvider`

유지해야 할 계약:

- dimensions
- normalize 여부
- provider/model identity
- batch embedding 가능 여부
- query-specific embedding 지원 여부

### 5.4 Unified search

현재 unified search는 최대 4 lane을 조합한다.

- BM25 text
- vector similarity
- structured memory evidence
- timeline fallback

fusion은 weighted RRF다.

`score(d) = Σ weight_lane / (rrfK + rank_lane(d))`

query classification은 deterministic rule-based다.

- factual
- semantic
- temporal
- exploratory

기본 weight:

- factual: BM25 0.7 / vector 0.3
- semantic: BM25 0.3 / vector 0.7
- temporal: BM25 0.25 / vector 0.25 / temporal 0.5
- exploratory: BM25 0.4 / vector 0.5 / temporal 0.1

이 부분은 engine implementation이 달라도 제품 동작을 크게 좌우하므로 Rust에서도 semantic contract를 유지해야 한다.

### 5.5 Structured memory

structured memory는 별도 storage engine이 아니라 text lane SQLite blob 내부 schema에 의존한다.

즉 현재 v1에서 structured memory portability 문제는 text lane 문제와 묶여 있다.

이 결론은 중요하다.

- text lane을 pure Rust로 새로 쓰면
- structured memory persistence도 같이 재설계해야 한다.

## 6. Rust 재작성 원칙

### 6.1 유지할 것

- `.wax` v1 core binary contract
- WAL recovery semantics
- frame metadata semantics
- vector segment read compatibility
- unified search lane semantics
- rule-based query classification

### 6.2 교체할 것

- CoreML embedder
- Accelerate/Metal 전용 vector execution
- Swift actor/runtime 의존 구조

### 6.3 분리할 것

- file-format compatibility layer
- runtime search engine layer
- ML provider layer
- platform acceleration layer

## 7. 제안 아키텍처

### 7.1 crate 구성

권장 workspace:

- `wax-core`
  - binary codec
  - file I/O
  - WAL
  - TOC/header/footer
  - frame store
- `wax-vector`
  - vector segment codec
  - flat index
  - optional hnsw backend
- `wax-text`
  - text lane abstraction
  - tokenizer / BM25 API
  - optional sqlite compatibility backend
- `wax-embed`
  - provider traits
  - model adapters
- `wax-search`
  - unified search
  - query classification
  - RRF / reranking
- `wax-rag`
  - context assembly
  - surrogate/snippet budget logic
- `wax-cli`
  - inspection, ingest, search, migration 도구

### 7.2 feature flags

권장 feature:

- `compat-v1-core`
- `compat-v1-sqlite-lex`
- `compat-v1-usearch-read`
- `portable-flat-vec`
- `embed-onnx`
- `embed-tract`
- `embed-candle`

기본 profile 권장:

- default: `compat-v1-core`, `portable-flat-vec`, `embed-onnx`

### 7.3 concurrency model

Swift actor를 Rust에서 그대로 복제할 필요는 없다.

권장 방식:

- file writer는 single-writer task 보장
- read path는 lock-free or RwLock 중심
- WAL append / commit / recovery는 명시적인 state machine으로 구현
- async API는 제공하되 core codec은 sync + deterministic 함수로 분리

## 8. 검색/ML 구현 스펙

### 8.1 Vector search 1차 권고안

1차 구현은 아래 조합을 권장한다.

- on-disk write format: `flat`
- runtime search backend: pure Rust HNSW 또는 flat SIMD brute-force
- committed `uSearch` segment는 read-only decode or migration 대상

이유:

- `flat`은 platform-neutral
- checksum과 manifest 계산이 단순
- vector engine 교체가 쉬움

### 8.2 Embedding 1차 권고안

provider trait는 유지하되 내장 모델은 Apple 전용 경로를 버린다.

권장 우선순위:

1. `ONNX Runtime` backend
   - 실용적인 cross-platform parity
   - 초기 제품 적합성 높음
2. `tract` 또는 `candle`
   - pure Rust 지향
   - 실험적 또는 제한적 모델 지원

즉 "pure Rust"를 절대 규칙으로 두기보다:

- core storage는 pure Rust
- embedding runtime은 pluggable
- default provider는 cross-platform 안정성을 우선

으로 정의하는 것이 현실적이다.

### 8.3 Text/structured search 권고안

이 부분이 가장 중요하다.

권장 전략:

1. `.wax` v1 core compatibility는 유지
2. lex/structured persistence는 별도 compatibility tier로 분리
3. 초기 스펙은 아래 두 모드를 동시에 정의

Mode A: `v1-compatible`

- 기존 SQLite FTS5 blob read 지원
- 기존 structured memory blob read 지원
- write/update는 `sqlite-compat` feature에서만 허용

Mode B: `portable-native`

- Rust-native text index 사용
- Rust-native structured memory store 사용
- 새 segment kind 또는 v2 lex manifest 필요

이 문서의 1차 결론은:

- strict pure Rust
- 기존 v1 lex read/write 완전 호환
- Swift Wax와 동일한 structured memory persistence

세 가지를 동시에 만족시키기는 어렵다.

따라서 스펙은 core compatibility와 lex compatibility를 분리해서 정의해야 한다.

## 9. 호환성 매트릭스

### 9.1 반드시 지원

- 기존 `.wax` header/footer/TOC/WAL read
- 기존 frame payload read
- 기존 compressed payload read (`lz4`, `deflate`, 가능하면 `lzfse`)
- existing `flat` vec segment read
- unified search request/response semantic parity

### 9.2 가능하면 지원

- existing `uSearch` vec segment read
- existing SQLite lex blob read
- existing structured memory blob read

### 9.3 초기 릴리스에서 선택적 지원

- SQLite lex blob write
- `lzfse` write
- exact score parity with Swift BM25 implementation
- GPU acceleration parity

## 10. 리스크와 의사결정

### 10.1 가장 큰 리스크

`lex index = serialized SQLite database blob` 라는 현재 구조가 pure Rust 전략과 충돌한다.

이 문제를 숨기면 안 된다.

### 10.2 외부 모델 검토 반영

외부 모델 검토에서 유의미한 지적이 나왔다.

- strict pure Rust와 modern embedding runtime은 긴장 관계가 큼
- SQLite blob 호환은 tokenizer / version / collation 차이까지 포함하는 operational risk가 있음
- Windows에서 header flip/fsync 보장은 별도 검증이 필요함
- score parity보다 rank parity를 먼저 목표로 삼는 것이 현실적임

이에 따라 본 스펙은 다음을 채택한다.

- core binary contract는 exact compatibility
- vector는 portable flat writer 우선
- text/structured는 compatibility tier 분리
- embedding은 trait + pluggable runtime

## 11. 권장 1차 구현 순서

Phase 0. Inspector

- `.wax` header/footer/TOC/WAL dump 도구
- vec/lex manifest inspector
- corruption test fixtures

Phase 1. Core compatibility

- header/footer/TOC codec
- frame payload read/write
- WAL append/replay/commit
- checksum validation

Phase 2. Vector compatibility

- `MV2V flat` read/write
- `uSearch` read path 또는 migration
- query embedding trait 연결

Phase 3. Unified search

- rule-based classifier
- weighted RRF
- timeline fallback
- diagnostics

Phase 4. Text/structured strategy

- `sqlite-compat` feature로 v1 lex read
- native Rust text lane 설계
- structured memory native design 분리

Phase 5. Migration

- v1 lex blob -> native Rust lex segment 변환기
- optional full-store rewrite 도구

## 12. 최종 권고

이번 Rust 재작성의 기준선은 다음으로 정의한다.

1. `.wax` v1 core는 그대로 유지한다.
2. vector segment는 읽기 호환을 유지하되 쓰기는 `flat`을 canonical format으로 삼는다.
3. unified search와 query classification의 semantic contract는 유지한다.
4. embedding은 Rust trait 기반으로 추상화하고, runtime은 pluggable로 둔다.
5. text/structured persistence는 v1 SQLite blob 호환과 pure Rust native path를 명시적으로 분리한다.

즉 1차 스펙의 핵심은 "Swift 구현을 그대로 번역"하는 것이 아니라:

- 호환되어야 하는 binary contract는 고정하고
- 플랫폼 종속적인 실행 엔진은 교체 가능하게 만들며
- 호환성과 pure Rust가 충돌하는 지점은 compatibility tier로 분리하는 것

이다.

## 13. 다음 문서 제안

이 다음 단계에서는 아래 두 문서를 별도로 작성하는 것이 적절하다.

- `wax-v1-binary-format.md`
  - byte-level codec, exact field tables, examples
- `wax-rust-search-strategy.md`
  - sqlite-compat vs native-text lane decision, migration plan
