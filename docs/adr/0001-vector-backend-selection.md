# ADR-0001: Vector Backend Selection

- Status: Accepted
- Decision: use CPU backend as baseline default, keep pluggable interface for future `usearch` integration.
- Rationale: deterministic behavior first, fewer deployment constraints.
- Consequence: performance path can be swapped without breaking public API.
