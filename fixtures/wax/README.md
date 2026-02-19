# Wax Fixture Notes

This directory hosts Wax-generated fixtures consumed by Rust parity tests.

## Generation hint (Swift Wax side)
1. Create/open a `.mv2s` file with Wax demo or integration harness.
2. Write at least one frame + commit.
3. Copy resulting file into `fixtures/wax/`.
4. Keep metadata in this README (source commit/date/command).

Current placeholder fixture:
- `minimal.mv2s` (bootstrap sentinel for harness wiring)
