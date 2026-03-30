# iOS Cold Label Contract

The host-driven iOS harness must never accept a generic `cold` label.

Accepted labels:

- `restart_cold`
- `pressure_cold`
- `reboot_cold`

Required rules:

1. A plain relaunch may only justify `restart_cold`.
2. Memory-pressure preparation must not be collapsed into `restart_cold`.
3. A host-driven run must stamp `cold_state` explicitly in the emitted payload.
4. "System cold" must never be inferred from a relaunch alone.

Shell contract:

```bash
swift run --package-path apps/ios-harness-host HostMain \
  describe-run \
  --target-bundle-id com.example.app \
  --artifact-dir /tmp/wax-ios-artifacts \
  --cold-state restart_cold
```

Negative example:

```bash
swift run --package-path apps/ios-harness-host HostMain \
  describe-run \
  --target-bundle-id com.example.app \
  --artifact-dir /tmp/wax-ios-artifacts \
  --cold-state cold
```

The negative example must fail.
