# iOS Harness Host

This package is the host-driven shell for physical iOS benchmark execution.

Current scope:

- validate benchmark labeling before any device run
- reject ambiguous `cold` labels
- stamp explicit `cold_state` and `thermal_state` metadata
- expose separate hook declarations for process relaunch and memory pressure

Non-goals for this shell:

- shipping a finished device controller
- inferring "system cold" from a plain relaunch
- hiding cache preparation behind generic labels

## Usage

```bash
swift run --package-path apps/ios-harness-host HostMain \
  describe-run \
  --target-bundle-id com.example.app \
  --artifact-dir /tmp/wax-ios-artifacts \
  --cold-state restart_cold \
  --thermal-state nominal
```

The command prints a JSON run plan. It rejects `--cold-state cold`.

Accepted cold-state labels:

- `restart_cold`
- `pressure_cold`
- `reboot_cold`

The shell declares hook availability for:

- process relaunch
- memory pressure

Those hooks are intentionally separate so benchmark reports never imply that a relaunch alone achieved a broader cold-state guarantee.
