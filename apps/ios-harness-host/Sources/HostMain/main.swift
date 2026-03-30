import Foundation

struct RunPlan: Encodable {
    struct Hooks: Encodable {
        let processRelaunchAvailable: Bool
        let memoryPressureHookAvailable: Bool
    }

    let targetBundleId: String
    let artifactDir: String
    let coldState: String
    let thermalState: String
    let hookPolicy: Hooks
    let notes: [String]
}

enum HostError: Error, CustomStringConvertible {
    case usage(String)
    case invalidColdState(String)
    case unknownFlag(String)
    case duplicateFlag(String)

    var description: String {
        switch self {
        case let .usage(message):
            return message
        case let .invalidColdState(value):
            return "invalid cold_state '\(value)'; use restart_cold, pressure_cold, or reboot_cold"
        case let .unknownFlag(flag):
            return "unknown flag '\(flag)'"
        case let .duplicateFlag(flag):
            return "duplicate flag '\(flag)'"
        }
    }
}

@main
struct HostMain {
    static func main() {
        do {
            let plan = try parseRunPlan(arguments: Array(CommandLine.arguments.dropFirst()))
            let encoder = JSONEncoder()
            encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
            let data = try encoder.encode(plan)
            FileHandle.standardOutput.write(data)
            FileHandle.standardOutput.write(Data("\n".utf8))
        } catch {
            FileHandle.standardError.write(Data("\(error)\n".utf8))
            Foundation.exit(1)
        }
    }

    private static func parseRunPlan(arguments: [String]) throws -> RunPlan {
        guard arguments.first == "describe-run" else {
            throw HostError.usage(
                "usage: HostMain describe-run --target-bundle-id <id> --artifact-dir <dir> --cold-state <restart_cold|pressure_cold|reboot_cold> [--thermal-state <nominal|fair|serious|critical>]"
            )
        }

        var values: [String: String] = [:]
        let allowedFlags: Set<String> = [
            "target-bundle-id",
            "artifact-dir",
            "cold-state",
            "thermal-state",
        ]
        var index = 1
        while index < arguments.count {
            let key = arguments[index]
            guard key.hasPrefix("--"), index + 1 < arguments.count else {
                throw HostError.usage("expected flag-value pairs after describe-run")
            }
            let normalizedKey = String(key.dropFirst(2))
            guard allowedFlags.contains(normalizedKey) else {
                throw HostError.unknownFlag(key)
            }
            guard values[normalizedKey] == nil else {
                throw HostError.duplicateFlag(key)
            }
            values[normalizedKey] = arguments[index + 1]
            index += 2
        }

        let targetBundleId = try requiredValue("target-bundle-id", from: values)
        let artifactDir = try requiredValue("artifact-dir", from: values)
        let coldState = try requiredValue("cold-state", from: values)
        let thermalState = values["thermal-state"] ?? "nominal"

        guard coldState != "cold" else {
            throw HostError.invalidColdState(coldState)
        }
        guard ["restart_cold", "pressure_cold", "reboot_cold"].contains(coldState) else {
            throw HostError.invalidColdState(coldState)
        }
        guard ["nominal", "fair", "serious", "critical"].contains(thermalState) else {
            throw HostError.usage("invalid thermal_state '\(thermalState)'")
        }

        return RunPlan(
            targetBundleId: targetBundleId,
            artifactDir: artifactDir,
            coldState: coldState,
            thermalState: thermalState,
            hookPolicy: .init(
                processRelaunchAvailable: true,
                memoryPressureHookAvailable: true
            ),
            notes: [
                "generic 'cold' labels are rejected",
                "plain relaunch does not imply system-cold semantics",
                "memory pressure and relaunch hooks are declared separately"
            ]
        )
    }

    private static func requiredValue(
        _ key: String,
        from values: [String: String]
    ) throws -> String {
        guard let value = values[key], !value.isEmpty else {
            throw HostError.usage("missing --\(key)")
        }
        return value
    }
}
