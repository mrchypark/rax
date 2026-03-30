// swift-tools-version: 5.9
import PackageDescription

let package = Package(
    name: "ios-harness-host",
    platforms: [
        .macOS(.v13),
    ],
    products: [
        .executable(name: "HostMain", targets: ["HostMain"]),
    ],
    targets: [
        .executableTarget(
            name: "HostMain",
            path: "Sources/HostMain"
        ),
    ]
)
