//
//  ScreenshotMode.swift
//  Approach Note
//
//  App Store Review Guideline 5.2.1 forbids showing protected third-party
//  album cover artwork in screenshots and previews. "Screenshot mode" swaps
//  every real album cover for original, procedurally generated artwork
//  (see `GenericCoverArt`) so the marketing screenshots never display a
//  copyrighted cover.
//
//  It can be turned on three ways, all resolving to the same UserDefaults key:
//    1. Launch argument `-screenshotMode YES`. UserDefaults parses `-key value`
//       launch arguments automatically, so this works out of the box with
//       fastlane `snapshot` (`app.launchArguments += ["-screenshotMode", "YES"]`).
//    2. Environment variable `SCREENSHOT_MODE=1` (or true/yes).
//    3. The DEBUG-only toggle in Settings, which writes the same key.
//

import Foundation

enum ScreenshotMode {
    /// UserDefaults key — also the launch-argument name (`-screenshotMode YES`).
    static let defaultsKey = "screenshotMode"

    /// Environment-variable form only. The launch-argument and in-app-toggle
    /// forms come through `UserDefaults`/`@AppStorage(defaultsKey)`, so views
    /// that bind `@AppStorage(ScreenshotMode.defaultsKey)` should OR their
    /// bound value with this to cover the env-var path too.
    static var envEnabled: Bool {
        guard let raw = ProcessInfo.processInfo.environment["SCREENSHOT_MODE"]?.lowercased() else {
            return false
        }
        return raw == "1" || raw == "true" || raw == "yes"
    }

    /// Convenience for non-SwiftUI contexts (covers env var + UserDefaults).
    static var isEnabled: Bool {
        UserDefaults.standard.bool(forKey: defaultsKey) || envEnabled
    }
}
