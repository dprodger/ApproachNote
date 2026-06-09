import Foundation
import os

/// Structured logging categories for the Approach Note app.
///
/// Usage:
///   Log.network.debug("Fetching songs")
///   Log.auth.info("User logged in")
///   Log.auth.error("Token refresh failed: \(error.localizedDescription)")
///
/// Privacy:
///   Log.auth.debug("Login for \(email, privacy: .private)")
///   Log.network.debug("GET \(endpoint, privacy: .public)")
enum Log {
    private nonisolated static let subsystem = Bundle.main.bundleIdentifier ?? "com.approachnote"

    // `Logger` is Sendable and thread-safe, so the loggers are `nonisolated`:
    // under the project's MainActor-by-default isolation they'd otherwise be
    // inferred main-actor-isolated and couldn't be used from background actors
    // (e.g. the oEmbed `TitleCache` actor in YouTube.swift).

    /// API calls, HTTP responses, request timing
    nonisolated static let network  = Logger(subsystem: subsystem, category: "network")
    /// Authentication, token refresh, keychain
    nonisolated static let auth     = Logger(subsystem: subsystem, category: "auth")
    /// View state, navigation, user interactions
    nonisolated static let ui       = Logger(subsystem: subsystem, category: "ui")
    /// Data import, persistence, repertoires, favorites
    nonisolated static let data     = Logger(subsystem: subsystem, category: "data")
    /// Research queue, background enrichment
    nonisolated static let research = Logger(subsystem: subsystem, category: "research")
}
