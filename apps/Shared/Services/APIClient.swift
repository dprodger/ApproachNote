import Foundation
import os

// MARK: - API Client

/// Shared infrastructure for all API services
enum APIClient {
    static let baseURL = "https://api.approachnote.com"

    /// API version segment prepended to every request path by `URL.api(path:)`.
    /// Always `/v1` in release builds. In DEBUG builds, the
    /// `APIVersionOverride` UserDefaults key (e.g. `"/v2"`, `""`) overrides
    /// it so we can hit pre-release versions or the unversioned legacy
    /// surface from a TestFlight-style menu without rebuilding.
    static var apiVersionPath: String {
        #if DEBUG
        if let override = UserDefaults.standard.string(forKey: "APIVersionOverride") {
            return override
        }
        #endif
        return "/v1"
    }

    // MARK: - Diagnostics

    private(set) static var requestCounter = 0
    static let diagnosticsEnabled = true

    static func logRequest(_ endpoint: String, startTime: Date) {
        guard diagnosticsEnabled else { return }
        requestCounter += 1
        let duration = Date().timeIntervalSince(startTime)
        let durationStr = String(format: "%.2f", duration)
        let callNumber = self.requestCounter
        Log.network.debug("API Call #\(callNumber, privacy: .public): \(endpoint, privacy: .public) (took \(durationStr)s)")
    }

    static func resetRequestCounter() {
        requestCounter = 0
        if diagnosticsEnabled {
            Log.network.debug("Request counter reset")
        }
    }

    static func printRequestSummary() {
        guard diagnosticsEnabled else { return }
        let totalCalls = self.requestCounter
        Log.network.debug("Total API calls in this session: \(totalCalls, privacy: .public)")
    }

    // MARK: - Search Text Normalization

    /// Normalize search text by converting straight apostrophes to smart apostrophes.
    /// The database stores song titles with smart apostrophes (\u{2019}), so we convert
    /// user input to match (e.g., "We'll" becomes "We\u{2019}ll").
    static func normalizeSearchText(_ text: String) -> String {
        text.replacingOccurrences(of: "'", with: "\u{2019}")
    }

    // MARK: - Preview Mode

    static var isPreviewMode: Bool {
        ProcessInfo.processInfo.environment["XCODE_RUNNING_FOR_PREVIEWS"] == "1"
    }
}

// MARK: - URL Helper

extension URL {
    /// Constructs an API URL from a path relative to `APIClient.baseURL`.
    /// `APIClient.apiVersionPath` (default `/v1`) is inserted between the
    /// base URL and the path, so call sites pass resource paths only
    /// (e.g. `/songs/123`).
    /// Crashes with a descriptive message instead of a generic force-unwrap failure.
    static func api(path: String) -> URL {
        let full = "\(APIClient.baseURL)\(APIClient.apiVersionPath)\(path)"
        guard let url = URL(string: full) else {
            preconditionFailure("Invalid API URL: \(full)")
        }
        return url
    }
}
