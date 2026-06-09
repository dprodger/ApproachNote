//
//  ArtistImageCredit.swift
//  Approach Note
//
//  Shared license/attribution credit line shown beneath the artist image
//  carousel on iOS and Mac. Lives in Shared so both platforms format the
//  credit identically.
//

import SwiftUI

// MARK: - Image Credit Line

/// One-line license/attribution credit shown beneath the carousel, e.g.
/// "Photo: Tom Palumbo · CC BY-SA". Reflects whichever image is on screen;
/// the host passes the currently-centered image. Renders nothing when the
/// image carries no usable attribution or license.
struct ArtistImageCreditLine: View {
    let image: ArtistImage

    var body: some View {
        if let credit = Self.creditText(for: image) {
            Text(credit)
                .font(ApproachNoteTheme.caption2())
                .foregroundColor(ApproachNoteTheme.textSecondary)
        }
    }

    /// Builds "Photo: <author> · <license>" from the image metadata. Falls back
    /// to author-only or license-only when one part is missing; nil when both are.
    nonisolated static func creditText(for image: ArtistImage) -> String? {
        let author = authorName(from: image.attribution)
        let license = image.licenseType.map(shortLicense)
        switch (author, license) {
        case let (author?, license?): return "Photo: \(author) · \(license)"
        case let (author?, nil): return "Photo: \(author)"
        case let (nil, license?): return license
        default: return nil
        }
    }

    /// Author name from the attribution string, with HTML stripped and any
    /// trailing license clause (e.g. ", CC BY-SA 2.0") removed so it isn't
    /// duplicated alongside the friendly license label.
    private nonisolated static func authorName(from attribution: String?) -> String? {
        guard let raw = attribution else { return nil }
        let clean = raw
            .replacingOccurrences(of: "<[^>]+>", with: "", options: .regularExpression)
            .trimmingCharacters(in: .whitespacesAndNewlines)
        guard !clean.isEmpty else { return nil }

        let parts = clean.components(separatedBy: ",")
        if parts.count > 1, let last = parts.last, isLicenseClause(last) {
            let author = parts.dropLast().joined(separator: ",")
                .trimmingCharacters(in: .whitespaces)
            return author.isEmpty ? nil : author
        }
        return clean
    }

    /// Whether a comma-separated fragment looks like a license rather than a name.
    private nonisolated static func isLicenseClause(_ fragment: String) -> Bool {
        let lowered = fragment.lowercased()
        let markers = ["cc ", "cc-", "cc0", "cc by", "by-sa", "by sa",
                       "public domain", "creative commons", "gfdl", "license"]
        return markers.contains { lowered.contains($0) }
    }

    /// Short, human-friendly license label suited to a single caption line.
    private nonisolated static func shortLicense(_ license: String) -> String {
        switch license.lowercased() {
        case "cc-by-sa": return "CC BY-SA"
        case "cc-by": return "CC BY"
        case "cc-by-nc": return "CC BY-NC"
        case "cc-by-nd": return "CC BY-ND"
        case "cc0": return "CC0"
        case "public-domain", "pd": return "Public Domain"
        case "fair-use": return "Fair Use"
        default: return license.capitalized
        }
    }
}
