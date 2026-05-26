import Foundation
import SwiftUI

struct AuthorityRecommendationsResponse: Codable {
    let songId: String
    let recommendations: [AuthorityRecommendation]
    let totalCount: Int
    let matchedCount: Int
    let unmatchedCount: Int

    enum CodingKeys: String, CodingKey {
        case songId = "song_id"
        case recommendations
        case totalCount = "total_count"
        case matchedCount = "matched_count"
        case unmatchedCount = "unmatched_count"
    }
}

struct AuthorityRecommendation: Codable, Identifiable {
    let id: String
    let source: String?
    let recommendationText: String?
    let sourceUrl: String?
    let artistName: String?
    let albumTitle: String?
    let recordingYear: Int?
    let itunesAlbumId: Int?
    let itunesTrackId: Int?
    let recordingId: String?

    // Optional fields when matched to a recording
    let matchedAlbumTitle: String?
    let matchedYear: Int?
    let matchedSpotifyUrl: String?
    let matchedAlbumArt: String?

    // Fields from recording detail endpoint (uses different keys)
    let sourceName: String?
    let recAlbumTitle: String?

    enum CodingKeys: String, CodingKey {
        case id, source
        case recommendationText = "recommendation_text"
        case sourceUrl = "source_url"
        case artistName = "artist_name"
        case albumTitle = "album_title"
        case recordingYear = "recording_year"
        case itunesAlbumId = "itunes_album_id"
        case itunesTrackId = "itunes_track_id"
        case recordingId = "recording_id"
        case matchedAlbumTitle = "matched_album_title"
        case matchedYear = "matched_year"
        case matchedSpotifyUrl = "matched_spotify_url"
        case matchedAlbumArt = "matched_album_art"
        case sourceName = "source_name"
        case recAlbumTitle = "rec_album_title"
    }

    var isMatched: Bool {
        recordingId != nil
    }

    var displayName: String {
        artistName ?? "Unknown Artist"
    }

    var displayAlbum: String {
        matchedAlbumTitle ?? albumTitle ?? recAlbumTitle ?? "Unknown Album"
    }

    var displayYear: String? {
        if let year = matchedYear ?? recordingYear {
            return "\(year)"
        }
        return nil
    }

    var sourceDisplayName: String {
        let src = source ?? sourceName ?? ""
        switch src.lowercased() {
        case "jazzstandards.com":
            return "JazzStandards.com"
        case "allmusic":
            return "AllMusic"
        case "discogs":
            return "Discogs"
        default:
            return src.capitalized
        }
    }

    var sourceColor: Color {
        let src = source ?? sourceName ?? ""
        switch src.lowercased() {
        case "jazzstandards.com", "jazzstandards":
            return .blue
        case "ted_gioia":
            return .purple
        case "allmusic":
            return .orange
        default:
            return ApproachNoteTheme.textSecondary
        }
    }
}

struct AuthoritiesResponse: Codable {
    let recordingId: String
    let songId: String
    let authorities: [AuthorityRecommendation]
    let count: Int

    enum CodingKeys: String, CodingKey {
        case recordingId = "recording_id"
        case songId = "song_id"
        case authorities
        case count
    }
}
