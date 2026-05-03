import Foundation
import SwiftUI

// MARK: - Recording Model

struct Recording: Codable, Identifiable {
    let id: String
    let title: String?
    let songId: String?
    let songTitle: String?
    let albumTitle: String?
    let artistCredit: String?
    let recordingDate: String?
    let recordingYear: Int?
    let label: String?

    let defaultReleaseId: String?

    // Best cover art from releases (provided by API via subqueries)
    let bestCoverArtSmall: String?
    let bestCoverArtMedium: String?
    let bestCoverArtLarge: String?

    // Album art (from /recordings search endpoint)
    let albumArtSmall: String?
    let albumArtMedium: String?
    let albumArtLarge: String?

    // Back cover art (from Cover Art Archive via release_imagery)
    let backCoverArtSmall: String?
    let backCoverArtMedium: String?
    let backCoverArtLarge: String?
    let hasBackCover: Bool?

    // Album art source info (for watermark/attribution)
    let albumArtSource: String?
    let albumArtSourceUrl: String?
    let bestCoverArtSource: String?
    let bestCoverArtSourceUrl: String?
    let backCoverSource: String?
    let backCoverSourceUrl: String?

    // Best Spotify URL from releases (provided by API via subqueries)
    let bestSpotifyUrlFromRelease: String?

    // Direct Spotify URL (from /recordings search endpoint)
    let spotifyUrl: String?

    let musicbrainzId: String?
    let isCanonical: Bool?
    let notes: String?
    let performers: [Performer]?
    let composer: String?

    var authorityCount: Int?
    var authoritySources: [String]?
    var authorityRecommendations: [AuthorityRecommendation]?

    // Releases this recording appears on (only populated on recording detail)
    let releases: [Release]?

    // Transcriptions for this recording (only populated on recording detail)
    let transcriptions: [SoloTranscription]?

    // Streaming links by service (spotify, apple_music, youtube)
    let streamingLinks: [String: StreamingLink]?

    // Whether this recording has any streaming links available
    let hasStreaming: Bool?

    // Per-service availability flags (for filtering)
    let hasSpotify: Bool?
    let hasAppleMusic: Bool?
    let hasYoutube: Bool?

    // List of streaming services available for this recording
    let streamingServices: [String]?

    // MARK: - Favorites
    let favoriteCount: Int?
    let isFavorited: Bool?
    let favoritedBy: [FavoriteUser]?

    // MARK: - Community-Contributed Metadata
    let communityData: CommunityData?
    let userContribution: UserContribution?

    // MARK: - Shell-only fields
    //
    // Present on rows returned by GET /api/songs/<id>/recordings/shell
    // (first-paint payload for the song recordings list). They're flat
    // conveniences derived from data that is also reachable through
    // `performers[]` / `communityData` on hydrated rows — so row-level
    // render code never needs them, but RecordingGrouping's filters use
    // them to keep working on shell rows that haven't been hydrated yet
    // (no performers sidemen, no communityData jsonb yet).
    //
    // When a row is hydrated by POST /api/recordings/batch, the hydrated
    // response does NOT include these fields (the batch returns the same
    // shape as the list endpoint). That's intentional: the hydrated row
    // carries the full `performers[]` + `communityData` that the filter
    // can derive from directly, so the shell fields stop mattering for
    // that row. The fallback logic in RecordingGrouping handles both.
    let instrumentsPresent: [String]?
    let isInstrumentalConsensus: Bool?

    enum CodingKeys: String, CodingKey {
        case id, title, label, notes, composer, performers, releases, transcriptions
        case songId = "song_id"
        case songTitle = "song_title"
        case albumTitle = "album_title"
        case artistCredit = "artist_credit"
        case recordingDate = "recording_date"
        case recordingYear = "recording_year"
        case defaultReleaseId = "default_release_id"
        case bestCoverArtSmall = "best_cover_art_small"
        case bestCoverArtMedium = "best_cover_art_medium"
        case bestCoverArtLarge = "best_cover_art_large"
        case albumArtSmall = "album_art_small"
        case albumArtMedium = "album_art_medium"
        case albumArtLarge = "album_art_large"
        case backCoverArtSmall = "back_cover_art_small"
        case backCoverArtMedium = "back_cover_art_medium"
        case backCoverArtLarge = "back_cover_art_large"
        case hasBackCover = "has_back_cover"
        case albumArtSource = "album_art_source"
        case albumArtSourceUrl = "album_art_source_url"
        case bestCoverArtSource = "best_cover_art_source"
        case bestCoverArtSourceUrl = "best_cover_art_source_url"
        case backCoverSource = "back_cover_source"
        case backCoverSourceUrl = "back_cover_source_url"
        case bestSpotifyUrlFromRelease = "best_spotify_url"
        case spotifyUrl = "spotify_url"
        case isCanonical = "is_canonical"
        case musicbrainzId = "musicbrainz_id"
        case authorityCount = "authority_count"
        case authoritySources = "authority_sources"
        case authorityRecommendations = "authority_recommendations"
        case streamingLinks = "streaming_links"
        case hasStreaming = "has_streaming"
        case hasSpotify = "has_spotify"
        case hasAppleMusic = "has_apple_music"
        case hasYoutube = "has_youtube"
        case streamingServices = "streaming_services"
        case favoriteCount = "favorite_count"
        case isFavorited = "is_favorited"
        case favoritedBy = "favorited_by"
        case communityData = "community_data"
        case userContribution = "user_contribution"
        case instrumentsPresent = "instruments_present"
        case isInstrumentalConsensus = "is_instrumental"
    }

    // MARK: - Helper computed properties

    var displayTitle: String? {
        guard let recordingTitle = title else { return nil }
        guard let songTitle = songTitle else { return recordingTitle }
        if recordingTitle.lowercased() == songTitle.lowercased() {
            return nil
        }
        return recordingTitle
    }

    var hasDistinctTitle: Bool {
        displayTitle != nil
    }

    var hasAuthority: Bool {
        guard let count = authorityCount else { return false }
        return count > 0
    }

    var authorityBadgeText: String? {
        guard let count = authorityCount, count > 0 else { return nil }
        if count == 1, let source = authoritySources?.first {
            return authoritySourceDisplayName(source)
        }
        return "\(count) sources"
    }

    var primaryAuthoritySource: String? {
        authoritySources?.first
    }

    private func authoritySourceDisplayName(_ source: String) -> String {
        switch source.lowercased() {
        case "jazzstandards.com":
            return "JazzStandards.com"
        case "allmusic":
            return "AllMusic"
        case "discogs":
            return "Discogs"
        default:
            return source.capitalized
        }
    }

    // MARK: - Release-aware computed properties

    private var sortedReleases: [Release]? {
        releases?.sorted { r1, r2 in
            let r1HasSpotify = r1.spotifyAlbumId != nil
            let r2HasSpotify = r2.spotifyAlbumId != nil
            if r1HasSpotify != r2HasSpotify {
                return r1HasSpotify && !r2HasSpotify
            }
            switch (r1.releaseYear, r2.releaseYear) {
            case (nil, nil): return false
            case (nil, _): return false
            case (_, nil): return true
            case let (y1?, y2?): return y1 > y2
            }
        }
    }

    var bestSpotifyUrl: String? {
        if let directUrl = spotifyUrl {
            return directUrl
        }
        if let apiUrl = bestSpotifyUrlFromRelease {
            return apiUrl
        }
        if let release = sortedReleases?.first(where: { $0.spotifyTrackUrl != nil }) {
            return release.spotifyTrackUrl
        }
        return nil
    }

    var bestAlbumArtSmall: String? {
        if let directArt = albumArtSmall { return directArt }
        if let apiArt = bestCoverArtSmall { return apiArt }
        if let release = sortedReleases?.first(where: { $0.spotifyAlbumId != nil && $0.coverArtSmall != nil }) {
            return release.coverArtSmall
        }
        return nil
    }

    var bestAlbumArtMedium: String? {
        if let directArt = albumArtMedium { return directArt }
        if let apiArt = bestCoverArtMedium { return apiArt }
        if let release = sortedReleases?.first(where: { $0.spotifyAlbumId != nil && $0.coverArtMedium != nil }) {
            return release.coverArtMedium
        }
        return nil
    }

    var bestAlbumArtLarge: String? {
        if let directArt = albumArtLarge { return directArt }
        if let apiArt = bestCoverArtLarge { return apiArt }
        if let release = sortedReleases?.first(where: { $0.spotifyAlbumId != nil && $0.coverArtLarge != nil }) {
            return release.coverArtLarge
        }
        return nil
    }

    var displayAlbumArtSource: String? {
        albumArtSource ?? bestCoverArtSource ?? sortedReleases?.first(where: { $0.coverArtSource != nil })?.coverArtSource
    }

    var displayAlbumArtSourceUrl: String? {
        albumArtSourceUrl ?? bestCoverArtSourceUrl ?? sortedReleases?.first(where: { $0.coverArtSourceUrl != nil })?.coverArtSourceUrl
    }

    var displayBackCoverSource: String? { backCoverSource }
    var displayBackCoverSourceUrl: String? { backCoverSourceUrl }

    var canFlipToBackCover: Bool {
        hasBackCover == true && backCoverArtMedium != nil
    }

    var spotifyReleaseCount: Int {
        releases?.filter { $0.hasSpotify }.count ?? 0
    }

    var hasReleases: Bool {
        guard let releases = releases else { return false }
        return !releases.isEmpty
    }

    // MARK: - Streaming Link Helpers

    var hasAnyStreamingLink: Bool {
        if let hasStreaming = hasStreaming { return hasStreaming }
        guard let links = streamingLinks else { return false }
        return !links.isEmpty
    }

    func streamingLink(for service: String) -> StreamingLink? {
        streamingLinks?[service]
    }

    func playbackUrl(preferring preferredService: String) -> (service: String, url: String)? {
        if let link = streamingLinks?[preferredService], let url = link.bestPlaybackUrl {
            return (preferredService, url)
        }
        if let links = streamingLinks {
            for (service, link) in links {
                if let url = link.bestPlaybackUrl {
                    return (service, url)
                }
            }
        }
        if let url = bestSpotifyUrl {
            return ("spotify", url)
        }
        return nil
    }

    var availableStreamingServices: [String] {
        if let services = streamingServices { return services }
        var services: [String] = []
        if let links = streamingLinks {
            services = Array(links.keys).sorted()
        }
        if bestSpotifyUrl != nil && !services.contains("spotify") {
            services.append("spotify")
        }
        return services
    }

    // MARK: - Filter Helpers

    var isPlayable: Bool {
        hasSpotifyAvailable || hasAppleMusicAvailable || hasYoutubeAvailable
    }

    var hasSpotifyAvailable: Bool {
        if hasSpotify == true { return true }
        if bestSpotifyUrl != nil { return true }
        if streamingLinks?["spotify"] != nil { return true }
        return false
    }

    var hasAppleMusicAvailable: Bool {
        if hasAppleMusic == true { return true }
        if streamingLinks?["apple_music"] != nil { return true }
        return false
    }

    var hasYoutubeAvailable: Bool {
        if hasYoutube == true { return true }
        if streamingLinks?["youtube"] != nil { return true }
        return false
    }
}

// MARK: - Streaming Link Model

struct StreamingLink: Codable {
    let trackUrl: String?
    let albumUrl: String?
    let previewUrl: String?

    enum CodingKeys: String, CodingKey {
        case trackUrl = "track_url"
        case albumUrl = "album_url"
        case previewUrl = "preview_url"
    }

    var bestPlaybackUrl: String? {
        trackUrl ?? albumUrl
    }
}

// MARK: - Favorite User Model

struct FavoriteUser: Codable, Identifiable {
    let id: String
    let displayName: String?

    enum CodingKeys: String, CodingKey {
        case id
        case displayName = "display_name"
    }
}

// MARK: - Streaming Service Enum

enum StreamingService: String, CaseIterable, Identifiable {
    case spotify = "spotify"
    case appleMusic = "apple_music"
    case youtube = "youtube"

    var id: String { rawValue }

    var displayName: String {
        switch self {
        case .spotify: return "Spotify"
        case .appleMusic: return "Apple Music"
        case .youtube: return "YouTube"
        }
    }

    var iconName: String {
        switch self {
        case .spotify: return "music.note.list"
        case .appleMusic: return "music.note"
        case .youtube: return "play.rectangle.fill"
        }
    }

    /// Asset-catalog image name for the official brand mark, when the service
    /// requires one for attribution (Spotify, YouTube). Apple Music falls back
    /// to its SF Symbol.
    var brandAssetName: String? {
        switch self {
        case .spotify: return "SpotifyIcon"
        case .youtube: return "YouTubeIcon"
        case .appleMusic: return nil
        }
    }

    var brandColor: Color {
        switch self {
        case .spotify: return Color(red: 30/255, green: 215/255, blue: 96/255)
        case .appleMusic: return Color(red: 252/255, green: 60/255, blue: 68/255)
        case .youtube: return Color(red: 255/255, green: 0/255, blue: 0/255)
        }
    }

    var urlScheme: String? {
        switch self {
        case .spotify: return "spotify"
        case .appleMusic: return "music"
        case .youtube: return "youtube"
        }
    }

    init?(key: String) {
        self.init(rawValue: key)
    }
}
