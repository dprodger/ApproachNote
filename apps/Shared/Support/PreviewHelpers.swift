//
//  PreviewHelpers.swift
//  Approach Note
//
//  Preview mock data for SwiftUI previews
//

import Foundation

// MARK: - Mock Data Extensions
// Note: Preview data is not wrapped in #if DEBUG because #Preview macros
// are processed even in Release builds. Dead code stripping removes unused data.

extension Song {
    static let preview = Song(
        id: "preview-song-1",
        title: "Take Five",
        composer: "Paul Desmond",
        composedYear: 1959,
        composedKey: "Ebm",
        structure: "AABA form in 5/4 time",
        songReference: "One of the most iconic jazz compositions, known for its unusual 5/4 time signature.",
        musicbrainzId: "musicbrainz id",
        wikipediaUrl: "wiki id",
        externalReferences: [
            "wikipedia": "https://en.wikipedia.org/wiki/Take_Five",
            "jazzstandards": "https://www.jazzstandards.com/compositions/takefive.htm"
        ],
        createdAt: "2024-01-15T10:30:00Z",
        updatedAt: "2024-01-15T10:30:00Z",
        recordings: [Recording.preview1, Recording.preview2],
        recordingCount: 2,
        featuredRecordings: [Recording.preview1],
        transcriptions: [SoloTranscription.preview1, SoloTranscription.preview2],
        transcriptionCount: 2,
        hasAnyStreaming: true
    )

    static let previewNoRecordings = Song(
        id: "preview-song-2",
        title: "Blue in Green",
        composer: "Miles Davis / Bill Evans",
        composedYear: 1959,
        composedKey: nil,
        structure: "10-bar form",
        songReference: "A hauntingly beautiful ballad from Kind of Blue.",
        musicbrainzId: "musicbrainz id",
        wikipediaUrl: "wiki id",
        externalReferences: nil,
        createdAt: "2024-01-15T10:30:00Z",
        updatedAt: "2024-01-15T10:30:00Z",
        recordings: [],
        recordingCount: 0,
        featuredRecordings: nil,
        transcriptions: [],
        transcriptionCount: 0,
        hasAnyStreaming: false
    )
}

extension Recording {
    static let preview1 = Recording(
        id: "preview-recording-1",
        title: "Take Five",  // Same as song title
        songId: "preview-song-1",
        songTitle: "Take Five",
        albumTitle: "Time Out",
        artistCredit: "The Dave Brubeck Quartet",
        recordingDate: "1959-07-01",
        recordingYear: 1959,
        defaultReleaseId: "blah",
        // Placeholder images for previews (picsum.photos)
        bestCoverArtSmall: "https://picsum.photos/id/145/64/64",
        bestCoverArtMedium: "https://picsum.photos/id/145/300/300",
        bestCoverArtLarge: "https://picsum.photos/id/145/640/640",
        albumArtSmall: nil,
        albumArtMedium: nil,
        albumArtLarge: nil,
        // Placeholder back cover
        backCoverArtSmall: "https://picsum.photos/id/146/64/64",
        backCoverArtMedium: "https://picsum.photos/id/146/300/300",
        backCoverArtLarge: "https://picsum.photos/id/146/640/640",
        hasBackCover: true,
        // Album art source info
        albumArtSource: nil,
        albumArtSourceUrl: nil,
        bestCoverArtSource: "MusicBrainz",
        bestCoverArtSourceUrl: "https://coverartarchive.org/release/example",
        backCoverSource: "MusicBrainz",
        backCoverSourceUrl: "https://coverartarchive.org/release/example",
        bestSpotifyUrlFromRelease: "https://open.spotify.com/track/example",
        spotifyUrl: nil,
        musicbrainzId: "mbid",
        isCanonical: true,
        notes: "The definitive recording that made this song famous worldwide.",
        performers: [Performer.preview1, Performer.preview2, Performer.preview3],
        composer: "Paul Desmond",
        releases: [Release.preview, Release.previewNoSpotify],
        transcriptions: [SoloTranscription.preview1],
        streamingLinks: [
            "spotify": StreamingLink(
                trackUrl: "https://open.spotify.com/track/example",
                albumUrl: "https://open.spotify.com/album/example",
                previewUrl: nil
            ),
            "apple_music": StreamingLink(
                trackUrl: "https://music.apple.com/album/example",
                albumUrl: nil,
                previewUrl: nil
            ),
            "youtube": StreamingLink(
                trackUrl: "https://youtube.com/watch?v=example",
                albumUrl: nil,
                previewUrl: nil
            )
        ],
        hasStreaming: true,
        hasSpotify: true,
        hasAppleMusic: true,
        hasYoutube: true,
        streamingServices: ["spotify", "apple_music", "youtube"],
        favoriteCount: 5,
        isFavorited: true,
        favoritedBy: [
            FavoriteUser(id: "user-1", displayName: "John Doe"),
            FavoriteUser(id: "user-2", displayName: "Jane Smith")
        ],
        communityData: CommunityData.preview,
        userContribution: UserContribution.preview,
        instrumentsPresent: ["piano", "alto saxophone", "bass", "drums"],
        isInstrumentalConsensus: true
    )

    static let preview2 = Recording(
        id: "preview-recording-2",
        title: "Take Five (live)",  // Different from song title - will show
        songId: "preview-song-1",
        songTitle: "Take Five",
        albumTitle: "Jazz Impressions of Japan",
        artistCredit: "The Dave Brubeck Quartet",
        recordingDate: nil,
        recordingYear: 1964,
        defaultReleaseId: "blah",
        // Placeholder images for previews (picsum.photos)
        bestCoverArtSmall: "https://picsum.photos/id/160/64/64",
        bestCoverArtMedium: "https://picsum.photos/id/160/300/300",
        bestCoverArtLarge: "https://picsum.photos/id/160/640/640",
        albumArtSmall: nil,
        albumArtMedium: nil,
        albumArtLarge: nil,
        backCoverArtSmall: nil,
        backCoverArtMedium: nil,
        backCoverArtLarge: nil,
        hasBackCover: false,
        // Album art source info
        albumArtSource: nil,
        albumArtSourceUrl: nil,
        bestCoverArtSource: "Spotify",
        bestCoverArtSourceUrl: "https://open.spotify.com/album/example",
        backCoverSource: nil,
        backCoverSourceUrl: nil,
        bestSpotifyUrlFromRelease: "https://open.spotify.com/track/4vLYewWIvqHfKtJDk8c8tq",
        spotifyUrl: nil,
        musicbrainzId: "mbid",
        isCanonical: false,
        notes: "Live recording from their Japan tour.",
        performers: [Performer.preview1, Performer.preview4],
        composer: "Paul Desmond",
        releases: nil,
        transcriptions: nil,
        streamingLinks: [
            "spotify": StreamingLink(
                trackUrl: "https://open.spotify.com/track/example",
                albumUrl: nil,
                previewUrl: nil
            ),
            "youtube": StreamingLink(
                trackUrl: "https://youtube.com/watch?v=example2",
                albumUrl: nil,
                previewUrl: nil
            )
        ],
        hasStreaming: true,
        hasSpotify: true,
        hasAppleMusic: false,
        hasYoutube: true,
        streamingServices: ["spotify", "youtube"],
        favoriteCount: 2,
        isFavorited: false,
        favoritedBy: [
            FavoriteUser(id: "user-3", displayName: "Jazz Fan")
        ],
        communityData: CommunityData(
            consensus: CommunityConsensus.previewPartial,
            counts: ContributionCounts(key: 3, tempo: 0, instrumental: 0)
        ),
        userContribution: nil,
        instrumentsPresent: ["piano", "alto saxophone"],
        isInstrumentalConsensus: nil
    )

    static let previewMinimal = Recording(
        id: "preview-recording-3",
        title: nil,  // No recording title
        songId: nil,
        songTitle: "Blue Rondo à la Turk",
        albumTitle: "Time Out",
        artistCredit: nil,
        recordingDate: nil,
        recordingYear: 1959,
        defaultReleaseId: "blah",
        bestCoverArtSmall: nil,
        bestCoverArtMedium: nil,
        bestCoverArtLarge: nil,
        albumArtSmall: nil,
        albumArtMedium: nil,
        albumArtLarge: nil,
        backCoverArtSmall: nil,
        backCoverArtMedium: nil,
        backCoverArtLarge: nil,
        hasBackCover: nil,
        // Album art source info
        albumArtSource: nil,
        albumArtSourceUrl: nil,
        bestCoverArtSource: nil,
        bestCoverArtSourceUrl: nil,
        backCoverSource: nil,
        backCoverSourceUrl: nil,
        bestSpotifyUrlFromRelease: nil,
        spotifyUrl: nil,
        musicbrainzId: "mbid",
        isCanonical: false,
        notes: nil,
        performers: nil,
        composer: nil,
        releases: nil,
        transcriptions: nil,
        streamingLinks: nil,
        hasStreaming: false,
        hasSpotify: false,
        hasAppleMusic: false,
        hasYoutube: false,
        streamingServices: nil,
        favoriteCount: 0,
        isFavorited: nil,
        favoritedBy: nil,
        communityData: nil,
        userContribution: nil,
        instrumentsPresent: nil,
        isInstrumentalConsensus: nil
    )
}

// MARK: - Community Data Previews

extension CommunityConsensus {
    static let preview = CommunityConsensus(
        performanceKey: "Eb",
        tempoMarking: "Medium-Up",
        isInstrumental: true
    )

    static let previewPartial = CommunityConsensus(
        performanceKey: "C",
        tempoMarking: nil,
        isInstrumental: nil
    )

    static let previewEmpty = CommunityConsensus(
        performanceKey: nil,
        tempoMarking: nil,
        isInstrumental: nil
    )
}

extension ContributionCounts {
    static let preview = ContributionCounts(key: 8, tempo: 5, instrumental: 12)
    static let previewEmpty = ContributionCounts(key: 0, tempo: 0, instrumental: 0)
}

extension CommunityData {
    static let preview = CommunityData(
        consensus: .preview,
        counts: .preview
    )

    static let previewEmpty = CommunityData(
        consensus: .previewEmpty,
        counts: .previewEmpty
    )
}

extension UserContribution {
    static let preview = UserContribution(
        performanceKey: "Eb",
        tempoMarking: "Medium-Up",
        isInstrumental: true,
        updatedAt: "2025-01-10T14:30:00Z"
    )

    static let previewPartial = UserContribution(
        performanceKey: "Eb",
        tempoMarking: nil,
        isInstrumental: nil,
        updatedAt: "2025-01-08T10:00:00Z"
    )
}

extension Release {
    static let preview = Release(
        id: "preview-release-1",
        title: "Time Out",
        artistCredit: "The Dave Brubeck Quartet",
        releaseDate: "1959-12-14",
        releaseYear: 1959,
        country: "US",
        label: "Columbia",
        catalogNumber: "CL 1397",
        spotifyAlbumId: "0nTTEAhCZsbbeplyDMIFuA",
        spotifyAlbumUrl: "https://open.spotify.com/album/0nTTEAhCZsbbeplyDMIFuA",
        spotifyTrackId: "1YQWosTIljIvxAgHWTp7KP",
        spotifyTrackUrl: "https://open.spotify.com/track/1YQWosTIljIvxAgHWTp7KP",
        coverArtSmall: "https://picsum.photos/id/145/64/64",
        coverArtMedium: "https://picsum.photos/id/145/300/300",
        coverArtLarge: "https://picsum.photos/id/145/640/640",
        coverArtSource: "Spotify",
        coverArtSourceUrl: "https://open.spotify.com/album/0nTTEAhCZsbbeplyDMIFuA",
        backCoverArtSmall: nil,
        backCoverArtMedium: nil,
        backCoverArtLarge: nil,
        hasBackCover: false,
        backCoverSource: nil,
        backCoverSourceUrl: nil,
        discNumber: 1,
        trackNumber: 1,
        totalTracks: 7,
        formatName: "CD",
        statusName: "official",
        musicbrainzReleaseId: "b84ee12a-09ef-421b-82de-0441a926375b",
        performers: nil,
        performerCount: 4
    )

    static let previewNoSpotify = Release(
        id: "preview-release-2",
        title: "Time Out (Original Mono)",
        artistCredit: "The Dave Brubeck Quartet",
        releaseDate: "1959-12-14",
        releaseYear: 1959,
        country: "US",
        label: "Columbia",
        catalogNumber: "CL 1397",
        spotifyAlbumId: nil,
        spotifyAlbumUrl: nil,
        spotifyTrackId: nil,
        spotifyTrackUrl: nil,
        coverArtSmall: nil,
        coverArtMedium: nil,
        coverArtLarge: nil,
        coverArtSource: nil,
        coverArtSourceUrl: nil,
        backCoverArtSmall: nil,
        backCoverArtMedium: nil,
        backCoverArtLarge: nil,
        hasBackCover: false,
        backCoverSource: nil,
        backCoverSourceUrl: nil,
        discNumber: 1,
        trackNumber: 1,
        totalTracks: 7,
        formatName: "12\" Vinyl",
        statusName: "official",
        musicbrainzReleaseId: "c94ee12a-09ef-421b-82de-0441a926375c",
        performers: nil,
        performerCount: 4
    )
}

extension Performer {
    static let preview1 = Performer(
        id: "preview-performer-1",
        name: "Dave Brubeck",
        sortName: "Brubeck, Dave",
        instrument: "Piano",
        role: "leader",
        biography: nil,
        birthDate: nil,
        deathDate: nil
    )

    static let preview2 = Performer(
        id: "preview-performer-2",
        name: "Paul Desmond",
        sortName: "Desmond, Paul",
        instrument: "Alto Saxophone",
        role: "sideman",
        biography: nil,
        birthDate: nil,
        deathDate: nil
    )

    static let preview3 = Performer(
        id: "preview-performer-3",
        name: "Joe Morello",
        sortName: "Morello, Joe",
        instrument: "Drums",
        role: "sideman",
        biography: nil,
        birthDate: nil,
        deathDate: nil
    )

    static let preview4 = Performer(
        id: "preview-performer-4",
        name: "Eugene Wright",
        sortName: "Wright, Eugene",
        instrument: "Bass",
        role: "sideman",
        biography: nil,
        birthDate: nil,
        deathDate: nil
    )
}



extension PerformerDetail {
    static let preview = PerformerDetail(
        id: "preview-performer-detail-1",
        name: "Miles Davis",
        sortName: "Davis, Miles",
        biography: "Miles Dewey Davis III was an American jazz trumpeter, bandleader, and composer. He is among the most influential and acclaimed figures in the history of jazz and 20th-century music.",
        birthDate: "1926-05-26",
        deathDate: "1991-09-28",
        externalLinks: [
            "wikipedia": "https://en.wikipedia.org/wiki/Miles_Davis"
        ],
        wikipediaUrl: "https://en.wikipedia.org/wiki/Miles_Davis",
        musicbrainzId: "mb-id",
        instruments: [
            PerformerInstrument(name: "Trumpet", isPrimary: true),
            PerformerInstrument(name: "Flugelhorn", isPrimary: false)
        ],
        recordings: [
            PerformerRecording(
                songId: "song-1",
                songTitle: "So What",
                recordingId: "rec-1",
                albumTitle: "Kind of Blue",
                artistCredit: "Miles Davis",
                recordingYear: 1959,
                isCanonical: true,
                role: "leader",
                // Placeholder images for previews
                bestCoverArtSmall: "https://picsum.photos/id/250/64/64",
                bestCoverArtMedium: "https://picsum.photos/id/250/300/300"
            ),
            PerformerRecording(
                songId: "song-2",
                songTitle: "All Blues",
                recordingId: "rec-2",
                albumTitle: "Kind of Blue",
                artistCredit: "Miles Davis",
                recordingYear: 1959,
                isCanonical: true,
                role: "leader",
                // Placeholder images for previews
                bestCoverArtSmall: "https://picsum.photos/id/251/64/64",
                bestCoverArtMedium: "https://picsum.photos/id/251/300/300"
            ),
            PerformerRecording(
                songId: "song-3",
                songTitle: "Round Midnight",
                recordingId: "rec-3",
                albumTitle: "Round About Midnight",
                artistCredit: "Miles Davis",
                recordingYear: 1957,
                isCanonical: false,
                role: "leader",
                bestCoverArtSmall: nil,
                bestCoverArtMedium: nil
            )
        ],
        images: [
            ArtistImage(
                id: "img-1",
                url: "https://picsum.photos/id/453/900/600",
                source: "wikimedia",
                sourceIdentifier: "Miles_Davis_by_Palumbo.jpg",
                licenseType: "cc-by-sa",
                licenseUrl: "https://creativecommons.org/licenses/by-sa/2.0",
                attribution: "Tom Palumbo, CC BY-SA 2.0",
                width: 900,
                height: 600,
                thumbnailUrl: "https://picsum.photos/id/453/450/300",
                sourcePageUrl: "https://commons.wikimedia.org/wiki/File:Miles_Davis_by_Palumbo.jpg"
            ),
            ArtistImage(
                id: "img-2",
                url: "https://picsum.photos/id/454/600/800",
                source: "wikimedia",
                sourceIdentifier: "Miles_Davis_portrait.jpg",
                licenseType: "public-domain",
                licenseUrl: nil,
                attribution: "Public Domain",
                width: 600,
                height: 800,
                thumbnailUrl: "https://picsum.photos/id/454/300/400",
                sourcePageUrl: "https://commons.wikimedia.org/wiki/File:Miles_Davis_portrait.jpg"
            )
        ],
        recordingCount: 3
    )

    static let previewMinimal = PerformerDetail(
        id: "preview-performer-detail-2",
        name: "John Coltrane",
        sortName: "Coltrane, John",
        biography: nil,
        birthDate: nil,
        deathDate: nil,
        externalLinks: nil,
        wikipediaUrl: "wikipedia.org/john_coltrane",
        musicbrainzId: "mb-id",
        instruments: [
            PerformerInstrument(name: "Tenor Saxophone", isPrimary: true)
        ],
        recordings: [],
        images: nil,
        recordingCount: 0
    )
}

// MARK: - Preview Mode Support

extension NetworkManager {
    static var isPreviewMode: Bool {
        APIClient.isPreviewMode
    }

    static var preview: NetworkManager {
        let manager = NetworkManager()
        return manager
    }
}
