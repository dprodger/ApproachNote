//
//  RecordingFilters.swift
//  Approach Note
//
//  Shared filter enums used by both iOS and Mac recording views
//

import SwiftUI

// MARK: - Song Recording Filter Enum
enum SongRecordingFilter: String, CaseIterable, Identifiable {
    case all = "All"
    case playable = "Playable"
    case withSpotify = "Spotify"
    case withAppleMusic = "Apple Music"
    case withYoutube = "YouTube"

    var id: String { rawValue }

    var displayName: String { rawValue }

    var subtitle: String {
        switch self {
        case .all: return "Show all recordings"
        case .playable: return "Any streaming service available"
        case .withSpotify: return "Recordings available on Spotify"
        case .withAppleMusic: return "Recordings available on Apple Music"
        case .withYoutube: return "Recordings available on YouTube"
        }
    }

    var icon: String {
        switch self {
        case .all: return "music.note.list"
        case .playable: return "play.circle"
        case .withSpotify: return "play.circle.fill"
        case .withAppleMusic: return "play.circle.fill"
        case .withYoutube: return "play.rectangle.fill"
        }
    }

    var iconColor: Color {
        switch self {
        case .all: return ApproachNoteTheme.textSecondary
        case .playable: return ApproachNoteTheme.brand
        case .withSpotify: return StreamingService.spotify.brandColor
        case .withAppleMusic: return StreamingService.appleMusic.brandColor
        case .withYoutube: return StreamingService.youtube.brandColor
        }
    }
}

// MARK: - Vocal Filter Enum
enum VocalFilter: String, CaseIterable, Identifiable {
    case all = "All"
    case instrumental = "Instrumental"
    case vocal = "Vocal"

    var id: String { rawValue }

    var displayName: String { rawValue }

    var subtitle: String {
        switch self {
        case .all: return "Show all recordings"
        case .instrumental: return "Instrumental performances only"
        case .vocal: return "Vocal performances only"
        }
    }

    var icon: String {
        switch self {
        case .all: return "music.note.list"
        case .instrumental: return "pianokeys"
        case .vocal: return "mic"
        }
    }

    var iconColor: Color {
        switch self {
        case .all: return ApproachNoteTheme.textSecondary
        case .instrumental: return ApproachNoteTheme.textSecondary
        case .vocal: return ApproachNoteTheme.brand
        }
    }
}

// MARK: - Instrument Family Enum
enum InstrumentFamily: String, CaseIterable, Hashable, Identifiable {
    case guitar = "Guitar"
    case saxophone = "Saxophone"
    case trumpet = "Trumpet"
    case trombone = "Trombone"
    case piano = "Piano"
    case organ = "Organ"
    case bass = "Bass"
    case drums = "Drums"
    case clarinet = "Clarinet"
    case flute = "Flute"
    case vibraphone = "Vibraphone"
    case vocals = "Vocals"

    var id: String { rawValue }

    // Map specific instruments to their family
    static func family(for instrument: String) -> InstrumentFamily? {
        let normalized = instrument.lowercased()

        if normalized.contains("guitar") { return .guitar }
        if normalized.contains("sax") { return .saxophone }
        if normalized.contains("trumpet") || normalized.contains("flugelhorn") { return .trumpet }
        if normalized.contains("trombone") { return .trombone }
        if normalized.contains("piano") && !normalized.contains("organ") { return .piano }
        if normalized.contains("organ") { return .organ }
        if normalized.contains("bass") && !normalized.contains("brass") { return .bass }
        if normalized.contains("drum") || normalized == "percussion" { return .drums }
        if normalized.contains("clarinet") { return .clarinet }
        if normalized.contains("flute") { return .flute }
        if normalized.contains("vibraphone") || normalized.contains("vibes") { return .vibraphone }
        if normalized.contains("vocal") || normalized.contains("voice") || normalized.contains("singer") { return .vocals }

        return nil
    }

    var icon: String {
        switch self {
        case .guitar: return "guitars"
        case .saxophone: return "music.note"
        case .trumpet: return "music.note"
        case .trombone: return "music.note"
        case .piano: return "pianokeys"
        case .organ: return "pianokeys"
        case .bass: return "music.note"
        case .drums: return "drum"
        case .clarinet: return "music.note"
        case .flute: return "music.note"
        case .vibraphone: return "music.note"
        case .vocals: return "mic"
        }
    }
}
