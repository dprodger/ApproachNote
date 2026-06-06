//
//  RecordingGrouping.swift
//  Approach Note
//
//  Shared filtering, sorting, and grouping logic for recordings lists.
//  Used by iOS RecordingsSection and Mac SongDetailView to keep their
//  recording-grid behavior in sync.
//

import Foundation

enum RecordingGrouping {

    // MARK: - Shell ↔ hydrated field helpers
    //
    // For instrument filter / vocal filter: shell rows carry dedicated
    // top-level fields (`instrumentsPresent`, `isInstrumentalConsensus`)
    // while hydrated rows expose the same info via `performers[]` and
    // `communityData.consensus`. The helpers below pick whichever is
    // available so filters work before, during, and after hydration —
    // without the caller having to know which shape a row is in.

    /// All instrument names appearing anywhere on a recording (leader +
    /// sidemen). Prefers the shell's pre-computed flat array when
    /// present; falls back to scanning `performers[].instrument`.
    private static func instrumentNames(for recording: Recording) -> [String] {
        if let shellList = recording.instrumentsPresent {
            return shellList
        }
        return recording.performers?.compactMap(\.instrument) ?? []
    }

    /// The community-consensus "is this an instrumental track" value.
    /// Prefers the shell's flat bool when present; falls back to the
    /// hydrated `community_data.consensus.is_instrumental` path.
    private static func consensusIsInstrumental(for recording: Recording) -> Bool? {
        if let shellBool = recording.isInstrumentalConsensus {
            return shellBool
        }
        return recording.communityData?.consensus.isInstrumental
    }

    // MARK: - Available Instruments

    /// Distinct instrument families present across the given recordings,
    /// sorted by `InstrumentFamily.rawValue`.
    static func availableInstruments(in recordings: [Recording]) -> [InstrumentFamily] {
        var families = Set<InstrumentFamily>()
        for recording in recordings {
            for instrument in instrumentNames(for: recording) {
                if let family = InstrumentFamily.family(for: instrument) {
                    families.insert(family)
                }
            }
        }
        return families.sorted { $0.rawValue < $1.rawValue }
    }

    // MARK: - Filter

    /// Apply instrument / vocal / streaming filters to a recording list.
    /// Filter order is irrelevant to the resulting set, but follows
    /// instrument → vocal → streaming for readability.
    static func filter(
        _ recordings: [Recording],
        instrument: InstrumentFamily?,
        vocal: VocalFilter,
        streaming: SongRecordingFilter
    ) -> [Recording] {
        var result = recordings

        if let family = instrument {
            result = result.filter { recording in
                instrumentNames(for: recording).contains { name in
                    InstrumentFamily.family(for: name) == family
                }
            }
        }

        switch vocal {
        case .all:
            break
        case .instrumental:
            result = result.filter { consensusIsInstrumental(for: $0) == true }
        case .vocal:
            result = result.filter { consensusIsInstrumental(for: $0) == false }
        }

        switch streaming {
        case .all:
            break
        case .playable:
            result = result.filter { $0.isPlayable }
        case .withSpotify:
            result = result.filter { $0.hasSpotifyAvailable }
        case .withAppleMusic:
            result = result.filter { $0.hasAppleMusicAvailable }
        case .withYoutube:
            result = result.filter { $0.hasYoutubeAvailable }
        }

        return result
    }

    // MARK: - Group

    /// Group recordings according to the sort order.
    /// - `.year`: grouped by decade ("1960s", "1970s", …, "Unknown Year").
    /// - `.name`: every leader artist gets its own group (including artists
    ///   with a single recording), in the server's sort=name order.
    static func grouped(
        _ recordings: [Recording],
        sortOrder: RecordingSortOrder
    ) -> [(groupKey: String, recordings: [Recording])] {
        switch sortOrder {
        case .year:
            return groupByDecade(recordings)
        case .name:
            return groupByArtist(recordings)
        }
    }

    // MARK: - Decade Grouping (for Year sort)

    private static func groupByDecade(
        _ recordings: [Recording]
    ) -> [(groupKey: String, recordings: [Recording])] {
        var decadeOrder: [String] = []
        var decades: [String: [Recording]] = [:]

        for recording in recordings {
            let decadeKey: String
            if let year = recording.recordingYear {
                let decade = (year / 10) * 10
                decadeKey = "\(decade)s"
            } else {
                decadeKey = "Unknown Year"
            }

            if decades[decadeKey] == nil {
                decadeOrder.append(decadeKey)
            }
            decades[decadeKey, default: []].append(recording)
        }

        return decadeOrder.compactMap { key in
            guard let recs = decades[key] else { return nil }
            return (groupKey: key, recordings: recs)
        }
    }

    // MARK: - Artist Grouping (for Name sort)

    /// Group recordings by leader artist — one group per artist, including
    /// artists with a single recording. Groups appear in the order their
    /// first recording arrives, which follows the server's sort=name
    /// (ORDER BY leader sort-name), so the whole list reads in one
    /// consistent by-name order with no catch-all "More Recordings" bucket.
    private static func groupByArtist(
        _ recordings: [Recording]
    ) -> [(groupKey: String, recordings: [Recording])] {
        var order: [String] = []
        var groups: [String: [Recording]] = [:]

        for recording in recordings {
            let artist = recording.performers?.first { $0.role == "leader" }?.name ?? "Unknown"
            if groups[artist] == nil {
                order.append(artist)
            }
            groups[artist, default: []].append(recording)
        }

        return order.compactMap { key in
            guard let recs = groups[key] else { return nil }
            return (groupKey: key, recordings: recs)
        }
    }
}
