//
//  RecordingsListView.swift
//  Approach Note
//
//  macOS-specific recordings list view with master-detail layout and advanced filtering
//

import SwiftUI

// MARK: - Search Scope

enum RecordingSearchScope: String, CaseIterable, Identifiable {
    case all = "All"
    case artist = "Artist"
    case album = "Album"
    case song = "Song"

    var id: String { rawValue }
}

// MARK: - Availability Filter

enum RecordingAvailabilityFilter: String, CaseIterable, Identifiable {
    case all = "All"
    case playable = "Playable"
    case spotify = "Spotify"
    case appleMusic = "Apple Music"
    case youtube = "YouTube"

    var id: String { rawValue }

    var icon: String {
        switch self {
        case .all: return "music.note.list"
        case .playable: return "play.circle"
        case .spotify: return "play.circle.fill"
        case .appleMusic: return "play.circle.fill"
        case .youtube: return "play.rectangle.fill"
        }
    }

    var iconColor: Color {
        switch self {
        case .all: return ApproachNoteTheme.textSecondary
        case .playable: return ApproachNoteTheme.brand
        case .spotify: return .green
        case .appleMusic: return .pink
        case .youtube: return .red
        }
    }
}

// MARK: - Vocal/Instrumental Filter

enum RecordingVocalFilter: String, CaseIterable, Identifiable {
    case all = "All"
    case instrumental = "Instrumental"
    case vocal = "Vocal"

    var id: String { rawValue }

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

// MARK: - Main View

struct RecordingsListView: View {
    @StateObject private var recordingService = RecordingService()
    @State private var searchText = ""
    @State private var searchTask: Task<Void, Never>?
    @State private var selectedRecordingId: String?

    // Filter state
    @State private var searchScope: RecordingSearchScope = .all
    @State private var availabilityFilter: RecordingAvailabilityFilter = .all
    @State private var vocalFilter: RecordingVocalFilter = .all

    /// Normalize search text by converting straight apostrophes to smart apostrophes
    private var normalizedSearchText: String {
        searchText.replacingOccurrences(of: "'", with: "\u{2019}")
    }

    // Filtered recordings based on scope and availability
    private var filteredRecordings: [Recording] {
        var results = recordingService.recordings

        // Apply search scope filter (client-side refinement)
        // Use normalized search text to match smart apostrophes in data
        if !searchText.isEmpty && searchScope != .all {
            let normalizedSearch = normalizedSearchText.lowercased()
            switch searchScope {
            case .all:
                break
            case .artist:
                results = results.filter { recording in
                    let artistNames = recording.performers?.map { $0.name.lowercased() } ?? []
                    return artistNames.contains { $0.contains(normalizedSearch) }
                }
            case .album:
                results = results.filter { recording in
                    recording.albumTitle?.lowercased().contains(normalizedSearch) ?? false
                }
            case .song:
                results = results.filter { recording in
                    recording.songTitle?.lowercased().contains(normalizedSearch) ?? false
                }
            }
        }

        // Apply availability filter
        switch availabilityFilter {
        case .all:
            break
        case .playable:
            results = results.filter { $0.isPlayable }
        case .spotify:
            results = results.filter { $0.hasSpotifyAvailable }
        case .appleMusic:
            results = results.filter { $0.hasAppleMusicAvailable }
        case .youtube:
            results = results.filter { $0.hasYoutubeAvailable }
        }

        // Apply vocal/instrumental filter
        switch vocalFilter {
        case .all:
            break
        case .instrumental:
            results = results.filter { recording in
                recording.communityData?.consensus.isInstrumental == true
            }
        case .vocal:
            results = results.filter { recording in
                recording.communityData?.consensus.isInstrumental == false
            }
        }

        return results
    }

    private var hasActiveFilters: Bool {
        searchScope != .all || availabilityFilter != .all || vocalFilter != .all
    }

    var body: some View {
        HSplitView {
            // Recording list (left pane)
            VStack(spacing: 0) {
                // Filter toolbar
                filterToolbar

                // Results count when filtered
                if !recordingService.recordings.isEmpty && hasActiveFilters {
                    HStack {
                        Text("\(filteredRecordings.count) of \(recordingService.recordings.count) recordings")
                            .font(ApproachNoteTheme.caption())
                            .foregroundColor(ApproachNoteTheme.textSecondary)
                        Spacer()
                        Button("Clear Filters") {
                            clearFilters()
                        }
                        .font(ApproachNoteTheme.caption())
                        .foregroundColor(ApproachNoteTheme.brand)
                        .buttonStyle(.plain)
                    }
                    .padding(.horizontal)
                    .padding(.vertical, 8)
                    .background(ApproachNoteTheme.surface)
                }

                // No results message
                if !searchText.isEmpty && filteredRecordings.isEmpty && !recordingService.isLoading {
                    VStack(spacing: 12) {
                        Image(systemName: "magnifyingglass")
                            .font(.system(size: 32))
                            .foregroundColor(ApproachNoteTheme.textSecondary.opacity(0.5))
                        Text("No recordings match your filters")
                            .font(ApproachNoteTheme.subheadline())
                            .foregroundColor(ApproachNoteTheme.textSecondary)
                        if hasActiveFilters {
                            Button("Clear Filters") {
                                clearFilters()
                            }
                            .buttonStyle(.link)
                        }
                    }
                    .frame(maxWidth: .infinity, maxHeight: .infinity)
                    .background(ApproachNoteTheme.background)
                } else {
                    ScrollView {
                        LazyVStack(spacing: 0) {
                            ForEach(filteredRecordings) { recording in
                                RecordingRowView(recording: recording)
                                    .padding(.horizontal, 12)
                                    .padding(.vertical, 6)
                                    .background(selectedRecordingId == recording.id ? ApproachNoteTheme.brand.opacity(0.15) : Color.clear)
                                    .contentShape(Rectangle())
                                    .onTapGesture {
                                        selectedRecordingId = recording.id
                                    }
                                Divider()
                                    .padding(.leading, 74)
                            }
                        }
                    }
                    .background(Color.white)
                }
            }
            .frame(minWidth: 200, idealWidth: 250, maxWidth: 300)
            .background(ApproachNoteTheme.background)
            .environment(\.colorScheme, .light)

            // Recording detail (right pane)
            if let recordingId = selectedRecordingId {
                RecordingDetailView(recordingId: recordingId)
                    .frame(minWidth: 400)
            } else {
                VStack {
                    Image(systemName: "opticaldisc")
                        .font(.system(size: 60))
                        .foregroundColor(ApproachNoteTheme.textSecondary.opacity(0.5))
                    Text("Select a recording")
                        .font(ApproachNoteTheme.title2())
                        .foregroundColor(ApproachNoteTheme.textSecondary)
                }
                .frame(maxWidth: .infinity, maxHeight: .infinity)
                .background(ApproachNoteTheme.background)
            }
        }
        .onChange(of: searchText) { _, newValue in
            searchTask?.cancel()
            searchTask = Task {
                try? await Task.sleep(nanoseconds: 300_000_000)
                if !Task.isCancelled {
                    await recordingService.fetchRecordings(searchQuery: newValue)
                }
            }
        }
    }

    // MARK: - Filter Toolbar

    @ViewBuilder
    private var filterToolbar: some View {
        VStack(spacing: 8) {
            MacSearchBar(
                text: $searchText,
                placeholder: "Search recordings...",
                backgroundColor: ApproachNoteTheme.textSecondary
            )

            // Availability row
            HStack(spacing: 12) {
                Text("Availability:")
                    .font(ApproachNoteTheme.subheadline())
                    .foregroundColor(.white)

                // Availability filter menu
                Menu {
                    ForEach(RecordingAvailabilityFilter.allCases) { filter in
                        Button(action: { availabilityFilter = filter }) {
                            HStack {
                                Image(systemName: filter.icon)
                                    .foregroundColor(filter.iconColor)
                                Text(filter.rawValue)
                                if availabilityFilter == filter {
                                    Spacer()
                                    Image(systemName: "checkmark")
                                }
                            }
                        }
                    }
                } label: {
                    HStack(spacing: 4) {
                        Image(systemName: availabilityFilter.icon)
                            .foregroundColor(availabilityFilter == .all ? ApproachNoteTheme.textPrimary : availabilityFilter.iconColor)
                        Text(availabilityFilter.rawValue)
                            .font(ApproachNoteTheme.subheadline())
                            .foregroundColor(ApproachNoteTheme.textPrimary)
                        Image(systemName: "chevron.down")
                            .font(ApproachNoteTheme.caption2())
                            .foregroundColor(ApproachNoteTheme.textPrimary)
                    }
                    .padding(.horizontal, 10)
                    .padding(.vertical, 6)
                    .background(Color.white)
                    .cornerRadius(8)
                    .overlay(
                        RoundedRectangle(cornerRadius: 8)
                            .stroke(ApproachNoteTheme.textSecondary.opacity(0.3), lineWidth: 1)
                    )
                }
                .menuStyle(.borderlessButton)

                // Clear button when availability filter is active
                if availabilityFilter != .all {
                    Button(action: { availabilityFilter = .all }) {
                        Image(systemName: "xmark.circle.fill")
                            .foregroundColor(.white)
                            .font(.system(size: 16))
                    }
                    .buttonStyle(.plain)
                    .help("Clear availability filter")
                }

                Spacer()

                // Vocal/Instrumental filter
                Text("Type:")
                    .font(ApproachNoteTheme.subheadline())
                    .foregroundColor(.white)

                Menu {
                    ForEach(RecordingVocalFilter.allCases) { filter in
                        Button(action: { vocalFilter = filter }) {
                            HStack {
                                Image(systemName: filter.icon)
                                    .foregroundColor(filter.iconColor)
                                Text(filter.rawValue)
                                if vocalFilter == filter {
                                    Spacer()
                                    Image(systemName: "checkmark")
                                }
                            }
                        }
                    }
                } label: {
                    HStack(spacing: 4) {
                        Image(systemName: vocalFilter.icon)
                            .foregroundColor(vocalFilter == .all ? ApproachNoteTheme.textPrimary : vocalFilter.iconColor)
                        Text(vocalFilter.rawValue)
                            .font(ApproachNoteTheme.subheadline())
                            .foregroundColor(ApproachNoteTheme.textPrimary)
                        Image(systemName: "chevron.down")
                            .font(ApproachNoteTheme.caption2())
                            .foregroundColor(ApproachNoteTheme.textPrimary)
                    }
                    .padding(.horizontal, 10)
                    .padding(.vertical, 6)
                    .background(Color.white)
                    .cornerRadius(8)
                    .overlay(
                        RoundedRectangle(cornerRadius: 8)
                            .stroke(ApproachNoteTheme.textSecondary.opacity(0.3), lineWidth: 1)
                    )
                }
                .menuStyle(.borderlessButton)

                // Clear button when vocal filter is active
                if vocalFilter != .all {
                    Button(action: { vocalFilter = .all }) {
                        Image(systemName: "xmark.circle.fill")
                            .foregroundColor(.white)
                            .font(.system(size: 16))
                    }
                    .buttonStyle(.plain)
                    .help("Clear type filter")
                }
            }
            .padding(.horizontal)

            // Search scope picker
            HStack(spacing: 12) {
                Text("Search in:")
                    .font(ApproachNoteTheme.subheadline())
                    .foregroundColor(.white)

                Picker("", selection: $searchScope) {
                    ForEach(RecordingSearchScope.allCases) { scope in
                        Text(scope.rawValue).tag(scope)
                    }
                }
                .pickerStyle(.segmented)
                .labelsHidden()

                Spacer()
            }
            .padding(.horizontal)
            .padding(.bottom, 8)
        }
        .background(ApproachNoteTheme.textSecondary)
        .environment(\.colorScheme, .light)
    }

    // MARK: - Helpers

    private func clearFilters() {
        searchScope = .all
        availabilityFilter = .all
        vocalFilter = .all
    }
}

// MARK: - Recording Row View

struct RecordingRowView: View {
    let recording: Recording

    var body: some View {
        HStack(spacing: 12) {
            // Album art
            Group {
                if let albumArtUrl = recording.bestAlbumArtSmall ?? recording.bestAlbumArtMedium {
                    AsyncImage(url: URL(string: albumArtUrl)) { image in
                        image
                            .resizable()
                            .aspectRatio(contentMode: .fill)
                    } placeholder: {
                        Rectangle()
                            .fill(ApproachNoteTheme.surface)
                            .overlay { ProgressView().controlSize(.small) }
                    }
                } else {
                    Rectangle()
                        .fill(ApproachNoteTheme.surface)
                        .overlay {
                            Image(systemName: "music.note")
                                .foregroundColor(ApproachNoteTheme.textSecondary)
                        }
                }
            }
            .frame(width: 50, height: 50)
            .cornerRadius(4)

            VStack(alignment: .leading, spacing: 2) {
                Text(recording.albumTitle ?? "Unknown Album")
                    .font(ApproachNoteTheme.headline())
                    .foregroundColor(ApproachNoteTheme.textPrimary)
                    .lineLimit(1)
                    .help("Album: \(recording.albumTitle ?? "Unknown Album")")

                if let songTitle = recording.songTitle {
                    Text(songTitle)
                        .font(ApproachNoteTheme.subheadline())
                        .foregroundColor(ApproachNoteTheme.textSecondary)
                        .lineLimit(1)
                        .help("Song: \(songTitle)")
                }

                if let artistCredit = recording.artistCredit {
                    Text(artistCredit)
                        .font(ApproachNoteTheme.caption())
                        .foregroundColor(ApproachNoteTheme.textSecondary)
                        .lineLimit(1)
                        .help("Artist: \(artistCredit)")
                }

                HStack(spacing: 4) {
                    if let year = recording.recordingYear {
                        Text(String(year))
                            .font(ApproachNoteTheme.caption())
                            .foregroundColor(ApproachNoteTheme.textSecondary)
                    }

                    if let label = recording.label {
                        Text("•")
                            .font(ApproachNoteTheme.caption())
                            .foregroundColor(ApproachNoteTheme.textSecondary)
                        Text(label)
                            .font(ApproachNoteTheme.caption())
                            .foregroundColor(ApproachNoteTheme.textSecondary)
                            .lineLimit(1)
                    }
                }
            }

            Spacer()

            // Streaming indicators
            HStack(spacing: 4) {
                if recording.hasSpotifyAvailable {
                    Image(systemName: "play.circle.fill")
                        .foregroundColor(.green)
                        .font(ApproachNoteTheme.caption())
                }
                if recording.hasAppleMusicAvailable {
                    Image(systemName: "play.circle.fill")
                        .foregroundColor(.pink)
                        .font(ApproachNoteTheme.caption())
                }
                if recording.hasYoutubeAvailable {
                    Image(systemName: "play.rectangle.fill")
                        .foregroundColor(.red)
                        .font(ApproachNoteTheme.caption())
                }
            }

            // Authority badge
            if recording.hasAuthority, let badgeText = recording.authorityBadgeText {
                AuthorityBadge(text: badgeText, source: recording.primaryAuthoritySource)
            }
        }
        .padding(.vertical, 4)
    }
}

#Preview {
    RecordingsListView()
}
