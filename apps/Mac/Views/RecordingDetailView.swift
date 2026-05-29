//
//  RecordingDetailView.swift
//  Approach Note
//
//  macOS-specific recording detail view
//

import SwiftUI

struct RecordingDetailView: View {
    @Environment(\.openURL) private var openURL
    let recordingId: String

    // Shared data + network state lives on the view model; layout/presentation
    // state stays here.
    @StateObject private var viewModel = RecordingDetailViewModel()

    @State private var showingContributionSheet = false
    @State private var showingStreamingLinkSheet = false
    @State private var streamingLinkReleaseId: String?
    @State private var streamingLinkReleaseTitle: String?
    @Environment(\.dismiss) private var dismiss
    @EnvironmentObject var authManager: AuthenticationManager
    @EnvironmentObject var favoritesManager: FavoritesManager

    // Read-only aliases so the dozens of existing reference sites in this
    // view can keep using the short names unchanged.
    private var recording: Recording? { viewModel.recording }
    private var isLoading: Bool { viewModel.isLoading }
    private var selectedReleaseId: String? { viewModel.selectedReleaseId }
    private var localFavoriteCount: Int? { viewModel.localFavoriteCount }

    // MARK: - Computed Properties for Selected Release

    /// The currently selected release, or nil to use recording defaults
    private var selectedRelease: Release? {
        guard let releaseId = selectedReleaseId,
              let releases = recording?.releases else { return nil }
        return releases.first { $0.id == releaseId }
    }

    /// Front cover art URL - uses selected release if user picked one, otherwise uses bestAlbumArt*
    private var displayAlbumArtLarge: String? {
        if let release = selectedRelease {
            return release.coverArtLarge ?? release.coverArtMedium
        }
        return recording?.bestAlbumArtLarge ?? recording?.bestAlbumArtMedium
    }

    /// Release year for the currently selected (or default) release.
    private var displayReleaseYear: Int? {
        if let release = selectedRelease {
            return release.releaseYear
        }
        guard let releases = recording?.releases else { return nil }
        if let defaultId = recording?.defaultReleaseId,
           let defaultRelease = releases.first(where: { $0.id == defaultId }),
           let year = defaultRelease.releaseYear {
            return year
        }
        return releases.compactMap(\.releaseYear).first
    }

    /// Label for the currently selected (or default) release.
    private var displayLabel: String? {
        if let release = selectedRelease {
            return release.label
        }
        guard let releases = recording?.releases else { return nil }
        if let defaultId = recording?.defaultReleaseId,
           let defaultRelease = releases.first(where: { $0.id == defaultId }),
           let label = defaultRelease.label {
            return label
        }
        return releases.compactMap(\.label).first
    }

    /// Display title - selected release title or recording album title
    private var displayAlbumTitle: String {
        selectedRelease?.title ?? recording?.albumTitle ?? "Unknown Album"
    }

    /// Spotify URL - uses selected release if user picked one, otherwise uses bestSpotifyUrl
    private var displaySpotifyUrl: String? {
        if let release = selectedRelease {
            return release.spotifyTrackUrl
        }
        return recording?.bestSpotifyUrl
    }

    // MARK: - Favorites Computed Properties

    /// Whether the current user has favorited this recording
    private var isFavorited: Bool {
        favoritesManager.isFavorited(recordingId)
    }

    /// Display count for favorites (uses local count if available, otherwise from recording)
    private var displayFavoriteCount: Int {
        localFavoriteCount ?? recording?.favoriteCount ?? 0
    }

    var body: some View {
        ScrollView {
            if isLoading {
                ThemedProgressView(message: "Loading...")
                    .frame(maxWidth: .infinity, maxHeight: .infinity)
                    .padding(.top, 100)
            } else if let recording = recording {
                VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingXL) {
                    // Header with album art
                    recordingHeader(recording)

                    Divider()

                    // Streaming links
                    streamingSection(recording)

                    // Performers
                    if let performers = recording.performers, !performers.isEmpty {
                        performersSection(performers)
                    }

                    // Community Data
                    communityDataSection(recording)

                    // Releases
                    if let releases = recording.releases, !releases.isEmpty {
                        releasesSection(releases)
                    }
                }
                .padding()
            } else {
                Text("Recording not found")
                    .foregroundColor(.secondary)
                    .padding(.top, 100)
            }
        }
        .background(ApproachNoteTheme.background)
        .toolbar {
            ToolbarItem(placement: .cancellationAction) {
                Button("Close") {
                    dismiss()
                }
            }
            ToolbarItem(placement: .automatic) {
                Button {
                    NSPasteboard.general.clearContents()
                    NSPasteboard.general.setString(recordingId, forType: .string)
                } label: {
                    Label("Copy Recording ID", systemImage: "doc.on.doc")
                }
                .help("Copy Recording ID to clipboard")
            }
            ToolbarItem(placement: .primaryAction) {
                Button {
                    viewModel.handleFavoriteTap()
                } label: {
                    HStack(spacing: ApproachNoteTheme.spacingXXS) {
                        Image(systemName: isFavorited ? "heart.fill" : "heart")
                        if displayFavoriteCount > 0 {
                            Text("\(displayFavoriteCount)")
                                .font(ApproachNoteTheme.caption())
                        }
                    }
                }
                .buttonStyle(.borderedProminent)
                .tint(isFavorited ? .red : ApproachNoteTheme.brand)
                .help(isFavorited ? "Remove from favorites" : "Add to favorites")
            }
        }
        .alert("Sign In Required", isPresented: $viewModel.showingLoginAlert) {
            Button("OK", role: .cancel) { }
        } message: {
            Text("Please sign in to favorite recordings.")
        }
        .sheet(isPresented: $showingContributionSheet) {
            if let recording = recording {
                MacRecordingContributionEditView(
                    recordingId: recordingId,
                    recordingTitle: "\(recording.songTitle ?? "Recording") - \(recording.albumTitle ?? "")",
                    currentContribution: recording.userContribution,
                    onSave: {
                        Task {
                            await viewModel.refresh()
                        }
                    }
                )
                .environmentObject(authManager)
            }
        }
        .sheet(isPresented: $showingStreamingLinkSheet) {
            if let releaseId = streamingLinkReleaseId,
               let releaseTitle = streamingLinkReleaseTitle {
                MacAddStreamingLinkSheet(
                    recordingId: recordingId,
                    releaseId: releaseId,
                    releaseTitle: releaseTitle,
                    onSuccess: {
                        Task {
                            await viewModel.refresh()
                        }
                    }
                )
                .environmentObject(authManager)
            }
        }
        .task(id: recordingId) {
            viewModel.configure(
                recordingId: recordingId,
                authManager: authManager,
                favoritesManager: favoritesManager
            )
            await viewModel.load()
        }
    }

    // MARK: - View Components

    @ViewBuilder
    private func recordingHeader(_ recording: Recording) -> some View {
        VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingMD) {
            // Full-width album art
            Group {
                if let frontUrl = displayAlbumArtLarge {
                    AsyncImage(url: URL(string: frontUrl)) { image in
                        image
                            .resizable()
                            .aspectRatio(contentMode: .fit)
                    } placeholder: {
                        Rectangle()
                            .fill(ApproachNoteTheme.surface)
                            .aspectRatio(1, contentMode: .fit)
                            .overlay {
                                ProgressView()
                                    .tint(ApproachNoteTheme.textSecondary)
                            }
                    }
                } else {
                    albumArtPlaceholder
                        .aspectRatio(1, contentMode: .fit)
                }
            }
            .frame(maxWidth: .infinity)
            .cornerRadius(8)
            .shadow(radius: 4)
            .animation(.easeInOut(duration: 0.3), value: selectedReleaseId)

            // Song title, album title, and artist below the image
            VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingXS) {
                // Recording Name (Year) — matches SongDetailView title pattern
                HStack(alignment: .firstTextBaseline, spacing: ApproachNoteTheme.spacingXS) {
                    if let songTitle = recording.songTitle {
                        (
                            Text(songTitle)
                                .font(ApproachNoteTheme.largeTitle(weight: .bold))
                                .foregroundColor(ApproachNoteTheme.textPrimary)
                            + Text(recording.recordingYear.map { " (\(String($0)))" } ?? "")
                                .font(ApproachNoteTheme.largeTitle(weight: .regular))
                                .foregroundColor(ApproachNoteTheme.textSecondary)
                        )
                    }
                }

                // Recording title (when different from song title)
                if let recordingTitle = recording.displayTitle {
                    Text("as \"\(recordingTitle)\"")
                        .font(ApproachNoteTheme.subheadline(italic: true))
                        .foregroundColor(ApproachNoteTheme.textSecondary)
                }

                // Release Name (uses selected release if available)
                Text(displayAlbumTitle)
                    .font(ApproachNoteTheme.title2())
                    .foregroundColor(ApproachNoteTheme.textSecondary)
                    .animation(.easeInOut(duration: 0.3), value: selectedReleaseId)

                // Leader names
                if let performers = recording.performers {
                    let leaders = performers.filter { $0.role == "leader" }
                    if !leaders.isEmpty {
                        Text(leaders.map { $0.name }.joined(separator: ", "))
                            .font(ApproachNoteTheme.title3())
                            .foregroundColor(ApproachNoteTheme.textSecondary)
                    }
                }

                if let composer = recording.composer {
                    Text("Composed by \(composer)")
                        .font(ApproachNoteTheme.body())
                        .bodyLineSpacing()
                        .foregroundColor(ApproachNoteTheme.textPrimary)
                }

                // Recording metadata surfaced from the old collapsible section
                recordingMetadataBlock(recording)

                if recording.hasAuthority, let badgeText = recording.authorityBadgeText {
                    AuthorityBadge(text: badgeText, source: recording.primaryAuthoritySource)
                }
            }
        }
    }

    @ViewBuilder
    private func recordingMetadataBlock(_ recording: Recording) -> some View {
        let hasMetadata = displayReleaseYear != nil ||
                          recording.recordingDate != nil ||
                          displayLabel != nil ||
                          recording.notes != nil ||
                          recording.musicbrainzId != nil

        if hasMetadata {
            VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingXXS) {
                if let year = displayReleaseYear {
                    metadataLine(label: "RELEASE YEAR", value: String(year))
                }
                if let date = recording.recordingDate {
                    metadataLine(label: "RECORDED", value: date)
                }
                if let label = displayLabel {
                    metadataLine(label: "LABEL", value: label)
                }
                if let notes = recording.notes, !notes.isEmpty {
                    Text(notes)
                        .font(ApproachNoteTheme.body())
                        .bodyLineSpacing()
                        .foregroundColor(ApproachNoteTheme.textPrimary)
                        .fixedSize(horizontal: false, vertical: true)
                        .padding(.top, ApproachNoteTheme.spacingXXS)
                }
                if let mbId = recording.musicbrainzId,
                   let mbUrl = URL(string: "https://musicbrainz.org/recording/\(mbId)") {
                    Link("Learn more on MusicBrainz", destination: mbUrl)
                        .font(ApproachNoteTheme.body())
                        .bodyLineSpacing()
                        .foregroundColor(ApproachNoteTheme.brand)
                        .padding(.top, ApproachNoteTheme.spacingXXS)
                }
            }
            .padding(.top, ApproachNoteTheme.spacingXXS)
        }
    }

    private func metadataLine(label: String, value: String) -> some View {
        (
            Text("\(label): ")
                .font(ApproachNoteTheme.subheadline(weight: .bold))
                .foregroundColor(ApproachNoteTheme.textPrimary)
            + Text(value)
                .font(ApproachNoteTheme.subheadline())
                .foregroundColor(ApproachNoteTheme.textPrimary)
        )
    }

    private var albumArtPlaceholder: some View {
        Rectangle()
            .fill(ApproachNoteTheme.surface)
            .overlay {
                Image(systemName: "music.note")
                    .font(.system(size: 50))
                    .foregroundColor(ApproachNoteTheme.textSecondary)
            }
    }

    /// Get Spotify URL - uses selected release if available, otherwise falls back to streaming links or legacy field
    private func spotifyUrl(for recording: Recording) -> String? {
        // First check if we have a selected release with a Spotify URL
        if let release = selectedRelease, let trackUrl = release.spotifyTrackUrl {
            return trackUrl
        }
        // Fall back to streamingLinks or legacy field
        if let link = recording.streamingLinks?["spotify"], let url = link.bestPlaybackUrl {
            return url
        }
        return recording.bestSpotifyUrl
    }

    private func appleMusicUrl(for recording: Recording) -> String? {
        recording.streamingLinks?["apple_music"]?.bestPlaybackUrl
    }

    private func youtubeUrl(for recording: Recording) -> String? {
        recording.streamingLinks?["youtube"]?.bestPlaybackUrl
    }

    @ViewBuilder
    private func streamingSection(_ recording: Recording) -> some View {
        VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingSM) {
            Text("Listen")
                .font(ApproachNoteTheme.headline())
                .foregroundColor(ApproachNoteTheme.textPrimary)

            HStack(spacing: ApproachNoteTheme.spacingSM) {
                if let spotifyUrlString = spotifyUrl(for: recording),
                   let url = URL(string: spotifyUrlString) {
                    Link(destination: url) {
                        StreamingIcon(service: .spotify, size: 48)
                    }
                    .buttonStyle(.plain)
                    .help("Open in Spotify")
                }

                if let appleMusicUrlString = appleMusicUrl(for: recording),
                   let url = URL(string: appleMusicUrlString) {
                    Link(destination: url) {
                        StreamingIcon(service: .appleMusic, size: 48)
                    }
                    .buttonStyle(.plain)
                    .help("Open in Apple Music")
                }

                if let youtubeUrlString = youtubeUrl(for: recording),
                   let url = URL(string: youtubeUrlString) {
                    Link(destination: url) {
                        StreamingIcon(service: .youtube, size: 48)
                    }
                    .buttonStyle(.plain)
                    .help("Open in YouTube")
                }
            }
        }
    }

    @ViewBuilder
    private func performersSection(_ performers: [Performer]) -> some View {
        VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingSM) {
            Text("Personnel (\(performers.count.formatted()))")
                .font(ApproachNoteTheme.headline())
                .foregroundColor(ApproachNoteTheme.textPrimary)

            LazyVGrid(columns: [GridItem(.flexible()), GridItem(.flexible())], spacing: ApproachNoteTheme.spacingSM) {
                ForEach(performers) { performer in
                    HStack {
                        VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingXXS) {
                            Text(performer.name)
                                .font(ApproachNoteTheme.subheadline(weight: .medium))
                                .foregroundColor(ApproachNoteTheme.textPrimary)

                            if let instrument = performer.instrument {
                                Text(instrument)
                                    .font(ApproachNoteTheme.caption())
                                    .foregroundColor(ApproachNoteTheme.textSecondary)
                            }
                        }

                        Spacer()

                        if let role = performer.role {
                            Text(role.capitalized)
                                .font(ApproachNoteTheme.caption2())
                                .padding(.horizontal, 6)
                                .padding(.vertical, 2)
                                .background(role == "leader" ? ApproachNoteTheme.brand : ApproachNoteTheme.textSecondary.opacity(0.3))
                                .foregroundColor(role == "leader" ? .white : ApproachNoteTheme.textPrimary)
                                .cornerRadius(4)
                        }
                    }
                    .padding(ApproachNoteTheme.spacingXS)
                    .background(ApproachNoteTheme.surface)
                    .cornerRadius(8)
                }
            }
        }
    }

    @ViewBuilder
    private func communityDataSection(_ recording: Recording) -> some View {
        MacCommunityDataSection(
            recordingId: recording.id,
            communityData: recording.communityData,
            userContribution: recording.userContribution,
            isAuthenticated: authManager.isAuthenticated,
            onEditTapped: {
                showingContributionSheet = true
            }
        )
    }

    @ViewBuilder
    private func releasesSection(_ releases: [Release]) -> some View {
        VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingSM) {
            HStack {
                Text("Releases (\(releases.count.formatted()))")
                    .font(ApproachNoteTheme.headline())
                    .foregroundColor(ApproachNoteTheme.textPrimary)

                Spacer()

                Text("Click to change cover art")
                    .font(ApproachNoteTheme.caption())
                    .foregroundColor(ApproachNoteTheme.textSecondary)
            }

            ForEach(releases) { release in
                let isSelected = selectedReleaseId == release.id

                Button {
                    withAnimation(.easeInOut(duration: 0.3)) {
                        if selectedReleaseId == release.id {
                            // Deselect if already selected
                            viewModel.selectedReleaseId = nil
                        } else {
                            viewModel.selectedReleaseId = release.id
                        }
                    }
                } label: {
                    HStack(spacing: ApproachNoteTheme.spacingSM) {
                        // Selection indicator
                        Image(systemName: isSelected ? "checkmark.circle.fill" : "circle")
                            .font(ApproachNoteTheme.title3())
                            .foregroundColor(isSelected ? ApproachNoteTheme.brand : ApproachNoteTheme.textSecondary.opacity(0.5))

                        // Release cover art
                        AsyncImage(url: URL(string: release.coverArtSmall ?? "")) { image in
                            image
                                .resizable()
                                .aspectRatio(contentMode: .fill)
                        } placeholder: {
                            Rectangle()
                                .fill(ApproachNoteTheme.surface)
                                .overlay {
                                    Image(systemName: "opticaldisc")
                                        .foregroundColor(ApproachNoteTheme.textSecondary)
                                }
                        }
                        .frame(width: 50, height: 50)
                        .cornerRadius(4)
                        .overlay(
                            RoundedRectangle(cornerRadius: 4)
                                .stroke(isSelected ? ApproachNoteTheme.brand : Color.clear, lineWidth: 2)
                        )

                        VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingXXS) {
                            Text(release.title)
                                .font(ApproachNoteTheme.subheadline(weight: isSelected ? .bold : .medium))
                                .foregroundColor(isSelected ? ApproachNoteTheme.brand : ApproachNoteTheme.textPrimary)
                                .lineLimit(1)

                            HStack(spacing: ApproachNoteTheme.spacingXS) {
                                Text(release.yearDisplay)
                                    .font(ApproachNoteTheme.caption())
                                    .foregroundColor(ApproachNoteTheme.textSecondary)

                                if let format = release.formatName {
                                    Text("•")
                                        .foregroundColor(ApproachNoteTheme.textSecondary)
                                    Text(format)
                                        .font(ApproachNoteTheme.caption())
                                        .foregroundColor(ApproachNoteTheme.textSecondary)
                                }

                                if let label = release.label {
                                    Text("•")
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
                        HStack(spacing: ApproachNoteTheme.spacingXXS) {
                            if release.hasSpotify {
                                Image(systemName: "music.note")
                                    .foregroundColor(.green)
                                    .help("Available on Spotify")
                            }

                            // Add streaming link button (only for authenticated users)
                            if authManager.isAuthenticated {
                                Button {
                                    streamingLinkReleaseId = release.id
                                    streamingLinkReleaseTitle = release.title
                                    showingStreamingLinkSheet = true
                                } label: {
                                    Image(systemName: "plus.circle")
                                        .foregroundColor(ApproachNoteTheme.textSecondary)
                                }
                                .buttonStyle(.plain)
                                .help("Add Spotify or Apple Music link")
                            }
                        }
                    }
                    .padding(ApproachNoteTheme.spacingXS)
                    .background(isSelected ? ApproachNoteTheme.brand.opacity(0.1) : ApproachNoteTheme.surface)
                    .cornerRadius(8)
                    .contentShape(Rectangle())
                }
                .buttonStyle(.plain)
                .contextMenu {
                    if authManager.isAuthenticated {
                        Button {
                            streamingLinkReleaseId = release.id
                            streamingLinkReleaseTitle = release.title
                            showingStreamingLinkSheet = true
                        } label: {
                            Label("Add Streaming Link", systemImage: "link.badge.plus")
                        }
                    }

                    Button {
                        NSPasteboard.general.clearContents()
                        NSPasteboard.general.setString(release.id, forType: .string)
                    } label: {
                        Label("Copy Release ID", systemImage: "doc.on.doc")
                    }
                }
            }
        }
    }

}

#Preview {
    RecordingDetailView(recordingId: "preview-id")
        .environmentObject(AuthenticationManager())
        .environmentObject(FavoritesManager())
}
