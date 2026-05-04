//
//  RecordingDetailView.swift
//  Approach Note
//
//  macOS-specific recording detail view
//

import SwiftUI

struct RecordingDetailView: View {
    let recordingId: String

    // Shared data + network state lives on the view model; layout/presentation
    // state stays here.
    @StateObject private var viewModel = RecordingDetailViewModel()

    @State private var showingBackCover = false
    @State private var showingContributionSheet = false
    @State private var showingStreamingLinkSheet = false
    @State private var streamingLinkReleaseId: String?
    @State private var streamingLinkReleaseTitle: String?
    @State private var isRecordingDetailsExpanded = false
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

    /// Back cover art URL - uses selected release if user picked one, otherwise uses recording defaults
    private var displayBackCoverArtLarge: String? {
        if let release = selectedRelease {
            return release.backCoverArtLarge ?? release.backCoverArtMedium
        }
        return recording?.backCoverArtLarge ?? recording?.backCoverArtMedium
    }

    /// Whether a back cover is available for flipping (from selected release or recording)
    private var canFlipToBackCover: Bool {
        if let release = selectedRelease {
            return release.canFlipToBackCover
        }
        return recording?.canFlipToBackCover ?? false
    }

    /// Back cover source for attribution (from selected release or recording)
    private var displayBackCoverSource: String? {
        if let release = selectedRelease {
            return release.backCoverSource
        }
        return recording?.backCoverSource
    }

    /// Back cover source URL for attribution (from selected release or recording)
    private var displayBackCoverSourceUrl: String? {
        if let release = selectedRelease {
            return release.backCoverSourceUrl
        }
        return recording?.backCoverSourceUrl
    }

    /// Image source for the currently displayed album art (for watermark badge)
    private var displayAlbumArtSource: String? {
        if let release = selectedRelease {
            return release.coverArtSource
        }
        return recording?.displayAlbumArtSource
    }

    /// Source URL for the currently displayed album art (for watermark badge)
    private var displayAlbumArtSourceUrl: String? {
        if let release = selectedRelease {
            return release.coverArtSourceUrl
        }
        return recording?.displayAlbumArtSourceUrl
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
                VStack(alignment: .leading, spacing: 24) {
                    // Header with album art
                    recordingHeader(recording)

                    Divider()

                    // Recording info
                    recordingInfo(recording)

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
        .background(ApproachNoteTheme.backgroundLight)
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
                    HStack(spacing: 4) {
                        Image(systemName: isFavorited ? "heart.fill" : "heart")
                        if displayFavoriteCount > 0 {
                            Text("\(displayFavoriteCount)")
                                .font(ApproachNoteTheme.caption())
                        }
                    }
                }
                .buttonStyle(.borderedProminent)
                .tint(isFavorited ? .red : ApproachNoteTheme.burgundy)
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
        VStack(alignment: .leading, spacing: 16) {
            // Full-width album art with flip support
            ZStack(alignment: .topTrailing) {
                // Card-flip container
                ZStack {
                    // Front cover
                    if let frontUrl = displayAlbumArtLarge {
                        AsyncImage(url: URL(string: frontUrl)) { image in
                            image
                                .resizable()
                                .aspectRatio(contentMode: .fit)
                        } placeholder: {
                            Rectangle()
                                .fill(ApproachNoteTheme.cardBackground)
                                .aspectRatio(1, contentMode: .fit)
                                .overlay {
                                    ProgressView()
                                        .tint(ApproachNoteTheme.brass)
                                }
                        }
                        .opacity(showingBackCover ? 0 : 1)
                    } else {
                        albumArtPlaceholder
                            .aspectRatio(1, contentMode: .fit)
                            .opacity(showingBackCover ? 0 : 1)
                    }

                    // Back cover (pre-rotated so it appears correct after flip)
                    if let backUrl = displayBackCoverArtLarge {
                        AsyncImage(url: URL(string: backUrl)) { image in
                            image
                                .resizable()
                                .aspectRatio(contentMode: .fit)
                        } placeholder: {
                            Rectangle()
                                .fill(ApproachNoteTheme.cardBackground)
                                .aspectRatio(1, contentMode: .fit)
                                .overlay {
                                    ProgressView()
                                        .tint(ApproachNoteTheme.brass)
                                }
                        }
                        .rotation3DEffect(.degrees(180), axis: (x: 0, y: 1, z: 0))
                        .opacity(showingBackCover ? 1 : 0)
                    }
                }
                .frame(maxWidth: .infinity)
                .cornerRadius(8)
                .rotation3DEffect(
                    .degrees(showingBackCover ? 180 : 0),
                    axis: (x: 0, y: 1, z: 0)
                )
                .shadow(radius: 4)
                .animation(.easeInOut(duration: 0.3), value: selectedReleaseId)

                // Flip button badge (shown when back cover available)
                if canFlipToBackCover {
                    Button {
                        withAnimation(.easeInOut(duration: 0.4)) {
                            showingBackCover.toggle()
                        }
                    } label: {
                        Image(systemName: showingBackCover ? "arrow.uturn.backward" : "arrow.trianglehead.2.clockwise.rotate.90")
                            .foregroundColor(.white)
                            .font(.system(size: 14, weight: .semibold))
                            .padding(10)
                            .background(Color.black.opacity(0.6))
                            .clipShape(Circle())
                    }
                    .buttonStyle(.plain)
                    .padding(12)
                    .help(showingBackCover ? "Show front cover" : "Show back cover")
                }

            }

            // Song title, album title, and artist below the image
            VStack(alignment: .leading, spacing: 8) {
                // Recording Name (Year) + image-source ⓘ button
                HStack {
                    if recording.isCanonical == true {
                        Image(systemName: "star.fill")
                            .foregroundColor(ApproachNoteTheme.gold)
                    }
                    if let songTitle = recording.songTitle {
                        let yearSuffix = recording.recordingYear.map { " (\($0))" } ?? ""
                        Text("\(songTitle)\(yearSuffix)")
                            .font(ApproachNoteTheme.largeTitle())
                            .foregroundColor(ApproachNoteTheme.charcoal)
                    }
                    Spacer()
                    if showingBackCover {
                        AlbumArtSourceBadge(
                            source: displayBackCoverSource,
                            sourceUrl: displayBackCoverSourceUrl
                        )
                    } else {
                        AlbumArtSourceBadge(
                            source: displayAlbumArtSource,
                            sourceUrl: displayAlbumArtSourceUrl
                        )
                    }
                }

                // Recording title (when different from song title)
                if let recordingTitle = recording.displayTitle {
                    Text("as \"\(recordingTitle)\"")
                        .font(ApproachNoteTheme.subheadline())
                        .italic()
                        .foregroundColor(ApproachNoteTheme.brass)
                }

                // Release Name (uses selected release if available)
                Text(displayAlbumTitle)
                    .font(ApproachNoteTheme.title2())
                    .foregroundColor(ApproachNoteTheme.smokeGray)
                    .animation(.easeInOut(duration: 0.3), value: selectedReleaseId)

                // Leader names
                if let performers = recording.performers {
                    let leaders = performers.filter { $0.role == "leader" }
                    if !leaders.isEmpty {
                        Text(leaders.map { $0.name }.joined(separator: ", "))
                            .font(ApproachNoteTheme.title3())
                            .foregroundColor(ApproachNoteTheme.brass)
                    }
                }

                // Label and Authority badge on the same row
                HStack(spacing: 12) {
                    if let label = recording.label {
                        Label(label, systemImage: "building.2")
                            .font(ApproachNoteTheme.subheadline())
                            .foregroundColor(ApproachNoteTheme.smokeGray)
                    }

                    if recording.hasAuthority, let badgeText = recording.authorityBadgeText {
                        AuthorityBadge(text: badgeText, source: recording.primaryAuthoritySource)
                    }
                }
            }
        }
    }

    private var albumArtPlaceholder: some View {
        Rectangle()
            .fill(ApproachNoteTheme.cardBackground)
            .overlay {
                Image(systemName: "music.note")
                    .font(.system(size: 50))
                    .foregroundColor(ApproachNoteTheme.smokeGray)
            }
    }

    @ViewBuilder
    private func recordingInfo(_ recording: Recording) -> some View {
        let hasContent = recording.recordingYear != nil ||
                         recording.recordingDate != nil ||
                         recording.label != nil ||
                         recording.notes != nil ||
                         recording.musicbrainzId != nil

        if hasContent {
            VStack(alignment: .leading, spacing: 0) {
                // Collapsible header
                Button {
                    withAnimation(.easeInOut(duration: 0.2)) {
                        isRecordingDetailsExpanded.toggle()
                    }
                } label: {
                    HStack {
                        Text("Recording Details")
                            .font(ApproachNoteTheme.headline())
                            .foregroundColor(ApproachNoteTheme.charcoal)

                        Spacer()

                        Image(systemName: isRecordingDetailsExpanded ? "chevron.up" : "chevron.down")
                            .font(ApproachNoteTheme.subheadline())
                            .foregroundColor(ApproachNoteTheme.brass)
                    }
                    .padding(12)
                    .background(ApproachNoteTheme.cardBackground)
                }
                .buttonStyle(.plain)

                // Expandable content
                if isRecordingDetailsExpanded {
                    VStack(alignment: .leading, spacing: 12) {
                        if let year = recording.recordingYear {
                            DetailRow(icon: "calendar", label: "Year", value: String(year))
                        }

                        if let date = recording.recordingDate {
                            DetailRow(icon: "calendar.badge.clock", label: "Recorded", value: date)
                        }

                        if let label = recording.label {
                            DetailRow(icon: "music.note.house", label: "Label", value: label)
                        }

                        if let notes = recording.notes, !notes.isEmpty {
                            VStack(alignment: .leading, spacing: 4) {
                                HStack {
                                    Image(systemName: "note.text")
                                        .foregroundColor(ApproachNoteTheme.brass)
                                        .frame(width: 24)
                                    Text("Notes")
                                        .font(ApproachNoteTheme.subheadline())
                                        .foregroundColor(ApproachNoteTheme.smokeGray)
                                }
                                Text(notes)
                                    .font(ApproachNoteTheme.body())
                                    .foregroundColor(ApproachNoteTheme.charcoal)
                                    .padding(.leading, 32)
                            }
                        }

                        // MusicBrainz link
                        if let mbId = recording.musicbrainzId,
                           let url = URL(string: "https://musicbrainz.org/recording/\(mbId)") {
                            Divider()
                                .padding(.vertical, 4)

                            VStack(alignment: .leading, spacing: 8) {
                                Text("Learn More")
                                    .font(ApproachNoteTheme.headline())
                                    .foregroundColor(ApproachNoteTheme.charcoal)

                                Link(destination: url) {
                                    HStack {
                                        Image(systemName: "waveform.circle.fill")
                                            .foregroundColor(ApproachNoteTheme.charcoal)
                                        Text("MusicBrainz")
                                            .font(ApproachNoteTheme.caption())
                                            .foregroundColor(ApproachNoteTheme.smokeGray)
                                        Spacer()
                                        Image(systemName: "arrow.up.right")
                                            .font(ApproachNoteTheme.caption())
                                            .foregroundColor(ApproachNoteTheme.smokeGray)
                                    }
                                    .padding(10)
                                    .background(ApproachNoteTheme.backgroundLight)
                                    .cornerRadius(8)
                                }
                                .buttonStyle(.plain)
                            }
                        }
                    }
                    .padding(12)
                    .background(ApproachNoteTheme.cardBackground)
                }
            }
            .cornerRadius(8)
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
        VStack(alignment: .leading, spacing: 12) {
            Text("Listen")
                .font(ApproachNoteTheme.headline())
                .foregroundColor(ApproachNoteTheme.charcoal)

            HStack(spacing: 12) {
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
        VStack(alignment: .leading, spacing: 12) {
            Text("Personnel (\(performers.count.formatted()))")
                .font(ApproachNoteTheme.headline())
                .foregroundColor(ApproachNoteTheme.charcoal)

            LazyVGrid(columns: [GridItem(.flexible()), GridItem(.flexible())], spacing: 12) {
                ForEach(performers) { performer in
                    HStack {
                        VStack(alignment: .leading, spacing: 2) {
                            Text(performer.name)
                                .font(ApproachNoteTheme.subheadline(weight: .medium))
                                .foregroundColor(ApproachNoteTheme.charcoal)

                            if let instrument = performer.instrument {
                                Text(instrument)
                                    .font(ApproachNoteTheme.caption())
                                    .foregroundColor(ApproachNoteTheme.smokeGray)
                            }
                        }

                        Spacer()

                        if let role = performer.role {
                            Text(role.capitalized)
                                .font(ApproachNoteTheme.caption2())
                                .padding(.horizontal, 6)
                                .padding(.vertical, 2)
                                .background(role == "leader" ? ApproachNoteTheme.burgundy : ApproachNoteTheme.brass.opacity(0.3))
                                .foregroundColor(role == "leader" ? .white : ApproachNoteTheme.charcoal)
                                .cornerRadius(4)
                        }
                    }
                    .padding(10)
                    .background(ApproachNoteTheme.cardBackground)
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
        VStack(alignment: .leading, spacing: 12) {
            HStack {
                Text("Releases (\(releases.count.formatted()))")
                    .font(ApproachNoteTheme.headline())
                    .foregroundColor(ApproachNoteTheme.charcoal)

                Spacer()

                Text("Click to change cover art")
                    .font(ApproachNoteTheme.caption())
                    .foregroundColor(ApproachNoteTheme.smokeGray)
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
                        // Reset back cover when changing release
                        showingBackCover = false
                    }
                } label: {
                    HStack(spacing: 12) {
                        // Selection indicator
                        Image(systemName: isSelected ? "checkmark.circle.fill" : "circle")
                            .font(ApproachNoteTheme.title3())
                            .foregroundColor(isSelected ? ApproachNoteTheme.burgundy : ApproachNoteTheme.smokeGray.opacity(0.5))

                        // Release cover art
                        AsyncImage(url: URL(string: release.coverArtSmall ?? "")) { image in
                            image
                                .resizable()
                                .aspectRatio(contentMode: .fill)
                        } placeholder: {
                            Rectangle()
                                .fill(ApproachNoteTheme.cardBackground)
                                .overlay {
                                    Image(systemName: "opticaldisc")
                                        .foregroundColor(ApproachNoteTheme.smokeGray)
                                }
                        }
                        .frame(width: 50, height: 50)
                        .cornerRadius(4)
                        .overlay(
                            RoundedRectangle(cornerRadius: 4)
                                .stroke(isSelected ? ApproachNoteTheme.burgundy : Color.clear, lineWidth: 2)
                        )

                        VStack(alignment: .leading, spacing: 2) {
                            Text(release.title)
                                .font(ApproachNoteTheme.subheadline(weight: isSelected ? .bold : .medium))
                                .foregroundColor(isSelected ? ApproachNoteTheme.burgundy : ApproachNoteTheme.charcoal)
                                .lineLimit(1)

                            HStack(spacing: 8) {
                                Text(release.yearDisplay)
                                    .font(ApproachNoteTheme.caption())
                                    .foregroundColor(ApproachNoteTheme.smokeGray)

                                if let format = release.formatName {
                                    Text("•")
                                        .foregroundColor(ApproachNoteTheme.smokeGray)
                                    Text(format)
                                        .font(ApproachNoteTheme.caption())
                                        .foregroundColor(ApproachNoteTheme.smokeGray)
                                }

                                if let label = release.label {
                                    Text("•")
                                        .foregroundColor(ApproachNoteTheme.smokeGray)
                                    Text(label)
                                        .font(ApproachNoteTheme.caption())
                                        .foregroundColor(ApproachNoteTheme.smokeGray)
                                        .lineLimit(1)
                                }
                            }
                        }

                        Spacer()

                        // Streaming indicators
                        HStack(spacing: 4) {
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
                                        .foregroundColor(ApproachNoteTheme.brass)
                                }
                                .buttonStyle(.plain)
                                .help("Add Spotify or Apple Music link")
                            }
                        }
                    }
                    .padding(10)
                    .background(isSelected ? ApproachNoteTheme.burgundy.opacity(0.1) : ApproachNoteTheme.cardBackground)
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
