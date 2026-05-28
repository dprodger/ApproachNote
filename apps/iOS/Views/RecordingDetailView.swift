//
//  RecordingDetailView.swift
//  Approach Note
//
//  Updated with ApproachNoteTheme color palette and consistent styling
//  UPDATED: Added back cover flip support with 3D animation
//

import SwiftUI
import Combine

// MARK: - Recording Detail View

struct RecordingDetailView: View {
    let recordingId: String
    var onCommunityDataChanged: (() -> Void)? = nil

    // Shared data + network state lives on the view model; layout/presentation
    // state stays here.
    @StateObject private var viewModel = RecordingDetailViewModel()

    // Environment objects for favorites
    @EnvironmentObject var authManager: AuthenticationManager
    @EnvironmentObject var favoritesManager: FavoritesManager

    @State private var reportingInfo: ReportingInfo?
    @State private var longPressOccurred = false
    @State private var showingSubmissionAlert = false
    @State private var submissionAlertMessage = ""
    @State private var showingAuthoritySheet = false
    @State private var showAllReleases = false
    @State private var showingContributionEditor = false
    private let maxReleasesToShow = 5

    // Custom collapsing header (issue #198): pops the pushed view, and scroll
    // offset drives the header height + the "Recording" -> album-title swap.
    @Environment(\.dismiss) private var dismiss
    @State private var scrollOffset: CGFloat = 0
    @State private var isHeaderTitleVisible = true

    private var headerHeight: CGFloat {
        DetailHeaderMetrics.expandedHeight
            - min(max(0, scrollOffset), DetailHeaderMetrics.collapseDistance)
    }
    private var headerOverscroll: CGFloat { max(0, -scrollOffset) }

    // Read-only aliases so the dozens of existing reference sites in this
    // view can keep using the short names unchanged.
    private var recording: Recording? { viewModel.recording }
    private var isLoading: Bool { viewModel.isLoading }
    private var selectedReleaseId: String? { viewModel.selectedReleaseId }
    private var localFavoriteCount: Int? { viewModel.localFavoriteCount }
    @Environment(\.openURL) var openURL
    
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
        // Use bestAlbumArt* which is consistent across API endpoints
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

    /// Spotify URL - uses selected release if user picked one, otherwise uses bestSpotifyUrl
    /// Only returns track URLs for consistency with has_spotify filter
    private var displaySpotifyUrl: String? {
        if let release = selectedRelease {
            // Only use track URL, not album URL, to match filter behavior
            return release.spotifyTrackUrl
        }
        // Use bestSpotifyUrl which only returns track URLs
        return recording?.bestSpotifyUrl
    }
    
    /// Display title - selected release title or recording album title
    private var displayAlbumTitle: String {
        selectedRelease?.title ?? recording?.albumTitle ?? "Unknown Album"
    }
    
    /// Performers from selected release if available, otherwise from recording
    private var displayPerformers: [Performer]? {
        // If a release is selected and has performers, use them
        if let release = selectedRelease, let releasePerformers = release.performers, !releasePerformers.isEmpty {
            return releasePerformers
        }
        // Fall back to recording performers
        return recording?.performers
    }
    
    /// Available streaming sources as (name, url) tuples
    /// Prefers new streamingLinks API, falls back to legacy fields
    private var availableStreamingSources: [(name: String, icon: String, url: String, color: Color, service: StreamingService)] {
        var sources: [(name: String, icon: String, url: String, color: Color, service: StreamingService)] = []

        // Use new streamingLinks API if available
        if let links = recording?.streamingLinks {
            if let spotifyLink = links["spotify"], let url = spotifyLink.bestPlaybackUrl {
                sources.append((name: "Spotify", icon: "music.note.list", url: url, color: StreamingService.spotify.brandColor, service: .spotify))
            }
            if let appleLink = links["apple_music"], let url = appleLink.bestPlaybackUrl {
                sources.append((name: "Apple Music", icon: "music.note", url: url, color: StreamingService.appleMusic.brandColor, service: .appleMusic))
            }
            if let youtubeLink = links["youtube"], let url = youtubeLink.bestPlaybackUrl {
                sources.append((name: "YouTube", icon: "play.rectangle.fill", url: url, color: StreamingService.youtube.brandColor, service: .youtube))
            }
        }

        // Fall back to legacy Spotify URL if no streamingLinks
        if sources.isEmpty {
            if let spotifyUrl = displaySpotifyUrl {
                sources.append((name: "Spotify", icon: "music.note.list", url: spotifyUrl, color: StreamingService.spotify.brandColor, service: .spotify))
            }
        }

        return sources
    }
    
    /// Whether any streaming source is available
    private var hasStreamingSource: Bool {
        !availableStreamingSources.isEmpty
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

    struct ReportingInfo: Identifiable {
        let id = UUID()
        let source: String
        let url: String
    }
    
    var body: some View {
        ScrollView {
            VStack(spacing: 0) {
                // Brand spacer sized to the expanded header so content starts
                // below it and rides up under the collapsing header overlay.
                ApproachNoteTheme.brand
                    .frame(height: DetailHeaderMetrics.expandedHeight)

            if isLoading {
                VStack {
                    Spacer()
                    ThemedProgressView(message: "Loading...", tintColor: ApproachNoteTheme.textSecondary)
                    Spacer()
                }
                .frame(maxWidth: .infinity, maxHeight: .infinity)
                .background(ApproachNoteTheme.background)
            } else if let recording = recording {
                VStack(alignment: .leading, spacing: 0) {
                    VStack(alignment: .leading, spacing: 16) {
                        // Album Information
                        VStack(alignment: .leading, spacing: 12) {
                            // Album artwork
                            Group {
                                if let frontUrl = displayAlbumArtLarge {
                                    CachedAsyncImage(
                                        url: URL(string: frontUrl),
                                        content: { image in
                                            image
                                                .resizable()
                                                .aspectRatio(contentMode: .fit)
                                                .frame(maxWidth: .infinity)
                                                .cornerRadius(12)
                                        },
                                        placeholder: {
                                            Rectangle()
                                                .fill(ApproachNoteTheme.surface)
                                                .aspectRatio(1, contentMode: .fit)
                                                .cornerRadius(12)
                                                .overlay(
                                                    ProgressView()
                                                        .tint(ApproachNoteTheme.textSecondary)
                                                )
                                        }
                                    )
                                } else {
                                    albumArtPlaceholder
                                }
                            }
                            .shadow(radius: 8)
                            .animation(.easeInOut(duration: 0.3), value: selectedReleaseId)

                            // Recording Name (Year) — matches SongDetailView title pattern
                            HStack(alignment: .firstTextBaseline, spacing: 8) {
                                if recording.isCanonical == true {
                                    Image(systemName: "star.fill")
                                        .foregroundColor(ApproachNoteTheme.accent)
                                        .font(ApproachNoteTheme.title2())
                                }
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

                            // Release Name
                            Text(displayAlbumTitle)
                                .font(ApproachNoteTheme.title2())
                                .foregroundColor(ApproachNoteTheme.textSecondary)
                                .animation(.easeInOut(duration: 0.3), value: selectedReleaseId)

                            if let composer = recording.composer {
                                Text("Composed by \(composer)")
                                    .font(ApproachNoteTheme.body())
                                    .bodyLineSpacing()
                                    .foregroundColor(ApproachNoteTheme.textPrimary)
                            }

                            // Recording metadata pulled out of the old collapsible section
                            recordingMetadataBlock(recording)

                            // Streaming services indicator
                            if hasStreamingSource {
                                streamingServicesIndicator
                            }

                            // Favorite control (relocated from the nav bar
                            // header, which now carries only back + authority).
                            favoriteControl
                        }
                        .padding(.horizontal, 20)
                        .padding(.vertical, 16)

                        // Releases Section - shows all releases containing this recording
                        if let releases = recording.releases, releases.count > 1 {
                            releasesSection(releases)
                        }
                        
                        Divider()
                            .padding(.horizontal, 20)

                        // Performers Section
                        VStack(alignment: .leading, spacing: 12) {
                            HStack {
                                Text("Performers")
                                    .font(ApproachNoteTheme.title2())
                                    .bold()
                                    .foregroundColor(ApproachNoteTheme.textPrimary)

                                // Indicator when showing release-specific performers
                                if selectedRelease != nil, let releasePerformers = selectedRelease?.performers, !releasePerformers.isEmpty {
                                    Text("(from selected release)")
                                        .font(ApproachNoteTheme.caption())
                                        .foregroundColor(ApproachNoteTheme.textSecondary)
                                }
                            }
                            .padding(.horizontal, 20)
                            
                            if let performers = displayPerformers, !performers.isEmpty {
                                ForEach(performers) { performer in
                                    NavigationLink(destination: PerformerDetailView(performerId: performer.id)) {
                                        PerformerRowView(performer: performer)
                                    }
                                    .buttonStyle(.plain)
                                }
                                .animation(.easeInOut(duration: 0.3), value: selectedReleaseId)
                            } else {
                                Text("No performer information available")
                                    .foregroundColor(ApproachNoteTheme.textSecondary)
                                    .padding()
                            }
                        }

                        // Community Data Section
                        CommunityDataSection(
                            recordingId: recordingId,
                            communityData: recording.communityData,
                            userContribution: recording.userContribution,
                            isAuthenticated: authManager.isAuthenticated,
                            onEditTapped: {
                                showingContributionEditor = true
                            }
                        )

                        // Favorited By Section
                        if let favoritedBy = recording.favoritedBy, !favoritedBy.isEmpty {
                            VStack(alignment: .leading, spacing: 12) {
                                HStack {
                                    Image(systemName: "heart.fill")
                                        .foregroundColor(.red)
                                    Text("Favorited by \(favoritedBy.count) \(favoritedBy.count == 1 ? "person" : "people")")
                                        .font(ApproachNoteTheme.headline())
                                        .foregroundColor(ApproachNoteTheme.textPrimary)
                                }
                                .padding(.horizontal, 20)

                                ScrollView(.horizontal, showsIndicators: false) {
                                    HStack(spacing: 12) {
                                        ForEach(favoritedBy) { user in
                                            VStack(spacing: 4) {
                                                Circle()
                                                    .fill(ApproachNoteTheme.textSecondary.opacity(0.2))
                                                    .frame(width: 40, height: 40)
                                                    .overlay(
                                                        Text(String((user.displayName ?? "?").prefix(1)).uppercased())
                                                            .font(ApproachNoteTheme.headline())
                                                            .foregroundColor(ApproachNoteTheme.textSecondary)
                                                    )
                                                Text(user.displayName ?? "User")
                                                    .font(ApproachNoteTheme.caption())
                                                    .foregroundColor(ApproachNoteTheme.textSecondary)
                                                    .lineLimit(1)
                                            }
                                            .frame(width: 60)
                                        }
                                    }
                                    .padding(.horizontal, 20)
                                }
                            }
                            .padding(.vertical, 8)
                        }

                        // Transcriptions Section
                        if let transcriptions = recording.transcriptions, !transcriptions.isEmpty {
                            TranscriptionsSection(transcriptions: transcriptions)
                        }
                    }
                    .padding(.vertical, 16)
                }
            } else {
                VStack {
                    Spacer()
                    Text("Recording not found")
                        .foregroundColor(ApproachNoteTheme.textSecondary)
                    Spacer()
                }
                .frame(maxWidth: .infinity, maxHeight: .infinity)
                .background(ApproachNoteTheme.background)
            }
            }
        }
        .onScrollGeometryChange(for: CGFloat.self) { geometry in
            geometry.contentOffset.y + geometry.contentInsets.top
        } action: { _, newValue in
            scrollOffset = newValue
            isHeaderTitleVisible = max(0, newValue) < DetailHeaderMetrics.titleSwapOffset
        }
        .background(ApproachNoteTheme.background)
        .refreshable {
            await viewModel.refresh()
        }
        .toolbar(.hidden, for: .navigationBar)
        .navigationBarBackButtonHidden(true)
        .background(SwipeBackEnabler())
        .overlay(alignment: .top) {
            DetailHeaderBar(
                title: isHeaderTitleVisible ? "Recording" : displayAlbumTitle,
                height: headerHeight,
                overscroll: headerOverscroll,
                onBack: { dismiss() }
            ) {
                DetailCircleButton(
                    systemName: "checkmark.seal",
                    accessibilityLabel: "Authority recommendations",
                    action: { showingAuthoritySheet = true }
                )
            }
        }
        .alert("Sign In Required", isPresented: $viewModel.showingLoginAlert) {
            Button("OK", role: .cancel) { }
        } message: {
            Text("Please sign in to favorite recordings.")
        }
        .sheet(item: $reportingInfo) { info in
            ReportLinkIssueView(
                entityType: "recording",
                entityId: recordingId,
                entityName: recording?.albumTitle ?? "Unknown Album",
                externalSource: info.source,
                externalUrl: info.url,
                onSubmit: { explanation in
                    submitLinkReport(
                        entityType: "recording",
                        entityId: recordingId,
                        entityName: recording?.albumTitle ?? "Unknown Album",
                        externalSource: info.source,
                        externalUrl: info.url,
                        explanation: explanation
                    )
                    reportingInfo = nil
                },
                onCancel: {
                    reportingInfo = nil
                }
            )
        }
        .sheet(isPresented: $showingAuthoritySheet) {
            AuthorityRecommendationsView(
                recordingId: recordingId,
                albumTitle: recording?.albumTitle ?? "Unknown Album",
                songId: recording?.songId  // ← Add this line
            )
        }
        .sheet(isPresented: $showingContributionEditor) {
            RecordingContributionEditView(
                recordingId: recordingId,
                recordingTitle: recording?.albumTitle ?? "Recording",
                currentContribution: recording?.userContribution,
                onSave: {
                    // Refresh recording data to get updated community data
                    Task {
                        await viewModel.refresh()
                    }
                    // Notify parent view that community data changed
                    onCommunityDataChanged?()
                }
            )
            .environmentObject(authManager)
        }
        .alert("Report Submitted", isPresented: $showingSubmissionAlert) {
            Button("OK", role: .cancel) { }
        } message: {
            Text(submissionAlertMessage)
        }
        .task {
            viewModel.configure(
                recordingId: recordingId,
                authManager: authManager,
                favoritesManager: favoritesManager
            )
            #if DEBUG
            if ProcessInfo.processInfo.environment["XCODE_RUNNING_FOR_PREVIEWS"] == "1" {
                viewModel.loadPreview()
                return
            }
            #endif

            await viewModel.load()
        }
    }
    
    // MARK: - Submit link report to API
    private func submitLinkReport(entityType: String, entityId: String, entityName: String, externalSource: String, externalUrl: String, explanation: String) {
        Task {
            do {
                let success = try await ContentService.submitContentReport(
                    entityType: entityType,
                    entityId: entityId,
                    entityName: entityName,
                    externalSource: externalSource,
                    externalUrl: externalUrl,
                    explanation: explanation
                )

                if success {
                    submissionAlertMessage = "Thank you for your report. We will review it shortly."
                } else {
                    submissionAlertMessage = "Failed to submit report. Please try again later."
                }
                showingSubmissionAlert = true

            } catch {
                submissionAlertMessage = "Failed to submit report: \(error.localizedDescription)"
                showingSubmissionAlert = true
            }
        }
    }
    
    // MARK: - Releases Section
    
    @ViewBuilder
    private func releasesSection(_ releases: [Release]) -> some View {
        VStack(alignment: .leading, spacing: 12) {
            HStack {
                Text("Also Available On")
                    .font(ApproachNoteTheme.headline())
                    .foregroundColor(ApproachNoteTheme.textPrimary)
                
                Spacer()
            }
            
            let displayedReleases = showAllReleases ? releases : Array(releases.prefix(maxReleasesToShow))
            
            ForEach(displayedReleases) { release in
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
                    releaseRow(release, isSelected: selectedReleaseId == release.id)
                }
                .buttonStyle(.plain)
            }
            
            // Show more/less button
            if releases.count > maxReleasesToShow {
                Button {
                    withAnimation {
                        showAllReleases.toggle()
                    }
                } label: {
                    HStack {
                        Text(showAllReleases ? "Show Less" : "Show All \(releases.count.formatted()) Releases")
                            .font(ApproachNoteTheme.subheadline())
                        Image(systemName: showAllReleases ? "chevron.up" : "chevron.down")
                            .font(ApproachNoteTheme.caption())
                    }
                    .foregroundColor(ApproachNoteTheme.brand)
                }
                .padding(.top, 4)
            }
        }
        .padding()
        .background(ApproachNoteTheme.surface)
        .cornerRadius(10)
        .padding(.horizontal, 20)
    }

    @ViewBuilder
    private func releaseRow(_ release: Release, isSelected: Bool) -> some View {
        HStack(alignment: .top, spacing: 12) {
            // Selection indicator
            if isSelected {
                Image(systemName: "checkmark.circle.fill")
                    .font(ApproachNoteTheme.title3())
                    .foregroundColor(ApproachNoteTheme.brand)
            } else {
                Image(systemName: "circle")
                    .font(ApproachNoteTheme.title3())
                    .foregroundColor(ApproachNoteTheme.textSecondary.opacity(0.5))
            }
            
            // Cover art or placeholder
            if let artUrl = release.coverArtSmall, let url = URL(string: artUrl) {
                CachedAsyncImage(
                    url: url,
                    content: { image in
                        image
                            .resizable()
                            .aspectRatio(contentMode: .fill)
                            .frame(width: 50, height: 50)
                            .clipped()
                            .cornerRadius(4)
                    },
                    placeholder: {
                        Rectangle()
                            .fill(ApproachNoteTheme.textSecondary.opacity(0.3))
                            .frame(width: 50, height: 50)
                            .cornerRadius(4)
                    }
                )
                .overlay(
                    RoundedRectangle(cornerRadius: 4)
                        .stroke(isSelected ? ApproachNoteTheme.brand : Color.clear, lineWidth: 2)
                )
            } else {
                Rectangle()
                    .fill(ApproachNoteTheme.textSecondary.opacity(0.2))
                    .frame(width: 50, height: 50)
                    .cornerRadius(4)
                    .overlay(
                        Image(systemName: "opticaldisc")
                            .foregroundColor(ApproachNoteTheme.textSecondary)
                    )
                    .overlay(
                        RoundedRectangle(cornerRadius: 4)
                            .stroke(isSelected ? ApproachNoteTheme.brand : Color.clear, lineWidth: 2)
                    )
            }
            
            VStack(alignment: .leading, spacing: 4) {
                // Release title
                Text(release.title)
                    .font(ApproachNoteTheme.subheadline())
                    .fontWeight(isSelected ? .bold : .medium)
                    .foregroundColor(isSelected ? ApproachNoteTheme.brand : ApproachNoteTheme.textPrimary)
                    .lineLimit(2)
                
                // Artist and year
                HStack(spacing: 4) {
                    if let artist = release.artistCredit {
                        Text(artist)
                            .lineLimit(1)
                    }
                    if release.releaseYear != nil && release.artistCredit != nil {
                        Text("•")
                    }
                    if let year = release.releaseYear {
                        Text(String(year))
                    }
                }
                .font(ApproachNoteTheme.caption())
                .foregroundColor(ApproachNoteTheme.textSecondary)
                
                // Track position
                if let trackPos = release.trackPositionDisplay {
                    Text(trackPos)
                        .font(ApproachNoteTheme.caption2())
                        .foregroundColor(ApproachNoteTheme.textSecondary)
                }
                
                // Format badge
                if let format = release.formatName {
                    Text(format)
                        .font(ApproachNoteTheme.caption2())
                        .padding(.horizontal, 6)
                        .padding(.vertical, 2)
                        .background(ApproachNoteTheme.textSecondary.opacity(0.2))
                        .cornerRadius(4)
                        .foregroundColor(ApproachNoteTheme.textPrimary)
                }
            }
            
            Spacer()
            
            // Spotify indicator (not a link - the whole row is tappable)
            if release.spotifyTrackUrl != nil || release.spotifyAlbumUrl != nil {
                Image(systemName: "music.note")
                    .font(ApproachNoteTheme.title3())
                    .foregroundColor(ApproachNoteTheme.accent)
            }
        }
        .padding(.vertical, 8)
        .padding(.horizontal, 8)
        .background(isSelected ? ApproachNoteTheme.brand.opacity(0.1) : Color.clear)
        .cornerRadius(8)
    }
    
    // MARK: - Streaming Services Indicator

    // MARK: - Favorite Control
    // Relocated from the nav bar into the body (issue #198); the custom header
    // carries only back + authority.
    private var favoriteControl: some View {
        Button {
            viewModel.handleFavoriteTap()
        } label: {
            HStack(spacing: 6) {
                Image(systemName: isFavorited ? "heart.fill" : "heart")
                    .foregroundColor(isFavorited ? .red : ApproachNoteTheme.textSecondary)
                Text(displayFavoriteCount > 0
                     ? "\(displayFavoriteCount) \(displayFavoriteCount == 1 ? "favorite" : "favorites")"
                     : "Favorite")
                    .font(ApproachNoteTheme.subheadline())
                    .foregroundColor(ApproachNoteTheme.textSecondary)
            }
        }
        .buttonStyle(.plain)
        .padding(.top, 8)
    }

    private var streamingServicesIndicator: some View {
        HStack(spacing: 12) {
            Text("Listen on")
                .font(ApproachNoteTheme.caption())
                .foregroundColor(ApproachNoteTheme.textSecondary)

            ForEach(availableStreamingSources, id: \.url) { source in
                Button {
                    if let url = URL(string: source.url) {
                        openURL(url)
                    }
                } label: {
                    StreamingIcon(service: source.service, size: 44)
                }
                .buttonStyle(.plain)
                .accessibilityLabel(source.name)
            }

            Spacer()
        }
        .padding(.top, 8)
    }

    // MARK: - Album Art Placeholder

    private var albumArtPlaceholder: some View {
        Rectangle()
            .fill(ApproachNoteTheme.surface)
            .frame(maxWidth: .infinity)
            .aspectRatio(1, contentMode: .fit)
            .cornerRadius(12)
            .overlay(
                Image(systemName: "music.note")
                    .font(.system(size: 80))
                    .foregroundColor(ApproachNoteTheme.textSecondary)
            )
    }
    
    // MARK: - Recording Metadata Block (surfaced on main page)

    @ViewBuilder
    private func recordingMetadataBlock(_ recording: Recording) -> some View {
        let hasMetadata = displayReleaseYear != nil ||
                          recording.recordingDate != nil ||
                          displayLabel != nil ||
                          recording.notes != nil ||
                          recording.musicbrainzId != nil

        if hasMetadata {
            VStack(alignment: .leading, spacing: 4) {
                if let year = displayReleaseYear {
                    metadataLine(label: "RELEASE YEAR", value: String(year))
                }
                if let date = recording.recordingDate {
                    metadataLine(label: "RECORDED", value: date)
                }
                if let label = displayLabel {
                    metadataLine(label: "LABEL", value: label)
                }
                if let notes = recording.notes {
                    Text(notes)
                        .font(ApproachNoteTheme.body())
                        .bodyLineSpacing()
                        .foregroundColor(ApproachNoteTheme.textPrimary)
                        .fixedSize(horizontal: false, vertical: true)
                        .padding(.top, 4)
                }
                if let mbId = recording.musicbrainzId,
                   let mbUrl = URL(string: "https://musicbrainz.org/recording/\(mbId)") {
                    Link("Learn more on MusicBrainz", destination: mbUrl)
                        .font(ApproachNoteTheme.body())
                        .bodyLineSpacing()
                        .foregroundColor(ApproachNoteTheme.brand)
                        .padding(.top, 4)
                }
            }
            .padding(.top, 4)
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
}

#Preview("Recording Detail - Full") {
    NavigationStack {
        RecordingDetailView(recordingId: "preview-recording-1")
            .environmentObject(AuthenticationManager())
            .environmentObject(FavoritesManager())
    }
}
#Preview("Recording Detail - Minimal") {
    NavigationStack {
        RecordingDetailView(recordingId: "preview-recording-3")
            .environmentObject(AuthenticationManager())
            .environmentObject(FavoritesManager())
    }
}
