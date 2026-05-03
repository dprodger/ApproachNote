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
    @State private var showingStreamingPicker = false
    @State private var isLearnMoreExpanded = false
    @State private var showingBackCover = false
    @State private var showingContributionEditor = false
    private let maxReleasesToShow = 5

    // Read-only aliases so the dozens of existing reference sites in this
    // view can keep using the short names unchanged.
    private var recording: Recording? { viewModel.recording }
    private var isLoading: Bool { viewModel.isLoading }
    private var selectedReleaseId: String? { viewModel.selectedReleaseId }
    private var localFavoriteCount: Int? { viewModel.localFavoriteCount }
    @Environment(\.openURL) var openURL
    @AppStorage("preferredStreamingService") private var preferredStreamingService: String = StreamingService.spotify.rawValue
    
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
            if isLoading {
                VStack {
                    Spacer()
                    ThemedProgressView(message: "Loading...", tintColor: ApproachNoteTheme.brass)
                    Spacer()
                }
                .frame(maxWidth: .infinity, maxHeight: .infinity)
                .background(ApproachNoteTheme.backgroundLight)
            } else if let recording = recording {
                VStack(alignment: .leading, spacing: 0) {
                    // Styled Header with Jazz Theme
                    HStack {
                        Image(systemName: "opticaldisc")
                            .font(ApproachNoteTheme.title2())
                            .foregroundColor(ApproachNoteTheme.cream)
                        Text("RECORDING")
                            .font(ApproachNoteTheme.headline())
                            .fontWeight(.semibold)
                            .foregroundColor(ApproachNoteTheme.cream)
                        Spacer()
                    }
                    .padding()
                    .background(ApproachNoteTheme.brassGradient)
                    
                    VStack(alignment: .leading, spacing: 20) {
                        // Album Information
                        VStack(alignment: .leading, spacing: 12) {
                            // Album artwork with play button overlay and flip support
                            ZStack(alignment: .topTrailing) {
                                // Card-flip container
                                ZStack {
                                    // Front cover
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
                                                    .fill(ApproachNoteTheme.cardBackground)
                                                    .aspectRatio(1, contentMode: .fit)
                                                    .cornerRadius(12)
                                                    .overlay(
                                                        ProgressView()
                                                            .tint(ApproachNoteTheme.brass)
                                                    )
                                            }
                                        )
                                        .opacity(showingBackCover ? 0 : 1)
                                    } else {
                                        albumArtPlaceholder
                                            .opacity(showingBackCover ? 0 : 1)
                                    }

                                    // Back cover (pre-rotated so it appears correct after flip)
                                    if let backUrl = displayBackCoverArtLarge {
                                        CachedAsyncImage(
                                            url: URL(string: backUrl),
                                            content: { image in
                                                image
                                                    .resizable()
                                                    .aspectRatio(contentMode: .fit)
                                                    .frame(maxWidth: .infinity)
                                                    .cornerRadius(12)
                                            },
                                            placeholder: {
                                                Rectangle()
                                                    .fill(ApproachNoteTheme.cardBackground)
                                                    .aspectRatio(1, contentMode: .fit)
                                                    .cornerRadius(12)
                                                    .overlay(
                                                        ProgressView()
                                                            .tint(ApproachNoteTheme.brass)
                                                    )
                                            }
                                        )
                                        .rotation3DEffect(.degrees(180), axis: (x: 0, y: 1, z: 0))
                                        .opacity(showingBackCover ? 1 : 0)
                                    }
                                }
                                .rotation3DEffect(
                                    .degrees(showingBackCover ? 180 : 0),
                                    axis: (x: 0, y: 1, z: 0)
                                )

                                // Play button overlay (only on front)
                                if hasStreamingSource && !showingBackCover {
                                    playButtonOverlay
                                }

                                // Flip button badge (shown when back cover available)
                                if canFlipToBackCover {
                                    Button(action: {
                                        withAnimation(.easeInOut(duration: 0.4)) {
                                            showingBackCover.toggle()
                                        }
                                    }) {
                                        Image(systemName: showingBackCover ? "arrow.uturn.backward" : "arrow.trianglehead.2.clockwise.rotate.90")
                                            .foregroundColor(.white)
                                            .font(.system(size: 14, weight: .semibold))
                                            .padding(10)
                                            .background(Color.black.opacity(0.6))
                                            .clipShape(Circle())
                                    }
                                    .buttonStyle(PlainButtonStyle())
                                    .padding(12)
                                }

                                // Source badge (shows front or back cover source)
                                VStack {
                                    Spacer()
                                    HStack {
                                        if showingBackCover {
                                            AlbumArtSourceBadge(
                                                source: displayBackCoverSource,
                                                sourceUrl: displayBackCoverSourceUrl
                                            )
                                            .padding(8)
                                        } else {
                                            AlbumArtSourceBadge(
                                                source: displayAlbumArtSource,
                                                sourceUrl: displayAlbumArtSourceUrl
                                            )
                                            .padding(8)
                                        }
                                        Spacer()
                                    }
                                }
                            }
                            .shadow(radius: 8)
                            .animation(.easeInOut(duration: 0.3), value: selectedReleaseId)
                            
                            // Recording Name (Year)
                            HStack {
                                if recording.isCanonical == true {
                                    Image(systemName: "star.fill")
                                        .foregroundColor(ApproachNoteTheme.gold)
                                        .font(ApproachNoteTheme.title2())
                                }
                                if let songTitle = recording.songTitle {
                                    let yearSuffix = recording.recordingYear.map { " (\($0))" } ?? ""
                                    Text("\(songTitle)\(yearSuffix)")
                                        .font(ApproachNoteTheme.largeTitle())
                                        .bold()
                                        .foregroundColor(ApproachNoteTheme.charcoal)
                                }
                            }

                            // Recording title (when different from song title)
                            if let recordingTitle = recording.displayTitle {
                                Text("as \"\(recordingTitle)\"")
                                    .font(ApproachNoteTheme.subheadline())
                                    .italic()
                                    .foregroundColor(ApproachNoteTheme.brass)
                            }

                            // Release Name
                            Text(displayAlbumTitle)
                                .font(ApproachNoteTheme.title2())
                                .foregroundColor(ApproachNoteTheme.smokeGray)
                                .animation(.easeInOut(duration: 0.3), value: selectedReleaseId)

                            if let composer = recording.composer {
                                Text("Composed by \(composer)")
                                    .font(ApproachNoteTheme.subheadline())
                                    .foregroundColor(ApproachNoteTheme.smokeGray)
                            }

                            // Streaming services indicator
                            if hasStreamingSource {
                                streamingServicesIndicator
                            }
                        }
                        .padding()
                        
                        // Recording Details Section (Collapsible)
                        learnMoreSection(recording)
                        
                        // Releases Section - shows all releases containing this recording
                        if let releases = recording.releases, releases.count > 1 {
                            releasesSection(releases)
                        }
                        
                        Divider()
                            .padding(.horizontal)
                        
                        // Performers Section
                        VStack(alignment: .leading, spacing: 12) {
                            HStack {
                                Text("Performers")
                                    .font(ApproachNoteTheme.title2())
                                    .bold()
                                    .foregroundColor(ApproachNoteTheme.charcoal)
                                
                                // Indicator when showing release-specific performers
                                if selectedRelease != nil, let releasePerformers = selectedRelease?.performers, !releasePerformers.isEmpty {
                                    Text("(from selected release)")
                                        .font(ApproachNoteTheme.caption())
                                        .foregroundColor(ApproachNoteTheme.smokeGray)
                                }
                            }
                            .padding(.horizontal)
                            
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
                                    .foregroundColor(ApproachNoteTheme.smokeGray)
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
                                        .foregroundColor(ApproachNoteTheme.charcoal)
                                }
                                .padding(.horizontal)

                                ScrollView(.horizontal, showsIndicators: false) {
                                    HStack(spacing: 12) {
                                        ForEach(favoritedBy) { user in
                                            VStack(spacing: 4) {
                                                Circle()
                                                    .fill(ApproachNoteTheme.brass.opacity(0.2))
                                                    .frame(width: 40, height: 40)
                                                    .overlay(
                                                        Text(String((user.displayName ?? "?").prefix(1)).uppercased())
                                                            .font(ApproachNoteTheme.headline())
                                                            .foregroundColor(ApproachNoteTheme.brass)
                                                    )
                                                Text(user.displayName ?? "User")
                                                    .font(ApproachNoteTheme.caption())
                                                    .foregroundColor(ApproachNoteTheme.smokeGray)
                                                    .lineLimit(1)
                                            }
                                            .frame(width: 60)
                                        }
                                    }
                                    .padding(.horizontal)
                                }
                            }
                            .padding(.vertical, 8)
                        }

                        // Transcriptions Section
                        if let transcriptions = recording.transcriptions, !transcriptions.isEmpty {
                            TranscriptionsSection(transcriptions: transcriptions)
                        }
                    }
                    .padding(.vertical)
                }
            } else {
                VStack {
                    Spacer()
                    Text("Recording not found")
                        .foregroundColor(ApproachNoteTheme.smokeGray)
                    Spacer()
                }
                .frame(maxWidth: .infinity, maxHeight: .infinity)
                .background(ApproachNoteTheme.backgroundLight)
            }
        }
        .background(ApproachNoteTheme.backgroundLight)
        .refreshable {
            await viewModel.refresh()
        }
        .jazzNavigationBar(title: displayAlbumTitle, color: ApproachNoteTheme.brass)
        .toolbar {
            ToolbarItem(placement: .navigationBarTrailing) {
                HStack(spacing: 16) {
                    // Favorite button
                    Button {
                        viewModel.handleFavoriteTap()
                    } label: {
                        HStack(spacing: 4) {
                            Image(systemName: isFavorited ? "heart.fill" : "heart")
                                .foregroundColor(isFavorited ? .red : ApproachNoteTheme.smokeGray)
                            if displayFavoriteCount > 0 {
                                Text("\(displayFavoriteCount)")
                                    .font(ApproachNoteTheme.caption())
                                    .foregroundColor(ApproachNoteTheme.smokeGray)
                            }
                        }
                    }

                    // Authority recommendations button
                    Button {
                        showingAuthoritySheet = true
                    } label: {
                        Image(systemName: "checkmark.seal")
                    }
                }
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
                    .foregroundColor(ApproachNoteTheme.charcoal)
                
                Spacer()
                
                // Spotify count badge
                let spotifyCount = releases.filter { $0.hasSpotify }.count
                if spotifyCount > 0 {
                    HStack(spacing: 4) {
                        Image(systemName: "music.note")
                            .font(ApproachNoteTheme.caption())
                            .foregroundColor(ApproachNoteTheme.teal)
                        Text("\(spotifyCount)")
                            .font(ApproachNoteTheme.caption())
                            .foregroundColor(ApproachNoteTheme.smokeGray)
                    }
                }
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
                        // Reset back cover view when changing releases
                        showingBackCover = false
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
                    .foregroundColor(ApproachNoteTheme.burgundy)
                }
                .padding(.top, 4)
            }
        }
        .padding()
        .background(ApproachNoteTheme.cardBackground)
        .cornerRadius(10)
        .padding(.horizontal)
    }
    
    @ViewBuilder
    private func releaseRow(_ release: Release, isSelected: Bool) -> some View {
        HStack(alignment: .top, spacing: 12) {
            // Selection indicator
            if isSelected {
                Image(systemName: "checkmark.circle.fill")
                    .font(ApproachNoteTheme.title3())
                    .foregroundColor(ApproachNoteTheme.burgundy)
            } else {
                Image(systemName: "circle")
                    .font(ApproachNoteTheme.title3())
                    .foregroundColor(ApproachNoteTheme.smokeGray.opacity(0.5))
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
                            .fill(ApproachNoteTheme.smokeGray.opacity(0.3))
                            .frame(width: 50, height: 50)
                            .cornerRadius(4)
                    }
                )
                .overlay(
                    RoundedRectangle(cornerRadius: 4)
                        .stroke(isSelected ? ApproachNoteTheme.burgundy : Color.clear, lineWidth: 2)
                )
            } else {
                Rectangle()
                    .fill(ApproachNoteTheme.smokeGray.opacity(0.2))
                    .frame(width: 50, height: 50)
                    .cornerRadius(4)
                    .overlay(
                        Image(systemName: "opticaldisc")
                            .foregroundColor(ApproachNoteTheme.smokeGray)
                    )
                    .overlay(
                        RoundedRectangle(cornerRadius: 4)
                            .stroke(isSelected ? ApproachNoteTheme.burgundy : Color.clear, lineWidth: 2)
                    )
            }
            
            VStack(alignment: .leading, spacing: 4) {
                // Release title
                Text(release.title)
                    .font(ApproachNoteTheme.subheadline())
                    .fontWeight(isSelected ? .bold : .medium)
                    .foregroundColor(isSelected ? ApproachNoteTheme.burgundy : ApproachNoteTheme.charcoal)
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
                .foregroundColor(ApproachNoteTheme.smokeGray)
                
                // Track position
                if let trackPos = release.trackPositionDisplay {
                    Text(trackPos)
                        .font(ApproachNoteTheme.caption2())
                        .foregroundColor(ApproachNoteTheme.smokeGray)
                }
                
                // Format badge
                if let format = release.formatName {
                    Text(format)
                        .font(ApproachNoteTheme.caption2())
                        .padding(.horizontal, 6)
                        .padding(.vertical, 2)
                        .background(ApproachNoteTheme.smokeGray.opacity(0.2))
                        .cornerRadius(4)
                        .foregroundColor(ApproachNoteTheme.charcoal)
                }
            }
            
            Spacer()
            
            // Spotify indicator (not a link - the whole row is tappable)
            if release.spotifyTrackUrl != nil || release.spotifyAlbumUrl != nil {
                Image(systemName: "music.note")
                    .font(ApproachNoteTheme.title3())
                    .foregroundColor(ApproachNoteTheme.teal)
            }
        }
        .padding(.vertical, 6)
        .padding(.horizontal, 8)
        .background(isSelected ? ApproachNoteTheme.burgundy.opacity(0.1) : Color.clear)
        .cornerRadius(8)
    }
    
    // MARK: - Streaming Services Indicator

    private var streamingServicesIndicator: some View {
        HStack(spacing: 12) {
            Text("Listen on")
                .font(ApproachNoteTheme.caption())
                .foregroundColor(ApproachNoteTheme.smokeGray)

            ForEach(availableStreamingSources, id: \.url) { source in
                Button {
                    if let url = URL(string: source.url) {
                        openURL(url)
                    }
                } label: {
                    StreamingIcon(service: source.service, size: 18)
                        .padding(12)
                        .background(source.color.opacity(0.15))
                        .clipShape(Circle())
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
            .fill(ApproachNoteTheme.cardBackground)
            .frame(maxWidth: .infinity)
            .aspectRatio(1, contentMode: .fit)
            .cornerRadius(12)
            .overlay(
                Image(systemName: "music.note")
                    .font(.system(size: 80))
                    .foregroundColor(ApproachNoteTheme.smokeGray)
            )
    }
    
    // MARK: - Play Button Overlay
    
    private var playButtonOverlay: some View {
        VStack {
            Spacer()
            HStack {
                Spacer()
                
                Button {
                    handlePlayButtonTap()
                } label: {
                    ZStack {
                        Circle()
                            .fill(.ultraThinMaterial)
                            .frame(width: 70, height: 70)
                        
                        Circle()
                            .fill(ApproachNoteTheme.burgundy)
                            .frame(width: 60, height: 60)
                        
                        Image(systemName: "play.fill")
                            .font(ApproachNoteTheme.title())
                            .foregroundColor(.white)
                            .offset(x: 2) // Slight offset for visual centering
                    }
                    .shadow(color: .black.opacity(0.3), radius: 8, x: 0, y: 4)
                }
                
                Spacer()
            }
            Spacer()
        }
        .confirmationDialog(
            "Listen on",
            isPresented: $showingStreamingPicker,
            titleVisibility: .visible
        ) {
            ForEach(availableStreamingSources, id: \.url) { source in
                Button {
                    if let url = URL(string: source.url) {
                        openURL(url)
                    }
                } label: {
                    Label(source.name, systemImage: source.icon)
                }
            }
            Button("Cancel", role: .cancel) { }
        }
    }
    
    private func handlePlayButtonTap() {
        let sources = availableStreamingSources

        if sources.count == 1, let firstSource = sources.first, let url = URL(string: firstSource.url) {
            // Single source - open directly
            openURL(url)
        } else if sources.count > 1 {
            // Check if preferred service is available
            if let preferredSource = sources.first(where: { $0.service.rawValue == preferredStreamingService }),
               let url = URL(string: preferredSource.url) {
                // Preferred service available - open directly
                openURL(url)
            } else {
                // Preferred service not available - show picker
                showingStreamingPicker = true
            }
        }
    }
    
    // MARK: - Recording Details Section (Collapsible)
    
    @ViewBuilder
    private func learnMoreSection(_ recording: Recording) -> some View {
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
                        isLearnMoreExpanded.toggle()
                    }
                } label: {
                    HStack {
                        Text("Recording Details")
                            .font(ApproachNoteTheme.headline())
                            .foregroundColor(ApproachNoteTheme.charcoal)

                        Spacer()

                        Image(systemName: isLearnMoreExpanded ? "chevron.up" : "chevron.down")
                            .font(ApproachNoteTheme.subheadline())
                            .foregroundColor(ApproachNoteTheme.brass)
                    }
                    .padding()
                    .background(ApproachNoteTheme.cardBackground)
                }
                .buttonStyle(.plain)
                
                // Expandable content
                if isLearnMoreExpanded {
                    VStack(alignment: .leading, spacing: 12) {
                        if let year = recording.recordingYear {
                            DetailRow(
                                icon: "calendar",
                                label: "Year",
                                value: String(year)
                            )
                        }
                        
                        if let date = recording.recordingDate {
                            DetailRow(
                                icon: "calendar.badge.clock",
                                label: "Recorded",
                                value: date
                            )
                        }
                        
                        if let label = recording.label {
                            DetailRow(
                                icon: "music.note.house",
                                label: "Label",
                                value: label
                            )
                        }
                        
                        if let notes = recording.notes {
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
                        
                        // External References within Learn More
                        if recording.musicbrainzId != nil {
                            Divider()
                                .padding(.vertical, 4)
                            
                            ExternalReferencesPanel(
                                musicbrainzId: recording.musicbrainzId,
                                recordingId: recordingId,
                                albumTitle: recording.albumTitle ?? "Unknown Album"
                            )
                            .padding(.horizontal, -16) // Counteract parent padding
                        }
                    }
                    .padding()
                    .background(ApproachNoteTheme.cardBackground)
                }
            }
            .cornerRadius(10)
            .padding(.horizontal)
        }
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
