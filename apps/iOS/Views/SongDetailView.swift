//
//  SongDetailView.swift
//  Approach Note
//
//  UPDATED: Replaced alert with toast notification for song queue confirmation
//  FIXED: Broken up body to avoid type-checker timeout
//  UPDATED: Grouped Structure, Learn More, and References into collapsible Summary Information section
//  FIXED: Recording sort order now consistently passed to all API calls
//  UPDATED: Replaced horizontal swipe navigation with explicit prev/next buttons (#109)
//

import SwiftUI
import Combine

// MARK: - Song Detail View
struct SongDetailView: View {
    let songId: String

    // Shared data + network state lives on the view model; layout/presentation
    // state stays here.
    @StateObject private var viewModel = SongDetailViewModel()

    // NEW: Repertoire management
    @EnvironmentObject var repertoireManager: RepertoireManager
    @State private var showAddToRepertoireSheet = false
    @State private var showErrorAlert = false
    @State private var alertMessage = ""
    @State private var isAddingToRepertoire = false

    // Song refresh management
    @State private var showRefreshConfirmation = false

    // NEW: Toast notification
    @State private var toast: ToastItem?

    // Read-only aliases so existing reference sites in this view can keep
    // using the short names unchanged.
    private var song: Song? { viewModel.song }
    private var isLoading: Bool { viewModel.isLoading }
    private var transcriptions: [SoloTranscription] { viewModel.transcriptions }
    private var backingTracks: [Video] { viewModel.backingTracks }
    private var isRefreshing: Bool { viewModel.isRefreshing }
    private var researchStatus: SongResearchStatus { viewModel.researchStatus }
    private var recordingSortOrder: RecordingSortOrder { viewModel.sortOrder }
    private var isRecordingsReloading: Bool { viewModel.isRecordingsReloading }
    private var isRecordingsLoading: Bool { viewModel.isRecordingsLoading }
    private var canQueueForRefresh: Bool { viewModel.canQueueForRefresh }

    // MARK: - Initializer
    init(songId: String) {
        self.songId = songId
    }

    // MARK: - Song Refresh

    /// Queue song for background research and show a toast with the result.
    private func refreshSongData(forceRefresh: Bool) {
        let refreshType = forceRefresh ? "full" : "quick"
        Task {
            let success = await viewModel.queueRefresh(songId: songId, forceRefresh: forceRefresh)
            if success {
                toast = ToastItem(
                    type: .success,
                    message: "Song queued for \(refreshType) refresh. Data will be updated in the background."
                )
            } else {
                toast = ToastItem(
                    type: .error,
                    message: "Failed to queue song for refresh. Please try again."
                )
            }
        }
    }

    // MARK: - Research Status Indicator

    /// Visual indicator showing research queue status
    @ViewBuilder
    private var researchStatusIndicator: some View {
        switch researchStatus {
        case .currentlyResearching(let progress):
            ResearchStatusBanner(
                icon: "waveform.circle.fill",
                iconColor: ApproachNoteTheme.brand,
                title: "Researching Now",
                message: viewModel.researchingMessage(progress: progress),
                helperText: "We're scouring the internet to learn more about this song... Check back in a while to see what we've found.",
                isAnimating: true
            )
        case .inQueue(let position):
            ResearchStatusBanner(
                icon: "clock.fill",
                iconColor: ApproachNoteTheme.accent,
                title: "In Research Queue",
                message: "Position \(position) in queue",
                helperText: "This song is in the queue to get researched... Check back in a while to see what we've found.",
                isAnimating: false
            )
        case .notInQueue:
            EmptyView()
        }
    }

    // MARK: - Summary predicates (delegated to the view model)

    private func hasSummaryContent(for song: Song) -> Bool {
        viewModel.hasSummaryContent(for: song)
    }

    private func hasAuthoritativeRecordings(for song: Song) -> Bool {
        viewModel.hasAuthoritativeRecordings(for: song)
    }

    /// Whether the song has at least one external link worth surfacing in the
    /// Learn More panel (Wikipedia, Jazz Standards, or MusicBrainz).
    private func hasLearnMoreLinks(for song: Song) -> Bool {
        return song.wikipediaUrl != nil
            || song.musicbrainzId != nil
            || song.externalReferences?["jazzstandards"] != nil
    }

    // MARK: - Song Content View
    
    @ViewBuilder
    private func songContentView(for song: Song) -> some View {
        VStack(alignment: .leading, spacing: 0) {
            VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingMD) {
            // Song Information Header
            VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingSM) {
                (
                    Text(song.title)
                        .font(ApproachNoteTheme.largeTitle(weight: .bold))
                        .foregroundColor(ApproachNoteTheme.textPrimary)
                    + Text(song.composedYear.map { " (\(String($0)))" } ?? "")
                        .font(ApproachNoteTheme.largeTitle(weight: .regular))
                        .foregroundColor(ApproachNoteTheme.textSecondary)
                )
                .onLongPressGesture {
                    if canQueueForRefresh {
                        showRefreshConfirmation = true
                    }
                }

                if let composer = song.composer {
                    Text("Composed by \(composer)")
                        .font(ApproachNoteTheme.body())
                        .bodyLineSpacing()
                        .foregroundColor(ApproachNoteTheme.textPrimary)
                }

                // Song Reference (if available)
                if let songRef = song.songReference {
                    HStack(alignment: .top, spacing: ApproachNoteTheme.spacingXS) {
                        Image(systemName: "book.closed.fill")
                            .foregroundColor(ApproachNoteTheme.textSecondary)
                            .font(ApproachNoteTheme.subheadline())
                        Text(songRef)
                            .font(ApproachNoteTheme.subheadline())
                            .foregroundColor(ApproachNoteTheme.textSecondary)
                            .fixedSize(horizontal: false, vertical: true)
                    }
                    .padding(.top, ApproachNoteTheme.spacingXXS)
                }

                // MARK: - Research Status Indicator
                researchStatusIndicator

                // MARK: - Summary Information Section (Collapsible)
                if hasSummaryContent(for: song) {
                    summaryInfoSection(for: song)
                }

                // MARK: - Learn More (JazzStandards.com / MusicBrainz)
                if hasLearnMoreLinks(for: song) {
                    ExternalReferencesPanel(
                        wikipediaUrl: song.wikipediaUrl,
                        musicbrainzId: song.musicbrainzId,
                        externalReferences: song.externalReferences,
                        entityId: song.id,
                        entityName: song.title,
                        showsBackground: false
                    )
                    .padding(.top, ApproachNoteTheme.spacingXS)
                }

                // MARK: - Authoritative Recordings Carousel
                if hasAuthoritativeRecordings(for: song) {
                    authoritativeRecordingsSection(for: song)
                }
            }
            .padding(.horizontal, ApproachNoteTheme.spacingXL)
            .padding(.top, ApproachNoteTheme.spacingXL)
            .padding(.bottom, ApproachNoteTheme.spacingMD)

            // MARK: - RECORDINGS SECTION
                RecordingsSection(
                    recordings: song.recordings ?? [],
                    parentSongTitle: song.title,
                    recordingSortOrder: $viewModel.sortOrder,
                    isReloading: isRecordingsReloading || isRecordingsLoading,
                    onSortOrderChanged: { [self] _ in
                        Task { await viewModel.reloadRecordings(songId: songId) }
                    },
                    onCommunityDataChanged: {
                        Task { await viewModel.reloadRecordings(songId: songId) }
                    },
                    onRequestHydration: { [weak viewModel] id in
                        viewModel?.requestHydration(for: id)
                    }
                )
            // MARK: - TRANSCRIPTIONS SECTION
            TranscriptionsSection(transcriptions: transcriptions)

            // MARK: - BACKING TRACKS SECTION
            BackingTracksSection(videos: backingTracks)
        }
        .padding(.bottom)
        }
    }
    
    // MARK: - Summary Information Section

    /// Collapsed-height cap for the Wikipedia intro (~5-6 lines) before the
    /// in-app "Read more" toggle reveals the rest.
    private static let summaryCollapsedHeight: CGFloat = 160

    @ViewBuilder
    private func summaryInfoSection(for song: Song) -> some View {
        if let structure = song.structure, !structure.isEmpty {
            VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingSM) {
                // The stored Wikipedia intro keeps its source paragraph breaks
                // (newline-separated); ExpandableProse renders them as discrete
                // paragraphs and caps the height with an in-app "Read more"
                // toggle so the intro opens up without the page running long.
                ExpandableProse(
                    text: structure,
                    maxCollapsedHeight: Self.summaryCollapsedHeight,
                    textColor: ApproachNoteTheme.textPrimary
                )
                // The Wikipedia link now lives in the Learn More panel below as
                // a peer to the Jazz Standards and MusicBrainz buttons.
            }
            .frame(maxWidth: .infinity, alignment: .leading)
            .padding(.top, ApproachNoteTheme.spacingXXS)
        }
    }
    
    // MARK: - Authoritative Recordings Carousel Section
    // Uses featuredRecordings from summary endpoint (already filtered server-side)
    @ViewBuilder
    private func authoritativeRecordingsSection(for song: Song) -> some View {
        VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingMD) {
            // Header
            Text("FEATURED RECORDINGS")
                .font(ApproachNoteTheme.title2())
                .bold()
                .foregroundColor(ApproachNoteTheme.textPrimary)

            // Introductory text
            Text("Take a look at these important recordings for this song.")
                .font(ApproachNoteTheme.subheadline())
                .foregroundColor(ApproachNoteTheme.textSecondary)

            // Horizontal scrolling carousel - use featuredRecordings from summary
            let featured = song.featuredRecordings ?? []
            let carouselHasAnyDistinctTitle = featured.contains { recording in
                recording.displayTitle(comparedTo: song.title) != nil
            }
            ScrollView(.horizontal, showsIndicators: false) {
                HStack(alignment: .top, spacing: ApproachNoteTheme.spacingLG) {
                    ForEach(featured) { recording in
                        NavigationLink(destination: RecordingDetailView(
                            recordingId: recording.id,
                            onCommunityDataChanged: {
                                Task { await viewModel.reloadRecordings(songId: songId) }
                            }
                        )) {
                            AuthoritativeRecordingCard(
                                recording: recording,
                                parentSongTitle: song.title,
                                shelfHasAnyDistinctTitle: carouselHasAnyDistinctTitle
                            )
                        }
                        .buttonStyle(.plain)
                    }
                }
                // Leading/trailing inset aligns the first card with the gutter;
                // cards bleed past the edges as you scroll (issue #200).
                .padding(.horizontal, ApproachNoteTheme.spacingXL)
            }
            // Cancel the section's 24pt gutter so the scroll view spans full width.
            .padding(.horizontal, -ApproachNoteTheme.spacingXL)
        }
        .padding(.top, ApproachNoteTheme.spacingMD)
    }
    
    // MARK: - Body (broken into smaller chunks to avoid type-checker timeout)
    
    var body: some View {
        contentView
    }
    
    // MARK: - View Builders
    
    private var contentView: some View {
        mainScrollView
            .collapsingDetailHeader(
                expandedTitle: "Song",
                collapsedTitle: song?.title ?? "Song"
            ) {
                DetailCircleButton(
                    systemName: "plus",
                    accessibilityLabel: "Add to repertoire",
                    action: { showAddToRepertoireSheet = true }
                )
            }
            .task {
                await viewModel.load(songId: songId)
            }
            .onReceive(NotificationCenter.default.publisher(for: .transcriptionCreated)) { notification in
                if let notifiedId = notification.userInfo?["songId"] as? String,
                   notifiedId == songId {
                    Task { await viewModel.load(songId: songId) }
                }
            }
            .onReceive(NotificationCenter.default.publisher(for: .videoCreated)) { notification in
                if let notifiedId = notification.userInfo?["songId"] as? String,
                   notifiedId == songId {
                    Task { await viewModel.refreshBackingTracks(songId: songId) }
                }
            }
            .sheet(isPresented: $showAddToRepertoireSheet) {
                repertoireSheet
            }
            .alert("Error", isPresented: $showErrorAlert) {
                Button("OK", role: .cancel) { }
            } message: {
                Text(alertMessage)
            }
            .confirmationDialog(
                "Refresh Song Data",
                isPresented: $showRefreshConfirmation,
                titleVisibility: .visible
            ) {
                Button("Quick Refresh") {
                    refreshSongData(forceRefresh: false)
                }
                Button("Full Refresh") {
                    refreshSongData(forceRefresh: true)
                }
                Button("Cancel", role: .cancel) { }
            } message: {
                Text("Quick refresh uses cached data for faster results. Full refresh re-fetches everything from external sources.")
            }
            .toast($toast)
            .onDisappear {
                viewModel.stopResearchStatusPolling()
            }
    }
    
    private var mainScrollView: some View {
        ScrollView {
            VStack(spacing: 0) {
                DetailHeaderSpacer()

                if isLoading {
                    loadingView
                } else if let song = song {
                    songContentView(for: song)
                } else {
                    notFoundView
                }
            }
        }
        .refreshable {
            await viewModel.forceRefresh(songId: songId)
        }
    }
    
    private var loadingView: some View {
        VStack {
            Spacer()
            ProgressView()
                .progressViewStyle(CircularProgressViewStyle(tint: ApproachNoteTheme.brand))
                .scaleEffect(1.5)
            Spacer()
        }
        .frame(maxWidth: .infinity, minHeight: 300)
    }
    
    private var notFoundView: some View {
        VStack(spacing: ApproachNoteTheme.spacingMD) {
            Image(systemName: "exclamationmark.triangle")
                .font(.system(size: 50))
                .foregroundColor(ApproachNoteTheme.accent)
            Text("Unable to load song")
                .font(ApproachNoteTheme.headline())
                .foregroundColor(ApproachNoteTheme.textPrimary)
            Text("There was a problem loading the song details.")
                .font(ApproachNoteTheme.subheadline())
                .foregroundColor(ApproachNoteTheme.textSecondary)
                .multilineTextAlignment(.center)
                .padding(.horizontal)
        }
        .frame(maxWidth: .infinity, minHeight: 300)
    }


    private var repertoireSheet: some View {
        AddToRepertoireSheet(
            songId: songId,
            songTitle: song?.title ?? "Unknown",
            repertoireManager: repertoireManager,
            isPresented: $showAddToRepertoireSheet,
            onSuccess: { message in
                toast = ToastItem(type: .success, message: message)
            },
            onError: { message in
                alertMessage = message
                showErrorAlert = true
            }
        )
    }
    
}

// MARK: - Authoritative Recording Card
struct AuthoritativeRecordingCard: View {
    let recording: Recording
    var parentSongTitle: String? = nil
    /// True when at least one card in the surrounding carousel has a
    /// distinct title. Set by the carousel so cards align in height
    /// without paying for an unused title line when none has one.
    var shelfHasAnyDistinctTitle: Bool = false

    private let artworkSize: CGFloat = 204

    // Get artist name - prefer artist_credit from default release, fall back to performers
    private var artistName: String {
        // Use artist_credit from the default release if available
        if let artistCredit = recording.artistCredit, !artistCredit.isEmpty {
            return artistCredit
        }
        // Fall back to performers lookup
        if let performers = recording.performers {
            // First try to find a performer with "leader" role
            if let leader = performers.first(where: { $0.role?.lowercased() == "leader" }) {
                return leader.name
            }
            // Fall back to first performer if no leader
            if let first = performers.first {
                return first.name
            }
        }
        return "Various Artists"
    }

    // Front cover URL
    private var frontCoverUrl: String? {
        recording.bestAlbumArtLarge ?? recording.bestAlbumArtMedium
    }

    var body: some View {
        VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingSM) {
            // Album Art
            Group {
                if let frontUrl = frontCoverUrl {
                    AsyncImage(url: URL(string: frontUrl)) { phase in
                        switch phase {
                        case .empty:
                            Rectangle()
                                .fill(ApproachNoteTheme.textSecondary.opacity(0.2))
                                .overlay {
                                    ProgressView()
                                        .tint(ApproachNoteTheme.brand)
                                }
                        case .success(let image):
                            image
                                .resizable()
                                .aspectRatio(contentMode: .fill)
                        case .failure:
                            Rectangle()
                                .fill(ApproachNoteTheme.textSecondary.opacity(0.2))
                                .overlay {
                                    Image(systemName: "music.note")
                                        .font(.system(size: 40))
                                        .foregroundColor(ApproachNoteTheme.textSecondary)
                                }
                        @unknown default:
                            EmptyView()
                        }
                    }
                } else {
                    Rectangle()
                        .fill(ApproachNoteTheme.textSecondary.opacity(0.2))
                        .overlay {
                            Image(systemName: "music.note")
                                .font(.system(size: 40))
                                .foregroundColor(ApproachNoteTheme.textSecondary)
                        }
                }
            }
            .frame(width: artworkSize, height: artworkSize)
            .clipShape(RoundedRectangle(cornerRadius: 12))
            .shadow(color: .black.opacity(0.15), radius: 8, x: 0, y: 4)

            // Recording Info - fixed height for consistent card sizing
            VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingXXS) {
                // Year
                Text(recording.recordingYear.map { String($0) } ?? " ")
                    .font(ApproachNoteTheme.subheadline(weight: .bold))
                    .foregroundColor(ApproachNoteTheme.textPrimary)

                // Artist
                Text(artistName)
                    .font(ApproachNoteTheme.subheadline(weight: .bold))
                    .foregroundColor(ApproachNoteTheme.textPrimary)
                    .lineLimit(1)

                // Album — wraps naturally to 1-2 lines so the song title
                // below can pull up when the album fits on one line.
                Text(recording.albumTitle ?? "Unknown Album")
                    .font(ApproachNoteTheme.subheadline())
                    .foregroundColor(ApproachNoteTheme.textPrimary)
                    .lineLimit(2)

                // Recording title — only allocated when some card in the
                // carousel has a distinct title. Cards without one render
                // an empty placeholder so card heights stay aligned.
                if shelfHasAnyDistinctTitle {
                    Text(recording.displayTitle(comparedTo: parentSongTitle).map { "(\($0))" } ?? " ")
                        .font(ApproachNoteTheme.caption(italic: true))
                        .foregroundColor(ApproachNoteTheme.textSecondary)
                        .lineLimit(1, reservesSpace: true)
                }
            }
            .frame(width: artworkSize, alignment: .topLeading)
        }
    }
}

// MARK: - Previews
#Preview("Song Detail - Full") {
    NavigationStack {
        SongDetailView(songId: "preview-song-1")
            .environmentObject(RepertoireManager())
    }
}

#Preview("Song Detail - Minimal") {
    NavigationStack {
        SongDetailView(songId: "preview-song-2")
            .environmentObject(RepertoireManager())
    }
}
