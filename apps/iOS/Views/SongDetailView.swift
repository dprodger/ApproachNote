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
    let allSongs: [Song]

    // Shared data + network state lives on the view model; layout/presentation
    // state stays here.
    @StateObject private var viewModel = SongDetailViewModel()

    @State private var currentSongId: String

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

    // NEW: Summary Information section expansion state (starts collapsed)
    @State private var isSummaryInfoExpanded = false

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

    // Playback preference
    @AppStorage("preferredStreamingService") private var preferredStreamingService: String = StreamingService.spotify.rawValue
    @Environment(\.openURL) private var openURL

    // MARK: - Initializer
    init(songId: String, allSongs: [Song] = []) {
        self.songId = songId
        self.allSongs = allSongs
        self._currentSongId = State(initialValue: songId)
    }
    
    // MARK: - Pager Row

    /// Previous / position / Next row shown above the song title when navigating
    /// within a list of songs. Hidden when there is only one (e.g. deep link).
    @ViewBuilder
    private var pagerRow: some View {
        if allSongs.count > 1, let index = currentIndex {
            HStack {
                Button(action: navigateToPrevious) {
                    Image(systemName: "chevron.left.circle.fill")
                        .font(.title2)
                        .foregroundColor(canNavigatePrevious ? ApproachNoteTheme.burgundy : ApproachNoteTheme.smokeGray.opacity(0.4))
                }
                .disabled(!canNavigatePrevious)

                Spacer()

                Text("\(index + 1) of \(allSongs.count)")
                    .font(ApproachNoteTheme.subheadline())
                    .foregroundColor(ApproachNoteTheme.smokeGray)

                Spacer()

                Button(action: navigateToNext) {
                    Image(systemName: "chevron.right.circle.fill")
                        .font(.title2)
                        .foregroundColor(canNavigateNext ? ApproachNoteTheme.burgundy : ApproachNoteTheme.smokeGray.opacity(0.4))
                }
                .disabled(!canNavigateNext)
            }
            .buttonStyle(.plain)
        }
    }

    // MARK: - Navigation Helpers

    private var currentIndex: Int? {
        allSongs.firstIndex { $0.id == currentSongId }
    }
    
    private var canNavigatePrevious: Bool {
        guard let index = currentIndex else { return false }
        return index > 0
    }
    
    private var canNavigateNext: Bool {
        guard let index = currentIndex else { return false }
        return index < allSongs.count - 1
    }
    
    private func navigateToPrevious() {
        guard let index = currentIndex, canNavigatePrevious else { return }
        let previousSong = allSongs[index - 1]
        currentSongId = previousSong.id
        toast = nil
        Task { await viewModel.load(songId: currentSongId, force: true) }
    }

    private func navigateToNext() {
        guard let index = currentIndex, canNavigateNext else { return }
        let nextSong = allSongs[index + 1]
        currentSongId = nextSong.id
        toast = nil
        Task { await viewModel.load(songId: currentSongId, force: true) }
    }

    // MARK: - Song Refresh

    /// Queue song for background research and show a toast with the result.
    private func refreshSongData(forceRefresh: Bool) {
        let refreshType = forceRefresh ? "full" : "quick"
        Task {
            let success = await viewModel.queueRefresh(songId: currentSongId, forceRefresh: forceRefresh)
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
                iconColor: ApproachNoteTheme.burgundy,
                title: "Researching Now",
                message: viewModel.researchingMessage(progress: progress),
                helperText: "We're scouring the internet to learn more about this song... Check back in a while to see what we've found.",
                isAnimating: true
            )
        case .inQueue(let position):
            ResearchStatusBanner(
                icon: "clock.fill",
                iconColor: ApproachNoteTheme.amber,
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

    // MARK: - Playback Helpers

    /// Whether any recording has streaming links available
    private var canPlay: Bool {
        guard let song = song else { return false }
        return song.hasAnyStreaming == true
    }

    /// Get the best playback URL for the song (from first featured or first available recording)
    private var bestPlaybackInfo: (service: String, url: String)? {
        guard let song = song else { return nil }

        // First try featured recordings
        if let featured = song.featuredRecordings {
            for recording in featured {
                if let playback = recording.playbackUrl(preferring: preferredStreamingService) {
                    return playback
                }
            }
        }

        // Then try all recordings
        if let recordings = song.recordings {
            for recording in recordings {
                if let playback = recording.playbackUrl(preferring: preferredStreamingService) {
                    return playback
                }
            }
        }

        return nil
    }

    /// Open the best available playback URL
    private func openPlayback() {
        guard let playback = bestPlaybackInfo,
              let url = URL(string: playback.url) else { return }
        openURL(url)
    }
    
    // MARK: - Song Content View
    
    @ViewBuilder
    private func songContentView(for song: Song) -> some View {
        VStack(alignment: .leading, spacing: 0) {
            VStack(alignment: .leading, spacing: 16) {
            // Song Information Header
            VStack(alignment: .leading, spacing: 12) {
                pagerRow
                HStack(alignment: .firstTextBaseline, spacing: 8) {
                    Text(song.title)
                        .font(ApproachNoteTheme.largeTitle())
                        .bold()
                        .foregroundColor(ApproachNoteTheme.charcoal)
                    if let year = song.composedYear {
                        Text("(\(String(year)))")
                            .font(ApproachNoteTheme.title2())
                            .foregroundColor(ApproachNoteTheme.smokeGray)
                    }
                }
                .onLongPressGesture {
                    if canQueueForRefresh {
                        showRefreshConfirmation = true
                    }
                }

                if let composer = song.composer {
                    HStack {
                        Image(systemName: "music.note.list")
                            .foregroundColor(ApproachNoteTheme.brass)
                        Text(composer)
                            .font(ApproachNoteTheme.title3())
                            .foregroundColor(ApproachNoteTheme.smokeGray)
                    }
                }

                // Song Reference (if available)
                if let songRef = song.songReference {
                    HStack(alignment: .top, spacing: 8) {
                        Image(systemName: "book.closed.fill")
                            .foregroundColor(ApproachNoteTheme.brass)
                            .font(ApproachNoteTheme.subheadline())
                        Text(songRef)
                            .font(ApproachNoteTheme.subheadline())
                            .foregroundColor(ApproachNoteTheme.smokeGray)
                            .fixedSize(horizontal: false, vertical: true)
                    }
                    .padding(.top, 4)
                }

                // MARK: - Research Status Indicator
                researchStatusIndicator

                // MARK: - Summary Information Section (Collapsible)
                if hasSummaryContent(for: song) {
                    summaryInfoSection(for: song)
                }
                
                // MARK: - Authoritative Recordings Carousel
                if hasAuthoritativeRecordings(for: song) {
                    authoritativeRecordingsSection(for: song)
                }
            }
            .padding()
            
            Divider()
                .padding(.horizontal)
            
            // MARK: - RECORDINGS SECTION
                RecordingsSection(
                    recordings: song.recordings ?? [],
                    recordingSortOrder: $viewModel.sortOrder,
                    isReloading: isRecordingsReloading || isRecordingsLoading,
                    onSortOrderChanged: { [self] _ in
                        Task { await viewModel.reloadRecordings(songId: currentSongId) }
                    },
                    onCommunityDataChanged: {
                        Task { await viewModel.reloadRecordings(songId: currentSongId) }
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
    
    @ViewBuilder
    private func summaryInfoSection(for song: Song) -> some View {
        VStack(alignment: .leading, spacing: 0) {
            // Collapsible Header
            Button(action: {
                withAnimation {
                    isSummaryInfoExpanded.toggle()
                }
            }) {
                HStack {
                    Text("Summary Information")
                        .font(ApproachNoteTheme.title3())
                        .bold()
                        .foregroundColor(ApproachNoteTheme.charcoal)
                    Spacer()
                    Image(systemName: isSummaryInfoExpanded ? "chevron.up" : "chevron.down")
                        .foregroundColor(ApproachNoteTheme.brass)
                }
                .padding()
                .background(ApproachNoteTheme.cardBackground)
            }
            .buttonStyle(.plain)
            
            // Expandable Content
            if isSummaryInfoExpanded {
                VStack(alignment: .leading, spacing: 16) {
                    // Structure
                    if let structure = song.structure {
                        VStack(alignment: .leading, spacing: 8) {
                            Text("Structure")
                                .font(ApproachNoteTheme.headline())
                                .foregroundColor(ApproachNoteTheme.charcoal)
                            Text(structure)
                                .font(ApproachNoteTheme.body())
                                .foregroundColor(ApproachNoteTheme.smokeGray)
                        }
                        .padding()
                        .frame(maxWidth: .infinity, alignment: .leading)
                        .background(Color.white)
                        .cornerRadius(8)
                    }

                    // Composed Key
                    if let composedKey = song.composedKey {
                        HStack(spacing: 8) {
                            Image(systemName: "tuningfork")
                                .foregroundColor(ApproachNoteTheme.brass)
                            Text("Original Key:")
                                .font(ApproachNoteTheme.headline())
                                .foregroundColor(ApproachNoteTheme.charcoal)
                            Text(composedKey)
                                .font(ApproachNoteTheme.body())
                                .foregroundColor(ApproachNoteTheme.smokeGray)
                        }
                        .padding()
                        .frame(maxWidth: .infinity, alignment: .leading)
                        .background(Color.white)
                        .cornerRadius(8)
                    }

                    // External References Panel (Learn More)
                    ExternalReferencesPanel(
                        wikipediaUrl: song.wikipediaUrl,
                        musicbrainzId: song.musicbrainzId,
                        externalReferences: song.externalReferences,
                        entityId: song.id,
                        entityName: song.title
                    )
                }
                .padding()
                .background(ApproachNoteTheme.cardBackground)
            }
        }
        .cornerRadius(10)
        .padding(.top, 8)
    }
    
    // MARK: - Authoritative Recordings Carousel Section
    // Uses featuredRecordings from summary endpoint (already filtered server-side)
    @ViewBuilder
    private func authoritativeRecordingsSection(for song: Song) -> some View {
        VStack(alignment: .leading, spacing: 16) {
            // Header
            Text("Featured Recordings")
                .font(ApproachNoteTheme.title2())
                .bold()
                .foregroundColor(ApproachNoteTheme.charcoal)

            // Introductory text
            Text("Take a look at these important recordings for this song.")
                .font(ApproachNoteTheme.subheadline())
                .foregroundColor(ApproachNoteTheme.smokeGray)

            // Horizontal scrolling carousel - use featuredRecordings from summary
            ScrollView(.horizontal, showsIndicators: false) {
                HStack(alignment: .top, spacing: 20) {
                    ForEach(song.featuredRecordings ?? []) { recording in
                        NavigationLink(destination: RecordingDetailView(
                            recordingId: recording.id,
                            onCommunityDataChanged: {
                                Task { await viewModel.reloadRecordings(songId: currentSongId) }
                            }
                        )) {
                            AuthoritativeRecordingCard(recording: recording)
                        }
                        .buttonStyle(.plain)
                    }
                }
                .padding(.horizontal, 4)
            }
        }
        .padding(16)
        .background(Color.white)
        .cornerRadius(12)
        .overlay(
            RoundedRectangle(cornerRadius: 12)
                .stroke(ApproachNoteTheme.smokeGray.opacity(0.3), lineWidth: 1)
        )
        .padding(.top, 8)
    }
    
    // MARK: - Body (broken into smaller chunks to avoid type-checker timeout)
    
    var body: some View {
        contentView
    }
    
    // MARK: - View Builders
    
    private var contentView: some View {
        mainScrollView
            .background(ApproachNoteTheme.backgroundLight)
            .jazzNavigationBar(title: song?.title ?? "")
            .toolbar {
                toolbarContent
            }
            .task {
                await viewModel.load(songId: currentSongId)
            }
            .onReceive(NotificationCenter.default.publisher(for: .transcriptionCreated)) { notification in
                // Refresh if this notification is for our song
                if let songId = notification.userInfo?["songId"] as? String,
                   songId == currentSongId {
                    Task { await viewModel.load(songId: currentSongId) }
                }
            }
            .onReceive(NotificationCenter.default.publisher(for: .videoCreated)) { notification in
                // Refresh backing tracks if this notification is for our song
                if let songId = notification.userInfo?["songId"] as? String,
                   songId == currentSongId {
                    Task { await viewModel.refreshBackingTracks(songId: currentSongId) }
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
            if isLoading {
                loadingView
            } else if let song = song {
                songContentView(for: song)
            } else {
                notFoundView
            }
        }
        .refreshable {
            await viewModel.forceRefresh(songId: currentSongId)
        }
    }
    
    private var loadingView: some View {
        VStack {
            Spacer()
            ProgressView()
                .progressViewStyle(CircularProgressViewStyle(tint: ApproachNoteTheme.burgundy))
                .scaleEffect(1.5)
            Spacer()
        }
        .frame(maxWidth: .infinity, minHeight: 300)
    }
    
    private var notFoundView: some View {
        VStack(spacing: 16) {
            Image(systemName: "exclamationmark.triangle")
                .font(.system(size: 50))
                .foregroundColor(ApproachNoteTheme.amber)
            Text("Unable to load song")
                .font(ApproachNoteTheme.headline())
                .foregroundColor(ApproachNoteTheme.charcoal)
            Text("There was a problem loading the song details.")
                .font(ApproachNoteTheme.subheadline())
                .foregroundColor(ApproachNoteTheme.smokeGray)
                .multilineTextAlignment(.center)
                .padding(.horizontal)
        }
        .frame(maxWidth: .infinity, minHeight: 300)
    }
    
    // MARK: - Toolbar Content

    @ToolbarContentBuilder
    private var toolbarContent: some ToolbarContent {
        ToolbarItem(placement: .navigationBarTrailing) {
            HStack(spacing: 16) {
                // Play button - only show if streaming is available
                if canPlay {
                    Button(action: openPlayback) {
                        Image(systemName: "play.circle.fill")
                            .font(.system(size: 22))
                            .foregroundColor(playButtonColor)
                    }
                }

                // Add to repertoire button
                Button(action: { showAddToRepertoireSheet = true }) {
                    Image(systemName: "plus.circle")
                }
            }
        }
    }

    /// Color for play button based on which service will be used
    private var playButtonColor: Color {
        guard let playback = bestPlaybackInfo,
              let service = StreamingService(key: playback.service) else {
            return ApproachNoteTheme.burgundy
        }
        return service.brandColor
    }
    
    private var repertoireSheet: some View {
        AddToRepertoireSheet(
            songId: currentSongId,
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
    @State private var showingBackCover = false

    private let artworkSize: CGFloat = 180

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

    // Back cover URL
    private var backCoverUrl: String? {
        recording.backCoverArtLarge ?? recording.backCoverArtMedium
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            // Album Art with flip support
            ZStack(alignment: .topTrailing) {
                // Album art with card-flip animation
                ZStack {
                    // Front cover
                    Group {
                        if let frontUrl = frontCoverUrl {
                            AsyncImage(url: URL(string: frontUrl)) { phase in
                                switch phase {
                                case .empty:
                                    Rectangle()
                                        .fill(ApproachNoteTheme.smokeGray.opacity(0.2))
                                        .overlay {
                                            ProgressView()
                                                .tint(ApproachNoteTheme.burgundy)
                                        }
                                case .success(let image):
                                    image
                                        .resizable()
                                        .aspectRatio(contentMode: .fill)
                                case .failure:
                                    Rectangle()
                                        .fill(ApproachNoteTheme.smokeGray.opacity(0.2))
                                        .overlay {
                                            Image(systemName: "music.note")
                                                .font(.system(size: 40))
                                                .foregroundColor(ApproachNoteTheme.smokeGray)
                                        }
                                @unknown default:
                                    EmptyView()
                                }
                            }
                        } else {
                            Rectangle()
                                .fill(ApproachNoteTheme.smokeGray.opacity(0.2))
                                .overlay {
                                    Image(systemName: "music.note")
                                        .font(.system(size: 40))
                                        .foregroundColor(ApproachNoteTheme.smokeGray)
                                }
                        }
                    }
                    .frame(width: artworkSize, height: artworkSize)
                    .clipShape(RoundedRectangle(cornerRadius: 12))
                    .opacity(showingBackCover ? 0 : 1)

                    // Back cover (pre-rotated so it appears correct after flip)
                    if let backUrl = backCoverUrl {
                        AsyncImage(url: URL(string: backUrl)) { phase in
                            switch phase {
                            case .empty:
                                Rectangle()
                                    .fill(ApproachNoteTheme.smokeGray.opacity(0.2))
                                    .overlay {
                                        ProgressView()
                                            .tint(ApproachNoteTheme.burgundy)
                                    }
                            case .success(let image):
                                image
                                    .resizable()
                                    .aspectRatio(contentMode: .fill)
                            case .failure:
                                Rectangle()
                                    .fill(ApproachNoteTheme.smokeGray.opacity(0.2))
                            @unknown default:
                                EmptyView()
                            }
                        }
                        .frame(width: artworkSize, height: artworkSize)
                        .clipShape(RoundedRectangle(cornerRadius: 12))
                        .rotation3DEffect(.degrees(180), axis: (x: 0, y: 1, z: 0))
                        .opacity(showingBackCover ? 1 : 0)
                    }
                }
                .rotation3DEffect(
                    .degrees(showingBackCover ? 180 : 0),
                    axis: (x: 0, y: 1, z: 0)
                )

                // Flip badge (shown when back cover available)
                if recording.canFlipToBackCover {
                    Button(action: {
                        withAnimation(.easeInOut(duration: 0.4)) {
                            showingBackCover.toggle()
                        }
                    }) {
                        Image(systemName: showingBackCover ? "arrow.uturn.backward" : "arrow.trianglehead.2.clockwise.rotate.90")
                            .foregroundColor(.white)
                            .font(.system(size: 10, weight: .semibold))
                            .padding(6)
                            .background(Color.black.opacity(0.6))
                            .clipShape(Circle())
                    }
                    .buttonStyle(PlainButtonStyle())
                    .padding(6)
                }

            }
            .frame(width: artworkSize, height: artworkSize)
            .shadow(color: .black.opacity(0.15), radius: 8, x: 0, y: 4)

            // Recording Info - fixed height for consistent card sizing
            VStack(alignment: .leading, spacing: 4) {
                // Artist Name
                Text(artistName)
                    .font(ApproachNoteTheme.subheadline())
                    .fontWeight(.semibold)
                    .foregroundColor(ApproachNoteTheme.brass)
                    .lineLimit(1)

                // Album Title + image-source ⓘ button
                HStack(alignment: .firstTextBaseline, spacing: 4) {
                    Text(recording.albumTitle ?? "Unknown Album")
                        .font(ApproachNoteTheme.body())
                        .fontWeight(.medium)
                        .foregroundColor(ApproachNoteTheme.charcoal)
                        .lineLimit(2)
                    if showingBackCover {
                        AlbumArtSourceBadge(
                            source: recording.backCoverSource,
                            sourceUrl: recording.backCoverSourceUrl
                        )
                    } else {
                        AlbumArtSourceBadge(
                            source: recording.displayAlbumArtSource,
                            sourceUrl: recording.displayAlbumArtSourceUrl
                        )
                    }
                }

                // Recording title (when different from song title)
                if let recordingTitle = recording.displayTitle {
                    Text("(\(recordingTitle))")
                        .font(ApproachNoteTheme.caption())
                        .italic()
                        .foregroundColor(ApproachNoteTheme.brass)
                        .lineLimit(1)
                }

                // Year (always reserve space)
                Text(recording.recordingYear.map { String($0) } ?? " ")
                    .font(ApproachNoteTheme.caption())
                    .foregroundColor(ApproachNoteTheme.smokeGray)
            }
            .frame(width: artworkSize, alignment: .topLeading)
        }
        .padding(12)
        .background(ApproachNoteTheme.cardBackground)
        .clipShape(RoundedRectangle(cornerRadius: 12))
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
