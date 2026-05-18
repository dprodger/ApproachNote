//
//  SongDetailView.swift
//  Approach Note
//
//  macOS-specific song detail view
//

import SwiftUI

// MARK: - Recording Filter Enum
// Filter enums (SongRecordingFilter, VocalFilter, InstrumentFamily) are in Shared/Support/RecordingFilters.swift

struct SongDetailView: View {
    let songId: String

    // Shared data + network state lives on the view model; layout/presentation
    // state stays here.
    @StateObject private var viewModel = SongDetailViewModel()

    @State private var selectedRecordingId: String?
    @State private var selectedFilter: SongRecordingFilter = .playable
    @State private var selectedVocalFilter: VocalFilter = .all
    @State private var selectedInstrument: InstrumentFamily? = nil
    @State private var isSummaryInfoExpanded = false
    @State private var showAddToRepertoire = false
    @State private var successMessage: String?
    @State private var errorMessage: String?
    @EnvironmentObject var repertoireManager: RepertoireManager
    @EnvironmentObject var authManager: AuthenticationManager

    // Read-only aliases so existing reference sites in this view can keep
    // using the short names unchanged.
    private var song: Song? { viewModel.song }
    private var isLoading: Bool { viewModel.isLoading }
    private var isRecordingsLoading: Bool { viewModel.isRecordingsLoading }
    private var sortOrder: RecordingSortOrder { viewModel.sortOrder }
    private var transcriptions: [SoloTranscription] { viewModel.transcriptions }
    private var backingTracks: [Video] { viewModel.backingTracks }
    private var isRefreshing: Bool { viewModel.isRefreshing }
    private var researchStatus: SongResearchStatus { viewModel.researchStatus }
    private var canQueueForRefresh: Bool { viewModel.canQueueForRefresh }

    // MARK: - Song Refresh

    /// Queue song for background research and show a success/error message.
    private func refreshSongData(forceRefresh: Bool) {
        let refreshType = forceRefresh ? "full" : "quick"
        Task {
            let success = await viewModel.queueRefresh(songId: songId, forceRefresh: forceRefresh)
            if success {
                successMessage = "Song queued for \(refreshType) refresh"
                try? await Task.sleep(nanoseconds: 3_000_000_000)
                successMessage = nil
            } else {
                errorMessage = "Failed to queue song for refresh"
            }
        }
    }

    /// Helper text for the research status tooltip
    private var researchStatusHelperText: String {
        switch researchStatus {
        case .currentlyResearching:
            return "We're scouring the internet to learn more about this song... Check back in a while to see what we've found."
        case .inQueue:
            return "This song is in the queue to get researched... Check back in a while to see what we've found."
        case .notInQueue:
            return ""
        }
    }

    var body: some View {
        ScrollView {
            if isLoading {
                ThemedProgressView(message: "Loading...")
                    .frame(maxWidth: .infinity, maxHeight: .infinity)
                    .padding(.top, 100)
            } else if let song = song {
                VStack(alignment: .leading, spacing: 16) {
                    // Header
                    songHeader(song)

                    // Summary Information (collapsible)
                    if hasSummaryContent(for: song) {
                        summaryInfoSection(song)
                    }

                    // Featured Recordings carousel
                    if let featured = song.featuredRecordings, !featured.isEmpty {
                        featuredRecordingsSection(featured)
                    }

                    Divider()

                    // Recordings - show section while loading or when we have recordings
                    if isRecordingsLoading || (song.recordings != nil && !song.recordings!.isEmpty) {
                        recordingsSection(song.recordings ?? [])
                    }

                    // Transcriptions
                    if !transcriptions.isEmpty {
                        transcriptionsSection
                    }

                    // Backing Tracks
                    if !backingTracks.isEmpty {
                        backingTracksSection
                    }
                }
                .padding()
            } else {
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
                }
                .frame(maxWidth: .infinity, maxHeight: .infinity)
                .padding(.top, 100)
            }
        }
        .background(ApproachNoteTheme.backgroundLight)
        .sheet(isPresented: $showAddToRepertoire) {
            if let song = song {
                MacAddToRepertoireSheet(
                    songId: songId,
                    songTitle: song.title,
                    repertoireManager: repertoireManager,
                    onSuccess: { message in
                        successMessage = message
                        // Auto-dismiss success message after 3 seconds
                        Task {
                            try? await Task.sleep(nanoseconds: 3_000_000_000)
                            await MainActor.run {
                                successMessage = nil
                            }
                        }
                    },
                    onError: { message in
                        errorMessage = message
                        // Auto-dismiss error message after 5 seconds
                        Task {
                            try? await Task.sleep(nanoseconds: 5_000_000_000)
                            await MainActor.run {
                                errorMessage = nil
                            }
                        }
                    }
                )
            }
        }
        .task(id: songId) {
            await viewModel.load(songId: songId)
        }
        .onChange(of: sortOrder) { _, _ in
            Task { await viewModel.reloadRecordings(songId: songId) }
        }
        .onReceive(NotificationCenter.default.publisher(for: .transcriptionCreated)) { notification in
            // Refresh if this notification is for our song
            if let notifSongId = notification.userInfo?["songId"] as? String,
               notifSongId == songId {
                Task { await viewModel.load(songId: songId) }
            }
        }
        .onReceive(NotificationCenter.default.publisher(for: .videoCreated)) { notification in
            // Refresh backing tracks if this notification is for our song
            if let notifSongId = notification.userInfo?["songId"] as? String,
               notifSongId == songId {
                Task { await viewModel.refreshBackingTracks(songId: songId) }
            }
        }
        .onDisappear {
            viewModel.stopResearchStatusPolling()
        }
    }

    // MARK: - View Components

    @ViewBuilder
    private func songHeader(_ song: Song) -> some View {
        VStack(alignment: .leading, spacing: 12) {
            // Title row with Add to Repertoire button
            HStack(alignment: .firstTextBaseline) {
                // Title with composed year
                Text(song.title)
                    .font(ApproachNoteTheme.largeTitle(weight: .bold))
                    .foregroundColor(ApproachNoteTheme.charcoal)
                + Text(song.composedYear.map { " (\(String($0)))" } ?? "")
                    .font(ApproachNoteTheme.largeTitle(weight: .regular))
                    .foregroundColor(ApproachNoteTheme.smokeGray)

                Spacer()

                // Refresh menu button
                Menu {
                    Button(action: { refreshSongData(forceRefresh: false) }) {
                        Label("Quick Refresh", systemImage: "arrow.clockwise")
                    }
                    Button(action: { refreshSongData(forceRefresh: true) }) {
                        Label("Full Refresh", systemImage: "arrow.clockwise.circle")
                    }
                } label: {
                    Label("Refresh", systemImage: isRefreshing ? "arrow.triangle.2.circlepath" : "arrow.clockwise")
                        .padding(.vertical, 4)
                }
                .menuStyle(.borderlessButton)
                .help(canQueueForRefresh ? "Quick: uses cached data (faster). Full: re-fetches everything." : researchStatusHelperText)
                .disabled(isRefreshing || !canQueueForRefresh)

                // Bulk Edit button (auth-gated)
                if authManager.isAuthenticated {
                    Button(action: {
                        SongBulkEditRecordingsView.openInWindow(
                            songTitle: song.title,
                            recordings: song.recordings ?? [],
                            authManager: authManager,
                            onDismiss: {
                                Task { await viewModel.reloadRecordings(songId: songId) }
                            }
                        )
                    }) {
                        Label("Bulk Edit", systemImage: "tablecells")
                            .padding(.vertical, 4)
                    }
                    .buttonStyle(.bordered)
                    .disabled(song.recordings == nil || song.recordings?.isEmpty == true || isRecordingsLoading)
                    .help("Edit key, tempo, and type for all recordings at once")
                }

                // Add to Repertoire button
                Button(action: { showAddToRepertoire = true }) {
                    Label("Add to Repertoire", systemImage: "plus.circle")
                        .padding(.vertical, 4)
                }
                .buttonStyle(.borderedProminent)
                .tint(ApproachNoteTheme.burgundy)
                .help("Add this song to a repertoire")
            }

            // Composer with icon
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

            // Success/Error messages
            if let message = successMessage {
                HStack {
                    Image(systemName: "checkmark.circle.fill")
                        .foregroundColor(.green)
                    Text(message)
                        .foregroundColor(.green)
                }
                .font(ApproachNoteTheme.subheadline())
                .padding(.vertical, 4)
            }

            if let message = errorMessage {
                HStack {
                    Image(systemName: "exclamationmark.circle.fill")
                        .foregroundColor(.red)
                    Text(message)
                        .foregroundColor(.red)
                }
                .font(ApproachNoteTheme.subheadline())
                .padding(.vertical, 4)
            }

            // Research status indicator
            researchStatusIndicator
        }
    }

    /// Visual indicator showing research queue status
    @ViewBuilder
    private var researchStatusIndicator: some View {
        switch researchStatus {
        case .currentlyResearching(let progress):
            MacResearchStatusBanner(
                icon: "waveform.circle.fill",
                iconColor: ApproachNoteTheme.burgundy,
                title: "Researching Now",
                message: viewModel.researchingMessage(progress: progress),
                helperText: researchStatusHelperText,
                isAnimating: true
            )
        case .inQueue(let position):
            MacResearchStatusBanner(
                icon: "clock.fill",
                iconColor: ApproachNoteTheme.amber,
                title: "In Research Queue",
                message: "Position \(position) in queue",
                helperText: researchStatusHelperText,
                isAnimating: false
            )
        case .notInQueue:
            EmptyView()
        }
    }

    // MARK: - Summary Information Helpers (delegated to the view model)

    private func hasSummaryContent(for song: Song) -> Bool {
        viewModel.hasSummaryContent(for: song)
    }

    private func hasExternalLinks(for song: Song) -> Bool {
        viewModel.hasExternalLinks(for: song)
    }

    @ViewBuilder
    private func externalLinkRow(icon: String, label: String, color: Color, url: URL) -> some View {
        Link(destination: url) {
            HStack {
                Image(systemName: icon)
                    .foregroundColor(color)
                    .frame(width: 24)
                Text(label)
                    .font(ApproachNoteTheme.body())
                    .foregroundColor(ApproachNoteTheme.charcoal)
                Spacer()
                Image(systemName: "arrow.up.right.square")
                    .foregroundColor(ApproachNoteTheme.smokeGray)
                    .font(ApproachNoteTheme.caption())
            }
            .padding(.vertical, 6)
        }
        .buttonStyle(.plain)
    }

    @ViewBuilder
    private func compactExternalLink(icon: String, label: String, color: Color, url: URL) -> some View {
        Link(destination: url) {
            HStack(spacing: 6) {
                Image(systemName: icon)
                    .foregroundColor(color)
                Text(label)
                    .font(ApproachNoteTheme.subheadline())
                    .foregroundColor(ApproachNoteTheme.charcoal)
                Image(systemName: "arrow.up.right")
                    .font(ApproachNoteTheme.caption2())
                    .foregroundColor(ApproachNoteTheme.smokeGray)
            }
            .padding(.horizontal, 12)
            .padding(.vertical, 8)
            .background(ApproachNoteTheme.cardBackground)
            .cornerRadius(8)
        }
        .buttonStyle(.plain)
    }

    @ViewBuilder
    private func summaryInfoSection(_ song: Song) -> some View {
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

                    // External References (Learn More)
                    if hasExternalLinks(for: song) {
                        VStack(alignment: .leading, spacing: 12) {
                            Text("Learn More")
                                .font(ApproachNoteTheme.headline())
                                .foregroundColor(ApproachNoteTheme.charcoal)

                            HStack(spacing: 12) {
                                // Wikipedia
                                if let wikipediaUrl = song.wikipediaUrl, let url = URL(string: wikipediaUrl) {
                                    compactExternalLink(icon: "book.fill", label: "Wikipedia", color: ApproachNoteTheme.teal, url: url)
                                }

                                // Jazz Standards
                                if let jazzStandardsUrl = song.externalReferences?["jazzstandards"], let url = URL(string: jazzStandardsUrl) {
                                    compactExternalLink(icon: "music.note.list", label: "JazzStandards.com", color: ApproachNoteTheme.brass, url: url)
                                }

                                // MusicBrainz
                                if let musicbrainzId = song.musicbrainzId, let url = URL(string: "https://musicbrainz.org/work/\(musicbrainzId)") {
                                    compactExternalLink(icon: "waveform.circle.fill", label: "MusicBrainz", color: ApproachNoteTheme.charcoal, url: url)
                                }
                            }
                        }
                        .padding()
                        .frame(maxWidth: .infinity, alignment: .leading)
                        .background(Color.white)
                        .cornerRadius(8)
                    }
                }
                .padding()
                .background(ApproachNoteTheme.cardBackground)
            }
        }
        .cornerRadius(10)
    }


    @ViewBuilder
    private func recordingsSection(_ recordings: [Recording]) -> some View {
        let filtered = RecordingGrouping.filter(
            recordings,
            instrument: selectedInstrument,
            vocal: selectedVocalFilter,
            streaming: selectedFilter
        )
        let grouped = RecordingGrouping.grouped(filtered, sortOrder: sortOrder)

        VStack(alignment: .leading, spacing: 12) {
            // Header with count, filter, and sort
            HStack {
                Image(systemName: "music.note.list")
                    .foregroundColor(ApproachNoteTheme.burgundy)
                Text("Recordings")
                    .font(ApproachNoteTheme.title2())
                    .foregroundColor(ApproachNoteTheme.charcoal)

                Text("(\(filtered.count))")
                    .font(ApproachNoteTheme.subheadline())
                    .foregroundColor(ApproachNoteTheme.smokeGray)

                Spacer()

                // Filter menu
                Menu {
                    ForEach(SongRecordingFilter.allCases) { filter in
                        Button(action: { selectedFilter = filter }) {
                            HStack {
                                Image(systemName: filter.icon)
                                    .foregroundColor(filter.iconColor)
                                Text(filter.displayName)
                                if selectedFilter == filter {
                                    Spacer()
                                    Image(systemName: "checkmark")
                                }
                            }
                        }
                    }
                } label: {
                    HStack(spacing: 4) {
                        Image(systemName: selectedFilter.icon)
                            .foregroundColor(selectedFilter == .all ? ApproachNoteTheme.charcoal : selectedFilter.iconColor)
                        Text(selectedFilter.displayName)
                            .font(ApproachNoteTheme.subheadline())
                            .foregroundColor(ApproachNoteTheme.charcoal)
                        Image(systemName: "chevron.down")
                            .font(ApproachNoteTheme.caption2())
                            .foregroundColor(ApproachNoteTheme.charcoal)
                    }
                    .padding(.horizontal, 10)
                    .padding(.vertical, 6)
                    .background(selectedFilter == .all ? ApproachNoteTheme.cardBackground : selectedFilter.iconColor.opacity(0.15))
                    .cornerRadius(8)
                }
                .menuStyle(.borderlessButton)

                // Vocal/Instrumental filter menu
                Menu {
                    ForEach(VocalFilter.allCases) { filter in
                        Button(action: { selectedVocalFilter = filter }) {
                            HStack {
                                Image(systemName: filter.icon)
                                    .foregroundColor(filter.iconColor)
                                Text(filter.rawValue)
                                if selectedVocalFilter == filter {
                                    Spacer()
                                    Image(systemName: "checkmark")
                                }
                            }
                        }
                    }
                } label: {
                    HStack(spacing: 4) {
                        Image(systemName: selectedVocalFilter.icon)
                            .foregroundColor(selectedVocalFilter == .all ? ApproachNoteTheme.charcoal : selectedVocalFilter.iconColor)
                        Text(selectedVocalFilter.rawValue)
                            .font(ApproachNoteTheme.subheadline())
                            .foregroundColor(ApproachNoteTheme.charcoal)
                        Image(systemName: "chevron.down")
                            .font(ApproachNoteTheme.caption2())
                            .foregroundColor(ApproachNoteTheme.charcoal)
                    }
                    .padding(.horizontal, 10)
                    .padding(.vertical, 6)
                    .background(selectedVocalFilter == .all ? ApproachNoteTheme.cardBackground : selectedVocalFilter.iconColor.opacity(0.15))
                    .cornerRadius(8)
                }
                .menuStyle(.borderlessButton)

                // Clear button when vocal filter is active
                if selectedVocalFilter != .all {
                    Button(action: { selectedVocalFilter = .all }) {
                        Image(systemName: "xmark.circle.fill")
                            .foregroundColor(ApproachNoteTheme.burgundy)
                            .font(.system(size: 16))
                    }
                    .buttonStyle(.plain)
                    .help("Clear type filter")
                }

                // Instrument filter menu (only show if instruments are available)
                let instruments = RecordingGrouping.availableInstruments(in: recordings)
                if !instruments.isEmpty {
                    Menu {
                        Button(action: { selectedInstrument = nil }) {
                            HStack {
                                Text("All Instruments")
                                if selectedInstrument == nil {
                                    Spacer()
                                    Image(systemName: "checkmark")
                                }
                            }
                        }

                        Divider()

                        ForEach(instruments) { family in
                            Button(action: { selectedInstrument = family }) {
                                HStack {
                                    Image(systemName: family.icon)
                                    Text(family.rawValue)
                                    if selectedInstrument == family {
                                        Spacer()
                                        Image(systemName: "checkmark")
                                    }
                                }
                            }
                        }
                    } label: {
                        HStack(spacing: 4) {
                            Image(systemName: selectedInstrument?.icon ?? "pianokeys")
                            Text(selectedInstrument?.rawValue ?? "Instrument")
                                .font(ApproachNoteTheme.subheadline())
                            Image(systemName: "chevron.down")
                                .font(ApproachNoteTheme.caption2())
                        }
                        .foregroundColor(ApproachNoteTheme.charcoal)
                        .padding(.horizontal, 10)
                        .padding(.vertical, 6)
                        .background(ApproachNoteTheme.cardBackground)
                        .cornerRadius(8)
                    }
                    .menuStyle(.borderlessButton)

                    // Clear button when instrument filter is active
                    if selectedInstrument != nil {
                        Button(action: { selectedInstrument = nil }) {
                            Image(systemName: "xmark.circle.fill")
                                .foregroundColor(ApproachNoteTheme.burgundy)
                                .font(.system(size: 16))
                        }
                        .buttonStyle(.plain)
                        .help("Clear instrument filter")
                    }
                }

                // Sort menu (matching iOS style)
                Menu {
                    ForEach(RecordingSortOrder.allCases) { order in
                        Button(action: { viewModel.sortOrder = order }) {
                            HStack {
                                Text(order.displayName)
                                if sortOrder == order {
                                    Image(systemName: "checkmark")
                                }
                            }
                        }
                    }
                } label: {
                    HStack(spacing: 3) {
                        Text(sortOrder.displayName)
                            .font(ApproachNoteTheme.caption())
                            .foregroundColor(ApproachNoteTheme.charcoal)
                        Image(systemName: "chevron.down")
                            .font(ApproachNoteTheme.caption2())
                            .foregroundColor(ApproachNoteTheme.charcoal)
                    }
                    .padding(.horizontal, 8)
                    .padding(.vertical, 5)
                    .background(ApproachNoteTheme.burgundy.opacity(0.2))
                    .cornerRadius(6)
                }
                .menuStyle(.borderlessButton)
            }

            // Recordings list
            if isRecordingsLoading {
                VStack(spacing: 12) {
                    ProgressView()
                        .scaleEffect(1.2)
                    Text("Loading recordings...")
                        .font(ApproachNoteTheme.subheadline())
                        .foregroundColor(ApproachNoteTheme.smokeGray)
                }
                .frame(maxWidth: .infinity)
                .padding(.vertical, 40)
            } else if filtered.isEmpty {
                VStack(spacing: 12) {
                    Image(systemName: "music.note")
                        .font(.system(size: 40))
                        .foregroundColor(ApproachNoteTheme.smokeGray.opacity(0.5))
                    Text("No recordings match the current filters")
                        .font(ApproachNoteTheme.subheadline())
                        .foregroundColor(ApproachNoteTheme.smokeGray)
                    Button("Clear Filters") {
                        selectedFilter = .all
                        selectedInstrument = nil
                    }
                    .buttonStyle(.link)
                }
                .frame(maxWidth: .infinity)
                .padding(.vertical, 40)
            } else {
                // Grouped recordings
                let parentSongTitle = song?.title
                ForEach(grouped, id: \.groupKey) { group in
                    let shelfHasAnyDistinctTitle = group.recordings.contains { recording in
                        recording.displayTitle(comparedTo: parentSongTitle) != nil
                    }
                    VStack(alignment: .leading, spacing: 8) {
                        // Group header
                        Text("\(group.groupKey) (\(group.recordings.count))")
                            .font(ApproachNoteTheme.headline())
                            .foregroundColor(ApproachNoteTheme.burgundy)
                            .padding(.top, 8)

                        // Horizontal scroll of recordings in this group
                        ScrollView(.horizontal, showsIndicators: false) {
                            HStack(alignment: .top, spacing: 16) {
                                ForEach(group.recordings) { recording in
                                    RecordingCard(
                                        recording: recording,
                                        parentSongTitle: parentSongTitle,
                                        shelfHasAnyDistinctTitle: shelfHasAnyDistinctTitle,
                                        onVisible: { [weak viewModel] id in
                                            viewModel?.requestHydration(for: id)
                                        }
                                    )
                                        .contentShape(Rectangle())
                                        .onTapGesture {
                                            selectedRecordingId = recording.id
                                        }
                                }
                            }
                            .padding(.horizontal, 4)
                        }
                    }
                }
            }
        }
        .sheet(isPresented: Binding(
            get: { selectedRecordingId != nil },
            set: { if !$0 { selectedRecordingId = nil } }
        )) {
            if let recordingId = selectedRecordingId {
                RecordingDetailView(recordingId: recordingId)
                    .frame(minWidth: 600, minHeight: 500)
            }
        }
    }

    // MARK: - Featured Recordings Carousel

    @ViewBuilder
    private func featuredRecordingsSection(_ recordings: [Recording]) -> some View {
        VStack(alignment: .leading, spacing: 16) {
            Text("Featured Recordings")
                .font(ApproachNoteTheme.title2())
                .foregroundColor(ApproachNoteTheme.charcoal)

            Text("Important recordings for this song")
                .font(ApproachNoteTheme.subheadline())
                .foregroundColor(ApproachNoteTheme.smokeGray)

            let parentSongTitle = song?.title
            let carouselHasAnyDistinctTitle = recordings.contains { recording in
                recording.displayTitle(comparedTo: parentSongTitle) != nil
            }
            ScrollView(.horizontal, showsIndicators: false) {
                HStack(alignment: .top, spacing: 20) {
                    ForEach(recordings) { recording in
                        FeaturedRecordingCard(
                            recording: recording,
                            parentSongTitle: parentSongTitle,
                            shelfHasAnyDistinctTitle: carouselHasAnyDistinctTitle
                        )
                        .onTapGesture {
                            selectedRecordingId = recording.id
                        }
                    }
                }
                .padding(.horizontal, 4)
            }
        }
        .padding(16)
        .background(Color.white)
        .cornerRadius(12)
    }

    // MARK: - Transcriptions Section

    @ViewBuilder
    private var transcriptionsSection: some View {
        VStack(alignment: .leading, spacing: 12) {
            HStack {
                Image(systemName: "music.quarternote.3")
                    .foregroundColor(ApproachNoteTheme.teal)
                Text("Solo Transcriptions")
                    .font(ApproachNoteTheme.title2())
                    .foregroundColor(ApproachNoteTheme.charcoal)

                Spacer()

                Text("\(transcriptions.count)")
                    .font(ApproachNoteTheme.subheadline())
                    .foregroundColor(ApproachNoteTheme.smokeGray)
                    .padding(.horizontal, 8)
                    .padding(.vertical, 4)
                    .background(ApproachNoteTheme.teal.opacity(0.1))
                    .cornerRadius(6)
            }

            ForEach(transcriptions) { transcription in
                TranscriptionRow(transcription: transcription)
            }
        }
        .padding(16)
        .background(ApproachNoteTheme.cardBackground)
        .cornerRadius(12)
    }

    // MARK: - Backing Tracks Section

    @ViewBuilder
    private var backingTracksSection: some View {
        VStack(alignment: .leading, spacing: 12) {
            HStack {
                Image(systemName: "play.circle.fill")
                    .foregroundColor(ApproachNoteTheme.green)
                Text("Backing Tracks")
                    .font(ApproachNoteTheme.title2())
                    .foregroundColor(ApproachNoteTheme.charcoal)

                Spacer()

                Text("\(backingTracks.count)")
                    .font(ApproachNoteTheme.subheadline())
                    .foregroundColor(ApproachNoteTheme.smokeGray)
                    .padding(.horizontal, 8)
                    .padding(.vertical, 4)
                    .background(ApproachNoteTheme.green.opacity(0.1))
                    .cornerRadius(6)
            }

            ForEach(backingTracks) { video in
                BackingTrackRow(video: video)
            }
        }
        .padding(16)
        .background(ApproachNoteTheme.cardBackground)
        .cornerRadius(12)
    }

}

#Preview {
    SongDetailView(songId: "preview-id")
        .environmentObject(RepertoireManager())
        .environmentObject(AuthenticationManager())
}
