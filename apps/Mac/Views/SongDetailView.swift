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
    @State private var playableOnly: Bool = true
    @State private var selectedServices: Set<StreamingService> = []
    @State private var selectedVocalFilter: VocalFilter = .all
    @State private var selectedInstrument: InstrumentFamily? = nil
    @State private var showFilterPopover: Bool = false
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

                    // Structure paragraph + Wikipedia link
                    if let structure = song.structure, !structure.isEmpty {
                        structureSection(song)
                    }

                    // Composed key
                    if let composedKey = song.composedKey {
                        composedKeyRow(composedKey)
                    }

                    // Learn More external links (JazzStandards / MusicBrainz / Wikipedia)
                    if hasExternalLinks(for: song) {
                        learnMoreSection(song)
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

            // Composer
            if let composer = song.composer {
                Text("Composed by \(composer)")
                    .font(ApproachNoteTheme.body())
                    .foregroundColor(ApproachNoteTheme.charcoal)
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
    private func structureSection(_ song: Song) -> some View {
        if let structure = song.structure, !structure.isEmpty {
            VStack(alignment: .leading, spacing: 8) {
                Text(structure)
                    .font(ApproachNoteTheme.body())
                    .foregroundColor(ApproachNoteTheme.charcoal)
                    .fixedSize(horizontal: false, vertical: true)

                if let wikiUrlString = song.wikipediaUrl,
                   let wikiUrl = URL(string: wikiUrlString) {
                    Link("Read more on Wikipedia", destination: wikiUrl)
                        .font(ApproachNoteTheme.body())
                        .foregroundColor(ApproachNoteTheme.burgundy)
                }
            }
            .frame(maxWidth: .infinity, alignment: .leading)
        }
    }

    @ViewBuilder
    private func composedKeyRow(_ composedKey: String) -> some View {
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
    }

    @ViewBuilder
    private func learnMoreSection(_ song: Song) -> some View {
        VStack(alignment: .leading, spacing: 10) {
            Text("Learn More")
                .font(ApproachNoteTheme.headline())
                .foregroundColor(ApproachNoteTheme.charcoal)

            HStack(spacing: 12) {
                if let wikipediaUrl = song.wikipediaUrl, let url = URL(string: wikipediaUrl) {
                    compactExternalLink(icon: "book.fill", label: "Wikipedia", color: ApproachNoteTheme.teal, url: url)
                }
                if let jazzStandardsUrl = song.externalReferences?["jazzstandards"], let url = URL(string: jazzStandardsUrl) {
                    compactExternalLink(icon: "music.note.list", label: "JazzStandards.com", color: ApproachNoteTheme.brass, url: url)
                }
                if let musicbrainzId = song.musicbrainzId, let url = URL(string: "https://musicbrainz.org/work/\(musicbrainzId)") {
                    compactExternalLink(icon: "waveform.circle.fill", label: "MusicBrainz", color: ApproachNoteTheme.charcoal, url: url)
                }
            }
        }
    }


    @ViewBuilder
    private func recordingsSection(_ recordings: [Recording]) -> some View {
        // Mirror iOS: run instrument + vocal through the shared helper with
        // streaming: .all, then layer the playable/per-service filters on top.
        let baseFiltered = RecordingGrouping.filter(
            recordings,
            instrument: selectedInstrument,
            vocal: selectedVocalFilter,
            streaming: .all
        )
        let filtered: [Recording] = {
            if !selectedServices.isEmpty {
                return baseFiltered.filter { recording in
                    selectedServices.contains(where: { hasService(recording, $0) })
                }
            } else if playableOnly {
                return baseFiltered.filter { $0.isPlayable }
            } else {
                return baseFiltered
            }
        }()
        let grouped = RecordingGrouping.grouped(filtered, sortOrder: sortOrder)
        let availableInstruments = RecordingGrouping.availableInstruments(in: recordings)

        VStack(alignment: .leading, spacing: 16) {
            // Heading
            HStack(alignment: .firstTextBaseline, spacing: 6) {
                Text("MORE RECORDINGS")
                    .font(ApproachNoteTheme.title2())
                    .bold()
                    .foregroundColor(ApproachNoteTheme.charcoal)

                Text("(\(filtered.count))")
                    .font(ApproachNoteTheme.subheadline())
                    .foregroundColor(ApproachNoteTheme.smokeGray)

                Spacer()
            }

            // Filter + Sort row
            HStack(spacing: 10) {
                Button(action: { showFilterPopover = true }) {
                    HStack(spacing: 6) {
                        Text("Filter")
                            .font(ApproachNoteTheme.subheadline())
                        Image(systemName: "slider.horizontal.3")
                            .font(.caption)
                    }
                    .foregroundColor(ApproachNoteTheme.charcoal)
                    .padding(.horizontal, 12)
                    .padding(.vertical, 8)
                    .background(ApproachNoteTheme.cardBackground)
                    .cornerRadius(8)
                }
                .buttonStyle(.plain)
                .popover(isPresented: $showFilterPopover, arrowEdge: .bottom) {
                    filterPopoverContent(availableInstruments: availableInstruments)
                }

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
                    HStack(spacing: 6) {
                        Text("Sort: \(sortOrder.displayName)")
                            .font(ApproachNoteTheme.subheadline())
                        Image(systemName: "chevron.down")
                            .font(.caption)
                    }
                    .foregroundColor(ApproachNoteTheme.charcoal)
                    .padding(.horizontal, 12)
                    .padding(.vertical, 8)
                    .background(ApproachNoteTheme.cardBackground)
                    .cornerRadius(8)
                }
                .menuStyle(.borderlessButton)
                .fixedSize()

                Spacer()
            }

            // Playable Only toggle (always visible)
            Toggle(isOn: $playableOnly) {
                VStack(alignment: .leading, spacing: 2) {
                    Text("Playable only?")
                        .font(ApproachNoteTheme.headline())
                        .foregroundColor(ApproachNoteTheme.charcoal)
                    Text("Toggle On to hide versions of this song without a linked recording to listen to.")
                        .font(ApproachNoteTheme.caption())
                        .foregroundColor(ApproachNoteTheme.smokeGray)
                        .fixedSize(horizontal: false, vertical: true)
                }
            }
            .tint(ApproachNoteTheme.burgundy)

            // Performance Type segmented (always visible)
            VStack(alignment: .leading, spacing: 8) {
                Text("Performance Type")
                    .font(ApproachNoteTheme.headline())
                    .foregroundColor(ApproachNoteTheme.charcoal)

                Picker("Performance Type", selection: $selectedVocalFilter) {
                    ForEach(VocalFilter.allCases) { filter in
                        Text(filter.displayName.uppercased()).tag(filter)
                    }
                }
                .pickerStyle(.segmented)
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
                        playableOnly = false
                        selectedServices.removeAll()
                        selectedVocalFilter = .all
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

    // MARK: - Filter helpers

    private func hasService(_ recording: Recording, _ service: StreamingService) -> Bool {
        switch service {
        case .spotify: return recording.hasSpotifyAvailable
        case .appleMusic: return recording.hasAppleMusicAvailable
        case .youtube: return recording.hasYoutubeAvailable
        }
    }

    @ViewBuilder
    private func filterPopoverContent(availableInstruments: [InstrumentFamily]) -> some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 24) {
                // Playback availability (multi-select)
                VStack(alignment: .leading, spacing: 8) {
                    Text("Playback availability")
                        .font(ApproachNoteTheme.headline())
                        .foregroundColor(ApproachNoteTheme.charcoal)
                    Text("Select which service(s) you'd like to include for playback")
                        .font(ApproachNoteTheme.subheadline())
                        .foregroundColor(ApproachNoteTheme.smokeGray)
                        .fixedSize(horizontal: false, vertical: true)

                    VStack(alignment: .leading, spacing: 4) {
                        ForEach(StreamingService.allCases) { service in
                            Toggle(isOn: Binding(
                                get: { selectedServices.contains(service) },
                                set: { isOn in
                                    if isOn {
                                        selectedServices.insert(service)
                                    } else {
                                        selectedServices.remove(service)
                                    }
                                }
                            )) {
                                Text(service.displayName)
                                    .font(ApproachNoteTheme.body())
                                    .foregroundColor(ApproachNoteTheme.charcoal)
                            }
                            .tint(ApproachNoteTheme.burgundy)
                        }
                    }
                    .padding(.top, 4)
                }

                // By Instrument
                if !availableInstruments.isEmpty {
                    VStack(alignment: .leading, spacing: 8) {
                        Text("By Instrument")
                            .font(ApproachNoteTheme.headline())
                            .foregroundColor(ApproachNoteTheme.charcoal)
                        Text("Select to filter for recordings that feature a specific instrument")
                            .font(ApproachNoteTheme.subheadline())
                            .foregroundColor(ApproachNoteTheme.smokeGray)
                            .fixedSize(horizontal: false, vertical: true)

                        LazyVGrid(columns: [
                            GridItem(.flexible()),
                            GridItem(.flexible()),
                            GridItem(.flexible())
                        ], spacing: 8) {
                            ForEach(availableInstruments, id: \.self) { family in
                                Button(action: {
                                    selectedInstrument = (selectedInstrument == family) ? nil : family
                                }) {
                                    HStack(spacing: 6) {
                                        Image(systemName: family.icon)
                                            .font(ApproachNoteTheme.caption())
                                        Text(family.rawValue)
                                            .font(ApproachNoteTheme.subheadline())
                                            .lineLimit(1)
                                            .minimumScaleFactor(0.8)
                                    }
                                    .frame(maxWidth: .infinity)
                                    .padding(.vertical, 8)
                                    .padding(.horizontal, 8)
                                    .background(selectedInstrument == family ? ApproachNoteTheme.brass : Color.white)
                                    .foregroundColor(selectedInstrument == family ? .white : ApproachNoteTheme.charcoal)
                                    .cornerRadius(8)
                                    .overlay(
                                        RoundedRectangle(cornerRadius: 8)
                                            .stroke(selectedInstrument == family ? Color.clear : ApproachNoteTheme.smokeGray.opacity(0.5), lineWidth: 1)
                                    )
                                }
                                .buttonStyle(.plain)
                            }
                        }
                        .padding(.top, 4)
                    }
                }

                HStack {
                    if !selectedServices.isEmpty || selectedInstrument != nil {
                        Button("Clear All") {
                            selectedServices.removeAll()
                            selectedInstrument = nil
                        }
                        .buttonStyle(.link)
                        .foregroundColor(ApproachNoteTheme.burgundy)
                    }
                    Spacer()
                    Button("Done") { showFilterPopover = false }
                        .keyboardShortcut(.defaultAction)
                }
                .padding(.top, 8)
            }
            .padding(16)
        }
        .frame(width: 360, height: 420)
    }

    // MARK: - Featured Recordings Carousel

    @ViewBuilder
    private func featuredRecordingsSection(_ recordings: [Recording]) -> some View {
        VStack(alignment: .leading, spacing: 12) {
            Text("FEATURED RECORDINGS")
                .font(ApproachNoteTheme.title2())
                .bold()
                .foregroundColor(ApproachNoteTheme.charcoal)

            Text("Take a look at these important recordings for this song.")
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
