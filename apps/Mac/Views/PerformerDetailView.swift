//
//  PerformerDetailView.swift
//  Approach Note
//
//  macOS-specific performer/artist detail view
//  Updated to match iOS layout with collapsible Biographical Information section
//

import SwiftUI

// MARK: - Recording Filter Enum
enum RecordingFilter: String, CaseIterable {
    case all = "All"
    case leader = "Leader"
    case sideman = "Sideman"
}

struct PerformerDetailView: View {
    let performerId: String
    @State private var performer: PerformerDetail?
    @State private var isLoading = true
    @State private var isRecordingsLoading = true
    @State private var isBiographicalInfoExpanded = false
    @State private var sortOrder: PerformerRecordingSortOrder = .year
    @State private var selectedFilter: RecordingFilter = .all
    @State private var searchText: String = ""
    @State private var selectedRecordingId: String?

    @StateObject private var performerService = PerformerService()

    var body: some View {
        ScrollView {
            if isLoading {
                ThemedProgressView(message: "Loading...")
                    .frame(maxWidth: .infinity, maxHeight: .infinity)
                    .padding(.top, 100)
            } else if let performer = performer {
                VStack(alignment: .leading, spacing: 24) {
                    // Header with image
                    performerHeader(performer)

                    // Biographical Information (collapsible)
                    biographicalInfoSection(performer)

                    Divider()

                    // Recordings with filtering and grouping
                    recordingsSection(performer.recordings ?? [])
                }
                .padding()
            } else {
                Text("Artist not found")
                    .foregroundColor(.secondary)
                    .padding(.top, 100)
            }
        }
        .background(ApproachNoteTheme.background)
        .task(id: performerId) {
            await loadPerformer()
        }
        .onChange(of: sortOrder) { _, newOrder in
            Task {
                await reloadRecordings(sortBy: newOrder)
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

    // MARK: - View Components

    @ViewBuilder
    private func performerHeader(_ performer: PerformerDetail) -> some View {
        HStack(alignment: .top, spacing: 24) {
            // Artist image
            if let image = performer.images?.first {
                AsyncImage(url: URL(string: image.thumbnailUrl ?? image.url)) { img in
                    img
                        .resizable()
                        .aspectRatio(contentMode: .fill)
                } placeholder: {
                    Rectangle()
                        .fill(ApproachNoteTheme.surface)
                        .overlay {
                            Image(systemName: "person.fill")
                                .font(.system(size: 40))
                                .foregroundColor(ApproachNoteTheme.textSecondary)
                        }
                }
                .frame(width: 150, height: 150)
                .clipShape(RoundedRectangle(cornerRadius: 12))
            } else {
                Rectangle()
                    .fill(ApproachNoteTheme.surface)
                    .overlay {
                        Image(systemName: "person.fill")
                            .font(.system(size: 40))
                            .foregroundColor(ApproachNoteTheme.textSecondary)
                    }
                    .frame(width: 150, height: 150)
                    .clipShape(RoundedRectangle(cornerRadius: 12))
            }

            VStack(alignment: .leading, spacing: 8) {
                Text(performer.name)
                    .font(ApproachNoteTheme.largeTitle())
                    .foregroundColor(ApproachNoteTheme.textPrimary)

                if let birthDate = performer.birthDate {
                    HStack(spacing: 4) {
                        Text(birthDate)
                        if let deathDate = performer.deathDate {
                            Text("–")
                            Text(deathDate)
                        }
                    }
                    .font(ApproachNoteTheme.subheadline())
                    .foregroundColor(ApproachNoteTheme.textSecondary)
                }

                // Primary instruments
                if let instruments = performer.instruments?.filter({ $0.isPrimary == true }) {
                    let instrumentNames = instruments.map { $0.name }.joined(separator: ", ")
                    if !instrumentNames.isEmpty {
                        Text(instrumentNames)
                            .font(ApproachNoteTheme.title3())
                            .foregroundColor(ApproachNoteTheme.textSecondary)
                    }
                }
            }

            Spacer()
        }
    }

    // MARK: - Biographical Information Section (Collapsible)

    @ViewBuilder
    private func biographicalInfoSection(_ performer: PerformerDetail) -> some View {
        VStack(alignment: .leading, spacing: 0) {
            // Collapsible Header
            Button(action: {
                withAnimation {
                    isBiographicalInfoExpanded.toggle()
                }
            }) {
                HStack {
                    Text("Biographical Information")
                        .font(ApproachNoteTheme.title3())
                        .foregroundColor(ApproachNoteTheme.textPrimary)
                    Spacer()
                    Image(systemName: isBiographicalInfoExpanded ? "chevron.up" : "chevron.down")
                        .foregroundColor(ApproachNoteTheme.textSecondary)
                }
                .padding()
                .background(ApproachNoteTheme.surface)
            }
            .buttonStyle(.plain)

            // Always show biography preview
            if let biography = performer.biography, !biography.isEmpty {
                Text(biography)
                    .font(ApproachNoteTheme.body())
                    .bodyLineSpacing()
                    .foregroundColor(ApproachNoteTheme.textPrimary)
                    .lineSpacing(4)
                    .lineLimit(isBiographicalInfoExpanded ? nil : 3)
                    .padding(.horizontal)
                    .padding(.top, 12)
            }

            // Expandable Content
            if isBiographicalInfoExpanded {
                VStack(alignment: .leading, spacing: 16) {
                    // Birth/Death dates
                    if performer.birthDate != nil || performer.deathDate != nil {
                        VStack(alignment: .leading, spacing: 8) {
                            if let birthDate = performer.birthDate {
                                HStack(spacing: 8) {
                                    Image(systemName: "calendar")
                                        .foregroundColor(ApproachNoteTheme.textSecondary)
                                    Text("Born: \(birthDate)")
                                        .font(ApproachNoteTheme.subheadline())
                                        .foregroundColor(ApproachNoteTheme.textSecondary)
                                }
                            }
                            if let deathDate = performer.deathDate {
                                HStack(spacing: 8) {
                                    Image(systemName: "calendar")
                                        .foregroundColor(ApproachNoteTheme.textSecondary)
                                    Text("Died: \(deathDate)")
                                        .font(ApproachNoteTheme.subheadline())
                                        .foregroundColor(ApproachNoteTheme.textSecondary)
                                }
                            }
                        }
                    }

                    // Instruments
                    if let instruments = performer.instruments, !instruments.isEmpty {
                        VStack(alignment: .leading, spacing: 8) {
                            Text("Instruments")
                                .font(ApproachNoteTheme.headline())
                                .foregroundColor(ApproachNoteTheme.textPrimary)

                            FlowLayout(spacing: 8) {
                                ForEach(instruments, id: \.name) { instrument in
                                    Text(instrument.name)
                                        .font(ApproachNoteTheme.caption())
                                        .padding(.horizontal, 12)
                                        .padding(.vertical, 6)
                                        .background(instrument.isPrimary == true ? ApproachNoteTheme.brand : ApproachNoteTheme.surface)
                                        .foregroundColor(instrument.isPrimary == true ? .white : ApproachNoteTheme.textPrimary)
                                        .cornerRadius(16)
                                }
                            }
                        }
                    }

                    // Learn More (Wikipedia, MusicBrainz)
                    learnMorePanel(performer)
                }
                .padding()
            }
        }
        .background(ApproachNoteTheme.surface)
        .cornerRadius(10)
    }

    // MARK: - Learn More Panel

    @ViewBuilder
    private func learnMorePanel(_ performer: PerformerDetail) -> some View {
        let hasWikipedia = performer.wikipediaUrl != nil
        let hasMusicbrainz = performer.musicbrainzId != nil

        if hasWikipedia || hasMusicbrainz {
            VStack(alignment: .leading, spacing: 8) {
                Text("Learn More")
                    .font(ApproachNoteTheme.headline())
                    .foregroundColor(ApproachNoteTheme.textPrimary)

                VStack(spacing: 8) {
                    // Wikipedia
                    if let wikipediaUrl = performer.wikipediaUrl, let url = URL(string: wikipediaUrl) {
                        Link(destination: url) {
                            HStack {
                                Image(systemName: "book.fill")
                                    .foregroundColor(ApproachNoteTheme.accent)
                                    .frame(width: 24)
                                Text("Wikipedia")
                                    .font(ApproachNoteTheme.body())
                                    .bodyLineSpacing()
                                    .foregroundColor(ApproachNoteTheme.textPrimary)
                                Spacer()
                                Image(systemName: "arrow.up.right.square")
                                    .foregroundColor(ApproachNoteTheme.textSecondary)
                                    .font(ApproachNoteTheme.caption())
                            }
                            .padding(.vertical, 8)
                            .padding(.horizontal, 12)
                            .background(Color.white)
                            .cornerRadius(8)
                        }
                        .buttonStyle(.plain)
                    }

                    // MusicBrainz
                    if let musicbrainzId = performer.musicbrainzId {
                        let mbUrl = URL(string: "https://musicbrainz.org/artist/\(musicbrainzId)")!
                        Link(destination: mbUrl) {
                            HStack {
                                Image(systemName: "waveform.circle.fill")
                                    .foregroundColor(ApproachNoteTheme.textPrimary)
                                    .frame(width: 24)
                                Text("MusicBrainz")
                                    .font(ApproachNoteTheme.body())
                                    .bodyLineSpacing()
                                    .foregroundColor(ApproachNoteTheme.textPrimary)
                                Spacer()
                                Image(systemName: "arrow.up.right.square")
                                    .foregroundColor(ApproachNoteTheme.textSecondary)
                                    .font(ApproachNoteTheme.caption())
                            }
                            .padding(.vertical, 8)
                            .padding(.horizontal, 12)
                            .background(Color.white)
                            .cornerRadius(8)
                        }
                        .buttonStyle(.plain)
                    }
                }
            }
            .padding()
            .background(ApproachNoteTheme.surface)
            .cornerRadius(10)
        }
    }

    // MARK: - Recordings Section

    @ViewBuilder
    private func recordingsSection(_ recordings: [PerformerRecording]) -> some View {
        let filtered = filteredRecordings(recordings)
        let grouped = groupedRecordings(filtered)

        VStack(alignment: .leading, spacing: 12) {
            // Header with count and sort
            HStack {
                Image(systemName: "music.note.list")
                    .foregroundColor(ApproachNoteTheme.brand)
                Text("Recordings")
                    .font(ApproachNoteTheme.title2())
                    .foregroundColor(ApproachNoteTheme.textPrimary)

                Text("(\(filtered.count))")
                    .font(ApproachNoteTheme.subheadline())
                    .foregroundColor(ApproachNoteTheme.textSecondary)

                Spacer()

                // Sort menu
                Menu {
                    ForEach(PerformerRecordingSortOrder.allCases) { order in
                        Button(action: { sortOrder = order }) {
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
                        Image(systemName: "chevron.down")
                            .font(.caption2)
                    }
                    .foregroundColor(ApproachNoteTheme.brand)
                    .padding(.horizontal, 8)
                    .padding(.vertical, 5)
                    .background(ApproachNoteTheme.brand.opacity(0.1))
                    .cornerRadius(6)
                }
                .menuStyle(.borderlessButton)
            }

            // Search and Filter Bar
            VStack(spacing: 12) {
                // Search Field
                HStack {
                    Image(systemName: "magnifyingglass")
                        .foregroundColor(ApproachNoteTheme.textSecondary)
                    TextField("Search recordings...", text: $searchText)
                        .textFieldStyle(.plain)
                    if !searchText.isEmpty {
                        Button(action: { searchText = "" }) {
                            Image(systemName: "xmark.circle.fill")
                                .foregroundColor(ApproachNoteTheme.textSecondary)
                        }
                        .buttonStyle(.plain)
                    }
                }
                .padding(10)
                .background(ApproachNoteTheme.surface)
                .cornerRadius(8)
                .overlay(
                    RoundedRectangle(cornerRadius: 8)
                        .stroke(ApproachNoteTheme.textSecondary.opacity(0.3), lineWidth: 1)
                )

                // Role Filter Picker
                Picker("Filter", selection: $selectedFilter) {
                    ForEach(RecordingFilter.allCases, id: \.self) { filter in
                        Text(filter.rawValue).tag(filter)
                    }
                }
                .pickerStyle(.segmented)
            }

            // Recordings content
            if isRecordingsLoading {
                VStack(spacing: 12) {
                    ProgressView()
                        .scaleEffect(1.2)
                    Text("Loading recordings...")
                        .font(ApproachNoteTheme.subheadline())
                        .foregroundColor(ApproachNoteTheme.textSecondary)
                }
                .frame(maxWidth: .infinity)
                .padding(.vertical, 40)
            } else if filtered.isEmpty {
                VStack(spacing: 12) {
                    Image(systemName: "music.note")
                        .font(.system(size: 40))
                        .foregroundColor(ApproachNoteTheme.textSecondary.opacity(0.5))
                    Text("No recordings match the current filters")
                        .font(ApproachNoteTheme.subheadline())
                        .foregroundColor(ApproachNoteTheme.textSecondary)
                    if selectedFilter != .all || !searchText.isEmpty {
                        Button("Clear Filters") {
                            selectedFilter = .all
                            searchText = ""
                        }
                        .buttonStyle(.link)
                    }
                }
                .frame(maxWidth: .infinity)
                .padding(.vertical, 40)
            } else {
                // Grouped recordings
                ForEach(grouped, id: \.groupKey) { group in
                    VStack(alignment: .leading, spacing: 8) {
                        // Group header
                        Text("\(group.groupKey) (\(group.recordings.count))")
                            .font(ApproachNoteTheme.headline())
                            .foregroundColor(ApproachNoteTheme.brand)
                            .padding(.top, 8)

                        // Horizontal scroll of recordings
                        ScrollView(.horizontal, showsIndicators: false) {
                            HStack(alignment: .top, spacing: 16) {
                                ForEach(group.recordings) { recording in
                                    PerformerRecordingCard(recording: recording)
                                        .contentShape(Rectangle())
                                        .onTapGesture {
                                            selectedRecordingId = recording.recordingId
                                        }
                                }
                            }
                            .padding(.horizontal, 4)
                        }
                    }
                }
            }
        }
    }

    // MARK: - Filtering and Grouping

    private func filteredRecordings(_ recordings: [PerformerRecording]) -> [PerformerRecording] {
        var result = recordings

        // Apply role filter
        switch selectedFilter {
        case .all:
            break
        case .leader:
            result = result.filter { $0.role?.lowercased() == "leader" }
        case .sideman:
            result = result.filter { $0.role?.lowercased() == "sideman" }
        }

        // Apply search filter
        if !searchText.isEmpty {
            let query = searchText.lowercased()
            result = result.filter { recording in
                recording.songTitle.lowercased().contains(query) ||
                (recording.albumTitle?.lowercased().contains(query) ?? false)
            }
        }

        return result
    }

    private func groupedRecordings(_ recordings: [PerformerRecording]) -> [(groupKey: String, recordings: [PerformerRecording])] {
        switch sortOrder {
        case .year:
            return groupByDecade(recordings)
        case .name:
            return groupBySongLetter(recordings)
        }
    }

    private func groupByDecade(_ recordings: [PerformerRecording]) -> [(groupKey: String, recordings: [PerformerRecording])] {
        var decadeOrder: [String] = []
        var decades: [String: [PerformerRecording]] = [:]

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

    private func groupBySongLetter(_ recordings: [PerformerRecording]) -> [(groupKey: String, recordings: [PerformerRecording])] {
        var letterOrder: [String] = []
        var letters: [String: [PerformerRecording]] = [:]

        for recording in recordings {
            let firstChar = recording.songTitle.prefix(1).uppercased()
            let letterKey = firstChar.first?.isLetter == true ? firstChar : "#"

            if letters[letterKey] == nil {
                letterOrder.append(letterKey)
            }
            letters[letterKey, default: []].append(recording)
        }

        // Sort letter order alphabetically
        letterOrder.sort()

        return letterOrder.compactMap { key in
            guard let recs = letters[key] else { return nil }
            return (groupKey: key, recordings: recs)
        }
    }

    // MARK: - Data Loading

    private func loadPerformer() async {
        isLoading = true
        isRecordingsLoading = true

        // Phase 1: Load summary (fast)
        let fetchedPerformer = await performerService.fetchPerformerSummary(id: performerId)
        performer = fetchedPerformer
        isLoading = false

        // Phase 2: Load recordings
        if let recordings = await performerService.fetchPerformerRecordings(id: performerId, sortBy: sortOrder) {
            performer?.recordings = recordings
        }
        isRecordingsLoading = false
    }

    private func reloadRecordings(sortBy order: PerformerRecordingSortOrder) async {
        isRecordingsLoading = true
        if let recordings = await performerService.fetchPerformerRecordings(id: performerId, sortBy: order) {
            performer?.recordings = recordings
        }
        isRecordingsLoading = false
    }
}

// MARK: - Performer Recording Card

struct PerformerRecordingCard: View {
    let recording: PerformerRecording
    @State private var isHovering = false

    private let artworkSize: CGFloat = 160

    private var coverUrl: String? {
        recording.bestCoverArtMedium ?? recording.bestCoverArtSmall
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            // Album artwork with badges
            ZStack(alignment: .topTrailing) {
                AsyncImage(url: URL(string: coverUrl ?? "")) { image in
                    image
                        .resizable()
                        .aspectRatio(contentMode: .fill)
                } placeholder: {
                    Rectangle()
                        .fill(ApproachNoteTheme.surface)
                        .overlay {
                            Image(systemName: "music.note")
                                .font(.system(size: 40))
                                .foregroundColor(ApproachNoteTheme.textSecondary)
                        }
                }
                .frame(width: artworkSize, height: artworkSize)
                .clipShape(RoundedRectangle(cornerRadius: 10))
                .shadow(color: .black.opacity(0.15), radius: 6, x: 0, y: 3)

                // Canonical badge
                if recording.isCanonical == true {
                    Image(systemName: "star.fill")
                        .foregroundColor(.yellow)
                        .font(ApproachNoteTheme.caption())
                        .padding(6)
                        .background(Color.black.opacity(0.6))
                        .clipShape(Circle())
                        .padding(6)
                }
            }

            // Song title
            Text(recording.songTitle)
                .font(ApproachNoteTheme.subheadline(weight: .semibold))
                .foregroundColor(ApproachNoteTheme.textSecondary)
                .lineLimit(1)
                .frame(width: artworkSize, alignment: .leading)

            // Album title
            Text(recording.albumTitle ?? "Unknown Album")
                .font(ApproachNoteTheme.body(weight: .medium))
                .bodyLineSpacing()
                .foregroundColor(ApproachNoteTheme.textPrimary)
                .lineLimit(2)
                .frame(width: artworkSize, alignment: .leading)

            // Year
            if let year = recording.recordingYear {
                Text(String(year))
                    .font(ApproachNoteTheme.caption())
                    .foregroundColor(ApproachNoteTheme.textSecondary)
                    .frame(width: artworkSize, alignment: .leading)
            }
        }
        .padding(12)
        .background(isHovering ? ApproachNoteTheme.background : ApproachNoteTheme.surface)
        .clipShape(RoundedRectangle(cornerRadius: 12))
        .overlay(
            RoundedRectangle(cornerRadius: 12)
                .stroke(isHovering ? ApproachNoteTheme.brand.opacity(0.5) : Color.clear, lineWidth: 2)
        )
        .onHover { hovering in
            isHovering = hovering
        }
        .animation(.easeInOut(duration: 0.15), value: isHovering)
    }
}

// MARK: - Flow Layout for Tags

struct FlowLayout: Layout {
    var spacing: CGFloat = 8

    func sizeThatFits(proposal: ProposedViewSize, subviews: Subviews, cache: inout ()) -> CGSize {
        let result = FlowResult(in: proposal.width ?? 0, subviews: subviews, spacing: spacing)
        return result.size
    }

    func placeSubviews(in bounds: CGRect, proposal: ProposedViewSize, subviews: Subviews, cache: inout ()) {
        let result = FlowResult(in: bounds.width, subviews: subviews, spacing: spacing)
        for (index, subview) in subviews.enumerated() {
            subview.place(at: CGPoint(x: bounds.minX + result.positions[index].x,
                                       y: bounds.minY + result.positions[index].y),
                         proposal: .unspecified)
        }
    }

    struct FlowResult {
        var size: CGSize = .zero
        var positions: [CGPoint] = []

        init(in maxWidth: CGFloat, subviews: Subviews, spacing: CGFloat) {
            var x: CGFloat = 0
            var y: CGFloat = 0
            var rowHeight: CGFloat = 0

            for subview in subviews {
                let size = subview.sizeThatFits(.unspecified)

                if x + size.width > maxWidth && x > 0 {
                    x = 0
                    y += rowHeight + spacing
                    rowHeight = 0
                }

                positions.append(CGPoint(x: x, y: y))
                rowHeight = max(rowHeight, size.height)
                x += size.width + spacing
            }

            self.size = CGSize(width: maxWidth, height: y + rowHeight)
        }
    }
}

#Preview {
    PerformerDetailView(performerId: "preview-id")
}
