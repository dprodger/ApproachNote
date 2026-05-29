//
//  PerformerDetailView.swift
//  Approach Note
//
//  macOS-specific performer/artist detail view. Mirrors the iOS layout:
//  an image carousel header, a typography-only BIOGRAPHY block with a
//  height-capped Read More, iOS-styled Learn More links, and a RECORDINGS
//  section with per-group +/- accordions and brand-styled controls.
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
    @State private var sortOrder: PerformerRecordingSortOrder = .year
    @State private var selectedFilter: RecordingFilter = .all
    @State private var searchText: String = ""
    @State private var selectedRecordingId: String?

    // Per-group expansion state — every shelf starts collapsed (mirrors iOS).
    @State private var expandedGroups: Set<String> = []

    // On-screen height of the scroll viewport; caps the collapsed biography.
    @State private var viewportHeight: CGFloat = 0

    @StateObject private var performerService = PerformerService()
    @Environment(\.openURL) private var openURL

    var body: some View {
        ScrollView {
            if isLoading {
                ThemedProgressView(message: "Loading...")
                    .frame(maxWidth: .infinity, maxHeight: .infinity)
                    .padding(.top, 100)
            } else if let performer = performer {
                // Section rhythm matches SongDetailView (spacingMD between sections).
                VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingMD) {
                    // Header with image carousel
                    performerHeader(performer)

                    // Biography (typography only, height-capped Read More)
                    biographySection(performer)

                    // Learn More links (iOS styling)
                    learnMorePanel(performer)

                    Divider()

                    // Recordings with filtering, sort, and +/- accordions
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
        .background(
            // ScrollView frame == viewport; reports the on-screen height used
            // to cap the collapsed biography at ~30%.
            GeometryReader { proxy in
                Color.clear
                    .onAppear { viewportHeight = proxy.size.height }
                    .onChange(of: proxy.size.height) { _, newValue in viewportHeight = newValue }
            }
        )
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

    // MARK: - Header

    @ViewBuilder
    private func performerHeader(_ performer: PerformerDetail) -> some View {
        HStack(alignment: .top, spacing: ApproachNoteTheme.spacingXL) {
            // Artist image(s) — pages through all images when there are several.
            PerformerImageCarousel(images: performer.images ?? [])

            VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingXS) {
                Text(performer.name)
                    .font(ApproachNoteTheme.largeTitle())
                    .foregroundColor(ApproachNoteTheme.textPrimary)

                // Lifespan: "1926 May 26" or "1926 May 26 – 1991 Sep 28"
                if let lifespan = formattedLifespan(birth: performer.birthDate, death: performer.deathDate) {
                    Text(lifespan)
                        .font(ApproachNoteTheme.body())
                        .foregroundColor(ApproachNoteTheme.textSecondary)
                }

                // All instruments, primary-first (matches iOS).
                if let instruments = performer.instruments, !instruments.isEmpty {
                    Text(instrumentList(instruments))
                        .font(ApproachNoteTheme.body())
                        .foregroundColor(ApproachNoteTheme.textSecondary)
                }
            }

            Spacer()
        }
    }

    // MARK: - Biography

    @ViewBuilder
    private func biographySection(_ performer: PerformerDetail) -> some View {
        if let biography = performer.biography, !biography.isEmpty {
            VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingSM) {
                Text("BIOGRAPHY")
                    .font(ApproachNoteTheme.title3())
                    .bold()
                    .foregroundColor(ApproachNoteTheme.textPrimary)

                ExpandableBiography(
                    biography: biography,
                    maxCollapsedHeight: viewportHeight > 0 ? viewportHeight * 0.30 : .greatestFiniteMagnitude
                )
            }
        }
    }

    // MARK: - Learn More

    @ViewBuilder
    private func learnMorePanel(_ performer: PerformerDetail) -> some View {
        let wikipediaURL = performer.wikipediaUrl
        let jazzStandardsURL = performer.externalLinks?["jazzstandards"]
        let musicbrainzURL = performer.musicbrainzId.map { "https://musicbrainz.org/artist/\($0)" }

        if wikipediaURL != nil || jazzStandardsURL != nil || musicbrainzURL != nil {
            VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingSM) {
                Text("Learn More:")
                    .font(ApproachNoteTheme.body(weight: .semibold))
                    .bodyLineSpacing()
                    .foregroundColor(ApproachNoteTheme.textPrimary)

                VStack(spacing: ApproachNoteTheme.spacingXS) {
                    if let wikipediaURL {
                        externalLinkButton("Wikipedia", url: wikipediaURL)
                    }
                    if let jazzStandardsURL {
                        externalLinkButton("Jazz Standards", url: jazzStandardsURL)
                    }
                    if let musicbrainzURL {
                        externalLinkButton("MusicBrainz", url: musicbrainzURL)
                    }
                }
                // Keep the iOS pill styling but at a sensible width on a wide window.
                .frame(maxWidth: 360, alignment: .leading)
            }
        }
    }

    @ViewBuilder
    private func externalLinkButton(_ label: String, url urlString: String) -> some View {
        ApproachNoteButton(label, style: .secondary, trailingSystemImage: "arrow.up.right.square") {
            if let url = URL(string: urlString) {
                openURL(url)
            }
        }
    }

    // MARK: - Recordings Section

    @ViewBuilder
    private func recordingsSection(_ recordings: [PerformerRecording]) -> some View {
        let filtered = filteredRecordings(recordings)
        let grouped = groupedRecordings(filtered)

        VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingMD) {
            // Header — typography only (no icon); sort pill sits next to the
            // title rather than pinned to the far right.
            HStack(alignment: .center, spacing: ApproachNoteTheme.spacingXS) {
                HStack(alignment: .firstTextBaseline, spacing: ApproachNoteTheme.spacingXS) {
                    Text("RECORDINGS")
                        .font(ApproachNoteTheme.title3())
                        .bold()
                        .foregroundColor(ApproachNoteTheme.textPrimary)

                    Text("(\(filtered.count))")
                        .font(ApproachNoteTheme.subheadline())
                        .foregroundColor(ApproachNoteTheme.textSecondary)
                }

                sortMenu
                    .fixedSize()

                Spacer()
            }

            // Controls — search + brand-styled role segmented (mirrors iOS).
            VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingMD) {
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
                .padding(ApproachNoteTheme.spacingXS)
                .background(ApproachNoteTheme.surface)
                .cornerRadius(8)
                .overlay(
                    RoundedRectangle(cornerRadius: 8)
                        .stroke(ApproachNoteTheme.textSecondary.opacity(0.5), lineWidth: 1)
                )

                VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingXS) {
                    Text("Role")
                        .font(ApproachNoteTheme.callout(weight: .semibold))
                        .foregroundColor(ApproachNoteTheme.textPrimary)

                    rolePicker
                }
            }

            // Content
            if isRecordingsLoading {
                VStack(spacing: ApproachNoteTheme.spacingSM) {
                    ProgressView()
                        .scaleEffect(1.2)
                    Text("Loading recordings...")
                        .font(ApproachNoteTheme.subheadline())
                        .foregroundColor(ApproachNoteTheme.textSecondary)
                }
                .frame(maxWidth: .infinity)
                .padding(.vertical, 40)
            } else if filtered.isEmpty {
                VStack(spacing: ApproachNoteTheme.spacingSM) {
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
                LazyVStack(alignment: .leading, spacing: 0) {
                    ForEach(grouped, id: \.groupKey) { group in
                        groupAccordion(group)
                    }
                }
                .padding(.top, ApproachNoteTheme.spacingXS)
            }
        }
    }

    // MARK: Sort menu (bordered pill)

    @ViewBuilder
    private var sortMenu: some View {
        Menu {
            ForEach(PerformerRecordingSortOrder.allCases) { order in
                Button(action: {
                    if sortOrder != order {
                        expandedGroups.removeAll()
                        sortOrder = order
                    }
                }) {
                    HStack {
                        Text(order.displayName)
                        if sortOrder == order {
                            Image(systemName: "checkmark")
                        }
                    }
                }
            }
        } label: {
            HStack(spacing: ApproachNoteTheme.spacingXS) {
                (
                    Text("Sort:")
                        .font(ApproachNoteTheme.subheadline(weight: .bold))
                    + Text(" \(sortOrder.displayName)")
                        .font(ApproachNoteTheme.subheadline())
                )
                .lineLimit(1)
                Image(systemName: "chevron.down")
                    .font(.caption)
            }
            .foregroundColor(ApproachNoteTheme.textPrimary)
            .padding(.horizontal, ApproachNoteTheme.spacingSM)
            .padding(.vertical, ApproachNoteTheme.spacingXS)
            .background(ApproachNoteTheme.surface)
            .cornerRadius(8)
            .overlay(
                RoundedRectangle(cornerRadius: 8)
                    .stroke(ApproachNoteTheme.textSecondary.opacity(0.5), lineWidth: 1)
            )
        }
        .menuStyle(.borderlessButton)
        .menuIndicator(.hidden)
    }

    // MARK: Role picker (brand-outlined segmented, matches iOS)

    @ViewBuilder
    private var rolePicker: some View {
        HStack(spacing: 0) {
            ForEach(Array(RecordingFilter.allCases.enumerated()), id: \.element) { index, filter in
                if index > 0 {
                    Spacer(minLength: 4)
                }
                let isSelected = selectedFilter == filter
                Button {
                    selectedFilter = filter
                } label: {
                    Text(filter.rawValue.uppercased())
                        .font(ApproachNoteTheme.footnote(weight: .semibold))
                        .lineLimit(1)
                        .minimumScaleFactor(0.85)
                        .foregroundColor(isSelected ? ApproachNoteTheme.textOnAccent : ApproachNoteTheme.brand)
                        .padding(.horizontal, ApproachNoteTheme.spacingMD)
                        .padding(.vertical, ApproachNoteTheme.spacingXS)
                        .background(
                            Capsule().fill(isSelected ? ApproachNoteTheme.brand : Color.clear)
                        )
                        .contentShape(Capsule())
                }
                .buttonStyle(.plain)
            }
        }
        .padding(.horizontal, ApproachNoteTheme.spacingXXS)
        .padding(.vertical, ApproachNoteTheme.spacingXXS)
        .frame(maxWidth: 360)
        .overlay(
            Capsule().stroke(ApproachNoteTheme.brand, lineWidth: 1.5)
        )
        .animation(.easeInOut(duration: 0.15), value: selectedFilter)
    }

    // MARK: Group accordion

    @ViewBuilder
    private func groupAccordion(_ group: (groupKey: String, recordings: [PerformerRecording])) -> some View {
        let isExpanded = expandedGroups.contains(group.groupKey)

        VStack(alignment: .leading, spacing: 0) {
            Divider()

            Button(action: {
                withAnimation(.easeInOut(duration: 0.2)) {
                    if isExpanded {
                        expandedGroups.remove(group.groupKey)
                    } else {
                        expandedGroups.insert(group.groupKey)
                    }
                }
            }) {
                HStack {
                    Text("\(group.groupKey) (\(group.recordings.count))")
                        .font(ApproachNoteTheme.headline())
                        .foregroundColor(ApproachNoteTheme.brand)
                    Spacer()
                    Image(systemName: isExpanded ? "minus" : "plus")
                        .font(ApproachNoteTheme.headline())
                        .foregroundColor(ApproachNoteTheme.brand)
                }
                .padding(.vertical, ApproachNoteTheme.spacingSM)
                .contentShape(Rectangle())
            }
            .buttonStyle(.plain)

            if isExpanded {
                ScrollView(.horizontal, showsIndicators: false) {
                    HStack(alignment: .top, spacing: ApproachNoteTheme.spacingMD) {
                        ForEach(group.recordings) { recording in
                            PerformerRecordingCard(recording: recording)
                                .contentShape(Rectangle())
                                .onTapGesture {
                                    selectedRecordingId = recording.recordingId
                                }
                        }
                    }
                    .padding(.vertical, ApproachNoteTheme.spacingXS)
                }
                .padding(.bottom, ApproachNoteTheme.spacingSM)
            }
        }
    }

    // MARK: - Filtering and Grouping

    private func filteredRecordings(_ recordings: [PerformerRecording]) -> [PerformerRecording] {
        var result = recordings

        switch selectedFilter {
        case .all:
            break
        case .leader:
            result = result.filter { $0.role?.lowercased() == "leader" }
        case .sideman:
            result = result.filter { $0.role?.lowercased() == "sideman" }
        }

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

        letterOrder.sort()

        return letterOrder.compactMap { key in
            guard let recs = letters[key] else { return nil }
            return (groupKey: key, recordings: recs)
        }
    }

    // MARK: - Formatting Helpers (match iOS)

    /// "1926 May 26" (birth only) or "1926 May 26 – 1991 Sep 28" (birth–death).
    private func formattedLifespan(birth: String?, death: String?) -> String? {
        switch (formatPartialDate(birth), formatPartialDate(death)) {
        case let (b?, d?): return "\(b) – \(d)"
        case let (b?, nil): return b
        case let (nil, d?): return d
        default: return nil
        }
    }

    /// Formats a (possibly partial) ISO date string as "YYYY Mon D".
    /// MusicBrainz dates can be year-only ("1926"), year-month ("1926-05"),
    /// or full ("1926-05-26"); each renders with as much detail as it carries.
    private func formatPartialDate(_ raw: String?) -> String? {
        guard let raw = raw?.trimmingCharacters(in: .whitespaces), !raw.isEmpty else { return nil }
        let parts = raw.split(separator: "-")
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "en_US_POSIX")

        if parts.count >= 3 {
            formatter.dateFormat = "yyyy-MM-dd"
            if let date = formatter.date(from: "\(parts[0])-\(parts[1])-\(parts[2])") {
                formatter.dateFormat = "yyyy MMM d"
                return formatter.string(from: date)
            }
        }
        if parts.count == 2 {
            formatter.dateFormat = "yyyy-MM"
            if let date = formatter.date(from: "\(parts[0])-\(parts[1])") {
                formatter.dateFormat = "yyyy MMM"
                return formatter.string(from: date)
            }
        }
        return String(parts[0])
    }

    /// Comma-separated instrument names, primary instruments first.
    private func instrumentList(_ instruments: [PerformerInstrument]) -> String {
        let sorted = instruments.sorted { ($0.isPrimary == true) && !($1.isPrimary == true) }
        return sorted.map(\.name).joined(separator: ", ")
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

// MARK: - Performer Image Carousel
//
// Fixed-size square hero. A single image shows on its own; with multiple
// images, the strip pages with a trackpad swipe, and hover-revealed arrow
// buttons + tappable page dots step through them for mouse users. All three
// share one scroll-position binding so swipe and clicks stay in sync.
private struct PerformerImageCarousel: View {
    let images: [ArtistImage]

    private let size: CGFloat = 320
    @State private var scrolledImageID: String?
    @State private var isHovering = false

    private var currentIndex: Int {
        guard let id = scrolledImageID,
              let idx = images.firstIndex(where: { $0.id == id }) else { return 0 }
        return idx
    }

    var body: some View {
        Group {
            if images.isEmpty {
                placeholder
            } else if images.count == 1 {
                thumbnail(images[0])
            } else {
                ScrollView(.horizontal, showsIndicators: false) {
                    LazyHStack(spacing: 0) {
                        ForEach(images) { image in
                            thumbnail(image)
                                .containerRelativeFrame(.horizontal)
                        }
                    }
                    .scrollTargetLayout()
                }
                .scrollTargetBehavior(.paging)
                .scrollPosition(id: $scrolledImageID)
                .overlay(alignment: .leading) {
                    if isHovering {
                        navButton(systemImage: "chevron.left") { step(-1) }
                    }
                }
                .overlay(alignment: .trailing) {
                    if isHovering {
                        navButton(systemImage: "chevron.right") { step(1) }
                    }
                }
                .overlay(alignment: .bottom) {
                    PageDots(count: images.count, current: currentIndex) { target in
                        withAnimation(.easeInOut(duration: 0.2)) {
                            scrolledImageID = images[target].id
                        }
                    }
                    .padding(.bottom, ApproachNoteTheme.spacingXS)
                }
                .onAppear {
                    if scrolledImageID == nil { scrolledImageID = images.first?.id }
                }
            }
        }
        .frame(width: size, height: size)
        .clipShape(RoundedRectangle(cornerRadius: 12))
        .onHover { isHovering = $0 }
    }

    private func step(_ delta: Int) {
        guard !images.isEmpty else { return }
        let target = (currentIndex + delta + images.count) % images.count
        withAnimation(.easeInOut(duration: 0.2)) {
            scrolledImageID = images[target].id
        }
    }

    private func thumbnail(_ image: ArtistImage) -> some View {
        AsyncImage(url: URL(string: image.thumbnailUrl ?? image.url)) { img in
            img
                .resizable()
                // Keep the full image and scale to fit (no cropping); the
                // square is filled behind it so letterboxing looks intentional.
                .aspectRatio(contentMode: .fit)
        } placeholder: {
            placeholder
        }
        .frame(width: size, height: size)
        .background(ApproachNoteTheme.surface)
    }

    private func navButton(systemImage: String, action: @escaping () -> Void) -> some View {
        Button(action: action) {
            Image(systemName: systemImage)
                .font(.system(size: 14, weight: .semibold))
                .foregroundColor(.white)
                .padding(ApproachNoteTheme.spacingXS)
                .background(Circle().fill(Color.black.opacity(0.45)))
        }
        .buttonStyle(.plain)
        .padding(ApproachNoteTheme.spacingXS)
    }

    private var placeholder: some View {
        Rectangle()
            .fill(ApproachNoteTheme.surface)
            .frame(width: size, height: size)
            .overlay {
                Image(systemName: "person.fill")
                    .font(.system(size: 40))
                    .foregroundColor(ApproachNoteTheme.textSecondary)
            }
    }
}

// MARK: - Page Dots

private struct PageDots: View {
    let count: Int
    let current: Int
    var onSelect: ((Int) -> Void)?

    var body: some View {
        HStack(spacing: ApproachNoteTheme.spacingXS) {
            ForEach(0..<count, id: \.self) { index in
                Circle()
                    .fill(Color.white.opacity(index == current ? 0.95 : 0.45))
                    .frame(width: 7, height: 7)
                    .contentShape(Circle())
                    .onTapGesture { onSelect?(index) }
            }
        }
        .padding(.horizontal, ApproachNoteTheme.spacingXS)
        .padding(.vertical, 6)
        .background(Capsule().fill(Color.black.opacity(0.35)))
    }
}

// MARK: - Expandable Biography
//
// Shows the biography clamped to `maxCollapsedHeight` (~30% of the screen).
// When the full text exceeds that height, a READ MORE button expands it
// inline. A hidden full-height copy measures the real height so we only
// offer READ MORE when the text actually overflows. (Mirrors iOS.)
private struct ExpandableBiography: View {
    let biography: String
    let maxCollapsedHeight: CGFloat

    @State private var isExpanded = false
    @State private var fullHeight: CGFloat = 0

    private var paragraphs: [String] {
        biography.components(separatedBy: "\n\n").filter { !$0.isEmpty }
    }

    private var isTruncatable: Bool {
        fullHeight > maxCollapsedHeight + 1
    }

    @ViewBuilder
    private var bioText: some View {
        VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingSM) {
            ForEach(Array(paragraphs.enumerated()), id: \.offset) { _, paragraph in
                Text(paragraph)
                    .font(ApproachNoteTheme.body())
                    .bodyLineSpacing()
                    .foregroundColor(ApproachNoteTheme.textSecondary)
                    .fixedSize(horizontal: false, vertical: true)
            }
        }
        .frame(maxWidth: .infinity, alignment: .leading)
    }

    var body: some View {
        VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingMD) {
            bioText
                .frame(maxHeight: isExpanded ? nil : maxCollapsedHeight, alignment: .top)
                .clipped()
                .background(
                    // Hidden full-height copy; .fixedSize forces the ideal
                    // height (ignoring the clamp above) so we can detect overflow.
                    bioText
                        .fixedSize(horizontal: false, vertical: true)
                        .background(
                            GeometryReader { proxy in
                                Color.clear
                                    .onAppear { fullHeight = proxy.size.height }
                                    .onChange(of: proxy.size.height) { _, newValue in fullHeight = newValue }
                            }
                        )
                        .hidden()
                )

            if isTruncatable && !isExpanded {
                ApproachNoteButton("Read More", style: .secondary) {
                    withAnimation(.easeInOut(duration: 0.2)) { isExpanded = true }
                }
                .frame(maxWidth: 200, alignment: .leading)
            }
        }
    }
}

// MARK: - Performer Recording Card
//
// De-carded shelf item mirroring the iOS card: square artwork, then
// Year (bold) · Recording title (bold) · Album title (normal).
struct PerformerRecordingCard: View {
    let recording: PerformerRecording
    @State private var isHovering = false

    private let artworkSize: CGFloat = 150

    private var coverUrl: String? {
        recording.bestCoverArtMedium ?? recording.bestCoverArtSmall
    }

    var body: some View {
        VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingXS) {
            // Album artwork with canonical badge
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
                .clipShape(RoundedRectangle(cornerRadius: 8))
                .shadow(color: .black.opacity(isHovering ? 0.25 : 0.15),
                        radius: isHovering ? 8 : 6, x: 0, y: 3)

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
            .frame(width: artworkSize)

            // Year (bold)
            if let year = recording.recordingYear {
                Text(String(year))
                    .font(ApproachNoteTheme.subheadline(weight: .bold))
                    .foregroundColor(ApproachNoteTheme.textPrimary)
                    .frame(width: artworkSize, alignment: .leading)
            }

            // Recording / song title (bold)
            Text(recording.songTitle)
                .font(ApproachNoteTheme.subheadline(weight: .bold))
                .foregroundColor(ApproachNoteTheme.textPrimary)
                .lineLimit(1)
                .frame(width: artworkSize, alignment: .leading)

            // Album title (normal)
            Text(recording.albumTitle ?? "Unknown Album")
                .font(ApproachNoteTheme.subheadline())
                .foregroundColor(ApproachNoteTheme.textPrimary)
                .lineLimit(2)
                .frame(width: artworkSize, alignment: .leading)
        }
        .frame(width: artworkSize)
        .onHover { isHovering = $0 }
        .animation(.easeInOut(duration: 0.15), value: isHovering)
    }
}

#Preview {
    PerformerDetailView(performerId: "preview-id")
}
