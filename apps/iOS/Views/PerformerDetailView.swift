//
//  PerformerDetailView.swift
//  Approach Note
//
//  Updated with ApproachNoteTheme color palette and ExternalReferencesPanel
//

import SwiftUI

enum RecordingFilter: String, CaseIterable {
    case all = "All"
    case leader = "Leader"
    case sideman = "Sideman"
}

struct PerformerDetailView: View {
    let performerId: String
    @State private var performer: PerformerDetail?
    @State private var isLoading = true
    @State private var selectedFilter: RecordingFilter = .all
    @State private var recordingSortOrder: PerformerRecordingSortOrder = .year
    @State private var isRecordingsReloading = false

    // Viewport height drives the collapsed biography cap (~75% of screen).
    @State private var viewportHeight: CGFloat = 0

    // Two-phase loading: summary loads first (fast), then recordings load in background
    @State private var isRecordingsLoading: Bool = true

    var body: some View {
        ScrollView {
            VStack(spacing: 0) {
                DetailHeaderSpacer()

            if isLoading {
                VStack {
                    Spacer()
                    ThemedProgressView(message: "Loading...", tintColor: ApproachNoteTheme.accent)
                    Spacer()
                }
                .frame(maxWidth: .infinity, maxHeight: .infinity)
                .background(ApproachNoteTheme.background)
            } else if let performer = performer {
                VStack(alignment: .leading, spacing: 0) {
                    // Image hero — full-bleed, above the name, swipeable.
                    if let images = performer.images, !images.isEmpty {
                        ArtistImageCarousel(images: images)
                    }

                    // Header + biography content (shares the 24pt screen gutter).
                    VStack(alignment: .leading, spacing: 16) {
                        // Artist Name
                        Text(performer.name)
                            .font(ApproachNoteTheme.largeTitle())
                            .bold()
                            .foregroundColor(ApproachNoteTheme.textPrimary)

                        // Lifespan: "1926 May 26" or "1926 May 26 – 1991 Sep 28"
                        if let lifespan = formattedLifespan(birth: performer.birthDate, death: performer.deathDate) {
                            Text(lifespan)
                                .font(ApproachNoteTheme.body())
                                .foregroundColor(ApproachNoteTheme.textSecondary)
                        }

                        // Instruments (after dates, before biography)
                        if let instruments = performer.instruments, !instruments.isEmpty {
                            Text(instrumentList(instruments))
                                .font(ApproachNoteTheme.body())
                                .foregroundColor(ApproachNoteTheme.textSecondary)
                        }

                        // Biography
                        if let biography = performer.biography, !biography.isEmpty {
                            VStack(alignment: .leading, spacing: 12) {
                                Text("BIOGRAPHY")
                                    .font(ApproachNoteTheme.title3())
                                    .bold()
                                    .foregroundColor(ApproachNoteTheme.textPrimary)

                                ExpandableBiography(
                                    biography: biography,
                                    maxCollapsedHeight: viewportHeight > 0 ? viewportHeight * 0.5625 : .greatestFiniteMagnitude
                                )
                            }
                            .padding(.top, 8)
                        }

                        // Learn More links (after the biography, before recordings)
                        ExternalReferencesPanel(
                            wikipediaUrl: performer.wikipediaUrl,
                            musicbrainzId: performer.musicbrainzId,
                            externalLinks: performer.externalLinks,
                            entityId: performer.id,
                            entityName: performer.name,
                            isArtist: true,
                            showsBackground: false
                        )
                        .padding(.top, 8)
                    }
                    .padding(.horizontal, 24)
                    .padding(.top, (performer.images?.isEmpty == false) ? 20 : 24)
                    .padding(.bottom, 16)

                    // Recordings Section (mirrors SongDetailView layout)
                    PerformerRecordingsSection(
                        recordings: performer.recordings ?? [],
                        performerName: performer.name,
                        sortOrder: $recordingSortOrder,
                        selectedFilter: $selectedFilter,
                        isReloading: isRecordingsReloading || isRecordingsLoading,
                        onSortOrderChanged: { newOrder in
                            Task {
                                isRecordingsReloading = true
                                let performerService = PerformerService()
                                if let recordings = await performerService.fetchPerformerRecordings(id: performerId, sortBy: newOrder) {
                                    self.performer?.recordings = recordings
                                }
                                isRecordingsReloading = false
                            }
                        }
                    )
                }
                .padding(.bottom, 16)
            } else {
                VStack {
                    Spacer()
                    Text("Performer not found")
                        .foregroundColor(ApproachNoteTheme.textSecondary)
                    Spacer()
                }
                .frame(maxWidth: .infinity, maxHeight: .infinity)
                .background(ApproachNoteTheme.background)
            }
            }
        }
        .background(
            // ScrollView frame == viewport, so this reports the on-screen
            // height used to cap the collapsed biography at ~75%.
            GeometryReader { proxy in
                Color.clear
                    .onAppear { viewportHeight = proxy.size.height }
                    .onChange(of: proxy.size.height) { _, newValue in viewportHeight = newValue }
            }
        )
        .collapsingDetailHeader(
            expandedTitle: "Artist",
            collapsedTitle: performer?.name ?? "Artist"
        )
        .task {
            #if DEBUG
            if ProcessInfo.processInfo.environment["XCODE_RUNNING_FOR_PREVIEWS"] == "1" {
                let performerService = PerformerService()
                performer = performerService.fetchPerformerDetailSync(id: performerId)
                isLoading = false
                isRecordingsLoading = false
                return
            }
            #endif

            let performerService = PerformerService()

            // Phase 1: Load summary (fast) - includes performer metadata, bio, instruments, images
            let fetchedPerformer = await performerService.fetchPerformerSummary(id: performerId)
            await MainActor.run {
                performer = fetchedPerformer
                isLoading = false
            }

            // Phase 2: Load all recordings in background
            if let recordings = await performerService.fetchPerformerRecordings(id: performerId, sortBy: recordingSortOrder) {
                await MainActor.run {
                    self.performer?.recordings = recordings
                    isRecordingsLoading = false
                }
            } else {
                await MainActor.run {
                    isRecordingsLoading = false
                }
            }
        }
    }

    // MARK: - Formatting Helpers

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
}

// MARK: - Expandable Biography
//
// Shows the biography clamped to `maxCollapsedHeight` (~75% of the screen).
// When the full text exceeds that height, a READ MORE button expands it
// inline. A hidden full-height copy measures the real height so we only
// offer READ MORE when the text actually overflows.
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
        VStack(alignment: .leading, spacing: 12) {
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
        VStack(alignment: .leading, spacing: 16) {
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
            }
        }
    }
}

#Preview("Performer - Full Details") {
    NavigationStack {
        PerformerDetailView(performerId: "preview-performer-detail-1")
    }
}
#Preview("Performer - Minimal") {
    NavigationStack {
        PerformerDetailView(performerId: "preview-performer-detail-2")
    }
}
