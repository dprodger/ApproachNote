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

    // Viewport size drives the collapsed biography cap (~75% of screen height)
    // and the artist image hero's height cap (keeps tall portraits on-screen).
    @State private var viewportHeight: CGFloat = 0
    @State private var viewportWidth: CGFloat = 0

    // The carousel image currently on screen, so the license line beneath it
    // tracks swipes between images.
    @State private var currentArtistImage: ArtistImage?

    // Two-phase loading: summary loads first (fast), then recordings load in background
    @State private var isRecordingsLoading: Bool = true

    /// Regular width (iPad) gets a two-column top section: metadata on the left,
    /// a smaller artist image on the right. Compact width keeps the full-bleed
    /// image hero stacked above the metadata.
    @Environment(\.horizontalSizeClass) private var horizontalSizeClass
    private var isWideLayout: Bool { horizontalSizeClass == .regular }
    private static let wideArtworkMaxWidth: CGFloat = 360

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
                    // Image + header — two-column on wide (iPad) layouts, stacked
                    // (full-bleed image above the metadata) on compact layouts.
                    performerTopSection(performer)

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
                .padding(.bottom, ApproachNoteTheme.spacingMD)
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
                    .onAppear {
                        viewportHeight = proxy.size.height
                        viewportWidth = proxy.size.width
                    }
                    .onChange(of: proxy.size.height) { _, newValue in viewportHeight = newValue }
                    .onChange(of: proxy.size.width) { _, newValue in viewportWidth = newValue }
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

    // MARK: - Top Section (image + header)

    /// Image hero + header metadata. On regular-width layouts the metadata sits
    /// to the left of a roughly half-size artist image; on compact layouts the
    /// image is a full-bleed hero stacked above the metadata, as before.
    @ViewBuilder
    private func performerTopSection(_ performer: PerformerDetail) -> some View {
        if isWideLayout {
            HStack(alignment: .top, spacing: ApproachNoteTheme.spacingXL) {
                performerHeader(performer)
                    .frame(maxWidth: .infinity, alignment: .leading)

                if let images = performer.images, !images.isEmpty {
                    VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingXS) {
                        ArtistImageCarousel(
                            images: images,
                            availableWidth: Self.wideArtworkMaxWidth,
                            maxHeight: viewportHeight,
                            currentImage: $currentArtistImage
                        )
                        .frame(maxWidth: Self.wideArtworkMaxWidth)
                        .cornerRadius(12)

                        if let current = currentArtistImage {
                            ArtistImageCreditLine(image: current)
                                .frame(maxWidth: Self.wideArtworkMaxWidth, alignment: .leading)
                        }
                    }
                }
            }
            .padding(.horizontal, ApproachNoteTheme.spacingXL)
            .padding(.top, ApproachNoteTheme.spacingXL)
            .padding(.bottom, ApproachNoteTheme.spacingMD)
        } else {
            VStack(alignment: .leading, spacing: 0) {
                let hasImages = performer.images?.isEmpty == false

                // Identity first (name, dates, instruments) — sits just below
                // the brand header so the artist's name leads the screen.
                performerIdentity(performer)
                    .padding(.horizontal, ApproachNoteTheme.spacingXL)
                    .padding(.top, ApproachNoteTheme.spacingMD)

                // Image hero — full-bleed, below the name, swipeable.
                if let images = performer.images, !images.isEmpty {
                    ArtistImageCarousel(
                        images: images,
                        availableWidth: viewportWidth,
                        maxHeight: viewportHeight,
                        currentImage: $currentArtistImage
                    )
                    .padding(.top, ApproachNoteTheme.spacingLG)

                    // License/attribution for the on-screen image. Indented to
                    // the screen gutter so it aligns with the name and bio
                    // (the image itself is full-bleed).
                    if let current = currentArtistImage {
                        ArtistImageCreditLine(image: current)
                            .frame(maxWidth: .infinity, alignment: .leading)
                            .padding(.horizontal, ApproachNoteTheme.spacingXL)
                            .padding(.top, ApproachNoteTheme.spacingXS)
                    }
                }

                // Biography + Learn More (shares the 24pt screen gutter).
                performerBioAndLinks(performer)
                    .padding(.horizontal, ApproachNoteTheme.spacingXL)
                    .padding(.top, hasImages ? ApproachNoteTheme.spacingLG : ApproachNoteTheme.spacingMD)
                    .padding(.bottom, ApproachNoteTheme.spacingMD)
            }
        }
    }

    /// Full header column (wide/iPad layout): identity, biography, and links
    /// stacked in a single left-hand column beside the artist image.
    @ViewBuilder
    private func performerHeader(_ performer: PerformerDetail) -> some View {
        VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingMD) {
            performerIdentity(performer)
            performerBioAndLinks(performer)
        }
    }

    /// Artist name, lifespan, and instrument line.
    @ViewBuilder
    private func performerIdentity(_ performer: PerformerDetail) -> some View {
        VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingMD) {
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
        }
    }

    /// Biography block and external reference ("Learn More") links.
    @ViewBuilder
    private func performerBioAndLinks(_ performer: PerformerDetail) -> some View {
        VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingMD) {
            // Biography
            if let biography = performer.biography, !biography.isEmpty {
                VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingSM) {
                    Text("BIOGRAPHY")
                        .font(ApproachNoteTheme.title3())
                        .bold()
                        .foregroundColor(ApproachNoteTheme.textPrimary)

                    ExpandableProse(
                        text: biography,
                        maxCollapsedHeight: biographyCollapsedHeight
                    )
                }
                .padding(.top, ApproachNoteTheme.spacingXS)
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
            .padding(.top, ApproachNoteTheme.spacingXS)
        }
    }

    /// Collapsed biography height cap. iPhone shows ~10 lines; the roomier iPad
    /// column shows ~15 before READ MORE. Unbounded until the viewport is
    /// measured.
    private var biographyCollapsedHeight: CGFloat {
        guard viewportHeight > 0 else { return .greatestFiniteMagnitude }
        return viewportHeight * (isWideLayout ? 0.28 : 0.375)
    }

    // MARK: - Formatting Helpers

    /// "May 26, 1926" (birth only) or "May 26, 1926 – September 28, 1991" (birth–death).
    private func formattedLifespan(birth: String?, death: String?) -> String? {
        switch (formatPartialDate(birth), formatPartialDate(death)) {
        case let (b?, d?): return "\(b) – \(d)"
        case let (b?, nil): return b
        case let (nil, d?): return d
        default: return nil
        }
    }

    /// Formats a (possibly partial) ISO date string as "Month D, YYYY".
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
                formatter.dateFormat = "MMMM d, yyyy"
                return formatter.string(from: date)
            }
        }
        if parts.count == 2 {
            formatter.dateFormat = "yyyy-MM"
            if let date = formatter.date(from: "\(parts[0])-\(parts[1])") {
                formatter.dateFormat = "MMMM yyyy"
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
