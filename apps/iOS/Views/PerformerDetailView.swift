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
    @State private var isBiographicalInfoExpanded = false
    @State private var recordingSortOrder: PerformerRecordingSortOrder = .year
    @State private var isRecordingsReloading = false

    // Two-phase loading: summary loads first (fast), then recordings load in background
    @State private var isRecordingsLoading: Bool = true

    // Tracks whether the in-page artist name is visible; drives nav bar title swap.
    @State private var isHeaderNameVisible = true
    
    var body: some View {
        ScrollView {
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
                    VStack(alignment: .leading, spacing: 16) {
                        // Artist Name - MOVED TO TOP
                        Text(performer.name)
                            .font(ApproachNoteTheme.largeTitle())
                            .bold()
                            .foregroundColor(ApproachNoteTheme.textPrimary)
                            .padding(.horizontal, 20)
                            .onScrollVisibilityChange(threshold: 0.1) { visible in
                                isHeaderNameVisible = visible
                            }
                        
                        // Image Carousel - MOVED AFTER NAME
                        if let images = performer.images, !images.isEmpty {
                            ArtistImageCarousel(images: images)
                                .padding(.top, 8)
                        }
                        
                        // Biographical Information Section - Expandable
                        VStack(alignment: .leading, spacing: 0) {
                            Button(action: {
                                withAnimation {
                                    isBiographicalInfoExpanded.toggle()
                                }
                            }) {
                                HStack {
                                    Text("Biographical Information")
                                        .font(ApproachNoteTheme.title2())
                                        .bold()
                                        .foregroundColor(ApproachNoteTheme.textPrimary)
                                    Spacer()
                                    Image(systemName: isBiographicalInfoExpanded ? "chevron.up" : "chevron.down")
                                        .foregroundColor(ApproachNoteTheme.textSecondary)
                                }
                                .padding()
                                .background(ApproachNoteTheme.surface)
                            }
                            .buttonStyle(.plain)
                            
                            VStack(alignment: .leading, spacing: 12) {
                                // Always show biography preview
                                if let biography = performer.biography {
                                    let paragraphs = biography.components(separatedBy: "\n\n").filter { !$0.isEmpty }
                                    VStack(alignment: .leading, spacing: 12) {
                                        ForEach(Array(paragraphs.enumerated()), id: \.offset) { _, paragraph in
                                            Text(paragraph)
                                                .font(ApproachNoteTheme.body())
                                                .bodyLineSpacing()
                                                .foregroundColor(ApproachNoteTheme.textSecondary)
                                        }
                                    }
                                    .lineLimit(isBiographicalInfoExpanded ? nil : 3)
                                    .padding(.horizontal)
                                    .padding(.top, 8)
                                }
                                
                                // Show details when expanded
                                if isBiographicalInfoExpanded {
                                    VStack(alignment: .leading, spacing: 12) {
                                        if let birthDate = performer.birthDate {
                                            HStack {
                                                Image(systemName: "calendar")
                                                    .foregroundColor(ApproachNoteTheme.textSecondary)
                                                Text("Born: \(birthDate)")
                                                    .font(ApproachNoteTheme.subheadline())
                                                    .foregroundColor(ApproachNoteTheme.textSecondary)
                                            }
                                        }

                                        if let deathDate = performer.deathDate {
                                            HStack {
                                                Image(systemName: "calendar")
                                                    .foregroundColor(ApproachNoteTheme.textSecondary)
                                                Text("Died: \(deathDate)")
                                                    .font(ApproachNoteTheme.subheadline())
                                                    .foregroundColor(ApproachNoteTheme.textSecondary)
                                            }
                                        }
                                        
                                        if let instruments = performer.instruments, !instruments.isEmpty {
                                            VStack(alignment: .leading, spacing: 8) {
                                                Text("Instruments")
                                                    .font(ApproachNoteTheme.headline())
                                                    .foregroundColor(ApproachNoteTheme.textPrimary)
                                                
                                                ForEach(instruments, id: \.name) { instrument in
                                                    HStack {
                                                        Image(systemName: "music.note")
                                                            .foregroundColor(ApproachNoteTheme.textSecondary)
                                                        Text(instrument.name)
                                                            .font(ApproachNoteTheme.subheadline())
                                                            .foregroundColor(ApproachNoteTheme.textPrimary)
                                                        if instrument.isPrimary == true {
                                                            Text("(Primary)")
                                                                .font(ApproachNoteTheme.caption())
                                                                .foregroundColor(ApproachNoteTheme.textSecondary)
                                                        }
                                                    }
                                                }
                                            }
                                            .padding(.top, 8)
                                        }
                                        
                                        ExternalReferencesPanel(
                                            wikipediaUrl: performer.wikipediaUrl,
                                            musicbrainzId: performer.musicbrainzId,
                                            externalLinks: performer.externalLinks,
                                            entityId: performer.id,
                                            entityName: performer.name,
                                            isArtist: true
                                        )
                                        .padding(.top, 8)
                                    }
                                    .padding(.horizontal)
                                    .padding(.bottom, 12)
                                }
                            }
                        }
                        .background(ApproachNoteTheme.surface)
                        .cornerRadius(10)
                        .padding(.horizontal, 20)
                        .padding(.top, 8)

                        Divider()

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
                }
                .padding(.top, 24)
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
        .background(ApproachNoteTheme.background)
        .jazzNavigationBar(title: isHeaderNameVisible ? "Artist" : (performer?.name ?? "Artist"))
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
