//
//  RecordingsSection.swift
//  Approach Note
//
//  Section displaying filtered recordings with filter chips + per-group accordions.
//  The outer section is always expanded; each group (decade or artist) starts
//  collapsed and can be opened individually.
//

import SwiftUI

// Filter enums (SongRecordingFilter, VocalFilter, InstrumentFamily) are in Shared/Support/RecordingFilters.swift

// MARK: - Recordings Section
struct RecordingsSection: View {
    let recordings: [Recording]

    /// Title of the song these recordings belong to. Used so each row
    /// can suppress the recording title when it duplicates the song
    /// name — the nested-under-song API responses don't populate
    /// `song_title` on individual recordings.
    var parentSongTitle: String? = nil

    // Binding for sort order (passed from parent)
    @Binding var recordingSortOrder: RecordingSortOrder

    // Loading state for sort order changes
    var isReloading: Bool = false

    // Callback when sort order changes (for parent to reload data)
    var onSortOrderChanged: ((RecordingSortOrder) -> Void)?

    // Callback when community data changes (for parent to reload recordings)
    var onCommunityDataChanged: (() -> Void)?

    // Callback fired when a recording row appears. Forwarded to
    // RecordingRowView's `onVisible` so SongDetailViewModel can drive
    // the shell+hydrate pattern. Nil means "don't hydrate" — useful for
    // any callers that already pass fully-loaded recordings.
    var onRequestHydration: ((String) -> Void)?

    @State private var playableOnly: Bool = true
    @State private var selectedServices: Set<StreamingService> = []
    @State private var selectedVocalFilter: VocalFilter = .all
    @State private var selectedInstrument: InstrumentFamily? = nil
    @State private var showFilterSheet: Bool = false

    // Per-group expansion state. Groups not in the set are collapsed.
    // Default is empty: all shelves start collapsed so users see a
    // scannable list of decades / artist names before drilling in.
    @State private var expandedGroups: Set<String> = []

    var body: some View {
        VStack(alignment: .leading, spacing: 16) {
            sectionHeader
                .padding(.horizontal, 24)

            controlsBar
                .padding(.horizontal, 24)

            LazyVStack(alignment: .leading, spacing: 8) {
                if !filteredRecordings.isEmpty {
                    ForEach(groupedRecordings, id: \.groupKey) { group in
                        groupAccordion(group: group)
                    }
                } else {
                    VStack(spacing: 12) {
                        Image(systemName: "music.note")
                            .font(.system(size: 48))
                            .foregroundColor(ApproachNoteTheme.textSecondary.opacity(0.5))
                        Text("No recordings match the current filters")
                            .font(ApproachNoteTheme.subheadline())
                            .foregroundColor(ApproachNoteTheme.textSecondary)
                            .multilineTextAlignment(.center)
                    }
                    .frame(maxWidth: .infinity)
                    .padding(.vertical, 40)
                }
            }
            .padding(.horizontal, 24)
            .padding(.top, 8)
            .overlay(alignment: .top) {
                if isReloading {
                    HStack(spacing: 8) {
                        ProgressView()
                            .tint(ApproachNoteTheme.brand)
                        Text("Reloading...")
                            .font(ApproachNoteTheme.subheadline())
                            .foregroundColor(ApproachNoteTheme.textSecondary)
                    }
                    .padding(.horizontal, 16)
                    .padding(.vertical, 10)
                    .background(.ultraThinMaterial)
                    .cornerRadius(8)
                    .shadow(color: .black.opacity(0.1), radius: 4, y: 2)
                    .padding(.top, 40)
                }
            }
            .opacity(isReloading ? 0.5 : 1.0)
            .animation(.easeInOut(duration: 0.2), value: isReloading)
        }
        .background(ApproachNoteTheme.background)
        .sheet(isPresented: $showFilterSheet) {
            RecordingFilterSheet(
                selectedServices: $selectedServices,
                selectedInstrument: $selectedInstrument,
                availableInstruments: availableInstruments
            )
        }
    }

    // MARK: - Section Header

    @ViewBuilder
    private var sectionHeader: some View {
        HStack(alignment: .firstTextBaseline, spacing: 6) {
            Text("MORE RECORDINGS")
                .font(ApproachNoteTheme.title2())
                .bold()
                .foregroundColor(ApproachNoteTheme.textPrimary)

            Text("(\(filteredRecordings.count))")
                .font(ApproachNoteTheme.subheadline())
                .foregroundColor(ApproachNoteTheme.textSecondary)

            Spacer()
        }
    }

    // MARK: - Controls Bar (Filter + Sort buttons, Playable toggle, Performance Type segmented)

    @ViewBuilder
    private var controlsBar: some View {
        VStack(alignment: .leading, spacing: 16) {
            // Filter + Sort row
            HStack(spacing: 10) {
                Button(action: { showFilterSheet = true }) {
                    HStack(spacing: 6) {
                        Text("Filter")
                            .font(ApproachNoteTheme.subheadline())
                        Image(systemName: "slider.horizontal.3")
                            .font(.caption)
                    }
                    .foregroundColor(ApproachNoteTheme.textPrimary)
                    .padding(.horizontal, 12)
                    .padding(.vertical, 8)
                    .background(ApproachNoteTheme.surface)
                    .cornerRadius(8)
                }
                .buttonStyle(.plain)

                Menu {
                    ForEach(RecordingSortOrder.allCases) { sortOrder in
                        Button(action: {
                            if recordingSortOrder != sortOrder {
                                expandedGroups.removeAll()
                                recordingSortOrder = sortOrder
                                onSortOrderChanged?(sortOrder)
                            }
                        }) {
                            HStack {
                                Text(sortOrder.displayName)
                                if recordingSortOrder == sortOrder {
                                    Image(systemName: "checkmark")
                                }
                            }
                        }
                    }
                } label: {
                    HStack(spacing: 6) {
                        Text("Sort: \(recordingSortOrder.displayName)")
                            .font(ApproachNoteTheme.subheadline())
                        Image(systemName: "chevron.down")
                            .font(.caption)
                    }
                    .foregroundColor(ApproachNoteTheme.textPrimary)
                    .padding(.horizontal, 12)
                    .padding(.vertical, 8)
                    .background(ApproachNoteTheme.surface)
                    .cornerRadius(8)
                }

                Spacer()
            }

            // Playable Only toggle
            Toggle(isOn: $playableOnly) {
                VStack(alignment: .leading, spacing: 2) {
                    Text("Playable only?")
                        .font(ApproachNoteTheme.headline())
                        .foregroundColor(ApproachNoteTheme.textPrimary)
                    Text("Toggle On to hide versions of this song without a linked recording to listen to.")
                        .font(ApproachNoteTheme.caption())
                        .foregroundColor(ApproachNoteTheme.textSecondary)
                        .fixedSize(horizontal: false, vertical: true)
                }
            }
            .tint(ApproachNoteTheme.brand)

            // Performance Type segmented
            VStack(alignment: .leading, spacing: 8) {
                Text("Performance Type")
                    .font(ApproachNoteTheme.headline())
                    .foregroundColor(ApproachNoteTheme.textPrimary)

                Picker("Performance Type", selection: $selectedVocalFilter) {
                    ForEach(VocalFilter.allCases) { filter in
                        Text(filter.displayName.uppercased()).tag(filter)
                    }
                }
                .pickerStyle(.segmented)
            }
        }
    }

    // MARK: - Group Accordion Row

    @ViewBuilder
    private func groupAccordion(group: (groupKey: String, recordings: [Recording])) -> some View {
        let isExpanded = expandedGroups.contains(group.groupKey)

        VStack(alignment: .leading, spacing: 0) {
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
                    Image(systemName: isExpanded ? "chevron.up" : "chevron.down")
                        .foregroundColor(ApproachNoteTheme.textSecondary)
                }
                .padding(.horizontal, 12)
                .padding(.vertical, 10)
                .contentShape(Rectangle())
            }
            .buttonStyle(.plain)

            if isExpanded {
                let shelfHasAnyDistinctTitle = group.recordings.contains { recording in
                    recording.displayTitle(comparedTo: parentSongTitle) != nil
                }
                ScrollView(.horizontal, showsIndicators: false) {
                    LazyHStack(alignment: .top, spacing: 16) {
                        ForEach(group.recordings, id: \.id) { recording in
                            NavigationLink(destination: RecordingDetailView(
                                recordingId: recording.id,
                                onCommunityDataChanged: onCommunityDataChanged
                            )) {
                                RecordingRowView(
                                    recording: recording,
                                    parentSongTitle: parentSongTitle,
                                    shelfHasAnyDistinctTitle: shelfHasAnyDistinctTitle,
                                    onVisible: onRequestHydration
                                )
                            }
                            .buttonStyle(.plain)
                        }
                    }
                    .padding(.horizontal, 12)
                }
                .padding(.bottom, 8)
            }
        }
        .background(ApproachNoteTheme.surface)
        .cornerRadius(8)
    }

    // MARK: - Computed Properties
    // Filtering and grouping logic lives in Shared/Support/RecordingGrouping.swift
    // so iOS and Mac stay in sync. These wrappers let in-body call sites stay unchanged.

    private var availableInstruments: [InstrumentFamily] {
        RecordingGrouping.availableInstruments(in: recordings)
    }

    private var filteredRecordings: [Recording] {
        // Run instrument + vocal filters through the shared helper, then
        // apply the new playable / per-service filters locally so we don't
        // disturb the SongRecordingFilter enum that Mac still uses.
        var result = RecordingGrouping.filter(
            recordings,
            instrument: selectedInstrument,
            vocal: selectedVocalFilter,
            streaming: .all
        )

        if !selectedServices.isEmpty {
            result = result.filter { recording in
                selectedServices.contains(where: { hasService(recording, $0) })
            }
        } else if playableOnly {
            result = result.filter { $0.isPlayable }
        }

        return result
    }

    private func hasService(_ recording: Recording, _ service: StreamingService) -> Bool {
        switch service {
        case .spotify: return recording.hasSpotifyAvailable
        case .appleMusic: return recording.hasAppleMusicAvailable
        case .youtube: return recording.hasYoutubeAvailable
        }
    }

    private var groupedRecordings: [(groupKey: String, recordings: [Recording])] {
        RecordingGrouping.grouped(filteredRecordings, sortOrder: recordingSortOrder)
    }
}

// MARK: - Previews

#Preview("Recordings Section") {
    struct PreviewWrapper: View {
        @State private var sortOrder: RecordingSortOrder = .year

        var body: some View {
            NavigationStack {
                ScrollView {
                    RecordingsSection(
                        recordings: [.preview1, .preview2, .previewMinimal],
                        recordingSortOrder: $sortOrder
                    )
                }
            }
        }
    }
    return PreviewWrapper()
}
