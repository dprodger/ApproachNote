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

    @State private var selectedFilter: SongRecordingFilter = .playable
    @State private var selectedVocalFilter: VocalFilter = .all
    @State private var selectedInstrument: InstrumentFamily? = nil
    @State private var showFilterSheet: Bool = false

    // Per-group expansion state. Groups not in the set are collapsed.
    // Default is empty: all shelves start collapsed so users see a
    // scannable list of decades / artist names before drilling in.
    @State private var expandedGroups: Set<String> = []

    var body: some View {
        VStack(alignment: .leading, spacing: 0) {
            sectionHeader
                .padding(.horizontal, 16)
                .padding(.vertical, 12)

            if hasActiveFilters || !availableInstruments.isEmpty {
                filterChipsBar
                    .padding(.vertical, 8)
                    .padding(.horizontal, 4)
                    .background(ApproachNoteTheme.cardBackground)
                    .cornerRadius(8)
                    .padding(.horizontal, 16)
            }

            LazyVStack(alignment: .leading, spacing: 8) {
                if !filteredRecordings.isEmpty {
                    ForEach(groupedRecordings, id: \.groupKey) { group in
                        groupAccordion(group: group)
                    }
                } else {
                    VStack(spacing: 12) {
                        Image(systemName: "music.note")
                            .font(.system(size: 48))
                            .foregroundColor(ApproachNoteTheme.smokeGray.opacity(0.5))
                        Text("No recordings match the current filters")
                            .font(ApproachNoteTheme.subheadline())
                            .foregroundColor(ApproachNoteTheme.smokeGray)
                            .multilineTextAlignment(.center)
                    }
                    .frame(maxWidth: .infinity)
                    .padding(.vertical, 40)
                }
            }
            .padding(.horizontal, 16)
            .padding(.top, 8)
            .overlay(alignment: .top) {
                if isReloading {
                    HStack(spacing: 8) {
                        ProgressView()
                            .tint(ApproachNoteTheme.burgundy)
                        Text("Reloading...")
                            .font(ApproachNoteTheme.subheadline())
                            .foregroundColor(ApproachNoteTheme.smokeGray)
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
        .background(ApproachNoteTheme.backgroundLight)
        .sheet(isPresented: $showFilterSheet) {
            RecordingFilterSheet(
                selectedFilter: $selectedFilter,
                selectedVocalFilter: $selectedVocalFilter,
                selectedInstrument: $selectedInstrument,
                availableInstruments: availableInstruments
            )
        }
    }

    // MARK: - Section Header (no expand/collapse — section is always visible)

    @ViewBuilder
    private var sectionHeader: some View {
        HStack(alignment: .center) {
            Image(systemName: "music.note.list")
                .foregroundColor(ApproachNoteTheme.burgundy)

            Text("Recordings")
                .font(ApproachNoteTheme.title2())
                .bold()
                .foregroundColor(ApproachNoteTheme.charcoal)

            Text("(\(filteredRecordings.count))")
                .font(ApproachNoteTheme.subheadline())
                .foregroundColor(ApproachNoteTheme.smokeGray)

            Spacer()

            Menu {
                ForEach(RecordingSortOrder.allCases) { sortOrder in
                    Button(action: {
                        if recordingSortOrder != sortOrder {
                            // Sort change rebuilds group keys entirely
                            // (decades ↔ artist names), so previous
                            // expansion state no longer applies.
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
                HStack(spacing: 3) {
                    Text(recordingSortOrder.displayName)
                        .font(ApproachNoteTheme.caption())
                    Image(systemName: "chevron.down")
                        .font(.caption2)
                }
                .foregroundColor(ApproachNoteTheme.burgundy)
                .padding(.horizontal, 8)
                .padding(.vertical, 5)
                .background(ApproachNoteTheme.burgundy.opacity(0.1))
                .cornerRadius(6)
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
                        .foregroundColor(ApproachNoteTheme.burgundy)
                    Spacer()
                    Image(systemName: isExpanded ? "chevron.up" : "chevron.down")
                        .foregroundColor(ApproachNoteTheme.brass)
                }
                .padding(.horizontal, 12)
                .padding(.vertical, 10)
                .contentShape(Rectangle())
            }
            .buttonStyle(.plain)

            if isExpanded {
                ScrollView(.horizontal, showsIndicators: false) {
                    LazyHStack(alignment: .top, spacing: 0) {
                        ForEach(Array(group.recordings.enumerated()), id: \.element.id) { index, recording in
                            HStack(alignment: .top, spacing: 0) {
                                if index > 0 {
                                    Rectangle()
                                        .fill(ApproachNoteTheme.burgundy.opacity(0.4))
                                        .frame(width: 2, height: 150)
                                        .padding(.horizontal, 8)
                                }

                                NavigationLink(destination: RecordingDetailView(
                                    recordingId: recording.id,
                                    onCommunityDataChanged: onCommunityDataChanged
                                )) {
                                    RecordingRowView(
                                        recording: recording,
                                        showArtistName: recordingSortOrder == .year || group.groupKey == "More Recordings",
                                        onVisible: onRequestHydration
                                    )
                                }
                                .buttonStyle(.plain)
                            }
                        }
                    }
                    .padding(.horizontal, 12)
                }
                .padding(.bottom, 8)
            }
        }
        .background(ApproachNoteTheme.cardBackground)
        .cornerRadius(8)
    }

    // MARK: - Filter Chips Bar

    @ViewBuilder
    private var filterChipsBar: some View {
        HStack(spacing: 8) {
            // Active filter chips for streaming service
            if selectedFilter != .all {
                FilterChip(
                    label: selectedFilter.displayName,
                    icon: selectedFilter.icon,
                    iconColor: selectedFilter.iconColor,
                    onRemove: { selectedFilter = .all }
                )
            }

            // Active filter chip for vocal/instrumental
            if selectedVocalFilter != .all {
                FilterChip(
                    label: selectedVocalFilter.displayName,
                    icon: selectedVocalFilter.icon,
                    iconColor: selectedVocalFilter.iconColor,
                    onRemove: { selectedVocalFilter = .all }
                )
            }

            if let instrument = selectedInstrument {
                FilterChip(
                    label: instrument.rawValue,
                    icon: nil,
                    onRemove: { selectedInstrument = nil }
                )
            }

            // Add/Edit Filter button
            Button(action: { showFilterSheet = true }) {
                HStack(spacing: 4) {
                    Image(systemName: hasActiveFilters ? "slider.horizontal.3" : "plus")
                        .font(.caption.weight(.medium))
                    Text(hasActiveFilters ? "Edit" : "Filter")
                        .font(ApproachNoteTheme.subheadline())
                }
                .foregroundColor(ApproachNoteTheme.burgundy)
                .padding(.horizontal, 10)
                .padding(.vertical, 5)
                .background(ApproachNoteTheme.burgundy.opacity(0.15))
                .cornerRadius(14)
            }
            .buttonStyle(.plain)

            Spacer()
        }
    }

    private var hasActiveFilters: Bool {
        selectedFilter != .all || selectedVocalFilter != .all || selectedInstrument != nil
    }

    // MARK: - Computed Properties
    // Filtering and grouping logic lives in Shared/Support/RecordingGrouping.swift
    // so iOS and Mac stay in sync. These wrappers let in-body call sites stay unchanged.

    private var availableInstruments: [InstrumentFamily] {
        RecordingGrouping.availableInstruments(in: recordings)
    }

    private var filteredRecordings: [Recording] {
        RecordingGrouping.filter(
            recordings,
            instrument: selectedInstrument,
            vocal: selectedVocalFilter,
            streaming: selectedFilter
        )
    }

    private var groupedRecordings: [(groupKey: String, recordings: [Recording])] {
        RecordingGrouping.grouped(filteredRecordings, sortOrder: recordingSortOrder)
    }
}

// MARK: - Filter Chip Component

struct FilterChip: View {
    let label: String
    let icon: String?
    var iconColor: Color? = nil
    var backgroundColor: Color? = nil
    let onRemove: () -> Void

    var body: some View {
        HStack(spacing: 4) {
            if let icon = icon {
                Image(systemName: icon)
                    .font(ApproachNoteTheme.caption())
                    .foregroundColor(iconColor ?? .white)
            }

            Text(label)
                .font(ApproachNoteTheme.subheadline())

            Button(action: onRemove) {
                Image(systemName: "xmark")
                    .font(.caption2.weight(.semibold))
            }
            .buttonStyle(.plain)
        }
        .foregroundColor(.white)
        .padding(.horizontal, 10)
        .padding(.vertical, 6)
        .background(backgroundColor ?? ApproachNoteTheme.brass)
        .cornerRadius(16)
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

#Preview("Filter Chips") {
    VStack(spacing: 12) {
        FilterChip(label: "Playable", icon: "play.circle", iconColor: ApproachNoteTheme.burgundy) {}
        FilterChip(label: "Spotify", icon: "music.note.list", iconColor: .green) {}
        FilterChip(label: "Piano", icon: nil) {}
    }
    .padding()
}
