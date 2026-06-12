//
//  RecordingsSection.swift
//  Approach Note
//
//  Section displaying filtered recordings with filter chips + per-group accordions.
//  Each group (decade or artist) is a white card shelf on the cream page; the
//  first auto-opens on appearance, and the rest can be opened individually.
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
    // Default is empty; the first shelf auto-opens on first appearance (see
    // autoExpandFirstIfNeeded) so the page lands with content already showing.
    @State private var expandedGroups: Set<String> = []

    // Guards the one-time auto-expansion of the first shelf so it doesn't
    // re-fire when filters/sort later clear the expansion set.
    @State private var didAutoExpand = false

    // On iPad (regular width) the filter controls are capped to a comfortable
    // width and hugged to the leading edge, so Filter/Sort stay paired, the
    // Playable switch sits near its label, and the Performance Type pill doesn't
    // stretch across the whole screen. Compact width keeps the full-width stack.
    @Environment(\.horizontalSizeClass) private var horizontalSizeClass
    private var isWideLayout: Bool { horizontalSizeClass == .regular }
    private static let wideControlsMaxWidth: CGFloat = 640
    private static let widePerformancePickerMaxWidth: CGFloat = 420

    var body: some View {
        VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingMD) {
            sectionHeader
                .padding(.horizontal, ApproachNoteTheme.spacingXL)

            controlsBar
                // iPad: cap the controls block and pin it to the leading edge
                // so it reads as a compact toolbar rather than spanning the
                // full window. iPhone keeps the full-width stack.
                .frame(maxWidth: isWideLayout ? Self.wideControlsMaxWidth : .infinity, alignment: .leading)
                .frame(maxWidth: .infinity, alignment: .leading)
                .padding(.horizontal, ApproachNoteTheme.spacingXL)

            LazyVStack(alignment: .leading, spacing: ApproachNoteTheme.spacingSM) {
                if !filteredRecordings.isEmpty {
                    ForEach(groupedRecordings, id: \.groupKey) { group in
                        groupAccordion(group: group)
                    }
                } else {
                    VStack(spacing: ApproachNoteTheme.spacingSM) {
                        Image(systemName: "music.note")
                            .font(.system(size: 48))
                            .foregroundColor(ApproachNoteTheme.textSecondary.opacity(0.5))
                        Text("No recordings match the current filters")
                            .font(ApproachNoteTheme.subheadline())
                            .foregroundColor(ApproachNoteTheme.textSecondary)
                            .multilineTextAlignment(.center)
                    }
                    .frame(maxWidth: .infinity)
                    .padding(.horizontal, ApproachNoteTheme.spacingXL)
                    .padding(.vertical, 40)
                }
            }
            // No horizontal padding here: the accordion shelves bleed to the
            // device edges (their inner content carries its own leading inset).
            .padding(.top, ApproachNoteTheme.spacingXS)
            .overlay(alignment: .top) {
                if isReloading {
                    HStack(spacing: ApproachNoteTheme.spacingXS) {
                        ProgressView()
                            .tint(ApproachNoteTheme.brand)
                        Text("Reloading...")
                            .font(ApproachNoteTheme.subheadline())
                            .foregroundColor(ApproachNoteTheme.textSecondary)
                    }
                    .padding(.horizontal, ApproachNoteTheme.spacingMD)
                    .padding(.vertical, ApproachNoteTheme.spacingXS)
                    .background(.ultraThinMaterial)
                    .cornerRadius(8)
                    .shadow(color: .black.opacity(0.1), radius: 4, y: 2)
                    .padding(.top, 40)
                }
            }
            .opacity(isReloading ? 0.5 : 1.0)
            .animation(.easeInOut(duration: 0.2), value: isReloading)
            .onAppear { autoExpandFirstIfNeeded() }
            .onChange(of: groupedRecordings.first?.groupKey) { _, _ in autoExpandFirstIfNeeded() }
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
        HStack(alignment: .firstTextBaseline, spacing: ApproachNoteTheme.spacingXS) {
            Text("ALL RECORDINGS")
                .font(ApproachNoteTheme.title3())
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
        VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingMD) {
            // Filter + Sort row: Filter and Sort paired on the left.
            HStack(spacing: ApproachNoteTheme.spacingXS) {
                Button(action: { showFilterSheet = true }) {
                    HStack(spacing: ApproachNoteTheme.spacingXS) {
                        Text("Filter")
                            .font(ApproachNoteTheme.subheadline(weight: .bold))
                        Image(systemName: "slider.horizontal.3")
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
                    HStack(spacing: ApproachNoteTheme.spacingXS) {
                        (
                            Text("Sort:")
                                .font(ApproachNoteTheme.subheadline(weight: .bold))
                            + Text(" \(recordingSortOrder.displayName)")
                                .font(ApproachNoteTheme.subheadline())
                        )
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

                Spacer()
            }

            // Playable Only toggle
            Toggle(isOn: $playableOnly) {
                VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingXXS) {
                    Text("Playable only?")
                        .font(ApproachNoteTheme.callout(weight: .semibold))
                        .foregroundColor(ApproachNoteTheme.textPrimary)
                    Text("Toggle On to hide versions of this song without a linked recording to listen to.")
                        .font(ApproachNoteTheme.caption())
                        .foregroundColor(ApproachNoteTheme.textSecondary)
                        .fixedSize(horizontal: false, vertical: true)
                }
            }
            .tint(ApproachNoteTheme.brand)
            // iPad: cap the toggle row so the switch aligns with the right edge
            // of the Performance Type picker below instead of floating out at the
            // full controls width. iPhone keeps the full-width row.
            .frame(maxWidth: isWideLayout ? Self.widePerformancePickerMaxWidth : .infinity, alignment: .leading)

            // Performance Type segmented
            VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingXS) {
                Text("Performance Type")
                    .font(ApproachNoteTheme.callout(weight: .semibold))
                    .foregroundColor(ApproachNoteTheme.textPrimary)

                performanceTypePicker
            }
        }
    }

    // MARK: - Performance Type Picker (custom segmented control)
    // Brand-outlined pill; the selected segment is filled with the brand color
    // and white text, unselected segments are brand-colored on a clear
    // background (issue #200).
    @ViewBuilder
    private var performanceTypePicker: some View {
        HStack(spacing: 0) {
            ForEach(Array(VocalFilter.allCases.enumerated()), id: \.element.id) { index, filter in
                // Flexible spacers between segments distribute the bar width;
                // each segment stays sized to its own text (no truncation,
                // no oversized pills).
                if index > 0 {
                    Spacer(minLength: 4)
                }
                let isSelected = selectedVocalFilter == filter
                Button {
                    selectedVocalFilter = filter
                } label: {
                    Text(filter.displayName.uppercased())
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
        // iPad: hug the three options at a sensible width instead of stretching
        // the segmented control across the whole screen. iPhone fills the row.
        .frame(maxWidth: isWideLayout ? Self.widePerformancePickerMaxWidth : .infinity, alignment: .leading)
        .overlay(
            Capsule().stroke(ApproachNoteTheme.brand, lineWidth: 1.5)
        )
        .animation(.easeInOut(duration: 0.15), value: selectedVocalFilter)
    }

    // MARK: - Group Accordion Row

    @ViewBuilder
    private func groupAccordion(group: (groupKey: String, recordings: [Recording])) -> some View {
        let isExpanded = expandedGroups.contains(group.groupKey)

        // Full-bleed shelf: a white band spanning edge to edge on the cream page,
        // with hairline borders on the top and bottom only (no side borders), so
        // the carousel inside can scroll right up to the device edges. The header
        // text and the first card keep a leading inset to align with the section
        // header above.
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
                        .font(ApproachNoteTheme.headline())
                        .foregroundColor(ApproachNoteTheme.brand)
                }
                .padding(.horizontal, ApproachNoteTheme.spacingXL)
                .padding(.vertical, ApproachNoteTheme.spacingSM)
                .contentShape(Rectangle())
            }
            .buttonStyle(.plain)

            if isExpanded {
                let shelfHasAnyDistinctTitle = group.recordings.contains { recording in
                    recording.displayTitle(comparedTo: parentSongTitle) != nil
                }
                ScrollView(.horizontal, showsIndicators: false) {
                    LazyHStack(alignment: .top, spacing: ApproachNoteTheme.spacingMD) {
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
                    // Leading inset aligns the first card with the header text;
                    // cards bleed off the device's trailing edge as you scroll.
                    .padding(.leading, ApproachNoteTheme.spacingXL)
                }
                .padding(.bottom, ApproachNoteTheme.spacingMD)
            }
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .background(ApproachNoteTheme.surface)
        // Top + bottom hairlines only; the band runs full width to the edges.
        .overlay(alignment: .top) {
            Rectangle()
                .fill(ApproachNoteTheme.surfaceMuted)
                .frame(height: 1)
        }
        .overlay(alignment: .bottom) {
            Rectangle()
                .fill(ApproachNoteTheme.surfaceMuted)
                .frame(height: 1)
        }
    }

    /// Expands the first shelf once, the first time grouped recordings are
    /// available, so the page opens with content already showing.
    private func autoExpandFirstIfNeeded() {
        guard !didAutoExpand, let first = groupedRecordings.first else { return }
        didAutoExpand = true
        expandedGroups.insert(first.groupKey)
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
