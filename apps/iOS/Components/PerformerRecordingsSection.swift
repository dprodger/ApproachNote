//
//  PerformerRecordingsSection.swift
//  Approach Note
//
//  Recordings section for PerformerDetailView. Mirrors the song
//  RecordingsSection: a typography-only header with an inline count, a
//  controls bar (search + sort + role segmented), and per-group white card
//  accordions (chevron toggle) whose carousels bleed to the card edge.
//

import SwiftUI

// MARK: - Performer Recordings Section
struct PerformerRecordingsSection: View {
    let recordings: [PerformerRecording]
    let performerName: String

    @Binding var sortOrder: PerformerRecordingSortOrder
    @Binding var selectedFilter: RecordingFilter

    var isReloading: Bool = false
    var onSortOrderChanged: ((PerformerRecordingSortOrder) -> Void)?

    @State private var searchText: String = ""

    // Per-group expansion state. Groups not in the set are collapsed.
    // Default empty; the first shelf auto-opens on first appearance (see
    // autoExpandFirstIfNeeded) so the page lands with content already showing
    // (mirrors RecordingsSection).
    @State private var expandedGroups: Set<String> = []

    // Guards the one-time auto-expansion of the first shelf so it doesn't
    // re-fire when filters/sort later clear the expansion set.
    @State private var didAutoExpand = false

    var body: some View {
        VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingMD) {
            sectionHeader
                .padding(.horizontal, ApproachNoteTheme.spacingXL)

            controlsBar
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
                    .padding(.vertical, 40)
                }
            }
            .padding(.horizontal, ApproachNoteTheme.spacingXL)
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
    }

    /// Expands the first shelf once, the first time grouped recordings are
    /// available, so the page opens with content already showing.
    private func autoExpandFirstIfNeeded() {
        guard !didAutoExpand, let first = groupedRecordings.first else { return }
        didAutoExpand = true
        expandedGroups.insert(first.groupKey)
    }

    // MARK: - Section Header

    @ViewBuilder
    private var sectionHeader: some View {
        HStack(alignment: .center, spacing: ApproachNoteTheme.spacingXS) {
            HStack(alignment: .firstTextBaseline, spacing: ApproachNoteTheme.spacingXS) {
                Text("RECORDINGS")
                    .font(ApproachNoteTheme.title3())
                    .bold()
                    .foregroundColor(ApproachNoteTheme.textPrimary)
                    // Single-line label; scale down slightly rather than wrap
                    // when the sort pill leaves it tight (large text / Display Zoom).
                    .lineLimit(1)
                    .minimumScaleFactor(0.7)

                Text("(\(filteredRecordings.count))")
                    .font(ApproachNoteTheme.subheadline())
                    .foregroundColor(ApproachNoteTheme.textSecondary)
                    .lineLimit(1)
            }

            Spacer(minLength: 8)

            // Pill keeps its intrinsic size; the heading yields first.
            sortMenu
                .fixedSize()
                .layoutPriority(1)
        }
    }

    // MARK: - Controls Bar (Search, Sort menu, Role segmented)

    @ViewBuilder
    private var controlsBar: some View {
        VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingMD) {
            // Search field
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

            // Role segmented (All / Leader / Sideman)
            VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingXS) {
                Text("Role")
                    .font(ApproachNoteTheme.callout(weight: .semibold))
                    .foregroundColor(ApproachNoteTheme.textPrimary)

                rolePicker
            }
        }
    }

    @ViewBuilder
    private var sortMenu: some View {
        Menu {
            ForEach(PerformerRecordingSortOrder.allCases) { order in
                Button(action: {
                    if sortOrder != order {
                        expandedGroups.removeAll()
                        sortOrder = order
                        onSortOrderChanged?(order)
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
    }

    // MARK: - Role Picker (custom segmented control)
    // Brand-outlined pill; the selected segment is filled with the brand
    // color and white text, unselected segments are brand-colored on a clear
    // background — matches the SongDetailView Performance Type control.
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
        .frame(maxWidth: .infinity)
        .overlay(
            Capsule().stroke(ApproachNoteTheme.brand, lineWidth: 1.5)
        )
        .animation(.easeInOut(duration: 0.15), value: selectedFilter)
    }

    // MARK: - Group Accordion Row

    @ViewBuilder
    private func groupAccordion(group: (groupKey: String, recordings: [PerformerRecording])) -> some View {
        let isExpanded = expandedGroups.contains(group.groupKey)

        // Carded shelf: a pure-white surface card with a light border sitting on
        // the cream page, a header with a chevron toggle, and a carousel that
        // bleeds to the card's trailing edge (mirrors RecordingsSection).
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
                .padding(.horizontal, ApproachNoteTheme.spacingMD)
                .padding(.vertical, ApproachNoteTheme.spacingSM)
                .contentShape(Rectangle())
            }
            .buttonStyle(.plain)

            if isExpanded {
                ScrollView(.horizontal, showsIndicators: false) {
                    LazyHStack(alignment: .top, spacing: ApproachNoteTheme.spacingMD) {
                        ForEach(group.recordings, id: \.id) { recording in
                            NavigationLink(destination: RecordingDetailView(recordingId: recording.recordingId)) {
                                PerformerRecordingCardView(recording: recording)
                            }
                            .buttonStyle(.plain)
                        }
                    }
                    // Leading inset aligns the first card with the header text;
                    // cards bleed off the card's trailing edge as you scroll.
                    .padding(.leading, ApproachNoteTheme.spacingMD)
                }
                .padding(.bottom, ApproachNoteTheme.spacingMD)
            }
        }
        .background(ApproachNoteTheme.surface)
        .clipShape(RoundedRectangle(cornerRadius: 12, style: .continuous))
        .overlay(
            RoundedRectangle(cornerRadius: 12, style: .continuous)
                .stroke(ApproachNoteTheme.surfaceMuted, lineWidth: 1)
        )
    }

    // MARK: - Filtered Recordings
    private var filteredRecordings: [PerformerRecording] {
        var result = recordings

        // Role filter
        switch selectedFilter {
        case .all:
            break
        case .leader:
            result = result.filter { $0.role?.lowercased() == "leader" }
        case .sideman:
            result = result.filter { $0.role?.lowercased() == "sideman" }
        }

        // Search filter
        if !searchText.isEmpty {
            let query = searchText.lowercased()
            result = result.filter { recording in
                recording.songTitle.lowercased().contains(query) ||
                (recording.albumTitle?.lowercased().contains(query) ?? false)
            }
        }

        return result
    }

    // MARK: - Grouped Recordings
    private var groupedRecordings: [(groupKey: String, recordings: [PerformerRecording])] {
        switch sortOrder {
        case .year:
            return groupByDecade()
        case .name:
            return groupBySongTitle()
        }
    }

    private func groupByDecade() -> [(groupKey: String, recordings: [PerformerRecording])] {
        var decadeOrder: [String] = []
        var decades: [String: [PerformerRecording]] = [:]

        // Oldest-first: ascending by year (undated last). Iterating in this order
        // makes both the decade groups and the recordings within them ascending.
        let sorted = filteredRecordings.sorted {
            ($0.recordingYear ?? Int.max) < ($1.recordingYear ?? Int.max)
        }

        for recording in sorted {
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
            guard let recordings = decades[key] else { return nil }
            return (groupKey: key, recordings: recordings)
        }
    }

    private func groupBySongTitle() -> [(groupKey: String, recordings: [PerformerRecording])] {
        var titleOrder: [String] = []
        var titles: [String: [PerformerRecording]] = [:]

        for recording in filteredRecordings {
            let titleKey = recording.songTitle

            if titles[titleKey] == nil {
                titleOrder.append(titleKey)
            }
            titles[titleKey, default: []].append(recording)
        }

        titleOrder.sort { $0.localizedCaseInsensitiveCompare($1) == .orderedAscending }

        return titleOrder.compactMap { key in
            guard let recordings = titles[key] else { return nil }
            // Oldest-first within each song.
            return (groupKey: key, recordings: recordings.sorted {
                ($0.recordingYear ?? Int.max) < ($1.recordingYear ?? Int.max)
            })
        }
    }
}

// MARK: - Performer Recording Card View (mirrors RecordingRowView)
struct PerformerRecordingCardView: View {
    let recording: PerformerRecording

    private var coverUrl: String? {
        recording.bestCoverArtMedium ?? recording.bestCoverArtSmall
    }

    var body: some View {
        VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingXS) {
            // Album artwork
            ZStack(alignment: .topTrailing) {
                if let url = coverUrl {
                    CachedAsyncImage(
                        url: URL(string: url),
                        content: { image in
                            image
                                .resizable()
                                .aspectRatio(contentMode: .fill)
                                .frame(width: 150, height: 150)
                                .clipped()
                        },
                        placeholder: {
                            ZStack {
                                ApproachNoteTheme.surface
                                ProgressView()
                                    .tint(ApproachNoteTheme.textSecondary)
                            }
                            .frame(width: 150, height: 150)
                        }
                    )
                } else {
                    NoAlbumArtPlaceholder(cornerRadius: 8)
                        .frame(width: 150, height: 150)
                }

                // Canonical star badge
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
            .cornerRadius(8)
            .frame(width: 150)

            // Year
            if let year = recording.recordingYear {
                Text(String(format: "%d", year))
                    .font(ApproachNoteTheme.subheadline(weight: .bold))
                    .foregroundColor(ApproachNoteTheme.textPrimary)
                    .frame(width: 150, alignment: .leading)
            }

            // Song title — primary identifier on an artist page.
            Text(recording.songTitle)
                .font(ApproachNoteTheme.subheadline(weight: .bold))
                .foregroundColor(ApproachNoteTheme.textPrimary)
                .lineLimit(1)
                .frame(width: 150, alignment: .leading)

            // Album title — wraps naturally to 1-2 lines.
            Text(recording.albumTitle ?? "Unknown Album")
                .font(ApproachNoteTheme.subheadline())
                .foregroundColor(ApproachNoteTheme.textPrimary)
                .lineLimit(2)
                .frame(width: 150, alignment: .leading)
        }
        .frame(width: 150)
    }
}

// MARK: - Previews

#Preview("Performer Recordings") {
    struct PreviewWrapper: View {
        @State private var sortOrder: PerformerRecordingSortOrder = .year
        @State private var filter: RecordingFilter = .all

        var body: some View {
            NavigationStack {
                ScrollView {
                    PerformerRecordingsSection(
                        recordings: PerformerDetail.preview.recordings ?? [],
                        performerName: "Miles Davis",
                        sortOrder: $sortOrder,
                        selectedFilter: $filter
                    )
                }
            }
            .environmentObject(FavoritesManager())
        }
    }
    return PreviewWrapper()
}

#Preview("Recording Card") {
    PerformerRecordingCardView(
        recording: (PerformerDetail.preview.recordings ?? [])[0]
    )
    .padding()
}

#Preview("Recording Card - No Art") {
    PerformerRecordingCardView(
        recording: (PerformerDetail.preview.recordings ?? [])[2]
    )
    .padding()
}
