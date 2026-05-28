//
//  PerformerRecordingsSection.swift
//  Approach Note
//
//  Recordings section for PerformerDetailView that mirrors the RecordingsSection UI
//  Displays recordings with album art in horizontal scrolling rows, grouped by decade or song title
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

    @State private var isSectionExpanded: Bool = true
    @State private var searchText: String = ""

    // MARK: - Filtered Recordings
    private var filteredRecordings: [PerformerRecording] {
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

    // MARK: - Grouped Recordings
    private var groupedRecordings: [(groupKey: String, recordings: [PerformerRecording])] {
        switch sortOrder {
        case .year:
            return groupByDecade()
        case .name:
            return groupBySongLetter()
        }
    }

    private func groupByDecade() -> [(groupKey: String, recordings: [PerformerRecording])] {
        var decadeOrder: [String] = []
        var decades: [String: [PerformerRecording]] = [:]

        for recording in filteredRecordings {
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

    private func groupBySongLetter() -> [(groupKey: String, recordings: [PerformerRecording])] {
        var letterOrder: [String] = []
        var letters: [String: [PerformerRecording]] = [:]

        for recording in filteredRecordings {
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
            guard let recordings = letters[key] else { return nil }
            return (groupKey: key, recordings: recordings)
        }
    }

    // MARK: - Body
    var body: some View {
        HStack(spacing: 0) {
            Spacer().frame(width: 16)

            VStack(alignment: .leading, spacing: 0) {
                DisclosureGroup(
                    isExpanded: $isSectionExpanded,
                    content: {
                        VStack(alignment: .leading, spacing: 0) {
                            // Search and Filter Bar
                            filterBar
                                .padding(.vertical, 8)
                                .padding(.horizontal)

                            // Recordings List
                            LazyVStack(alignment: .leading, spacing: 12) {
                                if !filteredRecordings.isEmpty {
                                    ForEach(groupedRecordings, id: \.groupKey) { group in
                                        VStack(alignment: .leading, spacing: 8) {
                                            Text("\(group.groupKey) (\(group.recordings.count))")
                                                .font(ApproachNoteTheme.headline())
                                                .foregroundColor(ApproachNoteTheme.brand)
                                                .padding(.horizontal)
                                                .padding(.top, 8)

                                            ScrollView(.horizontal, showsIndicators: false) {
                                                LazyHStack(alignment: .top, spacing: 0) {
                                                    ForEach(Array(group.recordings.enumerated()), id: \.element.id) { index, recording in
                                                        HStack(alignment: .top, spacing: 0) {
                                                            if index > 0 {
                                                                Rectangle()
                                                                    .fill(ApproachNoteTheme.brand.opacity(0.4))
                                                                    .frame(width: 2, height: 150)
                                                                    .padding(.horizontal, 8)
                                                            }

                                                            NavigationLink(destination: RecordingDetailView(recordingId: recording.recordingId)) {
                                                                PerformerRecordingCardView(
                                                                    recording: recording,
                                                                    showRole: sortOrder == .year
                                                                )
                                                            }
                                                            .buttonStyle(.plain)
                                                        }
                                                    }
                                                }
                                                .padding(.horizontal)
                                            }
                                            // Cancel the section's 16pt gutter so the
                                            // carousel bleeds to the screen edges (issue #200).
                                            .padding(.horizontal, -16)
                                        }
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
                    },
                    label: {
                        HStack(alignment: .center) {
                            Image(systemName: "music.note.list")
                                .foregroundColor(ApproachNoteTheme.brand)

                            Text("Recordings")
                                .font(ApproachNoteTheme.title2())
                                .bold()
                                .foregroundColor(ApproachNoteTheme.textPrimary)

                            Text("(\(filteredRecordings.count))")
                                .font(ApproachNoteTheme.subheadline())
                                .foregroundColor(ApproachNoteTheme.textSecondary)

                            Spacer()

                            // Sort menu
                            Menu {
                                ForEach(PerformerRecordingSortOrder.allCases) { order in
                                    Button(action: {
                                        if sortOrder != order {
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
                        }
                        .padding(.vertical, 12)
                    }
                )
                .tint(ApproachNoteTheme.brand)
            }

            Spacer().frame(width: 16)
        }
        .background(ApproachNoteTheme.background)
    }

    // MARK: - Filter Bar
    @ViewBuilder
    private var filterBar: some View {
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
            .tint(ApproachNoteTheme.brand)
        }
    }
}

// MARK: - Performer Recording Card View (mirrors RecordingRowView)
struct PerformerRecordingCardView: View {
    let recording: PerformerRecording
    var showRole: Bool = false

    private var coverUrl: String? {
        recording.bestCoverArtMedium ?? recording.bestCoverArtSmall
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
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
                    Image(systemName: "opticaldisc")
                        .font(ApproachNoteTheme.largeTitle())
                        .foregroundColor(ApproachNoteTheme.textSecondary)
                        .frame(width: 150, height: 150)
                        .background(ApproachNoteTheme.surface)
                }

                // Badges
                VStack(alignment: .trailing, spacing: 4) {
                    if recording.isCanonical == true {
                        Image(systemName: "star.fill")
                            .foregroundColor(.yellow)
                            .font(ApproachNoteTheme.caption())
                            .padding(6)
                            .background(Color.black.opacity(0.6))
                            .clipShape(Circle())
                    }

                    if showRole, let role = recording.role {
                        Text(role.capitalized)
                            .font(.system(size: 9, weight: .semibold))
                            .foregroundColor(.white)
                            .padding(.horizontal, 6)
                            .padding(.vertical, 3)
                            .background(role.lowercased() == "leader" ? ApproachNoteTheme.textSecondary : ApproachNoteTheme.textSecondary)
                            .cornerRadius(4)
                    }
                }
                .padding(6)
            }
            .cornerRadius(8)
            .frame(width: 150)

            // Song title
            Text(recording.songTitle)
                .font(ApproachNoteTheme.subheadline())
                .fontWeight(.semibold)
                .foregroundColor(ApproachNoteTheme.textSecondary)
                .lineLimit(1)
                .frame(width: 150, alignment: .leading)

            // Album title
            Text(recording.albumTitle ?? "Unknown Album")
                .font(ApproachNoteTheme.subheadline())
                .fontWeight(.medium)
                .foregroundColor(ApproachNoteTheme.textPrimary)
                .lineLimit(2)
                .frame(width: 150, alignment: .leading)

            // Year
            if let year = recording.recordingYear {
                Text(String(format: "%d", year))
                    .font(ApproachNoteTheme.caption())
                    .foregroundColor(ApproachNoteTheme.textSecondary)
                    .frame(width: 150, alignment: .leading)
            }
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
        recording: (PerformerDetail.preview.recordings ?? [])[0],
        showRole: true
    )
    .padding()
}

#Preview("Recording Card - No Art") {
    PerformerRecordingCardView(
        recording: (PerformerDetail.preview.recordings ?? [])[2],
        showRole: false
    )
    .padding()
}
