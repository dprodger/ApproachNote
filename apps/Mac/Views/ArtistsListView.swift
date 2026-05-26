//
//  ArtistsListView.swift
//  Approach Note
//
//  macOS-specific artists list view with master-detail layout
//  Now uses lightweight performer index for faster loading
//

import SwiftUI

struct ArtistsListView: View {
    @StateObject private var performerService = PerformerService()
    @State private var searchText = ""
    @State private var searchTask: Task<Void, Never>?
    @State private var selectedPerformerId: String?
    @State private var hasPerformedInitialLoad = false

    // MARK: - Sort Name Helpers

    /// Get the effective sort name for a performer (sortName or fallback to name)
    private func effectiveSortName(for performer: Performer) -> String {
        performer.sortName ?? performer.name
    }

    /// Extract first letter for grouping (uses sortName for proper last-name ordering)
    private func firstLetter(for sortName: String) -> String {
        let firstChar: String

        if let commaIndex = sortName.firstIndex(of: ",") {
            // "Last, First" format - use first letter of last name
            let lastName = sortName[..<commaIndex]
            firstChar = String(lastName.prefix(1)).uppercased()
        } else {
            // Single name - use first letter
            firstChar = String(sortName.prefix(1)).uppercased()
        }

        // Check if it's a Latin letter (A-Z)
        if firstChar.rangeOfCharacter(from: CharacterSet(charactersIn: "ABCDEFGHIJKLMNOPQRSTUVWXYZ")) != nil {
            return firstChar
        } else if firstChar.rangeOfCharacter(from: .letters) != nil {
            return "•" // Non-Latin letters (Cyrillic, Asian scripts, etc.)
        } else {
            return "#" // Numbers and symbols
        }
    }

    /// Get the sort key (first word of sortName, typically last name)
    private func sortKey(for performer: Performer) -> String? {
        guard let sortName = performer.sortName else { return nil }
        if let commaIndex = sortName.firstIndex(of: ",") {
            return String(sortName[..<commaIndex])
        }
        return sortName.components(separatedBy: " ").first
    }

    var body: some View {
        HSplitView {
            artistListPane
            detailPane
        }
        .onChange(of: searchText) { _, newValue in
            searchTask?.cancel()
            searchTask = Task {
                try? await Task.sleep(nanoseconds: 300_000_000)
                if !Task.isCancelled {
                    await performerService.fetchPerformersIndex(searchQuery: newValue)
                }
            }
        }
        .task {
            // Only load on initial appear, not when returning from detail view
            if !hasPerformedInitialLoad {
                await performerService.fetchPerformersIndex(searchQuery: searchText)
                hasPerformedInitialLoad = true
            }
        }
    }

    // MARK: - View Components

    private var artistListPane: some View {
        VStack(spacing: 0) {
            MacSearchBar(
                text: $searchText,
                placeholder: "Search artists...",
                backgroundColor: ApproachNoteTheme.accent
            )

            List(selection: $selectedPerformerId) {
                ForEach(groupedPerformers, id: \.0) { letter, performers in
                    Section(header: sectionHeader(letter: letter)) {
                        ForEach(performers) { performer in
                            ArtistRowView(performer: performer, isSelected: selectedPerformerId == performer.id)
                                .tag(performer.id)
                                .listRowBackground(
                                    selectedPerformerId == performer.id
                                        ? ApproachNoteTheme.brand
                                        : ApproachNoteTheme.background
                                )
                        }
                    }
                }
            }
            .listStyle(.plain)
            .scrollContentBackground(.hidden)
            .background(ApproachNoteTheme.background)
            .listSectionSeparator(.hidden)
        }
        .frame(minWidth: 200, idealWidth: 250, maxWidth: 300)
        .environment(\.colorScheme, .light)
    }

    private func sectionHeader(letter: String) -> some View {
        HStack {
            Text(letter)
                .font(ApproachNoteTheme.headline())
                .foregroundColor(.white)
            Spacer()
        }
        .padding(.horizontal, 12)
        .padding(.vertical, 6)
        .frame(maxWidth: .infinity)
        .background(
            ApproachNoteTheme.accent
                .padding(.horizontal, -20)
                .padding(.vertical, -4)
        )
        .listRowInsets(EdgeInsets())
    }

    @ViewBuilder
    private var detailPane: some View {
        if let performerId = selectedPerformerId {
            PerformerDetailView(performerId: performerId)
                .frame(minWidth: 400)
        } else {
            VStack {
                Image(systemName: "person.2")
                    .font(.system(size: 60))
                    .foregroundColor(ApproachNoteTheme.textSecondary.opacity(0.5))
                Text("Select an artist")
                    .font(ApproachNoteTheme.title2())
                    .foregroundColor(ApproachNoteTheme.textSecondary)
            }
            .frame(maxWidth: .infinity, maxHeight: .infinity)
            .background(ApproachNoteTheme.background)
        }
    }

    // MARK: - Helper Methods

    /// Group performers by first letter of sortName for proper alphabetical ordering
    private var groupedPerformers: [(String, [Performer])] {
        let grouped = Dictionary(grouping: performerService.performersIndex) { performer in
            firstLetter(for: effectiveSortName(for: performer))
        }

        return grouped.sorted { lhs, rhs in
            // "#" always last
            if lhs.key == "#" { return false }
            if rhs.key == "#" { return true }
            // "•" second to last
            if lhs.key == "•" { return false }
            if rhs.key == "•" { return true }
            // Rest alphabetically
            return lhs.key < rhs.key
        }.map { (key, value) in
            // Sort within each group by sortName
            (key, value.sorted { effectiveSortName(for: $0) < effectiveSortName(for: $1) })
        }
    }
}

// MARK: - Artist Row View

struct ArtistRowView: View {
    let performer: Performer
    var isSelected: Bool = false

    /// Get the sort key (first word of sortName, typically last name)
    private func sortKey(for performer: Performer) -> String? {
        guard let sortName = performer.sortName else { return nil }
        if let commaIndex = sortName.firstIndex(of: ",") {
            return String(sortName[..<commaIndex])
        }
        return sortName.components(separatedBy: " ").first
    }

    /// Build formatted name with sort key bolded
    private func formattedName(for performer: Performer) -> Text {
        let textColor = isSelected ? Color.white : ApproachNoteTheme.textPrimary

        guard let key = sortKey(for: performer),
              let range = performer.name.range(of: key, options: .caseInsensitive) else {
            // No sort key or not found in name - just return plain name
            return Text(performer.name)
                .font(ApproachNoteTheme.headline())
                .foregroundColor(textColor)
        }

        // Split name into parts: before, the key, and after
        let before = String(performer.name[..<range.lowerBound])
        let keyText = String(performer.name[range])
        let after = String(performer.name[range.upperBound...])

        // Use regular weight for non-sort parts, semibold for sort key
        return Text(before)
            .font(ApproachNoteTheme.headline(weight: .regular))
            .foregroundColor(textColor)
        + Text(keyText)
            .font(ApproachNoteTheme.headline(weight: .semibold))
            .foregroundColor(textColor)
        + Text(after)
            .font(ApproachNoteTheme.headline(weight: .regular))
            .foregroundColor(textColor)
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 2) {
            formattedName(for: performer)
            if let instrument = performer.instrument {
                Text(instrument)
                    .font(ApproachNoteTheme.subheadline())
                    .foregroundStyle(isSelected ? Color.white.opacity(0.85) : ApproachNoteTheme.textSecondary)
            }
        }
        .padding(.vertical, 4)
        .padding(.horizontal, 8)
    }
}

#Preview {
    ArtistsListView()
}
