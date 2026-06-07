//
//  ArtistsListView.swift
//  Approach Note
//
//  Enhanced with custom scrollable alphabet index (iOS Contacts-style)
//  Updated to handle non-Latin characters better
//  Now uses pagination for faster initial load with infinite scroll
//

import SwiftUI

struct ArtistsListView: View {
    @StateObject private var performerService = PerformerService()
    @State private var searchText = ""
    @State private var isSearchActive = false
    @State private var searchTask: Task<Void, Never>?
    @State private var hasPerformedInitialLoad = false

    // Helper to get the effective sort name for a performer
    private func effectiveSortName(for performer: Performer) -> String {
        performer.sortName ?? performer.name
    }

    // Helper to extract first letter for grouping (uses sortName)
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

    // Helper to get the sort key (first word of sortName, typically last name)
    private func sortKey(for performer: Performer) -> String? {
        guard let sortName = performer.sortName else { return nil }
        // Get first word (before comma or space)
        if let commaIndex = sortName.firstIndex(of: ",") {
            return String(sortName[..<commaIndex])
        }
        return sortName.components(separatedBy: " ").first
    }

    // Computed property to group artists by first letter of sort_name
    // Uses the lightweight index (id, name, sort_name) for all 30k performers
    private var groupedArtists: [(String, [Performer])] {
        let filtered = performerService.performersIndex

        // Group by first letter of sortName (or name if sortName is nil)
        let grouped = Dictionary(grouping: filtered) { performer in
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

    // Get all section letters for the alphabet sidebar
    private var allSectionLetters: [String] {
        groupedArtists.map { $0.0 }
    }

    // Total count for display
    private var totalArtistsCount: Int {
        performerService.performersIndex.count
    }

    var body: some View {
        NavigationStack {
            contentView
                .background(ApproachNoteTheme.background)
                .jazzNavigationBar(title: "Artists (\(totalArtistsCount.formatted()))")
                .searchable(text: $searchText, isPresented: $isSearchActive, prompt: "Search artists")
                .onChange(of: searchText) { oldValue, newValue in
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
                .onReceive(NotificationCenter.default.publisher(for: .artistCreated)) { _ in
                    // Refresh artists list when a new artist is created
                    Task {
                        await performerService.fetchPerformersIndex(searchQuery: searchText)
                    }
                }
        }
        .tint(ApproachNoteTheme.accent)
    }
    
    // Break up the body into separate views for compiler
    @ViewBuilder
    private var contentView: some View {
        VStack(spacing: 0) {
            if performerService.isLoading {
                loadingView
            } else if let error = performerService.errorMessage {
                errorView(error: error)
            } else {
                artistsListView
            }
        }
    }
    
    private var loadingView: some View {
        VStack {
            Spacer()
            ThemedProgressView(message: "Loading artists...", tintColor: ApproachNoteTheme.accent)
            Spacer()
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
        .background(ApproachNoteTheme.background)
    }
    
    private func errorView(error: String) -> some View {
        VStack(spacing: ApproachNoteTheme.spacingMD) {
            Image(systemName: "exclamationmark.triangle")
                .font(.system(size: 50))
                .foregroundColor(ApproachNoteTheme.accent)
            Text("Error")
                .font(ApproachNoteTheme.headline())
                .foregroundColor(ApproachNoteTheme.textPrimary)
            Text(error)
                .font(ApproachNoteTheme.subheadline())
                .foregroundColor(ApproachNoteTheme.textSecondary)
                .multilineTextAlignment(.center)
                .padding(.horizontal)
            Button("Retry") {
                Task {
                    await performerService.fetchPerformers()
                }
            }
            .buttonStyle(.borderedProminent)
            .tint(ApproachNoteTheme.accent)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
        .background(ApproachNoteTheme.background)
    }
    
    private var artistsListView: some View {
        ScrollViewReader { proxy in
            List {
                ForEach(groupedArtists, id: \.0) { letter, artists in
                    Section(header: ArtistSectionHeaderView(letter: letter).listRowInsets(EdgeInsets())) {
                        ForEach(artists) { performer in
                            NavigationLink(destination: PerformerDetailView(performerId: performer.id)) {
                                artistRowView(performer: performer)
                            }
                            .listRowBackground(ApproachNoteTheme.surface)
                        }
                    }
                    .id(letter) // Anchor for scrolling
                }
            }
            .listStyle(.plain)
            .scrollContentBackground(.hidden)
            .background(ApproachNoteTheme.background)
            .overlay(alignment: .trailing) {
                // Custom alphabet index overlay
                AlphabetIndexView(
                    letters: allSectionLetters,
                    accentColor: ApproachNoteTheme.accent,
                    onTap: { letter in
                        // Use short animation to prevent conflicts during rapid scrubbing
                        withAnimation(.easeOut(duration: 0.1)) {
                            proxy.scrollTo(letter, anchor: .top)
                        }
                    },
                    onSearch: {
                        // Jump to the top of the list, then reveal + focus the search field.
                        if let first = allSectionLetters.first {
                            withAnimation(.easeOut(duration: 0.2)) {
                                proxy.scrollTo(first, anchor: .top)
                            }
                        }
                        isSearchActive = true
                    }
                )
                .padding(.trailing, ApproachNoteTheme.spacingXXS)
            }
        }
    }
    
    // Build attributed name with sort key bolded
    // Uses regular weight for non-sort parts, semibold for the sort key
    private func formattedName(for performer: Performer) -> Text {
        guard let key = sortKey(for: performer),
              let range = performer.name.range(of: key, options: .caseInsensitive) else {
            // No sort key or not found in name - just return plain name
            return Text(performer.name)
                .font(ApproachNoteTheme.headline())
                .foregroundColor(ApproachNoteTheme.textPrimary)
        }

        // Split name into parts: before, the key, and after
        let before = String(performer.name[..<range.lowerBound])
        let keyText = String(performer.name[range])
        let after = String(performer.name[range.upperBound...])

        // Use regular weight for non-sort parts, semibold (default) for sort key
        return Text(before)
            .font(ApproachNoteTheme.headline(weight: .regular))
            .foregroundColor(ApproachNoteTheme.textPrimary)
        + Text(keyText)
            .font(ApproachNoteTheme.headline(weight: .semibold))
            .foregroundColor(ApproachNoteTheme.textPrimary)
        + Text(after)
            .font(ApproachNoteTheme.headline(weight: .regular))
            .foregroundColor(ApproachNoteTheme.textPrimary)
    }

    private func artistRowView(performer: Performer) -> some View {
        VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingXXS) {
            formattedName(for: performer)

            if let instrument = performer.instrument {
                Text(instrument)
                    .font(ApproachNoteTheme.subheadline())
                    .foregroundColor(ApproachNoteTheme.textSecondary)
            }
        }
        .padding(.vertical, ApproachNoteTheme.spacingXXS)
    }
}

// Custom section header view for artists
struct ArtistSectionHeaderView: View {
    let letter: String
    
    var body: some View {
        Text(letter)
            .font(ApproachNoteTheme.headline())
            .fontWeight(.bold)
            .foregroundColor(ApproachNoteTheme.accent)
            .frame(maxWidth: .infinity, alignment: .leading)
            .padding(.vertical, ApproachNoteTheme.spacingXS)
            .padding(.horizontal)
            .background(ApproachNoteTheme.surfaceMuted)
    }
}

#Preview {
    ArtistsListView()
}
