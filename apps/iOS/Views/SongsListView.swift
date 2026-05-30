//
//  SongsListView.swift
//  Approach Note
//
//  Enhanced with repertoire filtering support
//

import SwiftUI

struct SongsListView: View {
    @StateObject private var songService = SongService()
    @EnvironmentObject var repertoireManager: RepertoireManager
    @EnvironmentObject var authManager: AuthenticationManager
    @State private var searchText = ""
    @State private var isSearchActive = false
    @State private var searchTask: Task<Void, Never>?
    @State private var showRepertoirePicker = false
    @State private var showLoginPrompt = false
    @State private var hasPerformedInitialLoad = false
    @State private var showMusicBrainzSearch = false
    @State private var removeErrorMessage: String?
    
    // Computed property to group songs by first letter
    private var groupedSongs: [(String, [Song])] {
        let filtered = songService.songs
        
        // Group songs by first letter
        let grouped = Dictionary(grouping: filtered) { song in
            let firstChar = song.title.prefix(1).uppercased()
            return firstChar.rangeOfCharacter(from: .letters) != nil ? firstChar : "#"
        }
        
        return grouped.sorted { lhs, rhs in
            if lhs.key == "#" { return false }
            if rhs.key == "#" { return true }
            return lhs.key < rhs.key
        }
    }
    
    // Get all section letters for the index
    private var sectionLetters: [String] {
        groupedSongs.map { $0.0 }
    }
    
    var body: some View {
        NavigationStack {
            contentView
                .background(ApproachNoteTheme.background)
                .jazzNavigationBar(title: "Songs (\(songService.songs.count.formatted()))")
                .searchable(text: $searchText, isPresented: $isSearchActive, prompt: "Search songs")
                .onChange(of: searchText) { oldValue, newValue in
                    searchTask?.cancel()
                    searchTask = Task {
                        try? await Task.sleep(nanoseconds: 300_000_000)
                        if !Task.isCancelled {
                            await loadSongs()
                        }
                    }
                }
                .onChange(of: repertoireManager.selectedRepertoire) { oldValue, newValue in
                    // Reload songs when repertoire changes
                    Task {
                        await loadSongs()
                    }
                }
                .task {
                    // Only load on initial appear, not when returning from detail view
                    if !hasPerformedInitialLoad {
                        await repertoireManager.loadRepertoires()
                        await loadSongs()
                        hasPerformedInitialLoad = true
                    }
                }
                .onReceive(NotificationCenter.default.publisher(for: .songCreated)) { _ in
                    // Refresh songs list when a new song is created
                    Task {
                        await loadSongs()
                    }
                }
                .onChange(of: authManager.isAuthenticated) { wasAuthenticated, isAuthenticated in
                    // Dismiss login prompt when user successfully authenticates
                    if isAuthenticated && showLoginPrompt {
                        showLoginPrompt = false
                        // After dismissing login, show the repertoire picker
                        DispatchQueue.main.asyncAfter(deadline: .now() + 0.3) {
                            showRepertoirePicker = true
                        }
                    }
                }
        }
        .tint(ApproachNoteTheme.brand)
    }
    
    // MARK: - Helper Methods
    
    private func loadSongs() async {
        if repertoireManager.selectedRepertoire.id != "all",
           let token = authManager.getAccessToken() {
            await songService.fetchSongsInRepertoire(
                repertoireId: repertoireManager.selectedRepertoire.id,
                searchQuery: searchText,
                authToken: token
            )
        } else {
            await songService.fetchSongsInRepertoire(
                repertoireId: repertoireManager.selectedRepertoire.id,
                searchQuery: searchText
            )
        }
    }

    /// Remove a song from the currently selected repertoire, then refresh the list.
    private func removeSongFromCurrentRepertoire(_ song: Song) {
        let repertoire = repertoireManager.selectedRepertoire
        guard repertoire.id != "all" else { return }

        Task {
            let success = await repertoireManager.removeSongFromRepertoire(
                songId: song.id,
                repertoireId: repertoire.id
            )
            if success {
                await loadSongs()
            } else {
                removeErrorMessage = repertoireManager.errorMessage
                    ?? "Couldn't remove \"\(song.title)\" from \(repertoire.name). Please try again."
            }
        }
    }

    // MARK: - Content Views
    
    @ViewBuilder
    private var contentView: some View {
        VStack(spacing: 0) {
            // Always show current repertoire header
            currentRepertoireBanner

            // Only show full loading view on initial load (no songs yet)
            // During pull-to-refresh, keep showing the list
            if songService.isLoading && songService.songs.isEmpty {
                loadingView
            } else if let error = songService.errorMessage {
                errorView(error: error)
            } else if songService.songs.isEmpty && !searchText.isEmpty {
                emptySearchResultsView
            } else {
                songsListView
            }
        }
        .sheet(isPresented: $showMusicBrainzSearch) {
            MusicBrainzSearchSheet(
                searchQuery: searchText,
                onSongImported: {
                    // Refresh the song list after import
                    Task {
                        await loadSongs()
                    }
                }
            )
        }
        .alert("Remove Failed", isPresented: Binding(
            get: { removeErrorMessage != nil },
            set: { if !$0 { removeErrorMessage = nil } }
        )) {
            Button("OK", role: .cancel) { }
        } message: {
            Text(removeErrorMessage ?? "")
        }
    }
    
    private var currentRepertoireBanner: some View {
        HStack {
            Text(repertoireManager.currentRepertoireDisplayName)
                .font(ApproachNoteTheme.subheadline())
                .fontWeight(.medium)
                .foregroundColor(ApproachNoteTheme.textPrimary)
            Spacer()
            Button(action: {
                if authManager.isAuthenticated {
                    showRepertoirePicker = true
                } else {
                    showLoginPrompt = true
                }
            }) {
                Text("Change")
                    .font(ApproachNoteTheme.subheadline())
                    .foregroundColor(ApproachNoteTheme.brand)
            }
            .sheet(isPresented: $showRepertoirePicker) {
                RepertoirePickerSheet(
                    repertoireManager: repertoireManager,
                    isPresented: $showRepertoirePicker
                )
                .presentationDetents([.medium, .large])
                .presentationDragIndicator(.visible)
            }
            .sheet(isPresented: $showLoginPrompt) {
                RepertoireLoginPromptView()
            }
        }
        .padding(.horizontal)
        .padding(.vertical, ApproachNoteTheme.spacingXS)
        .background(ApproachNoteTheme.accent.opacity(0.15))
    }
    
    private var loadingView: some View {
        VStack {
            Spacer()
            ThemedProgressView(message: "Loading songs...", tintColor: ApproachNoteTheme.brand)
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
                    await loadSongs()
                }
            }
            .buttonStyle(.borderedProminent)
            .tint(ApproachNoteTheme.brand)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
        .background(ApproachNoteTheme.background)
    }

    private var emptySearchResultsView: some View {
        VStack(spacing: ApproachNoteTheme.spacingMD) {
            Image(systemName: "magnifyingglass")
                .font(.system(size: 60))
                .foregroundColor(ApproachNoteTheme.textSecondary.opacity(0.5))

            Text("No Results")
                .font(ApproachNoteTheme.headline())
                .foregroundColor(ApproachNoteTheme.textPrimary)

            Text("No songs match \"\(searchText)\"")
                .font(ApproachNoteTheme.subheadline())
                .foregroundColor(ApproachNoteTheme.textSecondary)
                .multilineTextAlignment(.center)

            VStack(spacing: ApproachNoteTheme.spacingSM) {
                Text("Can't find what you're looking for?")
                    .font(ApproachNoteTheme.caption())
                    .foregroundColor(ApproachNoteTheme.textSecondary)

                Button(action: {
                    showMusicBrainzSearch = true
                }) {
                    HStack {
                        Image(systemName: "waveform")
                        Text("Search MusicBrainz")
                    }
                }
                .buttonStyle(.borderedProminent)
                .tint(ApproachNoteTheme.brand)
            }
            .padding(.top, ApproachNoteTheme.spacingXS)
        }
        .padding()
        .frame(maxWidth: .infinity, maxHeight: .infinity)
        .background(ApproachNoteTheme.background)
    }

    private var songsListView: some View {
        ScrollViewReader { proxy in
            List {
                ForEach(groupedSongs, id: \.0) { letter, songs in
                    Section(header: SectionHeaderView(letter: letter)) {
                        ForEach(songs) { song in
                            NavigationLink(destination: SongDetailView(songId: song.id)
                                                .environmentObject(repertoireManager)) {
                                songRowView(song: song)
                            }
                            .listRowBackground(ApproachNoteTheme.surface)
                            .contextMenu {
                                // Only offer removal when viewing a specific
                                // repertoire (not "All Songs"), so the target is unambiguous.
                                // A context menu avoids colliding with the trailing
                                // alphabet index that a swipe action would sit under.
                                if repertoireManager.selectedRepertoire.id != "all" {
                                    Button(role: .destructive) {
                                        removeSongFromCurrentRepertoire(song)
                                    } label: {
                                        Label("Remove from \(repertoireManager.selectedRepertoire.name)",
                                              systemImage: "minus.circle")
                                    }
                                }
                            }
                        }
                    }
                    .id(letter) // Anchor for scrolling
                }
            }
            .listStyle(.plain)
            .scrollContentBackground(.hidden)
            .background(ApproachNoteTheme.background)
            .refreshable {
                await loadSongs()
            }
            .overlay(alignment: .trailing) {
                // Custom alphabet index overlay
                AlphabetIndexView(
                    letters: sectionLetters,
                    accentColor: ApproachNoteTheme.brand,
                    onTap: { letter in
                        // Use short animation to prevent conflicts during rapid scrubbing
                        withAnimation(.easeOut(duration: 0.1)) {
                            proxy.scrollTo(letter, anchor: .top)
                        }
                    },
                    onSearch: {
                        // Jump to the top of the list, then reveal + focus the search field.
                        if let first = sectionLetters.first {
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
    
    private func songRowView(song: Song) -> some View {
        VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingXXS) {
            // Title + year as one string so the title spans the full width and
            // the year trails it (issue #197); each segment keeps its own
            // styling — bold title, normal/secondary year.
            (
                Text(song.title)
                    .font(ApproachNoteTheme.headline())
                    .foregroundColor(ApproachNoteTheme.textPrimary)
                + Text(song.composedYear.map { " (\(String($0)))" } ?? "")
                    .font(ApproachNoteTheme.subheadline())
                    .foregroundColor(ApproachNoteTheme.textSecondary)
            )
            .fixedSize(horizontal: false, vertical: true)
            if let composer = song.composer {
                Text(composer)
                    .font(ApproachNoteTheme.subheadline())
                    .foregroundColor(ApproachNoteTheme.textSecondary)
            }
        }
        .padding(.vertical, ApproachNoteTheme.spacingXXS)
    }
}

// MARK: - Repertoire Picker Sheet

struct RepertoirePickerSheet: View {
    @ObservedObject var repertoireManager: RepertoireManager
    @Binding var isPresented: Bool
    @EnvironmentObject var authManager: AuthenticationManager
    @State private var showCreateRepertoire = false
    @State private var repertoireToDelete: Repertoire?
    @State private var deleteErrorMessage: String?

    var body: some View {
        NavigationStack {
            List {
                ForEach(repertoireManager.repertoires) { repertoire in
                    Button(action: {
                        repertoireManager.selectRepertoire(repertoire)
                        isPresented = false
                    }) {
                        HStack {
                            VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingXXS) {
                                Text(repertoire.name)
                                    .font(ApproachNoteTheme.headline())
                                    .foregroundColor(ApproachNoteTheme.textPrimary)

                                if let description = repertoire.description {
                                    Text(description)
                                        .font(ApproachNoteTheme.subheadline())
                                        .foregroundColor(ApproachNoteTheme.textSecondary)
                                        .lineLimit(2)
                                }

                                if repertoire.id != "all" {
                                    Text("\(repertoire.songCount) songs")
                                        .font(ApproachNoteTheme.caption())
                                        .foregroundColor(ApproachNoteTheme.brand)
                                }
                            }

                            Spacer()

                            if repertoire.id == repertoireManager.selectedRepertoire.id {
                                Image(systemName: "checkmark.circle.fill")
                                    .foregroundColor(ApproachNoteTheme.brand)
                                    .font(ApproachNoteTheme.title3())
                            }
                        }
                        .contentShape(Rectangle())
                        .padding(.horizontal, ApproachNoteTheme.spacingMD)
                        .padding(.vertical, ApproachNoteTheme.spacingSM)
                        .frame(maxWidth: .infinity)
                        .background(
                            repertoire.id == repertoireManager.selectedRepertoire.id ?
                                ApproachNoteTheme.brand.opacity(0.1) :
                                ApproachNoteTheme.surface
                        )
                    }
                    .buttonStyle(.plain)
                    .listRowInsets(EdgeInsets())
                    .listRowSeparator(.visible)
                    .listRowSeparatorTint(ApproachNoteTheme.textSecondary.opacity(0.3))
                    .listRowBackground(Color.clear)
                    .swipeActions(edge: .trailing, allowsFullSwipe: false) {
                        if repertoire.id != "all" {
                            Button(role: .destructive) {
                                repertoireToDelete = repertoire
                            } label: {
                                Label("Delete", systemImage: "trash")
                            }
                        }
                    }
                }

                // Add "Create New Repertoire" button for authenticated users
                if authManager.isAuthenticated {
                    Button(action: {
                        showCreateRepertoire = true
                    }) {
                        HStack {
                            Image(systemName: "plus.circle.fill")
                                .foregroundColor(ApproachNoteTheme.brand)
                                .font(ApproachNoteTheme.title3())

                            Text("Create New Repertoire")
                                .font(ApproachNoteTheme.headline())
                                .foregroundColor(ApproachNoteTheme.brand)

                            Spacer()
                        }
                        .contentShape(Rectangle())
                        .padding(.horizontal, ApproachNoteTheme.spacingMD)
                        .padding(.vertical, ApproachNoteTheme.spacingSM)
                        .frame(maxWidth: .infinity)
                        .background(ApproachNoteTheme.surface)
                    }
                    .buttonStyle(.plain)
                    .listRowInsets(EdgeInsets())
                    .listRowSeparator(.visible)
                    .listRowSeparatorTint(ApproachNoteTheme.textSecondary.opacity(0.3))
                    .listRowBackground(Color.clear)
                }
            }
            .listStyle(.plain)
            .scrollContentBackground(.hidden)
            .background(ApproachNoteTheme.background)
            .navigationTitle("Select Repertoire")
            .navigationBarTitleDisplayMode(.inline)
            // Style the nav bar from the live palette (the global
            // UINavigationBar appearance is set once at launch and goes stale
            // when the palette changes), matching jazzNavigationBar.
            .toolbarBackground(ApproachNoteTheme.brand, for: .navigationBar)
            .toolbarBackground(.visible, for: .navigationBar)
            .toolbarColorScheme(.dark, for: .navigationBar)
            .toolbar {
                ToolbarItem(placement: .navigationBarTrailing) {
                    Button("Done") {
                        isPresented = false
                    }
                    .fontWeight(.semibold)
                    .foregroundColor(ApproachNoteTheme.textOnDark)
                }
            }
            .sheet(isPresented: $showCreateRepertoire) {
                CreateRepertoireView(repertoireManager: repertoireManager)
            }
            .alert("Delete Repertoire?",
                   isPresented: Binding(
                    get: { repertoireToDelete != nil },
                    set: { if !$0 { repertoireToDelete = nil } }
                   ),
                   presenting: repertoireToDelete) { repertoire in
                Button("Delete", role: .destructive) {
                    Task {
                        let success = await repertoireManager.deleteRepertoire(id: repertoire.id)
                        if !success {
                            deleteErrorMessage = "Couldn't delete \"\(repertoire.name)\". Please try again."
                        }
                    }
                }
                Button("Cancel", role: .cancel) { }
            } message: { repertoire in
                Text("Delete \u{201C}\(repertoire.name)\u{201D}? This removes the repertoire and all its song entries. This cannot be undone.")
            }
            .alert("Delete Failed",
                   isPresented: Binding(
                    get: { deleteErrorMessage != nil },
                    set: { if !$0 { deleteErrorMessage = nil } }
                   )) {
                Button("OK", role: .cancel) { }
            } message: {
                Text(deleteErrorMessage ?? "")
            }
        }
        .presentationDetents([.medium, .large])
        .presentationDragIndicator(.visible)
    }
}

// MARK: - Supporting Views

// Custom section header view
struct SectionHeaderView: View {
    let letter: String
    
    var body: some View {
        Text(letter)
            .font(ApproachNoteTheme.headline())
            .fontWeight(.bold)
            .foregroundColor(ApproachNoteTheme.brand)
            .frame(maxWidth: .infinity, alignment: .leading)
            .padding(.vertical, ApproachNoteTheme.spacingXS)
            .padding(.horizontal)
            .background(ApproachNoteTheme.background.opacity(0.8))
    }
}

// Reusable custom alphabet index view - ONLY DEFINED HERE, NOT IN ArtistsListView
// Supports both tap and drag gestures for easier navigation (iOS Contacts style)
struct AlphabetIndexView: View {
    let letters: [String]
    var accentColor: Color = ApproachNoteTheme.brand
    let onTap: (String) -> Void
    /// When provided, a magnifying-glass icon is shown at the top of the index
    /// (before the first letter, iOS Contacts style). Tapping it jumps the list
    /// to the top and activates the search field.
    var onSearch: (() -> Void)? = nil

    // Track which letter is currently being touched/dragged over
    @State private var highlightedLetter: String?
    @State private var isDragging = false
    @GestureState private var dragLocation: CGPoint = .zero

    // Height of each letter row (used for drag calculations)
    private let letterHeight: CGFloat = 18
    private let letterWidth: CGFloat = 32

    var body: some View {
        VStack(spacing: 0) {
            if let onSearch {
                Button(action: onSearch) {
                    Image(systemName: "magnifyingglass")
                        .font(.system(size: 11, weight: .semibold))
                        .foregroundColor(accentColor)
                        .frame(width: letterWidth, height: letterHeight)
                        .contentShape(Rectangle())
                }
                .buttonStyle(.plain)
                .accessibilityLabel("Search")
            }

            // Letters carry the tap/drag gestures in their own coordinate space,
            // kept separate from the search button above so the two don't conflict.
            lettersColumn
        }
        .padding(.vertical, ApproachNoteTheme.spacingXS)
        .padding(.horizontal, ApproachNoteTheme.spacingXXS)
        .background(
            RoundedRectangle(cornerRadius: 8)
                .fill(ApproachNoteTheme.background.opacity(0.95))
                .shadow(color: .black.opacity(0.15), radius: 3, x: -2, y: 0)
        )
    }

    private var lettersColumn: some View {
        VStack(spacing: 0) {
            ForEach(Array(letters.enumerated()), id: \.element) { index, letter in
                Text(letter)
                    .font(.system(size: 11, weight: .semibold))
                    .foregroundColor(highlightedLetter == letter ? .white : accentColor)
                    .frame(width: letterWidth, height: letterHeight)
                    .background(
                        RoundedRectangle(cornerRadius: 4)
                            .fill(highlightedLetter == letter ? accentColor : Color.white.opacity(0.7))
                    )
                    .contentShape(Rectangle()) // Expand touch target to full frame
            }
        }
        .gesture(
            DragGesture(minimumDistance: 0)
                .onChanged { value in
                    isDragging = true
                    let letter = letterAt(location: value.location)

                    // Update visual highlight and haptic, but DON'T scroll during drag
                    // (scrolling during drag causes SwiftUI List rendering bugs)
                    if letter != highlightedLetter, let letter = letter {
                        highlightedLetter = letter
                        // Haptic feedback on letter change
                        let generator = UIImpactFeedbackGenerator(style: .light)
                        generator.impactOccurred()
                    }
                }
                .onEnded { _ in
                    isDragging = false
                    // Scroll only when user lifts finger - avoids SwiftUI List bug
                    if let letter = highlightedLetter {
                        onTap(letter)
                    }
                    // Clear highlight after a short delay
                    DispatchQueue.main.asyncAfter(deadline: .now() + 0.2) {
                        if !isDragging {
                            highlightedLetter = nil
                        }
                    }
                }
        )
        // Also support simple tap (not drag)
        .onTapGesture { location in
            if let letter = letterAt(location: location) {
                highlightedLetter = letter
                onTap(letter)
                let generator = UIImpactFeedbackGenerator(style: .light)
                generator.impactOccurred()
                // Clear highlight after a short delay
                DispatchQueue.main.asyncAfter(deadline: .now() + 0.2) {
                    highlightedLetter = nil
                }
            }
        }
    }

    /// Calculate which letter is at the given Y location. The gesture lives on
    /// `lettersColumn`, whose local origin is the first letter, so no padding
    /// offset is needed.
    private func letterAt(location: CGPoint) -> String? {
        let index = Int(location.y / letterHeight)

        guard index >= 0 && index < letters.count else {
            return nil
        }
        return letters[index]
    }
}

#Preview {
    SongsListView()
        .environmentObject(RepertoireManager())
        .environmentObject(AuthenticationManager())
}
