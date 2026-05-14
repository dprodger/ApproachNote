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
    @State private var searchTask: Task<Void, Never>?
    @State private var showRepertoirePicker = false
    @State private var showLoginPrompt = false
    @State private var hasPerformedInitialLoad = false
    @State private var showMusicBrainzSearch = false
    
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
                .background(ApproachNoteTheme.backgroundLight)
                .jazzNavigationBar(title: "Songs (\(songService.songs.count.formatted()))")
                .searchable(text: $searchText, prompt: "Search songs")
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
        .tint(ApproachNoteTheme.burgundy)
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
    }
    
    private var currentRepertoireBanner: some View {
        HStack {
            Image(systemName: "music.note.list")
                .foregroundColor(ApproachNoteTheme.burgundy)
            Text(repertoireManager.currentRepertoireDisplayName)
                .font(ApproachNoteTheme.subheadline())
                .fontWeight(.medium)
                .foregroundColor(ApproachNoteTheme.charcoal)
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
                    .foregroundColor(ApproachNoteTheme.burgundy)
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
        .padding(.vertical, 8)
        .background(ApproachNoteTheme.amber.opacity(0.15))
    }
    
    private var loadingView: some View {
        VStack {
            Spacer()
            ThemedProgressView(message: "Loading songs...", tintColor: ApproachNoteTheme.burgundy)
            Spacer()
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
        .background(ApproachNoteTheme.backgroundLight)
    }
    
    private func errorView(error: String) -> some View {
        VStack(spacing: 16) {
            Image(systemName: "exclamationmark.triangle")
                .font(.system(size: 50))
                .foregroundColor(ApproachNoteTheme.amber)
            Text("Error")
                .font(ApproachNoteTheme.headline())
                .foregroundColor(ApproachNoteTheme.charcoal)
            Text(error)
                .font(ApproachNoteTheme.subheadline())
                .foregroundColor(ApproachNoteTheme.smokeGray)
                .multilineTextAlignment(.center)
                .padding(.horizontal)
            Button("Retry") {
                Task {
                    await loadSongs()
                }
            }
            .buttonStyle(.borderedProminent)
            .tint(ApproachNoteTheme.burgundy)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
        .background(ApproachNoteTheme.backgroundLight)
    }

    private var emptySearchResultsView: some View {
        VStack(spacing: 16) {
            Image(systemName: "magnifyingglass")
                .font(.system(size: 60))
                .foregroundColor(ApproachNoteTheme.smokeGray.opacity(0.5))

            Text("No Results")
                .font(ApproachNoteTheme.headline())
                .foregroundColor(ApproachNoteTheme.charcoal)

            Text("No songs match \"\(searchText)\"")
                .font(ApproachNoteTheme.subheadline())
                .foregroundColor(ApproachNoteTheme.smokeGray)
                .multilineTextAlignment(.center)

            VStack(spacing: 12) {
                Text("Can't find what you're looking for?")
                    .font(ApproachNoteTheme.caption())
                    .foregroundColor(ApproachNoteTheme.smokeGray)

                Button(action: {
                    showMusicBrainzSearch = true
                }) {
                    HStack {
                        Image(systemName: "waveform")
                        Text("Search MusicBrainz")
                    }
                }
                .buttonStyle(.borderedProminent)
                .tint(ApproachNoteTheme.burgundy)
            }
            .padding(.top, 8)
        }
        .padding()
        .frame(maxWidth: .infinity, maxHeight: .infinity)
        .background(ApproachNoteTheme.backgroundLight)
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
                            .listRowBackground(ApproachNoteTheme.cardBackground)
                        }
                    }
                    .id(letter) // Anchor for scrolling
                }
            }
            .listStyle(.plain)
            .scrollContentBackground(.hidden)
            .background(ApproachNoteTheme.backgroundLight)
            .refreshable {
                await loadSongs()
            }
            .overlay(alignment: .trailing) {
                // Custom alphabet index overlay
                AlphabetIndexView(
                    letters: sectionLetters,
                    accentColor: ApproachNoteTheme.burgundy,
                    onTap: { letter in
                        // Use short animation to prevent conflicts during rapid scrubbing
                        withAnimation(.easeOut(duration: 0.1)) {
                            proxy.scrollTo(letter, anchor: .top)
                        }
                    }
                )
                .padding(.trailing, 4)
            }
        }
    }
    
    private func songRowView(song: Song) -> some View {
        VStack(alignment: .leading, spacing: 4) {
            HStack(alignment: .firstTextBaseline, spacing: 6) {
                Text(song.title)
                    .font(ApproachNoteTheme.headline())
                    .foregroundColor(ApproachNoteTheme.charcoal)
                if let year = song.composedYear {
                    Text("(\(String(year)))")
                        .font(ApproachNoteTheme.subheadline())
                        .foregroundColor(ApproachNoteTheme.smokeGray)
                }
            }
            if let composer = song.composer {
                Text(composer)
                    .font(ApproachNoteTheme.subheadline())
                    .foregroundColor(ApproachNoteTheme.smokeGray)
            }
        }
        .padding(.vertical, 4)
    }
}

// MARK: - Repertoire Picker Sheet

struct RepertoirePickerSheet: View {
    @ObservedObject var repertoireManager: RepertoireManager
    @Binding var isPresented: Bool
    @EnvironmentObject var authManager: AuthenticationManager
    @State private var showCreateRepertoire = false
    
    var body: some View {
        NavigationStack {
            ScrollView {
                VStack(spacing: 0) {
                    ForEach(repertoireManager.repertoires) { repertoire in
                        Button(action: {
                            repertoireManager.selectRepertoire(repertoire)
                            isPresented = false
                        }) {
                            HStack {
                                VStack(alignment: .leading, spacing: 4) {
                                    Text(repertoire.name)
                                        .font(ApproachNoteTheme.headline())
                                        .foregroundColor(ApproachNoteTheme.charcoal)
                                    
                                    if let description = repertoire.description {
                                        Text(description)
                                            .font(ApproachNoteTheme.subheadline())
                                            .foregroundColor(ApproachNoteTheme.smokeGray)
                                            .lineLimit(2)
                                    }
                                    
                                    if repertoire.id != "all" {
                                        Text("\(repertoire.songCount) songs")
                                            .font(ApproachNoteTheme.caption())
                                            .foregroundColor(ApproachNoteTheme.burgundy)
                                    }
                                }
                                
                                Spacer()
                                
                                if repertoire.id == repertoireManager.selectedRepertoire.id {
                                    Image(systemName: "checkmark.circle.fill")
                                        .foregroundColor(ApproachNoteTheme.burgundy)
                                        .font(ApproachNoteTheme.title3())
                                }
                            }
                            .contentShape(Rectangle())
                            .padding(.horizontal, 16)
                            .padding(.vertical, 12)
                            .frame(maxWidth: .infinity)
                            .background(
                                repertoire.id == repertoireManager.selectedRepertoire.id ?
                                    ApproachNoteTheme.burgundy.opacity(0.1) :
                                    ApproachNoteTheme.cardBackground
                            )
                        }
                        .buttonStyle(.plain)
                        
                        if repertoire.id != repertoireManager.repertoires.last?.id {
                            Divider()
                                .background(ApproachNoteTheme.smokeGray.opacity(0.3))
                        }
                    }
                    
                    // Add "Create New Repertoire" button for authenticated users
                    if authManager.isAuthenticated {
                        Divider()
                            .background(ApproachNoteTheme.smokeGray.opacity(0.3))
                        
                        Button(action: {
                            showCreateRepertoire = true
                        }) {
                            HStack {
                                Image(systemName: "plus.circle.fill")
                                    .foregroundColor(ApproachNoteTheme.burgundy)
                                    .font(ApproachNoteTheme.title3())
                                
                                Text("Create New Repertoire")
                                    .font(ApproachNoteTheme.headline())
                                    .foregroundColor(ApproachNoteTheme.burgundy)
                                
                                Spacer()
                            }
                            .contentShape(Rectangle())
                            .padding(.horizontal, 16)
                            .padding(.vertical, 12)
                            .frame(maxWidth: .infinity)
                            .background(ApproachNoteTheme.cardBackground)
                        }
                        .buttonStyle(.plain)
                    }
                }
            }
            .background(ApproachNoteTheme.backgroundLight)
            .navigationTitle("Select Repertoire")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .navigationBarTrailing) {
                    Button("Done") {
                        isPresented = false
                    }
                    .foregroundColor(ApproachNoteTheme.burgundy)
                }
            }
            .sheet(isPresented: $showCreateRepertoire) {
                CreateRepertoireView(repertoireManager: repertoireManager)
            }
        }
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
            .foregroundColor(ApproachNoteTheme.burgundy)
            .frame(maxWidth: .infinity, alignment: .leading)
            .padding(.vertical, 8)
            .padding(.horizontal)
            .background(ApproachNoteTheme.backgroundLight.opacity(0.8))
    }
}

// Reusable custom alphabet index view - ONLY DEFINED HERE, NOT IN ArtistsListView
// Supports both tap and drag gestures for easier navigation (iOS Contacts style)
struct AlphabetIndexView: View {
    let letters: [String]
    var accentColor: Color = ApproachNoteTheme.burgundy
    let onTap: (String) -> Void

    // Track which letter is currently being touched/dragged over
    @State private var highlightedLetter: String?
    @State private var isDragging = false
    @GestureState private var dragLocation: CGPoint = .zero

    // Height of each letter row (used for drag calculations)
    private let letterHeight: CGFloat = 18
    private let letterWidth: CGFloat = 32

    var body: some View {
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
        .padding(.vertical, 8)
        .padding(.horizontal, 4)
        .background(
            RoundedRectangle(cornerRadius: 8)
                .fill(ApproachNoteTheme.backgroundLight.opacity(0.95))
                .shadow(color: .black.opacity(0.15), radius: 3, x: -2, y: 0)
        )
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

    /// Calculate which letter is at the given Y location
    private func letterAt(location: CGPoint) -> String? {
        // Account for vertical padding (8 points at top)
        let adjustedY = location.y - 8
        let index = Int(adjustedY / letterHeight)

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
