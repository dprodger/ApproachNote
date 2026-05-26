//
//  SongsListView.swift
//  Approach Note
//
//  macOS-specific songs list view with master-detail layout
//

import SwiftUI

struct SongsListView: View {
    @StateObject private var songService = SongService()
    @EnvironmentObject var repertoireManager: RepertoireManager
    @EnvironmentObject var authManager: AuthenticationManager
    @State private var searchText = ""
    @State private var searchTask: Task<Void, Never>?
    @State private var selectedSongId: String?
    @State private var showMusicBrainzSearch = false

    var body: some View {
        HSplitView {
            leftPane
            rightPane
        }
        .onChange(of: searchText) { _, newValue in
            searchTask?.cancel()
            searchTask = Task {
                try? await Task.sleep(nanoseconds: 300_000_000)
                if !Task.isCancelled {
                    await loadSongs()
                }
            }
        }
        .onChange(of: repertoireManager.selectedRepertoire) { _, _ in
            Task {
                await loadSongs()
            }
        }
        .task {
            await repertoireManager.loadRepertoires()
            await loadSongs()
        }
    }

    // MARK: - Left Pane (Song List)

    private var leftPane: some View {
        VStack(spacing: 0) {
            MacSearchBar(
                text: $searchText,
                placeholder: "Search songs...",
                backgroundColor: ApproachNoteTheme.brand
            )

            if songService.songs.isEmpty && !searchText.isEmpty {
                emptySearchResultsView
            } else {
                songListView
            }
        }
        .frame(minWidth: 200, idealWidth: 250, maxWidth: 300)
        .background(ApproachNoteTheme.background)
        .sheet(isPresented: $showMusicBrainzSearch) {
            MusicBrainzSearchSheet(
                searchQuery: searchText,
                onSongImported: {
                    Task {
                        await loadSongs()
                    }
                }
            )
        }
    }

    private var songListView: some View {
        List(selection: $selectedSongId) {
            ForEach(groupedSongs, id: \.0) { letter, songs in
                Section(header: sectionHeader(letter: letter)) {
                    ForEach(songs) { song in
                        SongRowView(song: song, isSelected: selectedSongId == song.id)
                            .tag(song.id)
                            .listRowBackground(
                                selectedSongId == song.id
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
            ApproachNoteTheme.brand
                .padding(.horizontal, -20)
                .padding(.vertical, -4)
        )
        .listRowInsets(EdgeInsets())
    }

    // MARK: - Right Pane (Detail)

    @ViewBuilder
    private var rightPane: some View {
        if let songId = selectedSongId {
            SongDetailView(songId: songId)
                .frame(minWidth: 400)
        } else {
            VStack {
                Image(systemName: "music.note")
                    .font(.system(size: 60))
                    .foregroundColor(ApproachNoteTheme.textSecondary.opacity(0.5))
                Text("Select a song")
                    .font(ApproachNoteTheme.title2())
                    .foregroundColor(ApproachNoteTheme.textSecondary)
            }
            .frame(maxWidth: .infinity, maxHeight: .infinity)
            .background(ApproachNoteTheme.background)
        }
    }

    // MARK: - Helper Methods

    private var groupedSongs: [(String, [Song])] {
        let grouped = Dictionary(grouping: songService.songs) { song in
            let firstChar = song.title.prefix(1).uppercased()
            return firstChar.rangeOfCharacter(from: .letters) != nil ? firstChar : "#"
        }

        return grouped.sorted { lhs, rhs in
            if lhs.key == "#" { return false }
            if rhs.key == "#" { return true }
            return lhs.key < rhs.key
        }
    }

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

    private var emptySearchResultsView: some View {
        VStack(spacing: 12) {
            Image(systemName: "magnifyingglass")
                .font(.system(size: 40))
                .foregroundColor(ApproachNoteTheme.textSecondary.opacity(0.5))

            Text("No Results")
                .font(ApproachNoteTheme.headline())
                .foregroundColor(ApproachNoteTheme.textPrimary)

            Text("No songs match \"\(searchText)\"")
                .font(ApproachNoteTheme.subheadline())
                .foregroundColor(ApproachNoteTheme.textSecondary)
                .multilineTextAlignment(.center)

            Button(action: {
                showMusicBrainzSearch = true
            }) {
                HStack(spacing: 4) {
                    Image(systemName: "waveform")
                    Text("Search MusicBrainz")
                }
                .font(ApproachNoteTheme.subheadline())
            }
            .buttonStyle(.borderedProminent)
            .tint(ApproachNoteTheme.brand)
            .padding(.top, 4)
        }
        .padding()
        .frame(maxWidth: .infinity, maxHeight: .infinity)
        .background(ApproachNoteTheme.background)
    }
}

// MARK: - Song Row View

struct SongRowView: View {
    let song: Song
    var isSelected: Bool = false

    var body: some View {
        VStack(alignment: .leading, spacing: 2) {
            Text(song.title)
                .font(ApproachNoteTheme.headline())
                .foregroundStyle(isSelected ? Color.white : ApproachNoteTheme.textPrimary)
            if let composer = song.composer {
                Text(composer)
                    .font(ApproachNoteTheme.subheadline())
                    .foregroundStyle(isSelected ? Color.white.opacity(0.85) : ApproachNoteTheme.textSecondary)
            }
        }
        .padding(.vertical, 4)
        .padding(.horizontal, 8)
    }
}

#Preview {
    SongsListView()
        .environmentObject(RepertoireManager())
        .environmentObject(AuthenticationManager())
}
