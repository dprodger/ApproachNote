//
//  MusicBrainzSearchSheet.swift
//  Approach Note
//
//  Search MusicBrainz for songs and import them into the database
//

import SwiftUI

struct MusicBrainzSearchSheet: View {
    let searchQuery: String
    let onSongImported: () -> Void

    @Environment(\.dismiss) private var dismiss
    @EnvironmentObject var authManager: AuthenticationManager
    @StateObject private var musicBrainzService = MusicBrainzService()

    @State private var searchResults: [MusicBrainzWork] = []
    @State private var isSearching = false
    @State private var selectedWork: MusicBrainzWork?
    @State private var isImporting = false
    @State private var importError: String?
    @State private var importSuccess = false

    var body: some View {
        NavigationStack {
            VStack(spacing: 0) {
                if isSearching {
                    loadingView
                } else if searchResults.isEmpty {
                    emptyView
                } else {
                    resultsList
                }
            }
            .background(ApproachNoteTheme.background)
            .navigationTitle("MusicBrainz Search")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .navigationBarLeading) {
                    Button("Cancel") {
                        dismiss()
                    }
                    .foregroundColor(ApproachNoteTheme.brand)
                }
            }
            .task {
                await performSearch()
            }
            .alert("Import Error", isPresented: .constant(importError != nil)) {
                Button("OK") {
                    importError = nil
                }
            } message: {
                if let error = importError {
                    Text(error)
                }
            }
            .alert("Song Imported", isPresented: $importSuccess) {
                Button("OK") {
                    onSongImported()
                    dismiss()
                }
            } message: {
                Text("The song has been added and is being enriched with recordings in the background.")
            }
            .confirmationDialog(
                "Import Song",
                isPresented: .constant(selectedWork != nil && !isImporting),
                titleVisibility: .visible
            ) {
                if let work = selectedWork {
                    Button("Import \"\(work.title)\"") {
                        Task {
                            await importSong(work)
                        }
                    }

                    if let url = URL(string: work.musicbrainzUrl) {
                        Link("View on MusicBrainz", destination: url)
                    }

                    Button("Cancel", role: .cancel) {
                        selectedWork = nil
                    }
                }
            } message: {
                if let work = selectedWork {
                    Text("Import \"\(work.title)\" by \(work.composerDisplay)?")
                }
            }
        }
        .presentationDetents([.medium, .large])
        .presentationDragIndicator(.visible)
    }

    // MARK: - Views

    private var loadingView: some View {
        VStack {
            Spacer()
            ThemedProgressView(message: "Searching MusicBrainz...", tintColor: ApproachNoteTheme.brand)
            Spacer()
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
    }

    private var emptyView: some View {
        VStack(spacing: ApproachNoteTheme.spacingMD) {
            Image(systemName: "magnifyingglass")
                .font(.system(size: 60))
                .foregroundColor(ApproachNoteTheme.textSecondary.opacity(0.5))

            Text("No Results Found")
                .font(ApproachNoteTheme.headline())
                .foregroundColor(ApproachNoteTheme.textPrimary)

            Text("No works matching \"\(searchQuery)\" were found on MusicBrainz.")
                .font(ApproachNoteTheme.subheadline())
                .foregroundColor(ApproachNoteTheme.textSecondary)
                .multilineTextAlignment(.center)
                .padding(.horizontal)

            Link(destination: URL(string: "https://musicbrainz.org/search?query=\(searchQuery.addingPercentEncoding(withAllowedCharacters: .urlQueryAllowed) ?? searchQuery)&type=work")!) {
                HStack {
                    Image(systemName: "safari")
                    Text("Search on MusicBrainz.org")
                }
            }
            .buttonStyle(.bordered)
            .tint(ApproachNoteTheme.brand)
            .padding(.top, ApproachNoteTheme.spacingXS)
        }
        .padding()
        .frame(maxWidth: .infinity, maxHeight: .infinity)
    }

    private var resultsList: some View {
        List {
            Section {
                ForEach(searchResults) { work in
                    Button(action: {
                        selectedWork = work
                    }) {
                        workRowView(work: work)
                    }
                    .buttonStyle(.plain)
                    .listRowBackground(ApproachNoteTheme.surface)
                }
            } header: {
                Text("Results for \"\(searchQuery)\"")
                    .font(ApproachNoteTheme.caption())
                    .foregroundColor(ApproachNoteTheme.textSecondary)
            } footer: {
                Text("Tap a result to import it into your library.")
                    .font(ApproachNoteTheme.caption())
                    .foregroundColor(ApproachNoteTheme.textSecondary)
            }
        }
        .listStyle(.insetGrouped)
        .scrollContentBackground(.hidden)
    }

    private func workRowView(work: MusicBrainzWork) -> some View {
        HStack(alignment: .top, spacing: ApproachNoteTheme.spacingSM) {
            // Score indicator
            scoreIndicator(score: work.score)

            VStack(alignment: .leading, spacing: ApproachNoteTheme.spacingXXS) {
                Text(work.title)
                    .font(ApproachNoteTheme.headline())
                    .foregroundColor(ApproachNoteTheme.textPrimary)

                Text(work.composerDisplay)
                    .font(ApproachNoteTheme.subheadline())
                    .foregroundColor(ApproachNoteTheme.textSecondary)

                if let type = work.type {
                    Text(type)
                        .font(ApproachNoteTheme.caption())
                        .foregroundColor(ApproachNoteTheme.brand)
                }
            }

            Spacer()

            // Link to MusicBrainz
            Link(destination: URL(string: work.musicbrainzUrl)!) {
                Image(systemName: "arrow.up.right.square")
                    .foregroundColor(ApproachNoteTheme.brand)
            }
            .buttonStyle(.plain)
        }
        .padding(.vertical, ApproachNoteTheme.spacingXXS)
    }

    private func scoreIndicator(score: Int?) -> some View {
        let scoreValue = score ?? 0
        let color: Color = {
            if scoreValue >= 90 { return .green }
            if scoreValue >= 70 { return ApproachNoteTheme.accent }
            return ApproachNoteTheme.textSecondary
        }()

        return VStack(spacing: 2) {
            Circle()
                .fill(color)
                .frame(width: 8, height: 8)
            Text("\(scoreValue)")
                .font(.system(size: 10, weight: .medium))
                .foregroundColor(color)
        }
        .frame(width: 24)
    }

    // MARK: - Actions

    private func performSearch() async {
        isSearching = true
        searchResults = await musicBrainzService.searchMusicBrainzWorks(query: searchQuery)
        isSearching = false
    }

    private func importSong(_ work: MusicBrainzWork) async {
        isImporting = true
        selectedWork = nil

        guard let token = authManager.getAccessToken() else {
            importError = "You must be logged in to import songs."
            isImporting = false
            return
        }

        if let response = await musicBrainzService.importSongFromMusicBrainz(work: work, authToken: token) {
            if response.success {
                importSuccess = true
            } else {
                importError = response.message
            }
        } else {
            importError = "Failed to import song. Please try again."
        }

        isImporting = false
    }
}

#Preview {
    MusicBrainzSearchSheet(
        searchQuery: "Autumn Leaves",
        onSongImported: {}
    )
    .environmentObject(AuthenticationManager())
}
