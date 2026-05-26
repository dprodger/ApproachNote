//
//  RecordingsListView.swift
//  Approach Note
//
//  Created by Dave Rodger on 11/29/25.
//  Recording list view with search by artist, recording name, or album/release name
//

import SwiftUI

struct RecordingsListView: View {
    @StateObject private var recordingService = RecordingService()
    @State private var searchText = ""
    @State private var searchTask: Task<Void, Never>?
    @State private var hasPerformedInitialLoad = false
    
    // Group recordings by album title for display
    private var groupedRecordings: [(String, [Recording])] {
        let filtered = recordingService.recordings
        
        let grouped = Dictionary(grouping: filtered) { recording in
            let albumTitle = recording.albumTitle ?? "Unknown Album"
            let firstChar = albumTitle.prefix(1).uppercased()
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
        groupedRecordings.map { $0.0 }
    }
    
    var body: some View {
        NavigationStack {
            contentView
                .background(ApproachNoteTheme.background)
                .jazzNavigationBar(title: "Recordings (\(recordingService.recordingsCount.formatted()))")
                .searchable(text: $searchText, prompt: "Artist, album, or song")
                .onChange(of: searchText) { oldValue, newValue in
                    searchTask?.cancel()
                    searchTask = Task {
                        try? await Task.sleep(nanoseconds: 300_000_000)
                        if !Task.isCancelled {
                            await recordingService.fetchRecordings(searchQuery: newValue)
                        }
                    }
                }
                .task {
                    // Only load on initial appear, not when returning from detail view
                    if !hasPerformedInitialLoad {
                        await recordingService.fetchRecordingsCount()
                        await recordingService.fetchRecordings(searchQuery: searchText)
                        hasPerformedInitialLoad = true
                    }
                }
        }
        .tint(ApproachNoteTheme.textSecondary)
    }
    
    // MARK: - Content Views
    
    @ViewBuilder
    private var contentView: some View {
        VStack(spacing: 0) {
            // Search hint banner
            searchHintBanner
            
            if recordingService.isLoading {
                loadingView
            } else if let error = recordingService.errorMessage {
                errorView(error: error)
            } else if recordingService.recordings.isEmpty {
                emptyStateView
            } else {
                recordingsListView
            }
        }
    }
    
    private var searchHintBanner: some View {
        HStack(spacing: 8) {
            Image(systemName: "magnifyingglass")
                .foregroundColor(ApproachNoteTheme.textSecondary)
                .font(ApproachNoteTheme.caption())
            Text("Search by artist, album name, or song title")
                .font(ApproachNoteTheme.caption())
                .foregroundColor(ApproachNoteTheme.textSecondary)
            Spacer()
        }
        .padding(.horizontal)
        .padding(.vertical, 8)
        .background(ApproachNoteTheme.textSecondary.opacity(0.1))
    }
    
    private var loadingView: some View {
        VStack {
            Spacer()
            ThemedProgressView(message: "Loading recordings...", tintColor: ApproachNoteTheme.textSecondary)
            Spacer()
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
        .background(ApproachNoteTheme.background)
    }
    
    private func errorView(error: String) -> some View {
        VStack(spacing: 16) {
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
                    await recordingService.fetchRecordings(searchQuery: searchText)
                }
            }
            .buttonStyle(.borderedProminent)
            .tint(ApproachNoteTheme.textSecondary)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
        .background(ApproachNoteTheme.background)
    }
    
    private var emptyStateView: some View {
        VStack(spacing: 16) {
            Image(systemName: searchText.isEmpty ? "magnifyingglass" : "opticaldisc")
                .font(.system(size: 60))
                .foregroundColor(ApproachNoteTheme.textSecondary.opacity(0.5))

            if searchText.isEmpty {
                Text("Search to Browse")
                    .font(ApproachNoteTheme.headline())
                    .foregroundColor(ApproachNoteTheme.textPrimary)
                Text("Enter an artist, album, or song title to find recordings")
                    .font(ApproachNoteTheme.subheadline())
                    .foregroundColor(ApproachNoteTheme.textSecondary)
                    .multilineTextAlignment(.center)
                    .padding(.horizontal, 32)
            } else {
                Text("No Results")
                    .font(ApproachNoteTheme.headline())
                    .foregroundColor(ApproachNoteTheme.textPrimary)
                Text("No recordings match \"\(searchText)\"")
                    .font(ApproachNoteTheme.subheadline())
                    .foregroundColor(ApproachNoteTheme.textSecondary)
                    .multilineTextAlignment(.center)
            }
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
        .background(ApproachNoteTheme.background)
    }
    
    private var recordingsListView: some View {
        ScrollViewReader { proxy in
            List {
                ForEach(groupedRecordings, id: \.0) { letter, recordings in
                    Section(header: RecordingSectionHeaderView(letter: letter)) {
                        ForEach(recordings) { recording in
                            NavigationLink(destination: RecordingDetailView(recordingId: recording.id)) {
                                recordingRowView(recording: recording)
                            }
                            .listRowBackground(ApproachNoteTheme.surface)
                        }
                    }
                    .id(letter)
                }
            }
            .listStyle(.plain)
            .scrollContentBackground(.hidden)
            .background(ApproachNoteTheme.background)
            .overlay(alignment: .trailing) {
                if sectionLetters.count > 5 {
                    AlphabetIndexView(
                        letters: sectionLetters,
                        accentColor: ApproachNoteTheme.textSecondary,
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
    }
    
    private func recordingRowView(recording: Recording) -> some View {
        HStack(spacing: 12) {
            // Album artwork thumbnail
            if let albumArtUrl = recording.bestAlbumArtSmall ?? recording.bestAlbumArtMedium {
                AsyncImage(url: URL(string: albumArtUrl)) { phase in
                    switch phase {
                    case .empty:
                        ProgressView()
                            .frame(width: 50, height: 50)
                    case .success(let image):
                        image
                            .resizable()
                            .aspectRatio(contentMode: .fill)
                            .frame(width: 50, height: 50)
                            .clipped()
                            .cornerRadius(6)
                    case .failure:
                        albumPlaceholder
                    @unknown default:
                        albumPlaceholder
                    }
                }
            } else {
                albumPlaceholder
            }
            
            // Recording info
            VStack(alignment: .leading, spacing: 4) {
                Text(recording.albumTitle ?? "Unknown Album")
                    .font(ApproachNoteTheme.headline())
                    .foregroundColor(ApproachNoteTheme.textPrimary)
                    .lineLimit(1)
                
                if let songTitle = recording.songTitle {
                    Text(songTitle)
                        .font(ApproachNoteTheme.subheadline())
                        .foregroundColor(ApproachNoteTheme.textSecondary)
                        .lineLimit(1)
                }
                
                // Show lead performer if available
                if let performers = recording.performers,
                   let leader = performers.first(where: { $0.role?.lowercased() == "leader" }) ?? performers.first {
                    Text(leader.name)
                        .font(ApproachNoteTheme.caption())
                        .foregroundColor(ApproachNoteTheme.textSecondary)
                        .lineLimit(1)
                }
            }
            
            Spacer()
            
            // Year and canonical indicator
            VStack(alignment: .trailing, spacing: 4) {
                if let year = recording.recordingYear {
                    Text(String(year))
                        .font(ApproachNoteTheme.caption())
                        .foregroundColor(ApproachNoteTheme.textSecondary)
                }
                
                if recording.isCanonical == true {
                    Image(systemName: "star.fill")
                        .font(ApproachNoteTheme.caption())
                        .foregroundColor(ApproachNoteTheme.accent)
                }
            }
        }
        .padding(.vertical, 4)
    }
    
    private var albumPlaceholder: some View {
        Image(systemName: "opticaldisc")
            .font(ApproachNoteTheme.title2())
            .foregroundColor(ApproachNoteTheme.textSecondary.opacity(0.5))
            .frame(width: 50, height: 50)
            .background(ApproachNoteTheme.surface)
            .cornerRadius(6)
    }
}

// MARK: - Section Header

struct RecordingSectionHeaderView: View {
    let letter: String
    
    var body: some View {
        Text(letter)
            .font(ApproachNoteTheme.headline())
            .fontWeight(.bold)
            .foregroundColor(ApproachNoteTheme.textSecondary)
            .frame(maxWidth: .infinity, alignment: .leading)
            .padding(.vertical, 8)
            .padding(.horizontal)
            .background(ApproachNoteTheme.background.opacity(0.8))
    }
}

#Preview {
    RecordingsListView()
}
