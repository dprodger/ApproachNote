//
//  YouTubeImportView.swift
//  Approach Note
//
//  View for completing YouTube video import by selecting a song
//

import SwiftUI
import os

extension Notification.Name {
    static let transcriptionCreated = Notification.Name("transcriptionCreated")
    static let videoCreated = Notification.Name("videoCreated")
}

struct YouTubeImportView: View {
    let youtubeData: ImportedYouTubeData
    let onSuccess: () -> Void
    let onCancel: () -> Void

    @State private var searchText = ""
    @State private var searchResults: [Song] = []
    @State private var isSearching = false
    @State private var selectedSong: Song?
    @State private var isImporting = false
    @State private var importError: String?
    @State private var showingRecordingPicker = false
    @State private var showingRecordingChoice = false
    @State private var showingLogin = false

    @Environment(\.dismiss) private var dismiss
    @EnvironmentObject var authManager: AuthenticationManager

    private let songService = SongService()
    private let contentService = ContentService()

    var body: some View {
        Group {
            if !authManager.isAuthenticated {
                authRequiredView
            } else {
                VStack(spacing: 0) {
                    // Video Info Header
                    videoInfoHeader

                    Divider()

                    // Song Search
                    songSearchSection

                    Spacer()

                    // Action Buttons
                    actionButtons
                }
            }
        }
        .background(ApproachNoteTheme.background)
        .navigationTitle("Link to Song")
        .navigationBarTitleDisplayMode(.inline)
        .toolbar {
            ToolbarItem(placement: .cancellationAction) {
                Button("Cancel") {
                    onCancel()
                    dismiss()
                }
            }
        }
        .onAppear {
            let channel = youtubeData.channelName ?? "(none)"
            Log.ui.debug("YouTubeImportView received data: title='\(youtubeData.title, privacy: .public)', url=\(youtubeData.url, privacy: .private), videoType=\(youtubeData.videoType.rawValue, privacy: .public), channel=\(channel, privacy: .public)")
        }
        .alert("Import Error", isPresented: Binding(
            get: { importError != nil },
            set: { if !$0 { importError = nil } }
        )) {
            Button("OK") { importError = nil }
        } message: {
            Text(importError ?? "Unknown error")
        }
        .sheet(isPresented: $showingRecordingPicker) {
            if let song = selectedSong {
                RecordingPickerView(
                    song: song,
                    youtubeData: youtubeData,
                    onSelect: { recordingId in
                        Task {
                            await importVideo(songId: song.id, recordingId: recordingId)
                        }
                    },
                    onCancel: {
                        showingRecordingPicker = false
                    }
                )
            }
        }
        .confirmationDialog(
            "Link to Recording?",
            isPresented: $showingRecordingChoice,
            titleVisibility: .visible
        ) {
            Button("Pick a Recording") {
                showingRecordingPicker = true
            }
            Button("Skip - Link to Song Only") {
                if let song = selectedSong {
                    Task {
                        await importVideo(songId: song.id, recordingId: nil)
                    }
                }
            }
            Button("Cancel", role: .cancel) {
                selectedSong = nil
            }
        } message: {
            Text("You can link this transcription to a specific recording, or just to the song.")
        }
        .onAppear {
            // Pre-populate search with video title keywords
            // Use a small delay to let the view settle before triggering search
            if authManager.isAuthenticated {
                Task {
                    try? await Task.sleep(nanoseconds: 100_000_000) // 0.1 second delay
                    let keywords = extractKeywords(from: youtubeData.title)
                    if !keywords.isEmpty {
                        await MainActor.run {
                            searchText = keywords
                        }
                        await searchSongs()
                    }
                }
            }
        }
        .onChange(of: authManager.isAuthenticated) { _, isAuthenticated in
            // When user logs in, trigger the search
            if isAuthenticated {
                Task {
                    try? await Task.sleep(nanoseconds: 100_000_000) // 0.1 second delay
                    let keywords = extractKeywords(from: youtubeData.title)
                    if !keywords.isEmpty {
                        await MainActor.run {
                            searchText = keywords
                        }
                        await searchSongs()
                    }
                }
            }
        }
        .sheet(isPresented: $showingLogin) {
            LoginView()
                .environmentObject(authManager)
        }
    }

    // MARK: - Auth Required View

    private var authRequiredView: some View {
        VStack(spacing: 0) {
            // Video Info Header (show what they're trying to import)
            videoInfoHeader

            Divider()

            Spacer()

            VStack(spacing: 24) {
                Image(systemName: "lock.fill")
                    .font(.system(size: 60))
                    .foregroundColor(ApproachNoteTheme.brand.opacity(0.6))

                Text("Sign In Required")
                    .font(ApproachNoteTheme.title2())
                    .fontWeight(.semibold)
                    .foregroundColor(ApproachNoteTheme.textPrimary)

                Text("You need to be signed in to import YouTube videos")
                    .font(ApproachNoteTheme.body())
                    .foregroundColor(.secondary)
                    .multilineTextAlignment(.center)
                    .padding(.horizontal, 32)

                Button(action: {
                    showingLogin = true
                }) {
                    Text("Sign In")
                        .fontWeight(.semibold)
                        .frame(maxWidth: .infinity)
                        .padding()
                        .background(ApproachNoteTheme.brand)
                        .foregroundColor(.white)
                        .cornerRadius(10)
                }
                .padding(.horizontal, 32)
            }

            Spacer()
        }
    }

    // MARK: - Video Info Header

    private var videoInfoHeader: some View {
        VStack(alignment: .leading, spacing: 12) {
            HStack(spacing: 12) {
                // YouTube icon
                Image(systemName: "play.rectangle.fill")
                    .font(.title)
                    .foregroundColor(.red)

                VStack(alignment: .leading, spacing: 4) {
                    Text(youtubeData.title)
                        .font(ApproachNoteTheme.headline())
                        .foregroundColor(ApproachNoteTheme.textPrimary)
                        .lineLimit(2)

                    HStack(spacing: 8) {
                        Text(youtubeData.videoType.displayName)
                            .font(ApproachNoteTheme.caption())
                            .foregroundColor(.white)
                            .padding(.horizontal, 8)
                            .padding(.vertical, 2)
                            .background(youtubeData.videoType == .transcription ? Color.blue : Color.green)
                            .cornerRadius(4)

                        if let channel = youtubeData.channelName {
                            Text(channel)
                                .font(ApproachNoteTheme.caption())
                                .foregroundColor(ApproachNoteTheme.textSecondary)
                        }
                    }
                }
            }
        }
        .padding()
        .frame(maxWidth: .infinity, alignment: .leading)
        .background(ApproachNoteTheme.surface)
    }

    // MARK: - Song Search Section

    private var songSearchSection: some View {
        VStack(alignment: .leading, spacing: 12) {
            Text("Search for a song to link this video to:")
                .font(ApproachNoteTheme.subheadline())
                .foregroundColor(ApproachNoteTheme.textSecondary)
                .padding(.horizontal)
                .padding(.top)

            // Search field
            HStack {
                Image(systemName: "magnifyingglass")
                    .foregroundColor(ApproachNoteTheme.textSecondary)

                TextField("Search songs...", text: $searchText)
                    .textFieldStyle(.plain)
                    .onSubmit {
                        Task { await searchSongs() }
                    }

                if !searchText.isEmpty {
                    Button(action: {
                        searchText = ""
                        searchResults = []
                    }) {
                        Image(systemName: "xmark.circle.fill")
                            .foregroundColor(ApproachNoteTheme.textSecondary)
                    }
                }

                if isSearching {
                    ProgressView()
                        .scaleEffect(0.8)
                }
            }
            .padding()
            .background(ApproachNoteTheme.surface)
            .cornerRadius(10)
            .padding(.horizontal)

            // Search Results
            if !searchResults.isEmpty {
                ScrollView {
                    LazyVStack(spacing: 0) {
                        ForEach(searchResults) { song in
                            songRow(song)
                        }
                    }
                }
            } else if !searchText.isEmpty && !isSearching {
                VStack(spacing: 8) {
                    Image(systemName: "music.note")
                        .font(.largeTitle)
                        .foregroundColor(ApproachNoteTheme.textSecondary)

                    Text("No songs found")
                        .font(ApproachNoteTheme.body())
                        .foregroundColor(ApproachNoteTheme.textSecondary)

                    Text("Try different keywords")
                        .font(ApproachNoteTheme.caption())
                        .foregroundColor(ApproachNoteTheme.textSecondary)
                }
                .frame(maxWidth: .infinity)
                .padding(.top, 40)
            }
        }
    }

    private func songRow(_ song: Song) -> some View {
        VStack(spacing: 0) {
            HStack {
                VStack(alignment: .leading, spacing: 4) {
                    Text(song.title)
                        .font(ApproachNoteTheme.headline())
                        .foregroundColor(ApproachNoteTheme.textPrimary)

                    if let composer = song.composer {
                        Text(composer)
                            .font(ApproachNoteTheme.subheadline())
                            .foregroundColor(ApproachNoteTheme.textSecondary)
                    }
                }

                Spacer()

                Image(systemName: "chevron.right")
                    .foregroundColor(ApproachNoteTheme.textSecondary)
            }
            .padding()
            .background(selectedSong?.id == song.id ? ApproachNoteTheme.brand.opacity(0.1) : Color.clear)
            .contentShape(Rectangle())
            .onTapGesture {
                selectedSong = song
                // For transcriptions, offer choice to pick a recording or skip
                // For backing tracks, we can link directly to the song
                if youtubeData.videoType == .transcription {
                    showingRecordingChoice = true
                } else {
                    Task {
                        await importVideo(songId: song.id, recordingId: nil)
                    }
                }
            }

            Divider()
                .padding(.leading)
        }
    }

    // MARK: - Action Buttons

    private var actionButtons: some View {
        VStack(spacing: 12) {
            if isImporting {
                ProgressView("Importing...")
                    .padding()
            }
        }
        .padding()
    }

    // MARK: - Helper Functions

    private func extractKeywords(from title: String) -> String {
        // Remove common YouTube video prefixes/suffixes and extract song-related keywords
        var cleaned = title
            .replacingOccurrences(of: "Solo Transcription", with: "", options: .caseInsensitive)
            .replacingOccurrences(of: "Transcription", with: "", options: .caseInsensitive)
            .replacingOccurrences(of: "Backing Track", with: "", options: .caseInsensitive)
            .replacingOccurrences(of: "Play Along", with: "", options: .caseInsensitive)
            .replacingOccurrences(of: "Play-Along", with: "", options: .caseInsensitive)

        // Remove content in parentheses
        if let regex = try? NSRegularExpression(pattern: "\\([^)]*\\)", options: []) {
            cleaned = regex.stringByReplacingMatches(in: cleaned, range: NSRange(cleaned.startIndex..., in: cleaned), withTemplate: "")
        }

        // Take first few words
        let words = cleaned.split(separator: " ").prefix(3)
        return words.joined(separator: " ").trimmingCharacters(in: .whitespaces)
    }

    private func searchSongs() async {
        guard !searchText.isEmpty else { return }

        isSearching = true
        defer { isSearching = false }

        do {
            searchResults = try await songService.searchSongs(query: searchText)
        } catch {
            Log.ui.error("Search error: \(error.localizedDescription)")
            searchResults = []
        }
    }

    private func importVideo(songId: String, recordingId: String?) async {
        isImporting = true
        defer { isImporting = false }

        let userId = authManager.currentUser?.id

        do {
            if youtubeData.videoType == .transcription {
                try await contentService.createTranscription(
                    songId: songId,
                    recordingId: recordingId,
                    youtubeUrl: youtubeData.url,
                    userId: userId
                )
            } else {
                try await contentService.createVideo(
                    songId: songId,
                    youtubeUrl: youtubeData.url,
                    videoType: "backing_track",
                    title: youtubeData.title,
                    userId: userId
                )
            }

            // Success - post notification to refresh song detail
            await MainActor.run {
                if youtubeData.videoType == .transcription {
                    NotificationCenter.default.post(
                        name: .transcriptionCreated,
                        object: nil,
                        userInfo: ["songId": songId]
                    )
                } else {
                    NotificationCenter.default.post(
                        name: .videoCreated,
                        object: nil,
                        userInfo: ["songId": songId]
                    )
                }
                onSuccess()
                dismiss()
            }
        } catch {
            importError = error.localizedDescription
        }
    }
}

// MARK: - Recording Picker View

struct RecordingPickerView: View {
    let song: Song
    let youtubeData: ImportedYouTubeData
    let onSelect: (String) -> Void
    let onCancel: () -> Void

    @State private var recordings: [Recording] = []
    @State private var isLoading = true
    @State private var loadError: String?
    @State private var searchText = ""

    @Environment(\.dismiss) private var dismiss

    private let songService = SongService()

    /// Recordings sorted by artist name
    private var sortedRecordings: [Recording] {
        recordings.sorted { r1, r2 in
            let artist1 = r1.artistCredit ?? ""
            let artist2 = r2.artistCredit ?? ""
            return artist1.localizedCaseInsensitiveCompare(artist2) == .orderedAscending
        }
    }

    /// Filtered recordings based on search text
    private var filteredRecordings: [Recording] {
        if searchText.isEmpty {
            return sortedRecordings
        }
        let query = searchText.lowercased()
        return sortedRecordings.filter { recording in
            (recording.artistCredit?.lowercased().contains(query) ?? false) ||
            (recording.albumTitle?.lowercased().contains(query) ?? false) ||
            (recording.recordingYear.map { String($0).contains(query) } ?? false)
        }
    }

    var body: some View {
        NavigationStack {
            VStack(spacing: 0) {
                if isLoading {
                    Spacer()
                    ProgressView("Loading recordings...")
                    Spacer()
                } else if let error = loadError {
                    Spacer()
                    VStack(spacing: 12) {
                        Image(systemName: "exclamationmark.triangle")
                            .font(.largeTitle)
                            .foregroundColor(.orange)
                        Text(error)
                            .font(ApproachNoteTheme.body())
                            .foregroundColor(ApproachNoteTheme.textSecondary)
                    }
                    Spacer()
                } else if recordings.isEmpty {
                    Spacer()
                    VStack(spacing: 12) {
                        Image(systemName: "opticaldisc")
                            .font(.largeTitle)
                            .foregroundColor(ApproachNoteTheme.textSecondary)
                        Text("No recordings found")
                            .font(ApproachNoteTheme.body())
                            .foregroundColor(ApproachNoteTheme.textSecondary)
                        Text("This song doesn't have any recordings yet")
                            .font(ApproachNoteTheme.caption())
                            .foregroundColor(ApproachNoteTheme.textSecondary)
                    }
                    Spacer()
                } else {
                    // Search field
                    HStack {
                        Image(systemName: "magnifyingglass")
                            .foregroundColor(ApproachNoteTheme.textSecondary)

                        TextField("Search by artist or album...", text: $searchText)
                            .textFieldStyle(.plain)

                        if !searchText.isEmpty {
                            Button(action: { searchText = "" }) {
                                Image(systemName: "xmark.circle.fill")
                                    .foregroundColor(ApproachNoteTheme.textSecondary)
                            }
                        }
                    }
                    .padding()
                    .background(ApproachNoteTheme.surface)

                    Divider()

                    // Results count
                    HStack {
                        Text("\(filteredRecordings.count) recording\(filteredRecordings.count == 1 ? "" : "s")")
                            .font(ApproachNoteTheme.caption())
                            .foregroundColor(ApproachNoteTheme.textSecondary)
                        Spacer()
                        Text("Sorted by artist")
                            .font(ApproachNoteTheme.caption())
                            .foregroundColor(ApproachNoteTheme.textSecondary)
                    }
                    .padding(.horizontal)
                    .padding(.vertical, 8)

                    // Recordings list
                    if filteredRecordings.isEmpty {
                        Spacer()
                        VStack(spacing: 8) {
                            Image(systemName: "magnifyingglass")
                                .font(.largeTitle)
                                .foregroundColor(ApproachNoteTheme.textSecondary)
                            Text("No matching recordings")
                                .font(ApproachNoteTheme.body())
                                .foregroundColor(ApproachNoteTheme.textSecondary)
                        }
                        Spacer()
                    } else {
                        List(filteredRecordings) { recording in
                            Button(action: {
                                onSelect(recording.id)
                                dismiss()
                            }) {
                                recordingRow(recording)
                            }
                            .buttonStyle(.plain)
                        }
                        .listStyle(.plain)
                    }
                }
            }
            .navigationTitle("Select Recording")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .cancellationAction) {
                    Button("Cancel") {
                        onCancel()
                        dismiss()
                    }
                }
            }
        }
        .task {
            await loadRecordings()
        }
    }

    private func recordingRow(_ recording: Recording) -> some View {
        HStack(spacing: 12) {
            // Album art
            if let artUrl = recording.bestAlbumArtSmall, let url = URL(string: artUrl) {
                AsyncImage(url: url) { image in
                    image.resizable().aspectRatio(contentMode: .fill)
                } placeholder: {
                    Color.gray.opacity(0.3)
                }
                .frame(width: 50, height: 50)
                .cornerRadius(4)
            } else {
                RoundedRectangle(cornerRadius: 4)
                    .fill(Color.gray.opacity(0.3))
                    .frame(width: 50, height: 50)
                    .overlay {
                        Image(systemName: "opticaldisc")
                            .foregroundColor(.gray)
                    }
            }

            VStack(alignment: .leading, spacing: 4) {
                Text(recording.albumTitle ?? "Unknown Album")
                    .font(ApproachNoteTheme.headline())
                    .foregroundColor(ApproachNoteTheme.textPrimary)

                HStack(spacing: 8) {
                    if let artist = recording.artistCredit {
                        Text(artist)
                            .font(ApproachNoteTheme.caption())
                            .foregroundColor(ApproachNoteTheme.textSecondary)
                    }

                    if let year = recording.recordingYear {
                        Text("(\(String(year)))")
                            .font(ApproachNoteTheme.caption())
                            .foregroundColor(ApproachNoteTheme.textSecondary)
                    }
                }
            }

            Spacer()

            Image(systemName: "chevron.right")
                .foregroundColor(ApproachNoteTheme.textSecondary)
        }
        .padding(.vertical, 4)
    }

    private func loadRecordings() async {
        isLoading = true
        defer { isLoading = false }

        if let songDetail = await songService.fetchSongDetail(id: song.id) {
            recordings = songDetail.recordings ?? []
        } else {
            loadError = "Failed to load recordings"
        }
    }
}

// MARK: - Preview

#Preview {
    NavigationStack {
        YouTubeImportView(
            youtubeData: ImportedYouTubeData(
                videoId: "abc123",
                title: "Chet Baker - Born to be Blue (Solo Transcription)",
                url: "https://www.youtube.com/watch?v=abc123",
                channelName: "Jazz Transcriptions",
                description: nil,
                videoType: .transcription
            ),
            onSuccess: { Log.ui.debug("Success") },
            onCancel: { Log.ui.debug("Cancel") }
        )
        .environmentObject(AuthenticationManager())
    }
}
