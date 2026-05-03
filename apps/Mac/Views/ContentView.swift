//
//  ContentView.swift
//  Approach Note
//
//  Main content view with tab navigation for macOS
//

import SwiftUI

enum NavigationItem: String, CaseIterable, Identifiable {
    case songs = "Songs"
    case artists = "Artists"
    case recordings = "Recordings"

    var id: String { rawValue }

    var icon: String {
        switch self {
        case .songs: return "music.note.list"
        case .artists: return "person.2.fill"
        case .recordings: return "opticaldisc"
        }
    }
}

struct ContentView: View {
    @State private var selectedTab: NavigationItem = .songs
    @State private var showCreateRepertoire = false
    @State private var showRepertoirePopover = false
    @State private var showLoginSheet = false
    @EnvironmentObject var authManager: AuthenticationManager
    @EnvironmentObject var repertoireManager: RepertoireManager

    var body: some View {
        TabView(selection: $selectedTab) {
            SongsListView()
                .tabItem {
                    Label("Songs", systemImage: "music.note.list")
                }
                .tag(NavigationItem.songs)

            ArtistsListView()
                .tabItem {
                    Label("Artists", systemImage: "person.2.fill")
                }
                .tag(NavigationItem.artists)

            RecordingsListView()
                .tabItem {
                    Label("Recordings", systemImage: "opticaldisc")
                }
                .tag(NavigationItem.recordings)
        }
        .toolbar {
            ToolbarItem(placement: .automatic) {
                if authManager.isAuthenticated {
                    Menu {
                        ForEach(repertoireManager.repertoires) { repertoire in
                            Button(action: { repertoireManager.selectRepertoire(repertoire) }) {
                                HStack {
                                    Text(repertoire.name)
                                    if repertoire.id == repertoireManager.selectedRepertoire.id {
                                        Image(systemName: "checkmark")
                                    }
                                }
                            }
                        }

                        Divider()

                        Button(action: { showCreateRepertoire = true }) {
                            Label("Create New Repertoire", systemImage: "plus.circle")
                        }
                    } label: {
                        HStack(spacing: 4) {
                            Image(systemName: "text.badge.checkmark")
                            Text(repertoireManager.currentRepertoireDisplayName)
                                .lineLimit(1)
                        }
                    }
                } else {
                    Button(action: { showRepertoirePopover = true }) {
                        HStack(spacing: 4) {
                            Image(systemName: "text.badge.checkmark")
                            Text("All Songs")
                            Image(systemName: "chevron.down")
                                .font(.system(size: 10))
                        }
                    }
                    .popover(isPresented: $showRepertoirePopover, arrowEdge: .bottom) {
                        VStack(spacing: 16) {
                            Text("Create and select repertoires to focus on a subset of songs.")
                                .font(ApproachNoteTheme.body())
                                .foregroundColor(ApproachNoteTheme.charcoal)
                                .multilineTextAlignment(.center)

                            Button(action: {
                                showRepertoirePopover = false
                                showLoginSheet = true
                            }) {
                                Text("Sign In")
                                    .font(ApproachNoteTheme.body())
                                    .foregroundColor(.white)
                                    .frame(minWidth: 100)
                                    .padding(.horizontal, 16)
                                    .padding(.vertical, 8)
                                    .background(ApproachNoteTheme.burgundy)
                                    .cornerRadius(6)
                            }
                            .buttonStyle(.plain)
                        }
                        .padding()
                        .frame(width: 260)
                    }
                }
            }
        }
        .sheet(isPresented: $showCreateRepertoire) {
            MacCreateRepertoireView(repertoireManager: repertoireManager)
        }
        .sheet(isPresented: $showLoginSheet) {
            MacLoginView()
                .environmentObject(authManager)
        }
        .onReceive(NotificationCenter.default.publisher(for: .navigateToSongs)) { _ in
            selectedTab = .songs
        }
        .onReceive(NotificationCenter.default.publisher(for: .navigateToArtists)) { _ in
            selectedTab = .artists
        }
        .onReceive(NotificationCenter.default.publisher(for: .navigateToRecordings)) { _ in
            selectedTab = .recordings
        }
        .preferredColorScheme(.light)
    }
}

// MARK: - Settings View (macOS Preferences)

struct SettingsView: View {
    @EnvironmentObject var authManager: AuthenticationManager
    @EnvironmentObject var favoritesManager: FavoritesManager
    @State private var selectedTab = 0

    var body: some View {
        TabView(selection: $selectedTab) {
            GeneralSettingsView()
                .tabItem {
                    Label("General", systemImage: "gearshape")
                }
                .tag(0)
            AccountSettingsView()
                .tabItem {
                    Label("Account", systemImage: "person.circle")
                }
                .tag(1)
            CommunitySettingsView()
                .tabItem {
                    Label("Community", systemImage: "person.3.fill")
                }
                .tag(2)
            AboutSettingsView()
                .tabItem {
                    Label("About", systemImage: "info.circle")
                }
                .tag(3)
        }
        .frame(width: 500, height: settingsHeight)
        .animation(.easeInOut, value: authManager.isAuthenticated)
    }

    private var settingsHeight: CGFloat {
        switch selectedTab {
        case 0: return 380  // General tab
        case 1: return authManager.isAuthenticated ? 400 : 550  // Account tab
        case 2: return authManager.isAuthenticated ? 420 : 350  // Community tab
        case 3: return 260  // About tab
        default: return 400
        }
    }
}

// MARK: - About Settings

struct AboutSettingsView: View {
    var body: some View {
        Form {
            Section("Legal") {
                Link(destination: URL(string: "https://approachnote.com/terms")!) {
                    HStack {
                        Image(systemName: "doc.plaintext")
                            .foregroundColor(ApproachNoteTheme.smokeGray)
                            .frame(width: 20)
                        Text("Terms of Service")
                            .foregroundColor(ApproachNoteTheme.charcoal)
                        Spacer()
                        Image(systemName: "arrow.up.right.square")
                            .foregroundColor(ApproachNoteTheme.smokeGray)
                    }
                    .contentShape(Rectangle())
                }
                .buttonStyle(.plain)

                Link(destination: URL(string: "https://approachnote.com/privacy")!) {
                    HStack {
                        Image(systemName: "lock.shield")
                            .foregroundColor(ApproachNoteTheme.smokeGray)
                            .frame(width: 20)
                        Text("Privacy Policy")
                            .foregroundColor(ApproachNoteTheme.charcoal)
                        Spacer()
                        Image(systemName: "arrow.up.right.square")
                            .foregroundColor(ApproachNoteTheme.smokeGray)
                    }
                    .contentShape(Rectangle())
                }
                .buttonStyle(.plain)
            }
        }
        .formStyle(.grouped)
    }
}

struct AccountSettingsView: View {
    @EnvironmentObject var authManager: AuthenticationManager
    @EnvironmentObject var favoritesManager: FavoritesManager
    @State private var selectedRecordingId: String?

    var body: some View {
        if authManager.isAuthenticated {
            authenticatedView
        } else {
            MacLoginView(isInline: true)
                .environmentObject(authManager)
        }
    }

    @ViewBuilder
    private var authenticatedView: some View {
        VStack(spacing: 0) {
            Form {
                Section {
                    HStack(spacing: 12) {
                        // Profile image or placeholder
                        if let imageUrl = authManager.currentUser?.profileImageUrl,
                           let url = URL(string: imageUrl) {
                            AsyncImage(url: url) { image in
                                image
                                    .resizable()
                                    .aspectRatio(contentMode: .fill)
                            } placeholder: {
                                Image(systemName: "person.circle.fill")
                                    .font(.system(size: 40))
                                    .foregroundColor(ApproachNoteTheme.smokeGray)
                            }
                            .frame(width: 50, height: 50)
                            .clipShape(Circle())
                        } else {
                            Image(systemName: "person.circle.fill")
                                .font(.system(size: 50))
                                .foregroundColor(ApproachNoteTheme.smokeGray)
                        }

                        VStack(alignment: .leading, spacing: 4) {
                            Text(authManager.currentUser?.displayName ?? "User")
                                .font(ApproachNoteTheme.headline())
                                .foregroundColor(ApproachNoteTheme.charcoal)
                            Text(authManager.currentUser?.email ?? "")
                                .font(ApproachNoteTheme.subheadline())
                                .foregroundColor(.secondary)
                        }

                        Spacer()

                        Button("Sign Out") {
                            authManager.logout()
                        }
                        .foregroundColor(.red)
                    }
                    .padding(.vertical, 8)
                }
            }
            .formStyle(.grouped)

            // Favorites Section
            VStack(alignment: .leading, spacing: 12) {
                HStack {
                    Image(systemName: "heart.fill")
                        .foregroundColor(.red)
                    Text("Favorite Recordings")
                        .font(ApproachNoteTheme.headline())
                        .foregroundColor(ApproachNoteTheme.charcoal)

                    if !favoritesManager.favoriteRecordings.isEmpty {
                        Text("(\(favoritesManager.favoriteRecordings.count))")
                            .font(ApproachNoteTheme.subheadline())
                            .foregroundColor(ApproachNoteTheme.charcoal.opacity(0.7))
                    }

                    Spacer()
                }
                .padding(.horizontal)

                if favoritesManager.isLoading {
                    HStack {
                        Spacer()
                        ProgressView()
                        Spacer()
                    }
                    .padding()
                } else if favoritesManager.favoriteRecordings.isEmpty {
                    Text("No favorite recordings yet")
                        .font(ApproachNoteTheme.body())
                        .foregroundColor(ApproachNoteTheme.charcoal.opacity(0.7))
                        .padding(.horizontal)
                } else {
                    ScrollView(.horizontal, showsIndicators: false) {
                        HStack(spacing: 12) {
                            ForEach(favoritesManager.favoriteRecordings, id: \.id) { recording in
                                FavoriteRecordingCard(recording: recording)
                                    .onTapGesture {
                                        selectedRecordingId = recording.id
                                    }
                            }
                        }
                        .padding(.horizontal)
                    }
                }
            }
            .padding(.vertical)
            .background(ApproachNoteTheme.backgroundLight)
        }
        .sheet(isPresented: Binding(
            get: { selectedRecordingId != nil },
            set: { if !$0 { selectedRecordingId = nil } }
        )) {
            if let recordingId = selectedRecordingId {
                RecordingDetailView(recordingId: recordingId)
                    .frame(minWidth: 600, minHeight: 500)
            }
        }
        .task {
            if favoritesManager.favoriteRecordings.isEmpty {
                await favoritesManager.loadFavorites()
            }
        }
    }
}

// MARK: - Favorite Recording Card

struct FavoriteRecordingCard: View {
    let recording: FavoriteRecordingResponse
    @State private var isHovering = false

    var body: some View {
        VStack(alignment: .leading, spacing: 6) {
            // Album art
            AsyncImage(url: URL(string: recording.bestAlbumArtSmall ?? "")) { image in
                image
                    .resizable()
                    .aspectRatio(contentMode: .fill)
            } placeholder: {
                Rectangle()
                    .fill(ApproachNoteTheme.cardBackground)
                    .overlay {
                        Image(systemName: "music.note")
                            .foregroundColor(ApproachNoteTheme.smokeGray)
                    }
            }
            .frame(width: 80, height: 80)
            .cornerRadius(8)

            // Song title
            Text(recording.songTitle ?? "Unknown")
                .font(ApproachNoteTheme.caption())
                .foregroundColor(ApproachNoteTheme.charcoal)
                .lineLimit(2)
                .frame(width: 80, alignment: .leading)
        }
        .padding(8)
        .background(isHovering ? ApproachNoteTheme.cardBackground : Color.clear)
        .cornerRadius(8)
        .onHover { hovering in
            isHovering = hovering
        }
    }
}

struct GeneralSettingsView: View {
    @AppStorage("preferredStreamingService") private var preferredStreamingService: String = "spotify"
    @State private var showingOnboarding = false

    // Research queue state
    @State private var queueSize: Int = 0
    @State private var workerActive: Bool = false
    @State private var currentSongName: String? = nil
    @State private var progress: ResearchProgress? = nil
    @State private var isLoadingQueue: Bool = true
    @State private var isRefreshing: Bool = false

    private let researchService = ResearchService()

    var body: some View {
        Form {
            Section("Playback") {
                Picker("Preferred Streaming Service", selection: $preferredStreamingService) {
                    Text("Spotify").tag("spotify")
                    Text("Apple Music").tag("apple_music")
                    Text("YouTube").tag("youtube")
                }
                .pickerStyle(.radioGroup)
            }

            Section("Tutorial") {
                HStack {
                    Text("Learn about Songs, Recordings, and Releases")
                    Spacer()
                    Button("View Tutorial") {
                        showingOnboarding = true
                    }
                }
            }

            Section("Research Queue") {
                if isLoadingQueue {
                    HStack {
                        ProgressView()
                            .scaleEffect(0.7)
                        Text("Loading...")
                            .foregroundColor(ApproachNoteTheme.smokeGray)
                    }
                } else {
                    HStack {
                        Image(systemName: workerActive ? "arrow.triangle.2.circlepath" : "clock")
                            .foregroundColor(workerActive ? ApproachNoteTheme.burgundy : ApproachNoteTheme.smokeGray)

                        Text("Queue Size: \(queueSize)")

                        Spacer()

                        Button(action: {
                            Task {
                                await refreshQueueStatus()
                            }
                        }) {
                            if isRefreshing {
                                ProgressView()
                                    .scaleEffect(0.7)
                            } else {
                                Image(systemName: "arrow.clockwise")
                            }
                        }
                        .disabled(isRefreshing)
                    }

                    if workerActive {
                        if let songName = currentSongName {
                            HStack {
                                Text("Processing:")
                                    .foregroundColor(ApproachNoteTheme.smokeGray)
                                Text(songName)
                                    .lineLimit(1)
                                    .truncationMode(.tail)
                            }
                        }

                        if let progress = progress {
                            VStack(alignment: .leading, spacing: 4) {
                                HStack {
                                    Text(progress.phaseLabel)
                                        .font(ApproachNoteTheme.caption())
                                        .foregroundColor(ApproachNoteTheme.smokeGray)
                                    Spacer()
                                    Text("\(progress.current)/\(progress.total)")
                                        .font(ApproachNoteTheme.caption())
                                        .foregroundColor(ApproachNoteTheme.charcoal)
                                }

                                ProgressView(value: progress.progressFraction)
                                    .tint(ApproachNoteTheme.burgundy)
                            }
                        }
                    }
                }
            }
        }
        .formStyle(.grouped)
        .padding()
        .sheet(isPresented: $showingOnboarding) {
            MacOnboardingView(isPresented: $showingOnboarding)
        }
        .task {
            await loadQueueStatus()
        }
    }

    private func loadQueueStatus() async {
        if let status = await researchService.fetchQueueStatus() {
            queueSize = status.queueSize
            workerActive = status.workerActive
            currentSongName = status.currentSong?.songName
            progress = status.progress
        }
        isLoadingQueue = false
    }

    private func refreshQueueStatus() async {
        guard !isRefreshing else { return }

        isRefreshing = true

        if let status = await researchService.fetchQueueStatus() {
            queueSize = status.queueSize
            workerActive = status.workerActive
            currentSongName = status.currentSong?.songName
            progress = status.progress
        }

        isRefreshing = false
    }
}

#Preview {
    ContentView()
        .environmentObject(AuthenticationManager())
        .environmentObject(RepertoireManager())
        .environmentObject(FavoritesManager())
}
